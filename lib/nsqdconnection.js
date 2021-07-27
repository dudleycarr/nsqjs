const {EventEmitter} = require('events')
const net = require('net')
const os = require('os')
const tls = require('tls')
const zlib = require('zlib')

const NodeState = require('node-state')
const _ = require('lodash')
const debug = require('./debug')

const wire = require('./wire')
const FrameBuffer = require('./framebuffer')
const Message = require('./message')
const version = require('./version')
const {ConnectionConfig, joinHostPort} = require('./config')

/**
 * NSQDConnection is a reader connection to a nsqd instance. It manages all
 * aspects of the nsqd connection with the exception of the RDY count which
 * needs to be managed across all nsqd connections for a given topic / channel
 * pair.
 *
 * This shouldn't be used directly. Use a Reader instead.
 *
 * Usage:
 *   const c = new NSQDConnection('127.0.0.1', 4150, 'test', 'default', 60, 30)
 *
 *   c.on(NSQDConnection.MESSAGE, (msg) => {
 *     console.log(`[message]: ${msg.attempts}, ${msg.body.toString()}`)
 *     console.log(`Timeout of message is ${msg.timeUntilTimeout()}`)
 *     setTimeout(() => console.log(`${msg.timeUntilTimeout()}`), 5000)
 *     msg.finish()
 *   })
 *
 *   c.on(NSQDConnection.FINISHED, () =>  c.setRdy(1))
 *
 *   c.on(NSQDConnection.READY, () => {
 *     console.log('Callback [ready]: Set RDY to 100')
 *     c.setRdy(10)
 *   })
 *
 *   c.on(NSQDConnection.CLOSED, () => {
 *     console.log('Callback [closed]: Lost connection to nsqd')
 *   })
 *
 *   c.on(NSQDConnection.ERROR, (err) => {
 *     console.log(`Callback [error]: ${err}`)
 *   })
 *
 *   c.on(NSQDConnection.BACKOFF, () => {
 *     console.log('Callback [backoff]: RDY 0')
 *     c.setRdy(0)
 *     setTimeout(() => {
 *       c.setRdy 100;
 *       console.log('RDY 100')
 *     }, 10 * 1000)
 *   })
 *
 *   c.connect()
 */
class NSQDConnection extends EventEmitter {
  // Events emitted by NSQDConnection
  static get BACKOFF() {
    return 'backoff'
  }
  static get CONNECTED() {
    return 'connected'
  }
  static get CLOSED() {
    return 'closed'
  }
  static get CONNECTION_ERROR() {
    return 'connection_error'
  }
  static get ERROR() {
    return 'error'
  }
  static get FINISHED() {
    return 'finished'
  }
  static get MESSAGE() {
    return 'message'
  }
  static get REQUEUED() {
    return 'requeued'
  }
  static get READY() {
    return 'ready'
  }

  /**
   * Instantiates a new NSQDConnection.
   *
   * @constructor
   * @param  {String} nsqdHost
   * @param  {String|Number} nsqdPort
   * @param  {String} topic
   * @param  {String} channel
   * @param  {Object} [options={}]
   */
  constructor(nsqdHost, nsqdPort, topic, channel, options = {}) {
    super(nsqdHost, nsqdPort, topic, channel, options)

    this.nsqdHost = nsqdHost
    this.nsqdPort = nsqdPort
    this.topic = topic
    this.channel = channel
    const connId = this.id().replace(':', '/')
    this.debug = debug(
      `nsqjs:reader:${this.topic}/${this.channel}:conn:${connId}`
    )

    this.config = new ConnectionConfig(options)
    this.config.validate()

    this.frameBuffer = new FrameBuffer()
    this.statemachine = this.connectionState()

    this.maxRdyCount = 0 // Max RDY value for a conn to this NSQD
    this.msgTimeout = 0 // Timeout time in milliseconds for a Message
    this.maxMsgTimeout = 0 // Max time to process a Message in millisecs
    this.nsqdVersion = null // Version returned by nsqd
    this.lastMessageTimestamp = null // Timestamp of last message received
    this.lastReceivedTimestamp = null // Timestamp of last data received
    this.conn = null // Socket connection to NSQD
    this.identifyTimeoutId = null // Timeout ID for triggering identifyFail
    this.messageCallbacks = [] // Callbacks on message sent responses

    this.writeQueue = []
    this.onDataFn = null
    this.outWriter = null
  }

  /**
   * The nsqd host:port pair.
   *
   * @return {[type]} [description]
   */
  id() {
    return joinHostPort(this.nsqdHost, this.nsqdPort)
  }

  /**
   * Instantiates or returns a new ConnectionState.
   *
   * @return {ConnectionState}
   */
  connectionState() {
    return this.statemachine || new ConnectionState(this)
  }

  /**
   * Creates a new nsqd connection.
   */
  connect() {
    this.statemachine.raise('connecting')

    // Using nextTick so that clients of Reader can register event listeners
    // right after calling connect.
    process.nextTick(() => {
      this.conn = net.connect(
        {port: this.nsqdPort, host: this.nsqdHost},
        () => {
          this.statemachine.raise('connected')
          this.emit(NSQDConnection.CONNECTED)

          // Once there's a socket connection, give it 5 seconds to receive an
          // identify response.
          this.identifyTimeoutId = setTimeout(() => {
            this.identifyTimeout()
          }, 5000)

          this.identifyTimeoutId
        }
      )
      this.conn.setNoDelay(true)
      this.outWriter = this.conn

      this.registerStreamListeners(this.conn)
    })
  }

  /**
   * Register event handlers for the nsqd connection.
   *
   * @param  {Object} conn
   */
  registerStreamListeners(conn) {
    this.onDataFn = (data) => this.receiveData(data)
    conn.on('data', this.onDataFn)
    conn.on('end', () => {
      this.statemachine.goto('CLOSED')
    })
    conn.on('error', (err) => {
      this.statemachine.goto('ERROR', err)
      this.emit('connection_error', err)
    })
    conn.on('close', () => this.statemachine.raise('close'))
    conn.setTimeout(this.config.idleTimeout * 1000, () =>
      this.statemachine.raise('close')
    )
  }

  /**
   * Connect via tls.
   *
   * @param  {Function} callback
   */
  startTLS(callback) {
    for (const event of ['data', 'error', 'close']) {
      this.conn.removeAllListeners(event)
    }

    const options = {
      socket: this.conn,
      rejectUnauthorized: this.config.tlsVerification,
      ca: this.config.ca,
      key: this.config.key,
      cert: this.config.cert,
    }

    let tlsConn = tls.connect(options, () => {
      this.conn = tlsConn
      typeof callback === 'function' ? callback() : undefined
    })

    this.outWriter = tlsConn
    this.registerStreamListeners(tlsConn)
  }

  /**
   * startCompression wraps the TCP connection stream.
   *
   * @param {Stream} inflater - Decompression stream
   * @param {Stream} deflater - Compression stream
   */
  startCompression(inflater, deflater) {
    this.inflater = inflater
    this.deflater = deflater

    this.conn.removeListener('data', this.onDataFn)
    this.conn.pipe(this.inflater)
    this.inflater.on('data', this.onDataFn)

    this.outWriter = this.deflater
    this.outWriter.pipe(this.conn)

    if (this.frameBuffer.buffer) {
      const b = this.frameBuffer.buffer
      this.frameBuffer.buffer = null
      setImmediate(() => this.inflater.write(b))
    }
  }

  /**
   * Begin deflating the frame buffer. Actualy deflating is handled by
   * zlib.
   *
   * @param  {Number} level
   */
  startDeflate(level) {
    this.startCompression(
      zlib.createInflateRaw({flush: zlib.constants.Z_SYNC_FLUSH}),
      zlib.createDeflateRaw({
        level,
        flush: zlib.constants.Z_SYNC_FLUSH,
      })
    )
  }

  /**
   * Create a snappy stream.
   */
  startSnappy() {
    const {SnappyStream, UnsnappyStream} = require('snappystream')
    this.startCompression(new UnsnappyStream(), new SnappyStream())
  }

  /**
   * Raise a `READY` event with the specified count.
   *
   * @param {Number} rdyCount
   */
  setRdy(rdyCount) {
    this.statemachine.raise('ready', rdyCount)
  }

  /**
   * Handle receiveing the message payload frame by frame.
   *
   * @param  {Object} data
   */
  receiveData(data) {
    this.lastReceivedTimestamp = Date.now()
    this.frameBuffer.consume(data)

    let frame = this.frameBuffer.nextFrame()

    while (frame) {
      const [frameId, payload] = Array.from(frame)
      switch (frameId) {
        case wire.FRAME_TYPE_RESPONSE:
          this.statemachine.raise('response', payload)
          break
        case wire.FRAME_TYPE_ERROR:
          this.statemachine.goto('ERROR', new Error(payload.toString()))
          break
        case wire.FRAME_TYPE_MESSAGE:
          this.lastMessageTimestamp = this.lastReceivedTimestamp
          this.statemachine.raise('consumeMessage', this.createMessage(payload))
          break
      }

      frame = this.frameBuffer.nextFrame()
    }
  }

  /**
   * Generates client metadata so that nsqd can identify connections.
   *
   * @return {Object} The connection metadata.
   */
  identify() {
    const longName = os.hostname()
    const shortName = longName.split('.')[0]

    const identify = {
      client_id: this.config.clientId || shortName,
      deflate: this.config.deflate,
      deflate_level: this.config.deflateLevel,
      feature_negotiation: true,
      heartbeat_interval: this.config.heartbeatInterval * 1000,
      hostname: longName,
      long_id: longName, // Remove when deprecating pre 1.0
      msg_timeout: this.config.messageTimeout,
      output_buffer_size: this.config.outputBufferSize,
      output_buffer_timeout: this.config.outputBufferTimeout,
      sample_rate: this.config.sampleRate,
      short_id: shortName, // Remove when deprecating pre 1.0
      snappy: this.config.snappy,
      tls_v1: this.config.tls,
      user_agent: `nsqjs/${version}`,
    }

    // Remove some keys when they're effectively not provided.
    const removableKeys = [
      'msg_timeout',
      'output_buffer_size',
      'output_buffer_timeout',
      'sample_rate',
    ]

    removableKeys.forEach((key) => {
      if (identify[key] === null) {
        delete identify[key]
      }
    })

    return identify
  }

  /**
   * Throws an error if the connection timed out while identifying the nsqd.
   */
  identifyTimeout() {
    this.statemachine.goto(
      'ERROR',
      new Error('Timed out identifying with nsqd')
    )
  }

  /**
   * Clears an identify timeout. Useful for retries.
   */
  clearIdentifyTimeout() {
    clearTimeout(this.identifyTimeoutId)
    this.identifyTimeoutId = null
  }

  /**
   * Create a new message from the payload.
   *
   * @param  {Buffer} msgPayload
   * @return {Message}
   */
  createMessage(msgPayload) {
    const msg = new Message(
      msgPayload,
      this.config.requeueDelay,
      this.msgTimeout,
      this.maxMsgTimeout
    )

    this.debug(`Received message [${msg.id}] [attempts: ${msg.attempts}]`)

    msg.on(Message.RESPOND, (responseType, wireData) => {
      this.write(wireData)

      if (responseType === Message.FINISH) {
        this.debug(
          `Finished message [${msg.id}] [timedout=${msg.timedout === true}, \
elapsed=${Date.now() - msg.receivedOn}ms, \
touch_count=${msg.touchCount}]`
        )
        this.emit(NSQDConnection.FINISHED)
      } else if (responseType === Message.REQUEUE) {
        this.debug(`Requeued message [${msg.id}]`)
        this.emit(NSQDConnection.REQUEUED)
      }
    })

    msg.on(Message.BACKOFF, () => this.emit(NSQDConnection.BACKOFF))

    return msg
  }

  /**
   * Write a message to the connection. Deflate it if necessary.
   * @param  {Object} data
   */
  write(data) {
    if (Buffer.isBuffer(data)) {
      this.outWriter.write(data)
    } else {
      this.outWriter.write(Buffer.from(data))
    }
  }

  _flush() {
    if (this.writeQueue.length > 0) {
      const data = Buffer.concat(this.writeQueue)

      if (this.deflater) {
        this.deflater.write(data, () => this.conn.write(this.deflater.read()))
      } else {
        this.conn.write(data)
      }
    }

    this.writeQueue = []
  }

  /**
   * Close the nsqd connection.
   */
  close() {
    if (
      !this.conn.destroyed &&
      this.statemachine.current_state !== 'CLOSED' &&
      this.statemachine.current_state !== 'ERROR'
    ) {
      try {
        this.conn.end(wire.close())
      } catch (e) {
        // Continue regardless of error.
      }
    }
    this.statemachine.goto('CLOSED')
  }

  /**
   * Destroy the nsqd connection.
   */
  destroy() {
    if (!this.conn.destroyed) {
      this.conn.destroy()
    }
  }
}

/**
 * A statemachine modeling the connection state of an nsqd connection.
 * @type {ConnectionState}
 */
class ConnectionState extends NodeState {
  /**
   * Instantiates a new instance of ConnectionState.
   *
   * @constructor
   * @param  {Object} conn
   */
  constructor(conn) {
    super({
      autostart: true,
      initial_state: 'INIT',
      sync_goto: true,
    })

    this.conn = conn
    this.identifyResponse = null
  }

  /**
   * @param  {*} message
   */
  log(message) {
    if (this.current_state_name !== 'INIT') {
      this.conn.debug(`${this.current_state_name}`)
    }
    if (message) {
      this.conn.debug(message)
    }
  }

  /**
   * @return {String}
   */
  afterIdentify() {
    return 'SUBSCRIBE'
  }
}

ConnectionState.prototype.states = {
  INIT: {
    connecting() {
      return this.goto('CONNECTING')
    },

    close() {
      return this.goto('CLOSED')
    },
  },

  CONNECTING: {
    connected() {
      return this.goto('CONNECTED')
    },

    close() {
      return this.goto('CLOSED')
    },
  },

  CONNECTED: {
    Enter() {
      return this.goto('SEND_MAGIC_IDENTIFIER')
    },
  },

  SEND_MAGIC_IDENTIFIER: {
    Enter() {
      // Send the magic protocol identifier to the connection
      this.conn.write(wire.MAGIC_V2)
      return this.goto('IDENTIFY')
    },
  },

  IDENTIFY: {
    Enter() {
      // Send configuration details
      const identify = this.conn.identify()
      this.conn.debug(identify)
      this.conn.write(wire.identify(identify))
      return this.goto('IDENTIFY_RESPONSE')
    },
  },

  IDENTIFY_RESPONSE: {
    response(data) {
      if (data.toString() === 'OK') {
        data = JSON.stringify({
          max_rdy_count: 2500,
          max_msg_timeout: 15 * 60 * 1000, // 15 minutes
          msg_timeout: 60 * 1000,
        }) //  1 minute
      }

      this.identifyResponse = JSON.parse(data)
      this.conn.debug(this.identifyResponse)
      this.conn.maxRdyCount = this.identifyResponse.max_rdy_count
      this.conn.maxMsgTimeout = this.identifyResponse.max_msg_timeout
      this.conn.msgTimeout = this.identifyResponse.msg_timeout
      this.conn.nsqdVersion = this.identifyResponse.version
      this.conn.clearIdentifyTimeout()

      if (this.identifyResponse.tls_v1) {
        return this.goto('TLS_START')
      }
      return this.goto('IDENTIFY_COMPRESSION_CHECK')
    },

    close() {
      return this.goto('CLOSED')
    },
  },

  IDENTIFY_COMPRESSION_CHECK: {
    Enter() {
      const {deflate, snappy} = this.identifyResponse

      if (deflate) {
        return this.goto('DEFLATE_START', this.identifyResponse.deflate_level)
      }
      if (snappy) {
        return this.goto('SNAPPY_START')
      }
      return this.goto('AUTH')
    },
  },

  TLS_START: {
    Enter() {
      this.conn.startTLS()
      return this.goto('TLS_RESPONSE')
    },
  },

  TLS_RESPONSE: {
    response(data) {
      if (data.toString() === 'OK') {
        return this.goto('IDENTIFY_COMPRESSION_CHECK')
      }
      return this.goto('ERROR', new Error('TLS negotiate error with nsqd'))
    },

    close() {
      return this.goto('CLOSED')
    },
  },

  DEFLATE_START: {
    Enter(level) {
      this.conn.startDeflate(level)
      return this.goto('COMPRESSION_RESPONSE')
    },
  },

  SNAPPY_START: {
    Enter() {
      this.conn.startSnappy()
      return this.goto('COMPRESSION_RESPONSE')
    },
  },

  COMPRESSION_RESPONSE: {
    response(data) {
      if (data.toString() === 'OK') {
        return this.goto('AUTH')
      }
      return this.goto(
        'ERROR',
        new Error('Bad response when enabling compression')
      )
    },

    close() {
      return this.goto('CLOSED')
    },
  },

  AUTH: {
    Enter() {
      if (!this.conn.config.authSecret) {
        return this.goto(this.afterIdentify())
      }
      this.conn.write(wire.auth(this.conn.config.authSecret))
      return this.goto('AUTH_RESPONSE')
    },
  },

  AUTH_RESPONSE: {
    response(data) {
      this.conn.auth = JSON.parse(data)
      return this.goto(this.afterIdentify())
    },

    close() {
      return this.goto('CLOSED')
    },
  },

  SUBSCRIBE: {
    Enter() {
      this.conn.write(wire.subscribe(this.conn.topic, this.conn.channel))
      return this.goto('SUBSCRIBE_RESPONSE')
    },
  },

  SUBSCRIBE_RESPONSE: {
    response(data) {
      if (data.toString() === 'OK') {
        this.goto('READY_RECV')
        // Notify listener that this nsqd connection has passed the subscribe
        // phase. Do this only once for a connection.
        return this.conn.emit(NSQDConnection.READY)
      }
    },

    close() {
      return this.goto('CLOSED')
    },
  },

  READY_RECV: {
    consumeMessage(msg) {
      return this.conn.emit(NSQDConnection.MESSAGE, msg)
    },

    response(data) {
      if (data.toString() === '_heartbeat_') {
        return this.conn.write(wire.nop())
      }
    },

    ready(rdyCount) {
      // RDY count for this nsqd cannot exceed the nsqd configured
      // max rdy count.
      if (rdyCount > this.conn.maxRdyCount) {
        rdyCount = this.conn.maxRdyCount
      }
      return this.conn.write(wire.ready(rdyCount))
    },

    close() {
      return this.goto('CLOSED')
    },
  },

  READY_SEND: {
    Enter() {
      // Notify listener that this nsqd connection is ready to send.
      return this.conn.emit(NSQDConnection.READY)
    },

    produceMessages(data) {
      const [topic, msgs, timeMs, callback] = Array.from(data)
      this.conn.messageCallbacks.push(callback)

      if (!_.isArray(msgs)) {
        throw new Error('Expect an array of messages to produceMessages')
      }

      if (msgs.length === 1) {
        if (!timeMs) {
          return this.conn.write(wire.pub(topic, msgs[0]))
        } else {
          return this.conn.write(wire.dpub(topic, msgs[0], timeMs))
        }
      }
      if (!timeMs) {
        return this.conn.write(wire.mpub(topic, msgs))
      } else {
        throw new Error('DPUB can only defer one message at a time')
      }
    },

    response(data) {
      let cb
      switch (data.toString()) {
        case 'OK':
          cb = this.conn.messageCallbacks.shift()
          return typeof cb === 'function' ? cb(null) : undefined
        case '_heartbeat_':
          return this.conn.write(wire.nop())
      }
    },

    close() {
      return this.goto('CLOSED')
    },
  },

  ERROR: {
    Enter(err) {
      // If there's a callback, pass it the error.
      const cb = this.conn.messageCallbacks.shift()
      if (typeof cb === 'function') {
        cb(err)
      }

      this.conn.emit(NSQDConnection.ERROR, err)

      // According to NSQ docs, the following errors are non-fatal and should
      // not close the connection. See here for more info:
      // http://nsq.io/clients/building_client_libraries.html
      if (!_.isString(err)) {
        err = err.toString()
      }
      const errorCode = err.split(/\s+/)[1]

      if (
        ['E_REQ_FAILED', 'E_FIN_FAILED', 'E_TOUCH_FAILED'].includes(errorCode)
      ) {
        return this.goto('READY_RECV')
      }
      return this.goto('CLOSED')
    },

    close() {
      return this.goto('CLOSED')
    },
  },

  CLOSED: {
    Enter() {
      if (!this.conn) {
        return
      }

      // If there are callbacks, then let them error on the closed connection.
      const err = new Error('nsqd connection closed')
      for (const cb of this.conn.messageCallbacks) {
        if (typeof cb === 'function') {
          cb(err)
        }
      }

      this.conn.messageCallbacks = []
      this.disable()
      this.conn.destroy()
      this.conn.emit(NSQDConnection.CLOSED)
      return delete this.conn
    },

    // No-op. Once closed, subsequent calls should do nothing.
    close() {},
  },
}

ConnectionState.prototype.transitions = {
  '*': {
    '*': function (data, callback) {
      this.log()
      return callback(data)
    },

    CONNECTED(data, callback) {
      this.log()
      return callback(data)
    },

    ERROR(err, callback) {
      this.log(`${err}`)
      return callback(err)
    },
  },
}

/**
 * WriterConnectionState
 *
 * Usage:
 *   c = new NSQDConnectionWriter '127.0.0.1', 4150, 30
 *   c.connect()
 *
 *   c.on NSQDConnectionWriter.CLOSED, ->
 *     console.log "Callback [closed]: Lost connection to nsqd"
 *
 *   c.on NSQDConnectionWriter.ERROR, (err) ->
 *     console.log "Callback [error]: #{err}"
 *
 *   c.on NSQDConnectionWriter.READY, ->
 *     c.produceMessages 'sample_topic', ['first message']
 *     c.produceMessages 'sample_topic', ['second message', 'third message']
 *     c.destroy()
 */
class WriterNSQDConnection extends NSQDConnection {
  /**
   * @constructor
   * @param  {String} nsqdHost
   * @param  {String|Number} nsqdPort
   * @param  {Object} [options={}]
   */
  constructor(nsqdHost, nsqdPort, options = {}) {
    super(nsqdHost, nsqdPort, null, null, options)
    this.debug = debug(`nsqjs:writer:conn:${nsqdHost}/${nsqdPort}`)
  }

  /**
   * Instantiates a new instance of WriterConnectionState or returns an
   * existing one.
   *
   * @return {WriterConnectionState}
   */
  connectionState() {
    return this.statemachine || new WriterConnectionState(this)
  }

  /**
   * Emits a `produceMessages` event with the specified topic, msgs, timeMs and a
   * callback.
   *
   * @param  {String}   topic
   * @param  {Array}    msgs
   * @param  {Number}   timeMs
   * @param  {Function} callback
   */
  produceMessages(topic, msgs, timeMs, callback) {
    this.statemachine.raise('produceMessages', [topic, msgs, timeMs, callback])
  }
}

/**
 * A statemachine modeling the various states a writer connection can be in.
 */
class WriterConnectionState extends ConnectionState {
  /**
   * Returned when the connection is ready to send messages.
   *
   * @return {String}
   */
  afterIdentify() {
    return 'READY_SEND'
  }
}

module.exports = {
  NSQDConnection,
  ConnectionState,
  WriterNSQDConnection,
  WriterConnectionState,
}
