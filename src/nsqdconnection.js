import Debug from 'debug'
import net from 'net'
import os from 'os'
import tls from 'tls'
import zlib from 'zlib'
import fs from 'fs'
import { EventEmitter } from 'events'
import { SnappyStream, UnsnappyStream } from 'snappystream'

import _ from 'underscore'
import NodeState from 'node-state'

import { ConnectionConfig } from './config'
import FrameBuffer from './framebuffer'
import Message from './message'
import * as wire from './wire'
import version from './version'

/*
NSQDConnection is a reader connection to a nsqd instance. It manages all
aspects of the nsqd connection with the exception of the RDY count which
needs to be managed across all nsqd connections for a given topic / channel
pair.

This shouldn't be used directly. Use a Reader instead.

Usage:

c = new NSQDConnection '127.0.0.1', 4150, 'test', 'default', 60, 30

c.on NSQDConnection.MESSAGE, (msg) ->
  console.log "Callback [message]: #{msg.attempts}, #{msg.body.toString()}"
  console.log "Timeout of message is #{msg.timeUntilTimeout()}"
  setTimeout (-> console.log "timeout = #{msg.timeUntilTimeout()}"), 5000
  msg.finish()

c.on NSQDConnection.FINISHED, ->
  c.setRdy 1

c.on NSQDConnection.READY, ->
  console.log "Callback [ready]: Set RDY to 100"
  c.setRdy 10

c.on NSQDConnection.CLOSED, ->
  console.log "Callback [closed]: Lost connection to nsqd"

c.on NSQDConnection.ERROR, (err) ->
  console.log "Callback [error]: #{err}"

c.on NSQDConnection.BACKOFF, ->
  console.log "Callback [backoff]: RDY 0"
  c.setRdy 0
  setTimeout (-> c.setRdy 100; console.log 'RDY 100'), 10 * 1000

c.connect()
*/
class NSQDConnection extends EventEmitter {
  static initClass () {
    // Events emitted by NSQDConnection
    this.BACKOFF = 'backoff'
    this.CONNECTED = 'connected'
    this.CLOSED = 'closed'
    this.CONNECTION_ERROR = 'connection_error'
    this.ERROR = 'error'
    this.FINISHED = 'finished'
    this.MESSAGE = 'message'
    this.REQUEUED = 'requeued'
    this.READY = 'ready'
  }

  constructor (nsqdHost, nsqdPort, topic, channel, options) {
    if (options == null) { options = {} }
    super(...arguments)

    this.nsqdHost = nsqdHost
    this.nsqdPort = nsqdPort
    this.topic = topic
    this.channel = channel
    const connId = this.id().replace(':', '/')
    this.debug = Debug(`nsqjs:reader:${this.topic}/${this.channel}:conn:${connId}`)

    this.config = new ConnectionConfig(options)
    this.config.validate()

    this.frameBuffer = new FrameBuffer()
    this.statemachine = this.connectionState()

    this.maxRdyCount = 0               // Max RDY value for a conn to this NSQD
    this.msgTimeout = 0                // Timeout time in milliseconds for a Message
    this.maxMsgTimeout = 0             // Max time to process a Message in millisecs
    this.nsqdVersion = null            // Version returned by nsqd
    this.lastMessageTimestamp = null   // Timestamp of last message received
    this.lastReceivedTimestamp = null  // Timestamp of last data received
    this.conn = null                   // Socket connection to NSQD
    this.identifyTimeoutId = null      // Timeout ID for triggering identifyFail
    this.messageCallbacks = []         // Callbacks on message sent responses
  }

  id () {
    return `${this.nsqdHost}:${this.nsqdPort}`
  }

  connectionState () {
    return this.statemachine || new ConnectionState(this)
  }

  connect () {
    this.statemachine.raise('connecting')

    // Using nextTick so that clients of Reader can register event listeners
    // right after calling connect.
    return process.nextTick(() => {
      this.conn = net.connect({ port: this.nsqdPort, host: this.nsqdHost }, () => {
        this.statemachine.raise('connected')
        this.emit(NSQDConnection.CONNECTED)
        // Once there's a socket connection, give it 5 seconds to receive an
        // identify response.
        return this.identifyTimeoutId = setTimeout(this.identifyTimeout.bind(this), 5000)
      },
      )

      return this.registerStreamListeners(this.conn)
    },
    )
  }

  registerStreamListeners (conn) {
    conn.on('data', data => this.receiveRawData(data))
    conn.on('end', (err) => {
      this.statemachine.goto('CLOSED')
      return this.emit('connection_error', err)
    })
    return conn.on('close', err => this.statemachine.raise('close'))
  }

  startTLS (callback) {
    for (const event of ['data', 'error', 'close']) { this.conn.removeAllListeners(event) }

    const options = {
      socket: this.conn,
      rejectUnauthorized: this.config.tlsVerification
    }
    var tlsConn = tls.connect(options, () => {
      this.conn = tlsConn
      return (typeof callback === 'function' ? callback() : undefined)
    },
    )

    return this.registerStreamListeners(tlsConn)
  }

  startDeflate (level) {
    this.inflater = zlib.createInflateRaw({ flush: zlib.Z_SYNC_FLUSH })
    this.deflater = zlib.createDeflateRaw({ level, flush: zlib.Z_SYNC_FLUSH })
    return this.reconsumeFrameBuffer()
  }

  startSnappy () {
    this.inflater = new UnsnappyStream()
    this.deflater = new SnappyStream()
    return this.reconsumeFrameBuffer()
  }

  reconsumeFrameBuffer () {
    if (this.frameBuffer.buffer && this.frameBuffer.buffer.length) {
      const data = this.frameBuffer.buffer
      delete this.frameBuffer.buffer
      return this.receiveRawData(data)
    }
  }

  setRdy (rdyCount) {
    return this.statemachine.raise('ready', rdyCount)
  }

  receiveRawData (data) {
    if (!this.inflater) {
      return this.receiveData(data)
    }
    return this.inflater.write(data, () => {
      const uncompressedData = this.inflater.read()
      if (uncompressedData) { return this.receiveData(uncompressedData) }
    },
      )
  }

  receiveData (data) {
    this.lastReceivedTimestamp = Date.now()
    this.frameBuffer.consume(data)

    return (() => {
      let frame
      const result = []
      while (frame = this.frameBuffer.nextFrame()) {
        let item
        const [frameId, payload] = Array.from(frame)
        switch (frameId) {
          case wire.FRAME_TYPE_RESPONSE:
            item = this.statemachine.raise('response', payload)
            break
          case wire.FRAME_TYPE_ERROR:
            item = this.statemachine.goto('ERROR', new Error(payload.toString()))
            break
          case wire.FRAME_TYPE_MESSAGE:
            this.lastMessageTimestamp = this.lastReceivedTimestamp
            item = this.statemachine.raise('consumeMessage', this.createMessage(payload))
            break
        }
        result.push(item)
      }
      return result
    })()
  }

  identify () {
    const longName = os.hostname()
    const shortName = longName.split('.')[0]

    const identify = {
      client_id: this.config.clientId || shortName,
      deflate: this.config.deflate,
      deflate_level: this.config.deflateLevel,
      feature_negotiation: true,
      heartbeat_interval: this.config.heartbeatInterval * 1000,
      long_id: longName,
      msg_timeout: this.config.messageTimeout,
      output_buffer_size: this.config.outputBufferSize,
      output_buffer_timeout: this.config.outputBufferTimeout,
      sample_rate: this.config.sampleRate,
      short_id: shortName,
      snappy: this.config.snappy,
      tls_v1: this.config.tls,
      user_agent: `nsqjs/${version}`
    }

    // Remove some keys when they're effectively not provided.
    const removableKeys = [
      'msg_timeout',
      'output_buffer_size',
      'output_buffer_timeout',
      'sample_rate'
    ]
    for (const key of Array.from(removableKeys)) { if (identify[key] === null) { delete identify[key] } }
    return identify
  }

  identifyTimeout () {
    return this.statemachine.goto('ERROR', new Error('Timed out identifying with nsqd'))
  }

  clearIdentifyTimeout () {
    clearTimeout(this.identifyTimeoutId)
    return this.identifyTimeoutId = null
  }

  // Create a Message object from the message payload received from nsqd.
  createMessage (msgPayload) {
    const msgComponents = wire.unpackMessage(msgPayload)
    const msg = new Message(...msgComponents, this.config.requeueDelay, this.msgTimeout,
      this.maxMsgTimeout)

    this.debug(`Received message [${msg.id}] [attempts: ${msg.attempts}]`)

    msg.on(Message.RESPOND, (responseType, wireData) => {
      this.write(wireData)

      if (responseType === Message.FINISH) {
        this.debug(`Finished message [${msg.id}] [timedout=${msg.timedout === true}, \
elapsed=${Date.now() - msg.receivedOn}ms, \
touch_count=${msg.touchCount}]`,
        )
        return this.emit(NSQDConnection.FINISHED)
      } else if (responseType === Message.REQUEUE) {
        this.debug(`Requeued message [${msg.id}]`)
        return this.emit(NSQDConnection.REQUEUED)
      }
    },
    )

    msg.on(Message.BACKOFF, () => this.emit(NSQDConnection.BACKOFF),
    )

    return msg
  }

  write (data) {
    if (this.deflater) {
      return this.deflater.write(data, () => this.conn.write(this.deflater.read()),
      )
    }
    return this.conn.write(data)
  }

  destroy () {
    return this.conn.destroy()
  }
}
NSQDConnection.initClass()

class ConnectionState extends NodeState {
  static initClass () {
    this.prototype.states = {
      INIT: {
        connecting () {
          return this.goto('CONNECTING')
        }
      },

      CONNECTING: {
        connected () {
          return this.goto('CONNECTED')
        }
      },

      CONNECTED: {
        Enter () {
          return this.goto('SEND_MAGIC_IDENTIFIER')
        }
      },

      SEND_MAGIC_IDENTIFIER: {
        Enter () {
          // Send the magic protocol identifier to the connection
          this.conn.write(wire.MAGIC_V2)
          return this.goto('IDENTIFY')
        }
      },

      IDENTIFY: {
        Enter () {
          // Send configuration details
          const identify = this.conn.identify()
          this.conn.debug(identify)
          this.conn.write(wire.identify(identify))
          return this.goto('IDENTIFY_RESPONSE')
        }
      },

      IDENTIFY_RESPONSE: {
        response (data) {
          if (data.toString() === 'OK') {
            data = JSON.stringify({
              max_rdy_count: 2500,
              max_msg_timeout: 15 * 60 * 1000,    // 15 minutes
              msg_timeout: 60 * 1000
            })             //  1 minute
          }

          this.identifyResponse = JSON.parse(data)
          this.conn.debug(this.identifyResponse)
          this.conn.maxRdyCount = this.identifyResponse.max_rdy_count
          this.conn.maxMsgTimeout = this.identifyResponse.max_msg_timeout
          this.conn.msgTimeout = this.identifyResponse.msg_timeout
          this.conn.nsqdVersion = this.identifyResponse.version
          this.conn.clearIdentifyTimeout()

          if (this.identifyResponse.tls_v1) { return this.goto('TLS_START') }
          return this.goto('IDENTIFY_COMPRESSION_CHECK')
        }
      },

      IDENTIFY_COMPRESSION_CHECK: {
        Enter () {
          const { deflate, snappy } = this.identifyResponse

          if (deflate) { return this.goto('DEFLATE_START', this.identifyResponse.deflate_level) }
          if (snappy) { return this.goto('SNAPPY_START') }
          return this.goto('AUTH')
        }
      },

      TLS_START: {
        Enter () {
          this.conn.startTLS()
          return this.goto('TLS_RESPONSE')
        }
      },

      TLS_RESPONSE: {
        response (data) {
          if (data.toString() === 'OK') {
            return this.goto('IDENTIFY_COMPRESSION_CHECK')
          }
          return this.goto('ERROR', new Error('TLS negotiate error with nsqd'))
        }
      },

      DEFLATE_START: {
        Enter (level) {
          this.conn.startDeflate(level)
          return this.goto('COMPRESSION_RESPONSE')
        }
      },

      SNAPPY_START: {
        Enter () {
          this.conn.startSnappy()
          return this.goto('COMPRESSION_RESPONSE')
        }
      },

      COMPRESSION_RESPONSE: {
        response (data) {
          if (data.toString() === 'OK') {
            return this.goto('AUTH')
          }
          return this.goto('ERROR', new Error('Bad response when enabling compression'))
        }
      },

      AUTH: {
        Enter () {
          if (!this.conn.config.authSecret) { return this.goto(this.afterIdentify()) }
          this.conn.write(wire.auth(this.conn.config.authSecret))
          return this.goto('AUTH_RESPONSE')
        }
      },

      AUTH_RESPONSE: {
        response (data) {
          this.conn.auth = JSON.parse(data)
          return this.goto(this.afterIdentify())
        }
      },

      SUBSCRIBE: {
        Enter () {
          this.conn.write(wire.subscribe(this.conn.topic, this.conn.channel))
          return this.goto('SUBSCRIBE_RESPONSE')
        }
      },

      SUBSCRIBE_RESPONSE: {
        response (data) {
          if (data.toString() === 'OK') {
            this.goto('READY_RECV')
            // Notify listener that this nsqd connection has passed the subscribe
            // phase. Do this only once for a connection.
            return this.conn.emit(NSQDConnection.READY)
          }
        }
      },

      READY_RECV: {
        consumeMessage (msg) {
          return this.conn.emit(NSQDConnection.MESSAGE, msg)
        },

        response (data) {
          if (data.toString() === '_heartbeat_') { return this.conn.write(wire.nop()) }
        },

        ready (rdyCount) {
          // RDY count for this nsqd cannot exceed the nsqd configured
          // max rdy count.
          if (rdyCount > this.conn.maxRdyCount) { rdyCount = this.conn.maxRdyCount }
          return this.conn.write(wire.ready(rdyCount))
        },

        close () {
          return this.goto('CLOSED')
        }
      },

      READY_SEND: {
        Enter () {
          // Notify listener that this nsqd connection is ready to send.
          return this.conn.emit(NSQDConnection.READY)
        },

        produceMessages (data) {
          const [topic, msgs, callback] = Array.from(data)
          this.conn.messageCallbacks.push(callback)

          if (!_.isArray(msgs)) {
            throw new Error('Expect an array of messages to produceMessages')
          }

          if (msgs.length === 1) {
            return this.conn.write(wire.pub(topic, msgs[0]))
          }
          return this.conn.write(wire.mpub(topic, msgs))
        },

        response (data) {
          switch (data.toString()) {
            case 'OK':
              const cb = this.conn.messageCallbacks.shift()
              return (typeof cb === 'function' ? cb(null) : undefined)
            case '_heartbeat_':
              return this.conn.write(wire.nop())
          }
        },

        close () {
          return this.goto('CLOSED')
        }
      },

      ERROR: {
        Enter (err) {
          // If there's a callback, pass it the error.
          const cb = this.conn.messageCallbacks.shift()
          if (typeof cb === 'function') {
            cb(err)
          }

          this.conn.emit(NSQDConnection.ERROR, err)

          // According to NSQ docs, the following errors are non-fatal and should
          // not close the connection. See here for more info:
          // http://nsq.io/clients/building_client_libraries.html
          if (!_.isString(err)) { err = err.toString() }
          const errorCode = __guard__(err.split(/\s+/), x => x[1])
          if (['E_REQ_FAILED', 'E_FIN_FAILED', 'E_TOUCH_FAILED'].includes(errorCode)) {
            return this.goto('READY_RECV')
          }
          return this.goto('CLOSED')
        },

        close () {
          return this.goto('CLOSED')
        }
      },

      CLOSED: {
        Enter () {
          if (!this.conn) { return }

          // If there are callbacks, then let them error on the closed connection.
          const err = new Error('nsqd connection closed')
          for (const cb of Array.from(this.conn.messageCallbacks)) {
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

        close () {}
      }
    }
          // No-op. Once closed, subsequent calls should do nothing.

    this.prototype.transitions = {
      '*': {
        '*': function (data, callback) {
          this.log()
          return callback(data)
        },

        CONNECTED (data, callback) {
          this.log()
          return callback(data)
        },

        ERROR (err, callback) {
          this.log(`${err}`)
          return callback(err)
        }
      }
    }
  }
  constructor (conn) {
    super({
      autostart: true,
      initial_state: 'INIT',
      sync_goto: true
    })

    this.conn = conn

    this.identifyResponse = null
  }

  log (message) {
    if (this.current_state_name !== 'INIT') { this.conn.debug(`${this.current_state_name}`) }
    if (message) { return this.conn.debug(message) }
  }

  afterIdentify () {
    return 'SUBSCRIBE'
  }
}
ConnectionState.initClass()

/*
c = new NSQDConnectionWriter '127.0.0.1', 4150, 30
c.connect()

c.on NSQDConnectionWriter.CLOSED, ->
  console.log "Callback [closed]: Lost connection to nsqd"

c.on NSQDConnectionWriter.ERROR, (err) ->
  console.log "Callback [error]: #{err}"

c.on NSQDConnectionWriter.READY, ->
  c.produceMessages 'sample_topic', ['first message']
  c.produceMessages 'sample_topic', ['second message', 'third message']
  c.destroy()
*/
class WriterNSQDConnection extends NSQDConnection {
  constructor (nsqdHost, nsqdPort, options) {
    if (options == null) { options = {} }
    super(nsqdHost, nsqdPort, null, null, options)
    this.debug = Debug(`nsqjs:writer:conn:${nsqdHost}/${nsqdPort}`)
  }

  connectionState () {
    return this.statemachine || new WriterConnectionState(this)
  }

  produceMessages (topic, msgs, callback) {
    return this.statemachine.raise('produceMessages', [topic, msgs, callback])
  }
}

class WriterConnectionState extends ConnectionState {
  afterIdentify () {
    return 'READY_SEND'
  }
}

export { NSQDConnection, ConnectionState, WriterNSQDConnection, WriterConnectionState }

function __guard__ (value, transform) {
  return (typeof value !== 'undefined' && value !== null) ? transform(value) : undefined
}
