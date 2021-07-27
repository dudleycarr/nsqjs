const _ = require('lodash')
const debug = require('./debug')
const {EventEmitter} = require('events')

const {ConnectionConfig, joinHostPort} = require('./config')
const {WriterNSQDConnection} = require('./nsqdconnection')

/**
 *  Publish messages to nsqds.
 *
 *  Usage:
 *    const writer = new Writer('127.0.0.1', 4150);
 *    writer.connect();
 *
 *    writer.on(Writer.READY, () => {
 *      // Send a single message
 *      writer.publish('sample_topic', 'one');
 *      // Send multiple messages
 *      writer.publish('sample_topic', ['two', 'three']);
 *    });
 *
 *    writer.on(Writer.CLOSED, () => {
 *      console.log('Writer closed');
 *    });
 */
class Writer extends EventEmitter {
  // Writer events
  static get READY() {
    return 'ready'
  }
  static get CLOSED() {
    return 'closed'
  }
  static get ERROR() {
    return 'error'
  }

  /**
   * Instantiates a new Writer.
   *
   * @constructor
   * @param {String} nsqdHost
   * @param {String} nsqdPort
   * @param {Object} options
   */
  constructor(nsqdHost, nsqdPort, options) {
    super()

    this.nsqdHost = nsqdHost
    this.nsqdPort = nsqdPort

    // Handy in the event that there are tons of publish calls
    // while the Writer is connecting.
    this.setMaxListeners(10000)

    this.debug = debug(`nsqjs:writer:${joinHostPort(this.nsqdHost, this.nsqdPort)}`)
    this.config = new ConnectionConfig(options)
    this.config.validate()
    this.ready = false

    this.debug('Configuration')
    this.debug(this.config)
  }

  /**
   * Connect establishes a new nsqd writer connection.
   */
  connect() {
    this.conn = new WriterNSQDConnection(
      this.nsqdHost,
      this.nsqdPort,
      this.config
    )

    this.debug('connect')
    this.conn.connect()

    this.conn.on(WriterNSQDConnection.READY, () => {
      this.debug('ready')
      this.ready = true
      this.emit(Writer.READY)
    })

    this.conn.on(WriterNSQDConnection.CLOSED, () => {
      this.debug('closed')
      this.ready = false
      this.emit(Writer.CLOSED)
    })

    this.conn.on(WriterNSQDConnection.ERROR, (err) => {
      this.debug('error', err)
      this.ready = false
      this.emit(Writer.ERROR, err)
    })

    this.conn.on(WriterNSQDConnection.CONNECTION_ERROR, (err) => {
      this.debug('error', err)
      this.ready = false
      this.emit(Writer.ERROR, err)
    })
  }

  /**
   * Publish a message or a list of messages to the connected nsqd. The contents
   * of the messages should either be strings or buffers with the payload encoded.

   * @param {String} topic
   * @param {String|Buffer|Object|Array} msgs - A string, a buffer, a
   *   JSON serializable object, or a list of string / buffers /
   *   JSON serializable objects.
   * @param {Function} callback
   * @return {undefined}
   */
  publish(topic, msgs, callback) {
    let err = this._checkStateValidity()
    err = err || this._checkMsgsValidity(msgs)

    if (err) {
      return this._throwOrCallback(err, callback)
    }

    // Call publish again once the Writer is ready.
    if (!this.ready) {
      const onReady = (err) => {
        if (err) return callback(err)
        this.publish(topic, msgs, callback)
      }
      this._callwhenReady(onReady)
    }

    if (!_.isArray(msgs)) {
      msgs = [msgs]
    }

    // Automatically serialize as JSON if the message isn't a String or a Buffer
    msgs = msgs.map(this._serializeMsg)

    return this.conn.produceMessages(topic, msgs, undefined, callback)
  }

  /**
   * Publish a message to the connected nsqd with delay. The contents
   * of the messages should either be strings or buffers with the payload encoded.

   * @param {String} topic
   * @param {String|Buffer|Object} msg - A string, a buffer, a
   *   JSON serializable object, or a list of string / buffers /
   *   JSON serializable objects.
   * @param {Number} timeMs - defer time
   * @param {Function} callback
   * @return {undefined}
   */
  deferPublish(topic, msg, timeMs, callback) {
    let err = this._checkStateValidity()
    err = err || this._checkMsgsValidity(msg)
    err = err || this._checkTimeMsValidity(timeMs)

    if (err) {
      return this._throwOrCallback(err, callback)
    }

    // Call publish again once the Writer is ready.
    if (!this.ready) {
      const onReady = (err) => {
        if (err) return callback(err)
        this.deferPublish(topic, msg, timeMs, callback)
      }
      this._callwhenReady(onReady)
    }

    return this.conn.produceMessages(topic, msg, timeMs, callback)
  }

  /**
   * Close the writer connection.
   * @return {undefined}
   */
  close() {
    return this.conn.close()
  }

  _serializeMsg(msg) {
    if (_.isString(msg) || Buffer.isBuffer(msg)) {
      return msg
    }
    return JSON.stringify(msg)
  }

  _checkStateValidity() {
    let connState = ''

    if (this.conn && this.conn.statemachine) {
      connState = this.conn.statemachine.current_state_name
    }

    if (!this.conn || ['CLOSED', 'ERROR'].includes(connState)) {
      return new Error('No active Writer connection to send messages')
    }
  }

  _checkMsgsValidity(msgs) {
    // maybe when an array check every message to not be empty
    if (!msgs || _.isEmpty(msgs)) {
      return new Error('Attempting to publish an empty message')
    }
  }

  _checkTimeMsValidity(timeMs) {
    return _.isNumber(timeMs) && timeMs > 0
      ? undefined
      : new Error('The Delay must be a (positiv) number')
  }

  _throwOrCallback(err, callback) {
    if (callback) {
      return callback(err)
    }
    throw err
  }

  _callwhenReady(fn) {
    const ready = () => {
      remove()
      fn()
    }

    const failed = (err) => {
      if (!err) {
        err = new Error('Connection closed!')
      }
      remove()
      fn(err)
    }

    const remove = () => {
      this.removeListener(Writer.READY, ready)
      this.removeListener(Writer.ERROR, failed)
      this.removeListener(Writer.CLOSED, failed)
    }

    this.on(Writer.READY, ready)
    this.on(Writer.ERROR, failed)
    this.on(Writer.CLOSED, failed)
  }
}

module.exports = Writer
