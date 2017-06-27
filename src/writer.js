const _ = require('lodash')
const ConnectionConfig = require('./config').ConnectionConfig
const debug = require('debug')
const EventEmitter = require('events')
const WriterNSQDConnection = require('./nsqdconnection').WriterNSQDConnection

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
  /**
   * Instantiates a new Writer.
   *
   * @constructor
   * @param {String} nsqdHost
   * @param {String} nsqdPort
   * @param {Object} options
   */
  constructor (nsqdHost, nsqdPort, options) {
    super()

    this.nsqdHost = nsqdHost
    this.nsqdPort = nsqdPort

    // Handy in the event that there are tons of publish calls
    // while the Writer is connecting.
    this.setMaxListeners(10000)

    this.debug = debug(`nsqjs:writer:${this.nsqdHost}/${this.nsqdPort}`)
    this.config = new ConnectionConfig(options)
    this.config.validate()
    this.ready = false

    this.debug('Configuration')
    this.debug(this.config)
  }

  /**
   * Connect establishes a new nsqd writer connection.
   */
  connect () {
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

    this.conn.on(WriterNSQDConnection.ERROR, err => {
      this.debug('error', err)
      this.ready = false
      this.emit(Writer.ERROR, err)
    })

    this.conn.on(WriterNSQDConnection.CONNECTION_ERROR, err => {
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
  publish (topic, msgs, callback) {
    let err
    let connState = ''

    if (this.conn && this.conn.statemachine) {
      connState = this.conn.statemachine.current_state_name
    }

    if (!this.conn || ['CLOSED', 'ERROR'].includes(connState)) {
      err = new Error('No active Writer connection to send messages')
    }

    if (!msgs || _.isEmpty(msgs)) {
      err = new Error('Attempting to publish an empty message')
    }

    if (err) {
      if (callback) {
        return callback(err)
      }
      throw err
    }

    // Call publish again once the Writer is ready.
    if (!this.ready) {
      const ready = () => {
        remove()
        this.publish(topic, msgs, callback)
      }

      const failed = function (err) {
        if (!err) {
          err = new Error('Connection closed!')
        }
        remove()
        callback(err)
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

    if (!_.isArray(msgs)) {
      msgs = [msgs]
    }

    // Automatically serialize as JSON if the message isn't a String or a Buffer
    msgs = msgs.map(msg => {
      if (_.isString(msg) || Buffer.isBuffer(msg)) {
        return msg
      }
      return JSON.stringify(msg)
    })

    return this.conn.produceMessages(topic, msgs, callback)
  }

  /**
   * Close the writer connection.
   * @return {undefined}
   */
  close () {
    return this.conn.destroy()
  }
}

Writer.READY = 'ready'
Writer.CLOSED = 'closed'
Writer.ERROR = 'error'

module.exports = Writer
