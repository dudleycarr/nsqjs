import Debug from 'debug'
import { EventEmitter } from 'events'

import _ from 'underscore'
import { ConnectionConfig } from './config'
import { WriterNSQDConnection } from './nsqdconnection'

/*
Publish messages to nsqds.

Usage:

w = new Writer '127.0.0.1', 4150
w.connect()

w.on Writer.READY, ->
  * Send a single message
  w.publish 'sample_topic', 'one'
  * Send multiple messages
  w.publish 'sample_topic', ['two', 'three']
w.on Writer.CLOSED, ->
  console.log 'Writer closed'
*/
class Writer extends EventEmitter {
  static initClass () {
    // Writer events
    this.READY = 'ready'
    this.CLOSED = 'closed'
    this.ERROR = 'error'
  }

  constructor (nsqdHost, nsqdPort, options) {
    super(...arguments)

    this.nsqdHost = nsqdHost
    this.nsqdPort = nsqdPort

    // Handy in the event that there are tons of publish calls
    // while the Writer is connecting.
    this.setMaxListeners(10000)

    this.debug = Debug(`nsqjs:writer:${this.nsqdHost}/${this.nsqdPort}`)
    this.config = new ConnectionConfig(options)
    this.config.validate()
    this.ready = false

    this.debug('Configuration')
    this.debug(this.config)
  }

  connect () {
    this.conn = new WriterNSQDConnection(this.nsqdHost, this.nsqdPort, this.config)
    this.debug('connect')
    this.conn.connect()

    this.conn.on(WriterNSQDConnection.READY, () => {
      this.debug('ready')
      this.ready = true
      return this.emit(Writer.READY)
    })

    this.conn.on(WriterNSQDConnection.CLOSED, () => {
      this.debug('closed')
      this.ready = false
      return this.emit(Writer.CLOSED)
    })

    this.conn.on(WriterNSQDConnection.ERROR, (err) => {
      this.debug('error', err)
      this.ready = false
      return this.emit(Writer.ERROR, err)
    })

    this.conn.on(WriterNSQDConnection.CONNECTION_ERROR, (err) => {
      this.debug('error', err)
      this.ready = false
      return this.emit(Writer.ERROR, err)
    })
  }

  /*
  Publish a message or a list of messages to the connected nsqd. The contents
  of the messages should either be strings or buffers with the payload encoded.

  Arguments:
    topic: A valid nsqd topic.
    msgs: A string, a buffer, a JSON serializable object, or
      a list of string / buffers / JSON serializable objects.
  */
  publish (topic, msgs, callback) {
    let err
    const connState = __guard__(this.conn != null ? this.conn.statemachine : undefined, x => x.current_state_name)

    if (!this.conn || ['CLOSED', 'ERROR'].includes(connState)) {
      err = new Error('No active Writer connection to send messages')
    }

    if (!msgs || _.isEmpty(msgs)) {
      err = new Error('Attempting to publish an empty message')
    }

    if (err) {
      if (callback) { return callback(err) }
      throw err
    }

    // Call publish again once the Writer is ready.
    if (!this.ready) {
      const remove = () => {
        this.removeListener(Writer.READY, ready)
        this.removeListener(Writer.ERROR, failed)
        this.removeListener(Writer.CLOSED, failed)
      }

      const ready = () => {
        remove()
        this.publish(topic, msgs, callback)
      }

      const failed = function (err) {
        if (!err) { err = new Error('Connection closed!') }
        remove()
        callback(err)
      }

      this.on(Writer.READY, ready)
      this.on(Writer.ERROR, failed)
      this.on(Writer.CLOSED, failed)
    }

    if (!_.isArray(msgs)) { msgs = [msgs] }

    // Automatically serialize as JSON if the message isn't a String or a Buffer
    msgs = Array.from(msgs).map(msg =>
      _.isString(msg) || Buffer.isBuffer(msg)
        ? msg
      : JSON.stringify(msg))

    return this.conn.produceMessages(topic, msgs, callback)
  }

  close () {
    return this.conn.destroy()
  }
}
Writer.initClass()

export default Writer

function __guard__ (value, transform) {
  return (typeof value !== 'undefined' && value !== null) ? transform(value) : undefined
}
