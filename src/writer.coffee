{EventEmitter} = require 'events'

_ = require 'underscore'
{WriterNSQDConnection} = require './nsqdconnection'

###
Publish messages to nsqds.

Usage:

w = new Writer '127.0.0.1', 4150
w.connect()

w.on Writer.READY, ->
  # Send a single message
  w.publish 'sample_topic', 'one'
  # Send multiple messages
  w.publish 'sample_topic', ['two', 'three']
w.on Writer.CLOSED, ->
  console.log 'Writer closed'
###
class Writer extends EventEmitter

  # Writer events
  @READY: 'ready'
  @CLOSED: 'closed'
  @ERROR: 'error'

  constructor: (@nsqdHost, @nsqdPort, options) ->
    defaults =
      tls: false
      tlsVerification: true
      deflate: false
      deflateLevel: 6
      snappy: false

    params = _.extend {}, defaults, options

    unless _.isBoolean params.tls
      throw new Error 'tls needs to be true or false'
    unless _.isBoolean params.tlsVerification
      throw new Error 'tlsVerification needs to be true or false'
    unless _.isBoolean params.snappy
      throw new Error 'snappy needs to be true or false'
    unless _.isBoolean params.deflate
      throw new Error 'deflate needs to be true or false'
    unless _.isNumber params.deflateLevel
      throw new Error 'deflateLevel needs to be a Number'

    _.extend @, params

  connect: ->
    @conn = new WriterNSQDConnection @nsqdHost, @nsqdPort, 30, @tls,
      @tlsVerification, @deflate, @deflateLevel, @snappy
    @conn.connect()

    @conn.on WriterNSQDConnection.READY, =>
      @emit Writer.READY

    @conn.on WriterNSQDConnection.CLOSED, =>
      @emit Writer.CLOSED

    @conn.on WriterNSQDConnection.ERROR, (err) =>
      @emit Writer.ERROR, err

    @conn.on WriterNSQDConnection.CONNECTION_ERROR, (err) =>
      @emit Writer.ERROR, err

  ###
  Publish a message or a list of messages to the connected nsqd. The contents
  of the messages should either be strings or buffers with the payload encoded.

  Arguments:
    topic: A valid nsqd topic.
    msgs: A string, a buffer, or a list of string/buffers.
  ###
  publish: (topic, msgs, callback) ->
    return callback(new Error('No active Writer connection to send messages')) unless @conn
    msgs = [msgs] unless _.isArray msgs
    @conn.produceMessages topic, msgs, callback

  close: ->
    @conn.destroy()

module.exports = Writer
