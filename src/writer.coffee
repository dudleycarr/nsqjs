Debug = require 'debug'
{EventEmitter} = require 'events'

_ = require 'underscore'
{ConnectionConfig} = require './config'
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
    @debug = Debug "nsqjs:writer:#{@nsqdHost}/#{@nsqdPort}"
    @config = new ConnectionConfig options
    @config.validate()

    @debug 'Configuration'
    @debug @config

  connect: ->
    @conn = new WriterNSQDConnection @nsqdHost, @nsqdPort, @config
    @debug 'connect'
    @conn.connect()

    @conn.on WriterNSQDConnection.READY, =>
      @debug 'ready'
      @emit Writer.READY

    @conn.on WriterNSQDConnection.CLOSED, =>
      @debug 'closed'
      @emit Writer.CLOSED

    @conn.on WriterNSQDConnection.ERROR, (err) =>
      @debug 'error', err
      @emit Writer.ERROR, err

    @conn.on WriterNSQDConnection.CONNECTION_ERROR, (err) =>
      @debug 'error', err
      @emit Writer.ERROR, err

  ###
  Publish a message or a list of messages to the connected nsqd. The contents
  of the messages should either be strings or buffers with the payload encoded.

  Arguments:
    topic: A valid nsqd topic.
    msgs: A string, a buffer, a JSON serializable object, or
      a list of string / buffers / JSON serializable objects.
  ###
  publish: (topic, msgs, callback) ->
    if not @conn or @conn.connectionState and @conn.connectionState().current_state_name is 'CLOSED'
      err = new Error 'No active Writer connection to send messages'

    if not msgs or _.isEmpty msgs
      err = new Error 'Attempting to publish an empty message'

    if err
      return callback err if callback
      throw err

    msgs = [msgs] unless _.isArray msgs

    # Automatically serialize as JSON if the message isn't a String or a Buffer
    msgs = for msg in msgs
      if _.isString(msg) or Buffer.isBuffer(msg)
        msg
      else
        JSON.stringify msg

    @conn.produceMessages topic, msgs, callback

  close: ->
    @conn.destroy()

module.exports = Writer
