assert = require 'assert'
{EventEmitter} = require 'events'

_ = require 'underscore'
{nodes} = require './lookupd'
{NSQDConnectionWriter} = require './nsqdconnection'

###
Write messages to nsqds. Allows the client to specify a particular nsqd or use
an arbitrary nsqd instance discovered the provided lookupds. Once connected,
this writer will only publish messages to one nsqd.

Usage:

w = new Writer {lookupdHTTPAddresses: ['127.0.0.1:4161', '127.0.0.1:5161']}
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

  constructor: (options) ->
    defaults =
      nsqdTCPAddress: null
      lookupdHTTPAddresses: []

    params = _.extend {}, defaults, options

    # Returns a compacted list given a list, string, integer, or object.
    makeList = (list) ->
      list = [list] unless _.isArray list
      (entry for entry in list when entry?)

    params.lookupdHTTPAddresses = makeList params.lookupdHTTPAddresses
    _.extend @, params

    @conn = null

  ###
  Query lookupds for nsqd nodes and return an arbitrary nsqd node.

  Arguments:
    callback: signature `(node) ->` where `node` is an object with a nsqd
    details.
  ###
  chooseNSQD: (callback) ->
    if @nsqdTCPAddress
      callback @nsqdTCPAddress
      return

    if _.isEmpty @lookupdHTTPAddresses
      callback null
      return

    nodes @lookupdHTTPAddresses, (err, nodes) ->
      if err
        callback null
      else
        node = _.sample nodes
        callback "#{node.broadcast_address}:#{node.tcp_port}"

  connect: ->
    @chooseNSQD (address) =>
      assert address, 'No nsqd address provided or nsqd discovered via lookupd'
      [host, port] = address.split ':'

      @conn = new NSQDConnectionWriter host, port, 30
      @conn.connect()

      @conn.on NSQDConnectionWriter.READY, =>
        @emit Writer.READY

      @conn.on NSQDConnectionWriter.CLOSED, =>
        @emit Writer.CLOSED

  ###
  Publish a message or a list of messages to the connected nsqd. The contents
  of the messages should either be strings or buffers with the payload encoded.

  Arguments:
    topic: A valid nsqd topic.
    msgs: A string, a buffer, or a list of string/buffers.
  ###
  publish: (topic, msgs) ->
    assert not _.isNull @conn, "No active Writer connection to send messages."
    msgs = [msgs] if not _.isArray msgs
    @conn.produceMessages topic, msgs

module.exports = Writer
