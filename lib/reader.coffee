_ = require 'underscore'
assert = require 'assert'
request = require 'request'
{EventEmitter} = require 'events'

{NSQDConnection} = require './nsqdconnection'
{ReaderRdy} = require './readerrdy'
RoundRobinList = require './roundrobinlist'
{lookup} = require './lookupd'


class Reader extends EventEmitter

  # Responsibilities
  # 1. Responsible for periodically querying the nsqlookupds
  # 2. Connects and subscribes to discovered/configured nsqd
  # 3. Consumes messages and triggers MESSAGE events
  # 4. Starts the ReaderRdy instance
  # 5. Hands nsqd connections to the ReaderRdy instance
  # 6. Stores Reader configurations

  # Reader events
  @MESSAGE: 'message'
  @DISCARD: 'discard'

  constructor: (@topic, @channel, options) ->
    defaults =
      name: null
      maxInFlight: 1
      heartbeatInterval: 30
      maxBackoffDuration: 128
      maxAttempts: 5
      requeueDelay: 90
      nsqdTCPAddresses: []
      lookupdHTTPAddresses: []
      lookupdPollInterval: 60
      lookupdPollJitter: 0.3

    params = _.extend {}, defaults, options

    assert _.isString(topic) and topic.length > 0, 'Invalid topic'
    assert _.isString(channel) and channel.length > 0, 'Invalid channel'
    assert _.isNumber(params.maxInFlight) and params.maxInFlight > 0,
      'maxInFlight needs to be an integer greater than 0'
    assert _.isNumber(params.heartbeatInterval) and
      params.heartbeatInterval >= 1,
      'heartbeatInterval needs to be an integer greater than 1'
    assert _.isNumber(params.maxBackoffDuration) and
      params.maxBackoffDuration > 0,
      'maxBackoffDuration needs to be a number greater than 1'
    assert params.name is null or _.isString params.name,
      'name needs to be unspecified or a string'
    assert _.isNumber params.lookupdPollInterval,
      'lookupdPollInterval needs to be a number'
    assert 0 <= params.lookupdPollInterval,
      'lookupdPollInterval needs to be greater than 0'
    assert _.isNumber params.lookupdPollJitter,
      'lookupdPollJitter needs to be a number'
    assert 0 <= params.lookupdPollJitter <= 1,
      'lookupdPollJitter needs to be between 0 and 1'

    # Returns a compacted list given a list, string, integer, or object.
    makeList = (list) ->
      list = [list] unless _.isArray list
      (entry for entry in list when entry?)

    params.nsqdTCPAddresses = makeList params.nsqdTCPAddresses
    params.lookupdHTTPAddresses = makeList params.lookupdHTTPAddresses

    anyNotEmpty = (lst...) -> _.some (e for e in lst when not _.isEmpty e)
    assert anyNotEmpty(params.nsqdTCPAddresses, params.lookupdHTTPAddresses),
      'Need to specify either nsqdTCPAddresses or lookupdHTTPAddresses option.'

    params.name = params.name or "#{topic}:#{channel}"
    params.requeueDelay = params.requeueDelay
    params.heartbeatInterval = params.heartbeatInterval

    _.extend @, params

    @roundrobinLookupd = new RoundRobinList @lookupdHTTPAddresses
    @readerRdy = new ReaderRdy @maxInFlight, @maxBackoffDuration
    @lookupdId = null
    @connectionIds = []

  connect: ->
    unless _.isEmpty @nsqdTCPAddresses
      for addr in @nsqdTCPAddresses
        [address, port] = addr.split ':'
        @connectToNSQD address, Number(port)

    return unless @lookupdHTTPAddresses

    # Force the first lookup now.
    @queryLookupd()

    interval = @lookupdPollInterval * 1000
    delayedStart = =>
      @lookupdId = setTimeout @queryLookupd.bind(this), interval

    delay = Math.random() * @lookupdPollJitter * interval
    setTimeout delayedStart, delay


  queryLookupd: ->
    # Trigger a query of the configured ``lookupdHTTPAddresses``
    endpoint = @roundrobinLookupd.next()
    lookup endpoint, @topic, (err, nodes) =>
      unless err
        for node in nodes
          @connectToNSQD node.broadcast_address, node.tcp_port

  connectToNSQD: (host, port) ->
    assert _.isString host
    assert _.isNumber port

    connectionId = "#{host}:#{port}"
    return if @connectionIds.indexOf(connectionId) isnt -1
    @connectionIds.push connectionId

    conn = new NSQDConnection host, port, @topic, @channel, @requeueDelay,
      @heartbeatInterval

    # On close, remove the connection id from this reader.
    conn.on NSQDConnection.CLOSED, =>
      # TODO(dudley): Update when switched to lo-dash
      index = @connectionIds.indexOf connectionId
      return if index is -1
      @connectionIds.splice index, 1

    # On message, send either a message or discard event depending on the
    # number of attempts.
    conn.on NSQDConnection.MESSAGE, (message) =>
      # Give the internal event listeners a chance at the events before clients
      # of the Reader.
      process.nextTick =>
        if message.attempts < @maxAttempts
          @emit Reader.MESSAGE, message
        else
          @emit Reader.DISCARD, message

    @readerRdy.addConnection conn
    conn.connect()


module.exports = Reader
