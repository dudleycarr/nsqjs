_ = require 'underscore'
request = require 'request'
{EventEmitter} = require 'events'

{ReaderConfig} = require './config'
{NSQDConnection} = require './nsqdconnection'
{ReaderRdy} = require './readerrdy'
RoundRobinList = require './roundrobinlist'
lookup = require './lookupd'


class Reader extends EventEmitter

  # Responsibilities
  # 1. Responsible for periodically querying the nsqlookupds
  # 2. Connects and subscribes to discovered/configured nsqd
  # 3. Consumes messages and triggers MESSAGE events
  # 4. Starts the ReaderRdy instance
  # 5. Hands nsqd connections to the ReaderRdy instance
  # 6. Stores Reader configurations

  # Reader events
  @ERROR: 'error'
  @MESSAGE: 'message'
  @DISCARD: 'discard'
  @NSQD_CONNECTED: 'nsqd_connected'
  @NSQD_CLOSED: 'nsqd_closed'

  constructor: (@topic, @channel, options) ->
    @config = new ReaderConfig options
    @config.validate()

    @roundrobinLookupd = new RoundRobinList @config.lookupdHTTPAddresses
    @readerRdy = new ReaderRdy @config.maxInFlight, @config.maxBackoffDuration
    @connectIntervalId = null
    @connectionIds = []

  connect: ->
    interval = @config.lookupdPollInterval * 1000
    delay = Math.random() * @config.lookupdPollJitter * interval

    # Connect to provided nsqds.
    if @config.nsqdTCPAddresses.length
      directConnect = =>
        # Don't establish new connections while the Reader is paused.
        return if @isPaused()

        if @connectionIds.length < @config.nsqdTCPAddresses.length
          for addr in @config.nsqdTCPAddresses
            [address, port] = addr.split ':'
            @connectToNSQD address, Number(port)

      delayedStart = =>
        @connectIntervalId = setInterval directConnect.bind(this), interval

      # Connect immediately.
      directConnect()
      # Start interval for connecting after delay.
      setTimeout delayedStart, delay

    # Connect to nsqds discovered via lookupd
    else
      delayedStart = =>
        @connectIntervalId = setInterval @queryLookupd.bind(this), interval

      # Connect immediately.
      @queryLookupd()
      # Start interval for querying lookupd after delay.
      setTimeout delayedStart, delay

  # Caution: in-flight messages will not get a chance to finish.
  close: ->
    clearInterval @connectIntervalId
    @readerRdy.close()

  pause: ->
    @readerRdy.pause()

  unpause: ->
    @readerRdy.unpause()

  isPaused: ->
    @readerRdy.paused

  queryLookupd: ->
    # Don't establish new connections while the Reader is paused.
    return if @isPaused()

    # Trigger a query of the configured ``lookupdHTTPAddresses``
    endpoint = @roundrobinLookupd.next()
    lookup endpoint, @topic, (err, nodes) =>
      @connectToNSQD n.broadcast_address, n.tcp_port for n in nodes unless err

  connectToNSQD: (host, port) ->
    conn = new NSQDConnection host, port, @topic, @channel, @config

    # Ensure a connection doesn't already exist to this nsqd instance.
    return if @connectionIds.indexOf(conn.id()) isnt -1
    @connectionIds.push conn.id()

    @registerConnectionListeners conn
    @readerRdy.addConnection conn

    conn.connect()

  registerConnectionListeners: (conn) ->
    conn.on NSQDConnection.CONNECTED, =>
      @emit Reader.NSQD_CONNECTED, conn.nsqdHost, conn.nsqdPort

    conn.on NSQDConnection.ERROR, (err) =>
      @emit Reader.ERROR, err

    conn.on NSQDConnection.CONNECTION_ERROR, (err) =>
      @emit Reader.ERROR, err

    # On close, remove the connection id from this reader.
    conn.on NSQDConnection.CLOSED, =>
      index = @connectionIds.indexOf conn.id()
      return if index is -1
      @connectionIds.splice index, 1

      @emit Reader.NSQD_CLOSED, conn.nsqdHost, conn.nsqdPort

    # On message, send either a message or discard event depending on the
    # number of attempts.
    conn.on NSQDConnection.MESSAGE, (message) =>
      @handleMessage message

  handleMessage: (message) ->
    # Give the internal event listeners a chance at the events before clients
    # of the Reader.
    process.nextTick =>
      # We discard only when max attempts is specified and a discard handler
      # is present.
      autoFinishMessage = 0 < @config.maxAttempts <= message.attempts

      if autoFinishMessage and @listeners(Reader.DISCARD).length > 0
        @emit Reader.DISCARD, message
      else
        @emit Reader.MESSAGE, message

      message.finish() if autoFinishMessage

module.exports = Reader
