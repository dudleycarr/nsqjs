_ = require 'underscore'
Debug = require 'debug'
{EventEmitter} = require 'events'

BackoffTimer = require './backofftimer'
NodeState = require 'node-state'
{NSQDConnection} = require './nsqdconnection'
RoundRobinList = require './roundrobinlist'

###
Maintains the RDY and in-flight counts for a nsqd connection. ConnectionRdy
ensures that the RDY count will not exceed the max set for this connection.
The max for the connection can be adjusted at any time.

Usage:

connRdy = ConnectionRdy conn
connRdy.setConnectionRdyMax 10

conn.on 'message', ->
  # On a successful message, bump up the RDY count for this connection.
  connRdy.raise 'bump'
conn.on 'requeue', ->
  # We're backing off when we encounter a requeue. Wait 5 seconds to try
  # again.
  connRdy.raise 'backoff'
  setTimeout (-> connRdy.raise 'bump'), 5000
###
class ConnectionRdy extends EventEmitter
  # Events emitted by ConnectionRdy
  @READY: 'ready'
  @STATE_CHANGE: 'statechange'

  constructor: (@conn) ->
    readerId = "#{@conn.topic}/#{@conn.channel}"
    connId = "#{@conn.id().replace ':', '/'}"
    @debug = Debug "nsqjs:reader:#{readerId}:rdy:conn:#{connId}"

    @maxConnRdy = 0      # The absolutely maximum the RDY count can be per conn.
    @inFlight = 0        # The num. messages currently in-flight for this conn.
    @lastRdySent = 0     # The RDY value last sent to the server.
    @availableRdy = 0    # The RDY count remaining on the server for this conn.
    @statemachine = new ConnectionRdyState this

    @conn.on NSQDConnection.ERROR, (err) =>
      @log err
    @conn.on NSQDConnection.MESSAGE, =>
      clearTimeout @idleId if @idleId?
      @idleId = null
      @inFlight += 1
      @availableRdy -= 1
    @conn.on NSQDConnection.FINISHED, =>
      @inFlight -= 1
    @conn.on NSQDConnection.REQUEUED, =>
      @inFlight -= 1
    @conn.on NSQDConnection.READY, =>
      @start()

  close: ->
    @conn.destroy()

  name: ->
    String @conn.conn.localPort

  start: ->
    @statemachine.start()
    @emit ConnectionRdy.READY

  setConnectionRdyMax: (maxConnRdy) ->
    @log "setConnectionRdyMax #{maxConnRdy}"
    # The RDY count for this connection should not exceed the max RDY count
    # configured for this nsqd connection.
    @maxConnRdy = Math.min maxConnRdy, @conn.maxRdyCount
    @statemachine.raise 'adjustMax'

  bump: ->
    @statemachine.raise 'bump'

  backoff: ->
    @statemachine.raise 'backoff'

  isStarved: ->
    throw new Error 'isStarved check is failing' unless @inFlight <= @maxConnRdy
    @inFlight is @lastRdySent

  setRdy: (rdyCount) ->
    @log "RDY #{rdyCount}"
    if rdyCount < 0 or rdyCount > @maxConnRdy
      return

    @conn.setRdy rdyCount
    @availableRdy = @lastRdySent = rdyCount

  log: (message) ->
    @debug message if message


class ConnectionRdyState extends NodeState

  constructor: (@connRdy) ->
    super
      autostart: false,
      initial_state: 'INIT'
      sync_goto: true

  log: (message) ->
    @connRdy.debug @current_state_name
    @connRdy.debug message if message

  states:
    INIT:
      # RDY is implicitly zero
      bump: ->
        @goto 'MAX' if @connRdy.maxConnRdy > 0
      backoff: -> # No-op
      adjustMax: -> # No-op

    BACKOFF:
      Enter: ->
        @connRdy.setRdy 0
      bump: ->
        @goto 'ONE' if @connRdy.maxConnRdy > 0
      backoff: -> # No-op
      adjustMax: -> # No-op

    ONE:
      Enter: ->
        @connRdy.setRdy 1
      bump: ->
        @goto 'MAX'
      backoff: ->
        @goto 'BACKOFF'
      adjustMax: -> # No-op

    MAX:
      Enter: ->
        @raise 'bump'
      bump: ->
        # No need to keep set the RDY count for versions of NSQD after 0.3.0.
        version = @connRdy.conn?.nsqdVersion
        if not version or version.split('.') < [0, 3, 0]
          if @connRdy.availableRdy <= @connRdy.lastRdySent * 0.25
            @connRdy.setRdy @connRdy.maxConnRdy
      backoff: ->
        @goto 'BACKOFF'
      adjustMax: ->
        @log "adjustMax RDY #{@connRdy.maxConnRdy}"
        @connRdy.setRdy @connRdy.maxConnRdy

  transitions:
    '*':
      '*': (data, callback) ->
        @log()
        callback data
        @connRdy.emit ConnectionRdy.STATE_CHANGE


###
Usage:

backoffTime = 90
heartbeat = 30

[topic, channel] = ['sample', 'default']
[host1, port1] = ['127.0.0.1', '4150']
c1 = new NSQDConnection host1, port1, topic, channel, backoffTime, heartbeat

readerRdy = new ReaderRdy 1, 128
readerRdy.addConnection c1

message = (msg) ->
  console.log "Callback [message]: #{msg.attempts}, #{msg.body.toString()}"
  if msg.attempts >= 5
    msg.finish()
    return

  if msg.body.toString() is 'requeue'
    msg.requeue()
  else
    msg.finish()

discard = (msg) ->
  console.log "Giving up on this message: #{msg.id}"
  msg.finish()

c1.on NSQDConnection.MESSAGE, message
c1.connect()
###

READER_COUNT = 0

class ReaderRdy extends NodeState

  # Class method
  @getId: ->
    READER_COUNT += 1
    READER_COUNT - 1

  ###
  Parameters:
  - maxInFlight        : Maximum number of messages in-flight across all
                           connections.
  - maxBackoffDuration : The longest amount of time (secs) for a backoff event.
  - readerId           : The descriptive id for the Reader
  - lowRdyTimeout      : Time (secs) to rebalance RDY count among connections
                           during low RDY conditions.
  ###
  constructor: (@maxInFlight, @maxBackoffDuration, @readerId,
    @lowRdyTimeout=1.5) ->
    @debug = Debug "nsqjs:reader:#{@readerId}:rdy"

    super
      autostart: true,
      initial_state: 'ZERO'
      sync_goto: true

    @id = ReaderRdy.getId()
    @backoffTimer = new BackoffTimer 0, @maxBackoffDuration
    @backoffId = null
    @balanceId = null
    @connections = []
    @roundRobinConnections = new RoundRobinList []

  close: ->
    clearTimeout @backoffId
    clearTimeout @balanceId
    conn.close() for conn in @connections

  pause: ->
    @raise 'pause'

  unpause: ->
    @raise 'unpause'

  isPaused: ->
    @current_state_name is 'PAUSE'

  log: (message) ->
    @debug @current_state_name
    @debug message if message

  isStarved: ->
    return false if _.isEmpty @connections
    not _.isEmpty (c for c in @connections if c.isStarved())

  createConnectionRdy: (conn) ->
    new ConnectionRdy conn

  isLowRdy: ->
    @maxInFlight < @connections.length

  onMessageSuccess: (connectionRdy) ->
    unless @isPaused()
      if @isLowRdy()
        # Balance the RDY count amoung existing connections given the low RDY
        # condition.
        @balance()
      else
        # Restore RDY count for connection to the connection max.
        connectionRdy.bump()

  addConnection: (conn) ->
    connectionRdy = @createConnectionRdy conn

    conn.on NSQDConnection.CLOSED, =>
      @removeConnection connectionRdy
      @balance()

    conn.on NSQDConnection.FINISHED, =>
      @raise 'success', connectionRdy

    conn.on NSQDConnection.REQUEUED, =>
      # Since there isn't a guaranteed order for the REQUEUED and BACKOFF
      # events, handle the case when we handle BACKOFF and then REQUEUED.
      if @current_state_name isnt 'BACKOFF' and not @isPaused()
        connectionRdy.bump()

    conn.on NSQDConnection.BACKOFF, =>
      @raise 'backoff'

    connectionRdy.on ConnectionRdy.READY, =>
      @connections.push connectionRdy
      @roundRobinConnections.add connectionRdy

      @balance()
      if @current_state_name is 'ZERO'
        @goto 'MAX'
      else if @current_state_name in ['TRY_ONE', 'MAX']
        connectionRdy.bump()

  removeConnection: (conn) ->
    @connections.splice @connections.indexOf(conn), 1
    @roundRobinConnections.remove conn

    if @connections.length is 0
      @goto 'ZERO'

  bump: ->
    for conn in @connections
      conn.bump()

  try: ->
    @balance()

  backoff: ->
    conn.backoff() for conn in @connections
    clearTimeout @backoffId if @backoffId

    onTimeout = =>
      @log 'Backoff done'
      @raise 'try'

    # Convert from the BigNumber representation to Number.
    delay = new Number(@backoffTimer.getInterval().valueOf()) * 1000
    @backoffId = setTimeout onTimeout, delay
    @log "Backoff for #{delay}"

  inFlight: ->
    add = (previous, conn) ->
      previous + conn.inFlight
    @connections.reduce add, 0

  ###
  Evenly or fairly distributes RDY count based on the maxInFlight across
  all nsqd connections.
  ###
  balance: ->
    ###
    In the perverse situation where there are more connections than max in
    flight, we do the following:

    There is a sliding window where each of the connections gets a RDY count
    of 1. When the connection has processed it's single message, then the RDY
    count is distributed to the next waiting connection. If the connection
    does nothing with it's RDY count, then it should timeout and give it's
    RDY count to another connection.
    ###

    @log 'balance'

    if @balanceId?
      clearTimeout @balanceId
      @balanceId = null

    max = switch @current_state_name
      when 'TRY_ONE' then 1
      when 'PAUSE' then 0
      else @maxInFlight

    perConnectionMax = Math.floor max / @connections.length

    # Low RDY and try conditions
    if perConnectionMax is 0
      # Backoff on all connections. In-flight messages from connections
      # will still be processed.
      for c in @connections
        c.backoff()

      # Distribute available RDY count to the connections next in line.
      for c in @roundRobinConnections.next max - @inFlight()
        c.setConnectionRdyMax 1
        c.bump()

      # Rebalance periodically. Needed when no messages are received.
      @balanceId = setTimeout @balance.bind(this), @lowRdyTimeout * 1000

    else
      rdyRemainder = @maxInFlight % @connectionsLength
      for i in [0...@connections.length]
        connMax = perConnectionMax

        # Distribute the remainder RDY count evenly between the first
        # n connections.
        if rdyRemainder > 0
          connMax += 1
          rdyRemainder -= 1

        @connections[i].setConnectionRdyMax connMax
        @connections[i].bump()


  ###
  The following events results in transitions in the ReaderRdy state machine:
  1. Adding the first connection
  2. Remove the last connections
  3. Finish event from message handling
  4. Backoff event from message handling
  5. Backoff timeout
  ###
  states:
    ZERO:
      Enter: ->
        clearTimeout @backoffId if @backoffId
      backoff: ->        # No-op
      success: ->        # No-op
      try: ->            # No-op
      pause: ->          # No-op
        @goto 'PAUSE'
      unpause: ->        # No-op

    PAUSE:
      Enter: ->
        conn.backoff() for conn in @connections
      backoff: ->        # No-op
      success: ->        # No-op
      try: ->            # No-op
      pause: ->          # No-op
      unpause: ->
        @goto 'TRY_ONE'

    TRY_ONE:
      Enter: ->
        @try()
      backoff: ->
        @goto 'BACKOFF'
      success: (connectionRdy) ->
        @backoffTimer.success()
        @onMessageSuccess connectionRdy
        @goto 'MAX'
      try: ->            # No-op
      pause: ->
        @goto 'PAUSE'
      unpause: ->        # No-op

    MAX:
      Enter: ->
        @balance()
        @bump()
      backoff: ->
        @goto 'BACKOFF'
      success: (connectionRdy) ->
        @backoffTimer.success()
        @onMessageSuccess connectionRdy
      try: ->           # No-op
      pause: ->
        @goto 'PAUSE'
      unpause: ->       # No-op


    BACKOFF:
      Enter: ->
        @backoffTimer.failure()
        @backoff()
      backoff: ->
        @backoffTimer.failure()
        @backoff()
      success: ->       # No-op
      try: ->
        @goto 'TRY_ONE'
      pause: ->
        @goto 'PAUSE'
      unpause: ->       # No-op

  transitions:
    '*':
      '*': (data, callback) ->
        @log()
        callback data


module.exports =
  ReaderRdy: ReaderRdy
  ConnectionRdy: ConnectionRdy
