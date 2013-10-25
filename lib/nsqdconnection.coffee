assert = require 'assert'
net = require 'net'
os = require 'os'
{EventEmitter} = require 'events'

_ = require 'underscore'
NodeState = require 'node-state'

FrameBuffer = require './framebuffer'
Message = require './message'
wire = require './wire'

# NSQDConnection is a reader connection to a nsqd instance. It manages all
# aspects of the nsqd connection with the exception of the RDY count which
# needs to be managed across all nsqd connections for a given topic / channel
# pair.
#
# This shouldn't be used directly. Use a Reader instead.
#
# Usage:
#
# c = new NSQDConnection '127.0.0.1', 4150, 'test', 'default', 100, 30
#
# c.on NSQDConnection.MESSAGE, (msg) ->
#   console.log "Callback [message]: #{msg.attempts}, #{msg.body.toString()}"
#   console.log "Timeout of message is #{msg.timeUntilTimeout()}"
#   setTimeout (-> console.log "timeout = #{msg.timeUntilTimeout()}"), 5000
#   msg.finish()
#
# c.on NSQDConnection.FINISHED, ->
#   c.setRdy 1
#
# c.on NSQDConnection.SUBSCRIBED, ->
#   console.log "Callback [subscribed]: Set RDY to 100"
#   c.setRdy 10
#
# c.on NSQDConnection.CLOSED, ->
#   console.log "Callback [closed]: Lost connection to nsqd"
#
# c.on NSQDConnection.ERROR, (err) ->
#   console.log "Callback [error]: #{err}"
#
# c.on NSQDConnection.BACKOFF, ->
#   console.log "Callback [backoff]: RDY 0"
#   c.setRdy 0
#   setTimeout (-> c.setRdy 100; console.log 'RDY 100'), 10 * 1000

# c.connect()


class NSQDConnection extends EventEmitter

  # Events emitted by NSQDConnection
  @BACKOFF: 'backoff'
  @CLOSED: 'closed'
  @ERROR: 'error'
  @FINISHED: 'finished'
  @MESSAGE: 'message'
  @REQUEUED: 'requeued'
  @SUBSCRIBED: 'subscribed'

  constructor: (@nsqdHost, @nsqdPort, @topic, @channel, @maxInFlight=1,
    @heartbeatInterval=30) ->
    @frameBuffer = new FrameBuffer()
    @statemachine = new ConnectionState @

    rdyCount: 0                  # RDY value given to the conn by the Reader
    maxRdyCount: 0               # Max RDY value for a conn to this NSQD
    msgTimeout: 0                # Timeout time in milliseconds for a Message
    maxMsgTimeout: 0             # Max time to process a Message in milliseconds
    inFlight: 0                  # No. messages processed by this conn.
    lastMessageTimestamp: null   # Timestamp of last message received
    lastReceivedTimestamp: null  # Timestamp of last data received
    conn: null                   # Socket connection to NSQD

  connect: ->
    callback = _.bind @statemachine.start, @statemachine
    @conn = net.connect @nsqdPort, @nsqdHost, callback
    @conn.on 'data', (data) =>
      @receiveData data
    @conn.on 'error', (err) =>
      @statemachine.goto 'ERROR', err
    @conn.on 'close', =>
      @statemachine.raise 'close'

  setRdy: (rdyCount) ->
    @statemachine.raise 'ready', rdyCount

  receiveData: (data) ->
    @lastReceivedTimestamp = Date.now()
    frames = @frameBuffer.consume data

    for [frameId, payload] in frames
      # TODO(dudley): What to do with frames when we encounter backoff in the
      #   state machine.
      switch frameId
        when wire.FRAME_TYPE_RESPONSE
          @statemachine.raise 'response', payload
        when wire.FRAME_TYPE_ERROR
          @statemachine.goto 'ERROR', payload
        when wire.FRAME_TYPE_MESSAGE
          @rdyCount -= 1
          @inFlight += 1
          @lastMessageTimestamp = @lastReceivedTimestamp
          @statemachine.raise 'message', @createMessage payload

  identify: ->
    short_id: os.hostname().split('.')[0]
    long_id: os.hostname()
    feature_negotiation: true,
    heartbeat_interval: @heartbeatInterval * 1000

  createMessage: (msgPayload) ->
    msgComponents = wire.unpackMessage msgPayload
    msg = new Message msgComponents..., @msgTimeout, @maxMsgTimeout

    msg.on Message.RESPOND, (responseType, wireData) =>
      @conn.write wireData
      @inFlight -= 1 if responseType in [Message.FINISH, Message.REQUEUE]
      if responseType is Message.FINISH
        @emit NSQDConnection.FINISHED
      else if responseType is Message.REQUEUE
        @emit NSQDConnection.REQUEUED

    msg.on Message.BACKOFF, =>
      @emit NSQDConnection.BACKOFF

    msg

  write: (data) ->
    @conn.write data

  destroy: ->
    @conn.destroy


class ConnectionState extends NodeState
  constructor: (@conn) ->
    super
      autostart: false,
      initial_state: 'CONNECTED'
      sync_goto: true

  states:
    CONNECTED:
      Enter: ->
        @goto 'SEND_MAGIC_IDENTIFIER'

    SEND_MAGIC_IDENTIFIER:
      Enter: ->
        # Send the magic protocol identifier to the connection
        @conn.write wire.MAGIC_V2
        @goto 'IDENTIFY'

    IDENTIFY:
      Enter: ->
        # Send configuration details
        @conn.write wire.identify @conn.identify()
        @goto 'IDENTIFY_RESPONSE'

    IDENTIFY_RESPONSE:
      response: (data) ->
        if data is 'OK'
          data = JSON.stringify
            max_rdy_count: 2500
            max_msg_timeout: 15 * 60 * 1000    # 15 minutes
            msg_timeout: 60 * 1000             #  1 minute

        identifyResponse = JSON.parse data
        @conn.maxRdyCount = identifyResponse.max_rdy_count
        @conn.maxMsgTimeout = identifyResponse.max_msg_timeout
        @conn.msgTimeout = identifyResponse.msg_timeout

        @goto 'SUBSCRIBE'

    SUBSCRIBE:
      Enter: ->
        @conn.write wire.subscribe(@conn.topic, @conn.channel)
        @goto 'SUBSCRIBE_RESPONSE'

    SUBSCRIBE_RESPONSE:
      response: (data) ->
        if data.toString() is 'OK'
          @goto 'WAIT_FOR_DATA'

          # Notify listener that this nsqd connection has passed the subscribe
          # phase.
          @conn.emit NSQDConnection.SUBSCRIBED

    WAIT_FOR_DATA:
      message: (msg) ->
        # Notify listener that this nsqd connection has passed the subscribe
        # phase.
        @conn.emit NSQDConnection.MESSAGE, msg

      response: (data) ->
        if data.toString() is '_heartbeat_'
          @conn.write wire.nop()

      ready: (rdyCount) ->
        # RDY count for this nsqd cannot exceed the nsqd configured
        # max rdy count.
        rdyCount = @conn.maxRdyCount if rdyCount > @conn.maxRdyCount

        @conn.rdyCount = rdyCount
        @conn.write wire.ready rdyCount

      close: ->
        @goto 'CLOSED'

    ERROR:
      Enter: (err) ->
        @conn.emit NSQDConnection.ERROR, err
        @goto 'CLOSED'

      close: ->
        @goto 'CLOSED'

    CLOSED:
      Enter: ->
        @stop()
        @conn.destroy()
        @conn.emit NSQDConnection.CLOSED
        @conn = null

      close: ->
        # No-op. Once closed, subsequent calls should do nothing.

  transitions:
    '*':
      '*': (data, callback) ->
        callback data


module.exports =
  NSQDConnection: NSQDConnection
  ConnectionState: ConnectionState
