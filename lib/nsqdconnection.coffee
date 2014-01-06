assert = require 'assert'
net = require 'net'
os = require 'os'
{EventEmitter} = require 'events'

_ = require 'underscore'
NodeState = require 'node-state'

FrameBuffer = require './framebuffer'
Message = require './message'
wire = require './wire'
StateChangeLogger = require './logging'

###
NSQDConnection is a reader connection to a nsqd instance. It manages all
aspects of the nsqd connection with the exception of the RDY count which
needs to be managed across all nsqd connections for a given topic / channel
pair.

This shouldn't be used directly. Use a Reader instead.

Usage:

c = new NSQDConnection '127.0.0.1', 4150, 'test', 'default', 60, 30

c.on NSQDConnection.MESSAGE, (msg) ->
  console.log "Callback [message]: #{msg.attempts}, #{msg.body.toString()}"
  console.log "Timeout of message is #{msg.timeUntilTimeout()}"
  setTimeout (-> console.log "timeout = #{msg.timeUntilTimeout()}"), 5000
  msg.finish()

c.on NSQDConnection.FINISHED, ->
  c.setRdy 1

c.on NSQDConnection.READY, ->
  console.log "Callback [ready]: Set RDY to 100"
  c.setRdy 10

c.on NSQDConnection.CLOSED, ->
  console.log "Callback [closed]: Lost connection to nsqd"

c.on NSQDConnection.ERROR, (err) ->
  console.log "Callback [error]: #{err}"

c.on NSQDConnection.BACKOFF, ->
  console.log "Callback [backoff]: RDY 0"
  c.setRdy 0
  setTimeout (-> c.setRdy 100; console.log 'RDY 100'), 10 * 1000

c.connect()
###
class NSQDConnection extends EventEmitter

  # Events emitted by NSQDConnection
  @BACKOFF: 'backoff'
  @CLOSED: 'closed'
  @ERROR: 'error'
  @FINISHED: 'finished'
  @MESSAGE: 'message'
  @REQUEUED: 'requeued'
  @READY: 'ready'

  constructor: (@nsqdHost, @nsqdPort, @topic, @channel, @requeueDelay,
    @heartbeatInterval, @isReader=true) ->
    @frameBuffer = new FrameBuffer()
    @statemachine = new ConnectionState this

    @maxRdyCount = 0               # Max RDY value for a conn to this NSQD
    @msgTimeout = 0                # Timeout time in milliseconds for a Message
    @maxMsgTimeout = 0             # Max time to process a Message in millisecs
    @lastMessageTimestamp = null   # Timestamp of last message received
    @lastReceivedTimestamp = null  # Timestamp of last data received
    @conn = null                   # Socket connection to NSQD
    @id = null                     # Id that comes from connection local port

  log: (message) ->
    StateChangeLogger.log 'NSQDConnection', @statemachine.current_state_name,
      @id, message

  connect: ->
    # Using nextTick so that clients of Reader can register event listeners
    # right after calling connect.
    process.nextTick =>
      @conn = net.connect @nsqdPort, @nsqdHost, =>
        @id = @conn.localPort
        @statemachine.start()
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
      switch frameId
        when wire.FRAME_TYPE_RESPONSE
          @statemachine.raise 'response', payload
        when wire.FRAME_TYPE_ERROR
          @statemachine.goto 'ERROR', payload
        when wire.FRAME_TYPE_MESSAGE
          @lastMessageTimestamp = @lastReceivedTimestamp
          @statemachine.raise 'consumeMessage', @createMessage payload

  identify: ->
    short_id: os.hostname().split('.')[0]
    long_id: os.hostname()
    feature_negotiation: true,
    heartbeat_interval: @heartbeatInterval * 1000

  # Create a Message object from the message payload received from nsqd.
  createMessage: (msgPayload) ->
    msgComponents = wire.unpackMessage msgPayload
    msg = new Message msgComponents..., @requeueDelay, @msgTimeout,
      @maxMsgTimeout

    msg.on Message.RESPOND, (responseType, wireData) =>
      @write wireData

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
    @conn.destroy()


###
c = new NSQDConnectionWriter '127.0.0.1', 4150, 30
c.connect()

c.on NSQDConnectionWriter.CLOSED, ->
  console.log "Callback [closed]: Lost connection to nsqd"

c.on NSQDConnectionWriter.ERROR, (err) ->
  console.log "Callback [error]: #{err}"

c.on NSQDConnectionWriter.READY, ->
  c.produceMessages 'sample_topic', ['first message']
  c.produceMessages 'sample_topic', ['second message', 'third message']
  c.destroy()
###
class NSQDConnectionWriter extends NSQDConnection

  constructor: (@nsqdHost, @nsqdPort, @heartbeatInterval) ->
    super @nsqdHost, @nsqdPort, null, null, 0, @heartbeatInterval, false

  produceMessages: (topic, msgs) ->
    err = 'Can\'t send messages when the connection is for a Reader'
    assert not @isReader, err
    @statemachine.raise 'produceMessages', [topic, msgs]


class ConnectionState extends NodeState
  constructor: (@conn) ->
    super
      autostart: false,
      initial_state: 'CONNECTED'
      sync_goto: true

  log: (message) ->
    @conn.log message

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

        if @conn.isReader
          @goto 'SUBSCRIBE'
        else
          @goto 'READY_SEND'

    SUBSCRIBE:
      Enter: ->
        @conn.write wire.subscribe(@conn.topic, @conn.channel)
        @goto 'SUBSCRIBE_RESPONSE'

    SUBSCRIBE_RESPONSE:
      response: (data) ->
        if data.toString() is 'OK'
          @goto 'READY_RECV'

    READY_RECV:
      Enter: ->
        # Notify listener that this nsqd connection has passed the subscribe
        # phase.
        @conn.emit NSQDConnection.READY

      consumeMessage: (msg) ->
        @conn.emit NSQDConnection.MESSAGE, msg

      response: (data) ->
        if data.toString() is '_heartbeat_'
          @conn.write wire.nop()

      ready: (rdyCount) ->
        # RDY count for this nsqd cannot exceed the nsqd configured
        # max rdy count.
        rdyCount = @conn.maxRdyCount if rdyCount > @conn.maxRdyCount
        @conn.write wire.ready rdyCount

      close: ->
        @goto 'CLOSED'

    READY_SEND:
      Enter: ->
        # Notify listener that this nsqd connection is ready to send.
        @conn.emit NSQDConnection.READY

      produceMessages: ([topic, msgs]) ->
        if msgs.length is 1
          @conn.write wire.pub topic, msgs[0]
        else
          @conn.write wire.mpub topic, msgs

      response: (data) ->
        if data.toString() is '_heartbeat_'
          @conn.write wire.nop()

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
        @log ''
        callback data

      CONNECTED: (data, callback) ->
        @log "#{@conn.nsqdHost}:#{@conn.nsqdPort}"
        callback data

      ERROR: (err, callback) ->
        @log "#{err}"
        callback err


module.exports =
  NSQDConnection: NSQDConnection
  NSQDConnectionWriter: NSQDConnectionWriter
  ConnectionState: ConnectionState
