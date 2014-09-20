net = require 'net'
os = require 'os'
tls = require 'tls'
zlib = require 'zlib'
fs = require 'fs'
{EventEmitter} = require 'events'
{SnappyStream, UnsnappyStream} = require 'snappystream'

_ = require 'underscore'
NodeState = require 'node-state'

FrameBuffer = require './framebuffer'
Message = require './message'
wire = require './wire'
StateChangeLogger = require './logging'
version = require './version'


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
  @CONNECTED: 'connected'
  @CLOSED: 'closed'
  @CONNECTION_ERROR: 'connection_error'
  @ERROR: 'error'
  @FINISHED: 'finished'
  @MESSAGE: 'message'
  @REQUEUED: 'requeued'
  @READY: 'ready'

  constructor: (@nsqdHost, @nsqdPort, @topic, @channel, @requeueDelay,
    @heartbeatInterval, @tls=false, @tlsVerification=true, @deflate=false,
    @deflateLevel=6, @snappy=false, @authSecret=null) ->

    @frameBuffer = new FrameBuffer()
    @statemachine = @connectionState()

    @maxRdyCount = 0               # Max RDY value for a conn to this NSQD
    @msgTimeout = 0                # Timeout time in milliseconds for a Message
    @maxMsgTimeout = 0             # Max time to process a Message in millisecs
    @lastMessageTimestamp = null   # Timestamp of last message received
    @lastReceivedTimestamp = null  # Timestamp of last data received
    @conn = null                   # Socket connection to NSQD
    @id = null                     # Id that comes from connection local port
    @identifyTimeoutId = null      # Timeout ID for triggering identifyFail
    @messageCallbacks = []         # Callbacks on message sent responses

  connectionState: ->
    @statemachine or new ConnectionState this

  connect: ->
    # Using nextTick so that clients of Reader can register event listeners
    # right after calling connect.
    process.nextTick =>
      @conn = net.connect @nsqdPort, @nsqdHost, =>
        @id = @conn.localPort
        @statemachine.start()
        @emit NSQDConnection.CONNECTED
        # Once there's a socket connection, give it 5 seconds to receive an
        # identify response.
        @identifyTimeoutId = setTimeout @identifyTimeout.bind(this), 5000

      @registerStreamListeners @conn
  
  registerStreamListeners: (conn) ->
    conn.on 'data', (data) =>
      @receiveRawData data
    conn.on 'error', (err) =>
      @statemachine.goto 'CLOSED'
      @emit 'connection_error', err
    conn.on 'close', (err) =>
      @statemachine.raise 'close'

  startTLS: (callback) ->
    @conn.removeAllListeners event for event in ['data', 'error', 'close']

    options =
      socket: @conn
      rejectUnauthorized: @tlsVerification
    tlsConn = tls.connect options, =>
      @conn = tlsConn
      callback?()

    @registerStreamListeners tlsConn

  startDeflate: (level) ->
    @inflater = zlib.createInflateRaw flush: zlib.Z_SYNC_FLUSH
    @deflater = zlib.createDeflateRaw level: level, flush: zlib.Z_SYNC_FLUSH
    @reconsumeFrameBuffer()

  startSnappy: ->
    @inflater = new UnsnappyStream()
    @deflater = new SnappyStream()
    @reconsumeFrameBuffer()

  reconsumeFrameBuffer: ->
    if @frameBuffer.buffer and @frameBuffer.buffer.length
      data = @frameBuffer.buffer
      delete @frameBuffer.buffer
      @receiveRawData data

  setRdy: (rdyCount) ->
    @statemachine.raise 'ready', rdyCount

  receiveRawData: (data) ->
    unless @inflater
      @receiveData data
    else
      @inflater.write data, =>
        uncompressedData = @inflater.read()
        @receiveData uncompressedData if uncompressedData

  receiveData: (data) ->
    @lastReceivedTimestamp = Date.now()
    @frameBuffer.consume data

    while frame = @frameBuffer.nextFrame()
      [frameId, payload] = frame
      switch frameId
        when wire.FRAME_TYPE_RESPONSE
          @statemachine.raise 'response', payload
        when wire.FRAME_TYPE_ERROR
          @statemachine.goto 'ERROR', new Error payload.toString()
        when wire.FRAME_TYPE_MESSAGE
          @lastMessageTimestamp = @lastReceivedTimestamp
          @statemachine.raise 'consumeMessage', @createMessage payload

  identify: ->
    short_id: os.hostname().split('.')[0]
    long_id: os.hostname()
    feature_negotiation: true,
    heartbeat_interval: @heartbeatInterval * 1000
    deflate: @deflate
    deflate_level: @deflateLevel
    snappy: @snappy
    tls_v1: @tls
    user_agent: "nsqjs/#{version}"

  identifyTimeout: ->
    @statemachine.goto 'ERROR', new Error 'Timed out identifying with nsqd'

  clearIdentifyTimeout: ->
    clearTimeout @identifyTimeoutId
    @identifyTimeoutId = null

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
    if @deflater
      @deflater.write data, =>
        @conn.write @deflater.read()
    else
      @conn.write data

  destroy: ->
    @conn.destroy()


class ConnectionState extends NodeState
  constructor: (@conn) ->
    super
      autostart: false,
      initial_state: 'CONNECTED'
      sync_goto: true

    @identifyResponse = null

  log: (message) ->
    StateChangeLogger.log 'NSQDConnection', @current_state_name, @conn?.id,
      message

  afterIdentify: ->
    'SUBSCRIBE'

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

        @identifyResponse = JSON.parse data
        @conn.maxRdyCount = @identifyResponse.max_rdy_count
        @conn.maxMsgTimeout = @identifyResponse.max_msg_timeout
        @conn.msgTimeout = @identifyResponse.msg_timeout
        @conn.clearIdentifyTimeout()

        return @goto 'TLS_START' if @identifyResponse.tls_v1
        @goto 'IDENTIFY_COMPRESSION_CHECK'

    IDENTIFY_COMPRESSION_CHECK:
      Enter: ->
        {deflate, snappy} = @identifyResponse

        return @goto 'DEFLATE_START', @identifyResponse.deflate_level if deflate
        return @goto 'SNAPPY_START' if snappy
        @goto 'AUTH'

    TLS_START:
      Enter: ->
        @conn.startTLS()
        @goto 'TLS_RESPONSE'

    TLS_RESPONSE:
      response: (data) ->
        if data.toString() is 'OK'
          @goto 'IDENTIFY_COMPRESSION_CHECK'
        else
          @goto 'ERROR', new Error 'TLS negotiate error with nsqd'

    DEFLATE_START:
      Enter: (level) ->
        @conn.startDeflate level
        @goto 'COMPRESSION_RESPONSE'

    SNAPPY_START:
      Enter: ->
        @conn.startSnappy()
        @goto 'COMPRESSION_RESPONSE'

    COMPRESSION_RESPONSE:
      response: (data) ->
        if data.toString() is 'OK'
          @goto 'AUTH'
        else
          @goto 'ERROR', new Error 'Bad response when enabling compression'

    AUTH:
      Enter: ->
        return @goto @afterIdentify() unless @conn.authSecret
        @conn.write wire.auth @conn.authSecret

    AUTH_RESPONSE:
      response: (data) ->
        @conn.auth = JSON.parse data
        @goto @afterIdentify()

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
        @conn.write wire.nop() if data.toString() is '_heartbeat_'

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

      produceMessages: (data) ->
        [topic, msgs, callback] = data
        @conn.messageCallbacks.push callback

        unless _.isArray msgs
          throw new Error 'Expect an array of messages to produceMessages'

        if msgs.length is 1
          @conn.write wire.pub topic, msgs[0]
        else
          @conn.write wire.mpub topic, msgs

      response: (data) ->
        switch data.toString()
          when 'OK'
            cb = @conn.messageCallbacks.shift()
            cb? null
          when '_heartbeat_'
            @conn.write wire.nop()

      close: ->
        @goto 'CLOSED'

    ERROR:
      Enter: (err) ->
        # If there's a callback, pass it the error.
        cb = @conn.messageCallbacks.shift()
        cb? err

        @conn.emit NSQDConnection.ERROR, err
        @goto 'CLOSED'

      close: ->
        @goto 'CLOSED'

    CLOSED:
      Enter: ->
        return unless @conn

        # If there are callbacks, then let them error on the closed connection.
        err = new Error 'nsqd connection closed'
        for cb in @conn.messageCallbacks
          cb? err
        @conn.messageCallbacks = []

        @disable()
        @conn.destroy()
        @conn.emit NSQDConnection.CLOSED
        delete @conn

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
class WriterNSQDConnection extends NSQDConnection

  constructor: (nsqdHost, nsqdPort, heartbeatInterval, tls,
    tlsVerification, deflate, deflateLevel, snappy) ->
    super nsqdHost, nsqdPort, null, null, 0, heartbeatInterval, tls,
      tlsVerification, deflate, deflateLevel, snappy

  connectionState: ->
    @statemachine or new WriterConnectionState this

  produceMessages: (topic, msgs, callback) ->
    @statemachine.raise 'produceMessages', [topic, msgs, callback]


class WriterConnectionState extends ConnectionState

  afterIdentify: ->
    'READY_SEND'


module.exports =
  NSQDConnection: NSQDConnection
  ConnectionState: ConnectionState
  WriterNSQDConnection: WriterNSQDConnection
  WriterConnectionState: WriterConnectionState
