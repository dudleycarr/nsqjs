_ = require 'underscore'
should = require 'should'
sinon = require 'sinon'

{ConnectionState, NSQDConnection, WriterNSQDConnection, WriterConnectionState} =
  require '../src/nsqdconnection'
wire = require '../src/wire'

describe 'Reader ConnectionState', ->
  state =
    sent: []
    connection: null
    statemachine: null

  beforeEach ->
    sent = []

    connection = new NSQDConnection '127.0.0.1', 4150, 'topic_test',
      'channel_test'
    sinon.stub connection, 'write', (data) ->
      sent.push data.toString()
    sinon.stub connection, 'destroy', ->

    statemachine = new ConnectionState connection

    _.extend state,
      sent: sent
      connection: connection
      statemachine: statemachine

  it 'handle initial handshake', ->
    {statemachine, sent} = state
    statemachine.raise 'connecting'
    statemachine.raise 'connected'

    sent[0].should.match /^  V2$/
    sent[1].should.match /^IDENTIFY/

  it 'handle OK identify response', ->
    {statemachine, connection} = state
    statemachine.raise 'connecting'
    statemachine.raise 'connected'
    statemachine.raise 'response', new Buffer('OK')

    connection.maxRdyCount.should.eql 2500
    connection.maxMsgTimeout.should.eql 900000 # 15 minutes
    connection.msgTimeout.should.eql 60000     # 1 minute

  it 'handle identify response', ->
    {statemachine, connection} = state
    statemachine.raise 'connecting'
    statemachine.raise 'connected'

    statemachine.raise 'response', JSON.stringify
      max_rdy_count: 1000
      max_msg_timeout: 10 * 60 * 1000      # 10 minutes
      msg_timeout: 2 * 60 * 1000           #  2 minutes

    connection.maxRdyCount.should.eql 1000
    connection.maxMsgTimeout.should.eql 600000  # 10 minutes
    connection.msgTimeout.should.eql 120000     #  2 minute

  it 'create a subscription', (done) ->
    {sent, statemachine, connection} = state
    connection.on NSQDConnection.READY,  ->
      # Subscribe notification
      done()

    statemachine.raise 'connecting'
    statemachine.raise 'connected'
    statemachine.raise 'response', 'OK' # Identify response

    sent[2].should.match /^SUB topic_test channel_test\n$/
    statemachine.raise 'response', 'OK' # Subscribe response


  it 'handle a message', (done) ->
    {statemachine, connection} = state
    connection.on NSQDConnection.MESSAGE, (msg) ->
      done()

    statemachine.raise 'connecting'
    statemachine.raise 'connected'
    statemachine.raise 'response', 'OK' # Identify response
    statemachine.raise 'response', 'OK' # Subscribe response

    statemachine.current_state_name.should.eql 'READY_RECV'

    statemachine.raise 'consumeMessage', {}
    statemachine.current_state_name.should.eql 'READY_RECV'

  it 'handle a message finish after a disconnect', (done) ->
    {statemachine, connection} = state
    sinon.stub wire, 'unpackMessage', ->
      ['1', 0, 0, new Buffer(''), 60, 60, 120]

    connection.on NSQDConnection.MESSAGE, (msg) ->
      fin = ->
        msg.finish()
        done()
      setTimeout fin, 10

    # Advance the connection to the READY state.
    statemachine.raise 'connecting'
    statemachine.raise 'connected'
    statemachine.raise 'response', 'OK' # Identify response
    statemachine.raise 'response', 'OK' # Subscribe response

    # Receive message
    msg = connection.createMessage('')
    statemachine.raise 'consumeMessage', msg

    # Close the connection before the message has been processed.
    connection.destroy()
    statemachine.goto 'CLOSED'

    # Undo stub
    wire.unpackMessage.restore()

  it 'handles non-fatal errors', (done) ->
    {connection, statemachine} = state

    # Note: we still want an error event raised, just not a closed connection
    connection.on NSQDConnection.ERROR, (err) ->
      done()

    # Yields an error if the connection actually closes
    connection.on NSQDConnection.CLOSED, ->
      done new Error 'Should not have closed!'

    statemachine.goto 'ERROR', new Error 'E_REQ_FAILED'

describe 'WriterConnectionState', ->
  state =
    sent: []
    connection: null
    statemachine: null

  beforeEach ->
    sent = []
    connection = new WriterNSQDConnection '127.0.0.1', 4150
    sinon.stub connection, 'destroy'

    write = sinon.stub connection, 'write', (data) ->
      sent.push data.toString()

    statemachine = new WriterConnectionState connection
    connection.statemachine = statemachine

    _.extend state,
      sent: sent
      connection: connection
      statemachine: statemachine

  it 'should generate a READY event after IDENTIFY', (done) ->
    {statemachine, connection} = state

    connection.on WriterNSQDConnection.READY, ->
      statemachine.current_state_name.should.eql 'READY_SEND'
      done()

    statemachine.raise 'connecting'
    statemachine.raise 'connected'
    statemachine.raise 'response', 'OK' # Identify response

  it 'should use PUB when sending a single message', (done) ->
    {statemachine, connection, sent} = state

    connection.on WriterNSQDConnection.READY, ->
      connection.produceMessages 'test', ['one']
      sent[sent.length-1].should.match /^PUB/
      done()

    statemachine.raise 'connecting'
    statemachine.raise 'connected'
    statemachine.raise 'response', 'OK' # Identify response

  it 'should use MPUB when sending multiplie messages', (done) ->
    {statemachine, connection, sent} = state

    connection.on WriterNSQDConnection.READY, ->
      connection.produceMessages 'test', ['one', 'two']
      sent[sent.length-1].should.match /^MPUB/
      done()

    statemachine.raise 'connecting'
    statemachine.raise 'connected'
    statemachine.raise 'response', 'OK' # Identify response

  it 'should call the callback when supplied on publishing a message', (done) ->
    {statemachine, connection, sent} = state

    connection.on WriterNSQDConnection.READY, ->
      connection.produceMessages 'test', ['one'], ->
        done()

      statemachine.raise 'response', 'OK' # Message response

    statemachine.raise 'connecting'
    statemachine.raise 'connected'
    statemachine.raise 'response', 'OK' # Identify response

  it 'should call the the right callback on several messages', (done) ->
    {statemachine, connection, sent} = state

    connection.on WriterNSQDConnection.READY, ->
      connection.produceMessages 'test', ['one']
      connection.produceMessages 'test', ['two'], ->
        # There should be no more callbacks
        connection.messageCallbacks.length.should.be.eql 0
        done()

      statemachine.raise 'response', 'OK' # Message response
      statemachine.raise 'response', 'OK' # Message response

    statemachine.raise 'connecting'
    statemachine.raise 'connected'
    statemachine.raise 'response', 'OK' # Identify response

  it 'should call all callbacks on nsqd disconnect', (done) ->
    {statemachine, connection, sent} = state

    firstCb = sinon.spy()
    secondCb = sinon.spy()

    connection.on WriterNSQDConnection.ERROR, ->
      # Nothing to do on error.

    connection.on WriterNSQDConnection.READY, ->
      connection.produceMessages 'test', ['one'], firstCb
      connection.produceMessages 'test', ['two'], secondCb
      statemachine.goto 'ERROR', 'lost connection'

    connection.on WriterNSQDConnection.CLOSED, ->
      firstCb.calledOnce.should.be.ok()
      secondCb.calledOnce.should.be.ok()
      done()

    statemachine.raise 'connecting'
    statemachine.raise 'connected'
    statemachine.raise 'response', 'OK' # Identify response
