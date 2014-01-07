_ = require 'underscore'

chai      = require 'chai'
expect    = chai.expect
should    = chai.should()
sinon     = require 'sinon'
sinonChai = require 'sinon-chai'

chai.use sinonChai

{ConnectionState, NSQDConnection, WriterNSQDConnection, WriterConnectionState} =
  require '../lib/nsqdconnection.coffee'

describe 'Reader ConnectionState', ->
  state =
    sent: []
    connection: null
    statemachine: null

  beforeEach ->
    sent = []

    connection = new NSQDConnection '127.0.0.1', 4150, 'topic_test',
      'channel_test'
    write = sinon.stub connection, 'write', (data) ->
      sent.push data.toString()

    statemachine = new ConnectionState connection

    _.extend state,
      sent: sent
      connection: connection
      statemachine: statemachine

  it 'handle initial handshake', ->
    {statemachine, sent} = state
    statemachine.start()

    sent[0].should.match /^  V2$/
    sent[1].should.match /^IDENTIFY/

  it 'handle OK identify response', ->
    {statemachine, connection} = state
    statemachine.start()
    statemachine.raise 'response', 'OK'

    connection.maxRdyCount.should.eq 2500
    connection.maxMsgTimeout.should.eq 900000 # 15 minutes
    connection.msgTimeout.should.eq 60000     # 1 minute

  it 'handle identify response', ->
    {statemachine, connection} = state
    statemachine.start()

    statemachine.raise 'response', JSON.stringify
      max_rdy_count: 1000
      max_msg_timeout: 10 * 60 * 1000      # 10 minutes
      msg_timeout: 2 * 60 * 1000           #  2 minutes

    connection.maxRdyCount.should.eq 1000
    connection.maxMsgTimeout.should.eq 600000  # 10 minutes
    connection.msgTimeout.should.eq 120000     #  2 minute

  it 'create a subscription', (done) ->
    {sent, statemachine, connection} = state
    connection.on NSQDConnection.READY,  ->
      # Subscribe notification
      done()

    statemachine.start()
    statemachine.raise 'response', 'OK' # Identify response

    sent[2].should.match /^SUB topic_test channel_test\n$/
    statemachine.raise 'response', 'OK' # Subscribe response


  it 'handle a message', (done) ->
    {statemachine, connection} = state
    connection.on NSQDConnection.MESSAGE, (msg) ->
      done()

    statemachine.start()
    statemachine.raise 'response', 'OK' # Identify response
    statemachine.raise 'response', 'OK' # Subscribe response

    statemachine.current_state_name.should.eq 'READY_RECV'

    statemachine.raise 'consumeMessage', {}
    statemachine.current_state_name.should.eq 'READY_RECV'

describe 'WriterConnectionState', ->
  state =
    sent: []
    connection: null
    statemachine: null

  beforeEach ->
    sent = []
    connection = new WriterNSQDConnection '127.0.0.1', 4150, 30

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
      statemachine.current_state_name.should.eq 'READY_SEND'
      done()

    statemachine.start()
    statemachine.raise 'response', 'OK' # Identify response

  it 'should use PUB when sending a single message', (done) ->
    {statemachine, connection, sent} = state

    connection.on WriterNSQDConnection.READY, ->
      connection.produceMessages 'test', ['one']
      sent[sent.length-1].should.match /^PUB/
      done()

    statemachine.start()
    statemachine.raise 'response', 'OK' # Identify response

  it 'should use MPUB when sending multiplie messages', (done) ->
    {statemachine, connection, sent} = state

    connection.on WriterNSQDConnection.READY, ->
      connection.produceMessages 'test', ['one', 'two']
      sent[sent.length-1].should.match /^MPUB/
      done()

    statemachine.start()
    statemachine.raise 'response', 'OK' # Identify response
