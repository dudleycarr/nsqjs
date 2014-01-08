_ = require 'underscore'

chai      = require 'chai'
expect    = chai.expect
should    = chai.should
sinon     = require 'sinon'
sinonChai = require 'sinon-chai'

chai.use sinonChai

{EventEmitter} = require 'events'
{NSQDConnection} = require '../src/nsqdconnection'
Message = require '../src/message'
{ReaderRdy, ConnectionRdy} = require '../src/readerrdy'
StateChangeLogger = require '../src/logging'


class StubNSQDConnection extends EventEmitter
  constructor: (@nsqdHost, @nsqdPort, @topic, @channel, @requeueDelay,
    @heartbeatInterval) ->
    @conn =
      localPort: 1
    @maxRdyCount = 2500
    @msgTimeout = 60 * 1000
    @maxMsgTimeout = 15 * 60 * 1000

  connect: ->
    # Empty
  setRdy: (rdyCount) ->
    # Empty
  createMessage: (msgId, msgTimestamp, attempts, msgBody) ->

    msgComponents = [msgId, msgTimestamp, attempts, msgBody]
    msgArgs = msgComponents.concat [@requeueDelay, @msgTimeout, @maxMsgTimeout]
    msg = new Message msgArgs...

    msg.on Message.RESPOND, (responseType, wireData) =>
      if responseType is Message.FINISH
        StateChangeLogger.log 'NSQDConnection', null, @conn.localPort,
          'msg finished'
        @emit NSQDConnection.FINISHED
      else if responseType is Message.REQUEUE
        StateChangeLogger.log 'NSQDConnection', null, @conn.localPort,
          'msg requeued'
        @emit NSQDConnection.REQUEUED
    msg.on Message.BACKOFF, =>
      @emit NSQDConnection.BACKOFF

    StateChangeLogger.log 'NSQDConnection', 'READY_RECV', '1',
      "message (#{msgId})"
    @emit NSQDConnection.MESSAGE, msg
    msg

createNSQDConnection = (id) ->
  conn = new StubNSQDConnection 'localhost', '4151', 'test', 'default', 60, 30
  conn.conn.localPort = id
  conn


describe 'ConnectionRdy', ->
  [conn, spy, cRdy] = [null, null, null]

  beforeEach ->
    conn = createNSQDConnection 1
    spy = sinon.spy conn, 'setRdy'
    cRdy = new ConnectionRdy conn
    cRdy.start()

  it 'should register listeners on a connection', ->
    conn = new NSQDConnection 'localhost', 1234, 'test', 'test'
    mock = sinon.mock conn
    mock.expects('on').withArgs NSQDConnection.ERROR
    mock.expects('on').withArgs NSQDConnection.FINISHED
    mock.expects('on').withArgs NSQDConnection.MESSAGE
    mock.expects('on').withArgs NSQDConnection.REQUEUED
    mock.expects('on').withArgs NSQDConnection.READY

    cRdy = new ConnectionRdy conn
    mock.verify()

  it 'should have a connection RDY max of zero', ->
    expect(cRdy.maxConnRdy).is.eql 0

  it 'should not increase RDY when connection RDY max has not been set', ->
    # This bump should be a no-op
    cRdy.bump()
    expect(cRdy.maxConnRdy).is.eql 0
    expect(spy.called).is.not.ok

  it 'should not allow RDY counts to be negative', ->
    cRdy.setConnectionRdyMax 10
    cRdy.setRdy -1

    expect(spy.notCalled).is.ok

  it 'should not allow RDY counts to exceed the connection max', ->
    cRdy.setConnectionRdyMax 10
    cRdy.setRdy 9
    cRdy.setRdy 10
    cRdy.setRdy 20

    expect(spy.calledTwice).is.ok
    expect(spy.firstCall.args[0]).is.eql 9
    expect(spy.secondCall.args[0]).is.eql 10

  it 'should set RDY to max after initial bump', ->
    cRdy.setConnectionRdyMax 3
    cRdy.bump()

    expect(spy.firstCall.args[0]).is.eql 3

  it 'should keep RDY at max after 1+ bumps', ->
    cRdy.setConnectionRdyMax 3
    for i in [1..3]
      cRdy.bump()

    expect(cRdy.maxConnRdy).is.eql 3
    for i in [0...spy.callCount]
      expect(spy.getCall(i).args[0]).is.at.most 3

  it 'should set RDY to zero from after first bump and then backoff', ->
    cRdy.setConnectionRdyMax 3
    cRdy.bump()
    cRdy.backoff()

    expect(spy.lastCall.args[0]).is.eql 0

  it 'should set RDY to zero after 1+ bumps and then a backoff', ->
    cRdy.setConnectionRdyMax 3
    cRdy.bump()
    cRdy.backoff()

    expect(spy.lastCall.args[0]).is.eql 0

  it 'should raise RDY when new connection RDY max is lower', ->
    cRdy.setConnectionRdyMax 3
    cRdy.bump()
    cRdy.setConnectionRdyMax 5

    expect(cRdy.maxConnRdy).is.eql 5
    expect(spy.lastCall.args[0]).is.eql 5

  it 'should reduce RDY when new connection RDY max is higher', ->
    cRdy.setConnectionRdyMax 3
    cRdy.bump()
    cRdy.setConnectionRdyMax 2

    expect(cRdy.maxConnRdy).is.eql 2
    expect(spy.lastCall.args[0]).is.eql 2

  it 'should update RDY when 75% of previous RDY is consumed', ->
    cRdy.setConnectionRdyMax 10
    cRdy.bump()

    expect(spy.firstCall.args[0]).is.eql 10

    for i in [1..7]
      msg = conn.createMessage "#{i}", Date.now(), 0, "Message #{i}"
      msg.finish()
      cRdy.bump()

    expect(spy.callCount).is.eql 1

    msg = conn.createMessage '8', Date.now(), 0, 'Message 8'
    msg.finish()
    cRdy.bump()

    expect(spy.callCount).is.eql 2
    expect(spy.lastCall.args[0]).is.eql 10


###
Helper functions for dealing with StateChangeLogger entries.
###

###
Returns log entries for the ConnectionRdy state that reflect the updated
RDY count. The RDY count is parsed out and added as an object property.
###
connRdyEntries = ->
  _.chain(StateChangeLogger.logs)
  # Only logs from ConnectionRdy instances
  .where({'component': 'ConnectionRdy'})
  # Only logs with messages updating RDY count
  .filter (entry) ->
    /RDY \d+/.test entry.message
  # Put the RDY count in the modified log entry
  .map (entry) ->
    rdy = Number /RDY (\d+)/.exec(entry.message)[1]
    _.extend {}, entry, rdy: rdy
  .map (entry) ->
    _.pick entry, ['id', 'rdy']
  .value()

###
In low RDY situations, the RDY count should alternate between 0 and 1 as
each connections loses and gains the shared RDY count via the periodic
balance call.
###
rdyAlternates = (entries) ->
  # Get an even number of entries
  entries = if entries.length % 2 is 0 then entries else entries[0...-1]

  # Since the RDY should be alternating between 0 and 1, seperate the
  # entries by even or odd index.
  evens = (entries[i] for i in [0...entries.length] by 2)
  odds = (entries[i+1] for i in [0...entries.length-1] by 2)

  # All the entry RDY values should be exactly the same.
  evensMatch = _.all evens, (entry) ->
    entry.rdy is evens[0].rdy

  oddsMatch = _.all odds, (entry) ->
    entry.rdy is odds[0].rdy

  return false unless evensMatch and oddsMatch

  # The values in the two lists should differ
  evens[0].rdy isnt odds[0].rdy


describe 'ReaderRdy', ->
  readerRdy = null

  beforeEach ->
    readerRdy = new ReaderRdy 1, 128
    StateChangeLogger.storeLogs = true
    StateChangeLogger.debug = false
    StateChangeLogger.logs = []

  afterEach ->
    readerRdy.close()

  it 'should register listeners on a connection', ->
    # Stub out creation of ConnectionRdy to ignore the events registered by
    # ConnectionRdy.
    sinon.stub readerRdy, 'createConnectionRdy', ->
      on: ->
        # Empty

    conn = createNSQDConnection 1
    mock = sinon.mock conn
    mock.expects('on').withArgs NSQDConnection.CLOSED
    mock.expects('on').withArgs NSQDConnection.FINISHED
    mock.expects('on').withArgs NSQDConnection.REQUEUED
    mock.expects('on').withArgs NSQDConnection.BACKOFF

    readerRdy.addConnection conn
    mock.verify()

  it 'should be in the zero state until a new connection is READY', ->
    conn = createNSQDConnection 1

    expect(readerRdy.current_state_name).is.eql 'ZERO'
    readerRdy.addConnection conn
    expect(readerRdy.current_state_name).is.eql 'ZERO'
    conn.emit NSQDConnection.READY
    expect(readerRdy.current_state_name).is.eql 'MAX'

  it 'should be in the zero state if it loses all connections', ->
    conn = createNSQDConnection 1

    readerRdy.addConnection conn
    conn.emit NSQDConnection.READY
    conn.emit NSQDConnection.CLOSED
    expect(readerRdy.current_state_name).is.eql 'ZERO'

  it 'should evenly distribute RDY count across connections', ->
    readerRdy = new ReaderRdy 100, 128

    conn1 = createNSQDConnection 1
    conn2 = createNSQDConnection 2

    setRdyStub1 = sinon.spy conn1, 'setRdy'
    setRdyStub2 = sinon.spy conn2, 'setRdy'

    readerRdy.addConnection conn1
    conn1.emit NSQDConnection.READY

    expect(setRdyStub1.lastCall.args[0]).is.eql 100

    readerRdy.addConnection conn2
    conn2.emit NSQDConnection.READY

    expect(setRdyStub1.lastCall.args[0]).is.eql 50
    expect(setRdyStub2.lastCall.args[0]).is.eql 50


  describe 'low RDY conditions', ->
    it 'should periodically redistribute RDY', (done) ->
      # Set to true to see the debug the test.
      StateChangeLogger.debug = false

      # Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy 1, 128, 0.01

      connections = for i in [1..2]
        createNSQDConnection i

      # Add the connections and trigger the NSQDConnection event that tells
      # listeners that the connections are connected and ready for message flow.
      for conn in connections
        readerRdy.addConnection conn
        conn.emit NSQDConnection.READY

      # Given the number of connections and the maxInFlight, we should be in low
      # RDY conditions.
      expect(readerRdy.isLowRdy()).is.eql true

      checkRdyCounts = ->
        entries = connRdyEntries()
        expect(rdyAlternates entries).should.be.ok
        done()

      # We have to wait a small period of time for log events to occur since the
      # `balance` call is invoked perdiocally.
      setTimeout checkRdyCounts, 50

    it 'should handle the transition from normal', (done) ->
      # Set to true to see the debug the test.
      StateChangeLogger.debug = false

      # Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy 1, 128, 0.01

      conn1 = createNSQDConnection 1
      conn2 = createNSQDConnection 2

      # Add the connections and trigger the NSQDConnection event that tells
      # listeners that the connections are connected and ready for message flow.
      readerRdy.addConnection conn1
      conn1.emit NSQDConnection.READY

      expect(readerRdy.isLowRdy()).is.eql false

      addConnection = ->
        readerRdy.addConnection conn2
        conn2.emit NSQDConnection.READY

        # Given the number of connections and the maxInFlight, we should be in
        # low RDY conditions.
        expect(readerRdy.isLowRdy()).is.eql true

      # Add the 2nd connections after some duration to simulate a new nsqd being
      # discovered and connected.
      setTimeout addConnection, 20

      checkRdyCounts = ->
        entries = connRdyEntries()
        expect(rdyAlternates entries).should.be.ok
        done()

      # We have to wait a small period of time for log events to occur since the
      # `balance` call is invoked perdiocally.
      setTimeout checkRdyCounts, 40

    it 'should handle the transition to normal conditions', (done) ->
      # Set to true to see the debug the test.
      StateChangeLogger.debug = false

      # Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy 1, 128, 0.01

      connections = for i in [1..2]
        createNSQDConnection i

      # Add the connections and trigger the NSQDConnection event that tells
      # listeners that the connections are connected and ready for message flow.
      for conn in connections
        readerRdy.addConnection conn
        conn.emit NSQDConnection.READY

      expect(readerRdy.isLowRdy()).is.eql true

      removeConnection = ->
        StateChangeLogger.log 'NSQDConnection', 'CLOSED', '2',
          'connection closed'
        connections[1].emit NSQDConnection.CLOSED

        setTimeout checkNormal, 20

      checkNormal = ->
        expect(readerRdy.isLowRdy()).is.eql false
        expect(readerRdy.balanceId).is.null

        expect(readerRdy.connections[0].lastRdySent).is.eql 1
        done()

      # Remove a connection after some period of time to get back to normal
      # conditions.
      setTimeout removeConnection, 20

    it 'should move to normal conditions with connections in backoff', (done) ->
      ###
      1. Create two nsqd connections
      2. Close the 2nd connection when the first connection is in the BACKOFF
          state.
      3. Check to see if the 1st connection does get it's RDY count.
      ###

      # Set to true to see the debug the test.
      StateChangeLogger.debug = false

      # Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy 1, 128, 0.01

      connections = for i in [1..2]
        createNSQDConnection i

      for conn in connections
        readerRdy.addConnection conn
        conn.emit NSQDConnection.READY

      expect(readerRdy.isLowRdy()).is.eql true

      removeConnection = _.once ->
        StateChangeLogger.log 'NSQDConnection', 'CLOSED', '2',
          'connection closed'
        connections[1].emit NSQDConnection.CLOSED
        setTimeout checkNormal, 30

      removeOnBackoff = ->
        connRdy1 = readerRdy.connections[0]
        connRdy1.on ConnectionRdy.STATE_CHANGE, ->
          if connRdy1.statemachine.current_state_name is 'BACKOFF'
            # If we don't do the connection CLOSED in the next tick, we remove
            # the connection immediately which leaves `@connections` within
            # `balance` in an inconsistent state which isn't possible normally.
            setTimeout removeConnection, 0

      checkNormal = ->
        expect(readerRdy.isLowRdy()).is.eql false
        expect(readerRdy.balanceId).is.null
        expect(readerRdy.connections[0].lastRdySent).is.eql 1
        done()

      # Remove a connection after some period of time to get back to normal
      # conditions.
      setTimeout removeOnBackoff, 20


    it 'should not exceed maxInFlight for long running message.', (done) ->
      # Set to true to see the debug the test.
      StateChangeLogger.debug = false

      # Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy 1, 128, 0.01

      connections = for i in [1..2]
        createNSQDConnection i

      for conn in connections
        readerRdy.addConnection conn
        conn.emit NSQDConnection.READY

      # Handle the message but delay finishing the message so that several
      # balance calls happen and the check to ensure that RDY count is zero for
      # all connections.
      handleMessage = (msg) ->
        finish = ->
          msg.finish()
          done()
        setTimeout finish, 40

      for conn in connections
        conn.on NSQDConnection.MESSAGE, handleMessage

      sendMessageOnce = _.once ->
        connections[1].createMessage '1', Date.now(), new Buffer('test')
        setTimeout checkRdyCount, 20

      # Send a message on the 2nd connection when we can. Only send the message
      # once so that we don't violate the maxInFlight count.
      sendOnRdy = ->
        connRdy2 = readerRdy.connections[1]
        connRdy2.on ConnectionRdy.STATE_CHANGE, ->
          if connRdy2.statemachine.current_state_name in ['ONE', 'MAX']
            sendMessageOnce()

      # When the message is in-flight, balance cannot give a RDY count out to
      # any of the connections.
      checkRdyCount = ->
        expect(readerRdy.isLowRdy()).is.eql true
        expect(readerRdy.connections[0].lastRdySent).is.eql 0
        expect(readerRdy.connections[1].lastRdySent).is.eql 0

      # We have to wait a small period of time for log events to occur since the
      # `balance` call is invoked perdiocally.
      setTimeout sendOnRdy, 20

    it 'should recover losing a connection with a message in-flight', (done) ->
      ###
      Detailed description:
      1. Connect to 5 nsqds and add them to the ReaderRdy
      2. When the 1st connection has the shared RDY count, it receives a
         message.
      3. On receipt of a message, the 1st connection will process the message
         for a long period of time.
      4. While the message is being processed, the 1st connection will close.
      5. Finally, check that the other connections are indeed now getting the
         RDY count.
      ###

      # Set to true to see the debug the test.
      StateChangeLogger.debug = false

      # Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy 1, 128, 0.01

      connections = for i in [1..5]
        createNSQDConnection i

      # Add the connections and trigger the NSQDConnection event that tells
      # listeners that the connections are connected and ready for message flow.
      for conn in connections
        readerRdy.addConnection conn
        conn.emit NSQDConnection.READY

      handleMessage = (msg) ->
        delayFinish = ->
          msg.finish()
          done()

        setTimeout closeConnection, 10
        setTimeout checkRdyCount, 30
        setTimeout delayFinish, 50

      for conn in connections
        conn.on NSQDConnection.MESSAGE, handleMessage

      closeConnection = _.once ->
        connections[0].emit NSQDConnection.CLOSED

      sendMessageOnce = _.once ->
        connections[0].createMessage '1', Date.now(), new Buffer('test')

      # Send a message on the 2nd connection when we can. Only send the message
      # once so that we don't violate the maxInFlight count.
      sendOnRdy = ->
        connRdy = readerRdy.connections[0]
        connRdy.on ConnectionRdy.STATE_CHANGE, ->
          if connRdy.statemachine.current_state_name in ['ONE', 'MAX']
            sendMessageOnce()

      # When the message is in-flight, balance cannot give a RDY count out to
      # any of the connections.
      checkRdyCount = ->
        expect(readerRdy.isLowRdy()).is.eql true

        rdyCounts = for connRdy in readerRdy.connections
          connRdy.lastRdySent

        expect(readerRdy.connections.length).is.eql 4
        expect(1 in rdyCounts).is.ok

      # We have to wait a small period of time for log events to occur since the
      # `balance` call is invoked perdiocally.
      setTimeout sendOnRdy, 10

  describe 'try', ->
    it 'should on completion of backoff attempt a single connection', (done) ->
      ###
      Detailed description:
      1. Create ReaderRdy with connections to 5 nsqds.
      2. Generate a message from an nsqd that causes a backoff.
      3. Verify that all the nsqds are in backoff mode.
      4. At the end of the backoff period, verify that only one ConnectionRdy
         is in the try one state and the others are still in backoff.
      ###

      # Set to true to see the debug the test.
      StateChangeLogger.debug = false

      # Shortening the periodica `balance` calls to every 10ms. Changing the
      # max backoff duration to 1 sec.
      readerRdy = new ReaderRdy 100, 1, 0.01

      connections = for i in [1..5]
        createNSQDConnection i

      for conn in connections
        readerRdy.addConnection conn
        conn.emit NSQDConnection.READY

      msg = connections[0].createMessage "1", Date.now(), 0,
        'Message causing a backoff'
      msg.requeue()

      checkInBackoff = ->
        for connRdy in readerRdy.connections
          expect(connRdy.statemachine.current_state_name).is.eql 'BACKOFF'

      setTimeout checkInBackoff, 0

      afterBackoff = ->
        states = for connRdy in readerRdy.connections
          connRdy.statemachine.current_state_name

        ones = (s for s in states when s is 'ONE')
        backoffs = (s for s in states when s is 'BACKOFF')

        expect(ones).to.have.length 1
        expect(backoffs).to.have.length 4
        done()

      delay = readerRdy.backoffTimer.getInterval() + 100
      setTimeout afterBackoff, delay * 1000

    it 'should after backoff with a successful message go to MAX', (done) ->
      ###
      Detailed description:
      1. Create ReaderRdy with connections to 5 nsqds.
      2. Generate a message from an nsqd that causes a backoff.
      3. At the end of backoff, generate a message that will succeed.
      4. Verify that ReaderRdy is in MAX and ConnectionRdy instances are in
         either ONE or MAX. At least on ConnectionRdy should be in MAX as well.
      ###

      # Set to true to see the debug the test.
      StateChangeLogger.debug = false

      # Shortening the periodica `balance` calls to every 10ms. Changing the
      # max backoff duration to 1 sec.
      readerRdy = new ReaderRdy 100, 1, 0.01

      connections = for i in [1..5]
        createNSQDConnection i

      for conn in connections
        readerRdy.addConnection conn
        conn.emit NSQDConnection.READY

      msg = connections[0].createMessage "1", Date.now(), 0,
        'Message causing a backoff'
      msg.requeue()

      afterBackoff = ->
        [connRdy] = for connRdy in readerRdy.connections
          if connRdy.statemachine.current_state_name is 'ONE'
            connRdy

        msg = connRdy.conn.createMessage "1", Date.now(), 0, 'Success'
        msg.finish()

        verifyMax = ->
          states = for connRdy in readerRdy.connections
            connRdy.statemachine.current_state_name

          max = (s for s in states when s in ['ONE', 'MAX'])

          expect(max).to.have.length 5
          expect(states).to.contain 'MAX'
          done()

        setTimeout verifyMax, 0

      delay = readerRdy.backoffTimer.getInterval() + 100
      setTimeout afterBackoff, delay * 1000
