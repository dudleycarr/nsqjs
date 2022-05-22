const {EventEmitter} = require('events')
const _ = require('lodash')
const should = require('should')
const sinon = require('sinon')
const rawMessage = require('./rawmessage')

const Message = require('../lib/message')
const {NSQDConnection} = require('../lib/nsqdconnection')
const {ReaderRdy, ConnectionRdy} = require('../lib/readerrdy')

class StubNSQDConnection extends EventEmitter {
  constructor(
    nsqdHost,
    nsqdPort,
    topic,
    channel,
    requeueDelay,
    heartbeatInterval
  ) {
    super()
    this.nsqdHost = nsqdHost
    this.nsqdPort = nsqdPort
    this.topic = topic
    this.channel = channel
    this.requeueDelay = requeueDelay
    this.heartbeatInterval = heartbeatInterval
    this.conn = {localPort: 1}
    this.maxRdyCount = 2500
    this.msgTimeout = 60 * 1000
    this.maxMsgTimeout = 15 * 60 * 1000
    this.rdyCounts = []
  }

  id() {
    return `${this.nsqdHost}:${this.nsqdPort}`
  }

  // Empty
  connect() {}

  // Empty
  close() {}

  // Empty
  destroy() {}

  // Empty
  setRdy(rdyCount) {
    this.rdyCounts.push(rdyCount)
  }

  createMessage(msgId, msgTimestamp, attempts, msgBody) {
    const msgArgs = [
      rawMessage(msgId, msgTimestamp, attempts, msgBody),
      this.requeueDelay,
      this.msgTimeout,
      this.maxMsgTimeout,
    ]
    const msg = new Message(...msgArgs)

    msg.on(Message.RESPOND, (responseType) => {
      if (responseType === Message.FINISH) {
        this.emit(NSQDConnection.FINISHED)
      } else if (responseType === Message.REQUEUE) {
        this.emit(NSQDConnection.REQUEUED)
      }
    })

    msg.on(Message.BACKOFF, () => this.emit(NSQDConnection.BACKOFF))

    this.emit(NSQDConnection.MESSAGE, msg)
    return msg
  }
}

const createNSQDConnection = (id) => {
  const conn = new StubNSQDConnection(
    `host${id}`,
    '4150',
    'test',
    'default',
    60,
    30
  )
  conn.conn.localPort = id
  return conn
}

describe('ConnectionRdy', () => {
  let [conn, spy, cRdy] = Array.from([null, null, null])

  beforeEach(() => {
    conn = createNSQDConnection(1)
    spy = sinon.spy(conn, 'setRdy')
    cRdy = new ConnectionRdy(conn)
    cRdy.start()
  })

  it('should register listeners on a connection', () => {
    conn = new NSQDConnection('localhost', 1234, 'test', 'test')
    const mock = sinon.mock(conn)
    mock.expects('on').withArgs(NSQDConnection.ERROR)
    mock.expects('on').withArgs(NSQDConnection.FINISHED)
    mock.expects('on').withArgs(NSQDConnection.MESSAGE)
    mock.expects('on').withArgs(NSQDConnection.REQUEUED)
    mock.expects('on').withArgs(NSQDConnection.READY)
    cRdy = new ConnectionRdy(conn)
    mock.verify()
  })

  it('should have a connection RDY max of zero', () => {
    should.equal(cRdy.maxConnRdy, 0)
  })

  it('should not increase RDY when connection RDY max has not been set', () => {
    // This bump should be a no-op
    cRdy.bump()
    should.equal(cRdy.maxConnRdy, 0)
    should.equal(spy.called, false)
  })

  it('should not allow RDY counts to be negative', () => {
    cRdy.setConnectionRdyMax(10)
    cRdy.setRdy(-1)
    should.equal(spy.notCalled, true)
  })

  it('should not allow RDY counts to exceed the connection max', () => {
    cRdy.setConnectionRdyMax(10)
    cRdy.setRdy(9)
    cRdy.setRdy(10)
    cRdy.setRdy(20)
    should.equal(spy.calledTwice, true)
    should.equal(spy.firstCall.args[0], 9)
    should.equal(spy.secondCall.args[0], 10)
  })

  it('should set RDY to max after initial bump', () => {
    cRdy.setConnectionRdyMax(3)
    cRdy.bump()
    should.equal(spy.firstCall.args[0], 3)
  })

  it('should keep RDY at max after 1+ bumps', () => {
    cRdy.setConnectionRdyMax(3)
    for (let i = 1; i <= 3; i++) {
      cRdy.bump()
    }

    cRdy.maxConnRdy.should.eql(3)
    for (let i = 0; i < spy.callCount; i++) {
      should.ok(spy.getCall(i).args[0] <= 3)
    }
  })

  it('should set RDY to zero from after first bump and then backoff', () => {
    cRdy.setConnectionRdyMax(3)
    cRdy.bump()
    cRdy.backoff()
    should.equal(spy.lastCall.args[0], 0)
  })

  it('should set RDY to zero after 1+ bumps and then a backoff', () => {
    cRdy.setConnectionRdyMax(3)
    cRdy.bump()
    cRdy.backoff()
    should.equal(spy.lastCall.args[0], 0)
  })

  it('should raise RDY when new connection RDY max is lower', () => {
    cRdy.setConnectionRdyMax(3)
    cRdy.bump()
    cRdy.setConnectionRdyMax(5)
    should.equal(cRdy.maxConnRdy, 5)
    should.equal(spy.lastCall.args[0], 5)
  })

  it('should reduce RDY when new connection RDY max is higher', () => {
    cRdy.setConnectionRdyMax(3)
    cRdy.bump()
    cRdy.setConnectionRdyMax(2)
    should.equal(cRdy.maxConnRdy, 2)
    should.equal(spy.lastCall.args[0], 2)
  })

  it('should update RDY when 75% of previous RDY is consumed', () => {
    let msg
    cRdy.setConnectionRdyMax(10)
    cRdy.bump()

    should.equal(spy.firstCall.args[0], 10)

    for (let i = 1; i <= 7; i++) {
      msg = conn.createMessage(`${i}`, Date.now(), 0, `Message ${i}`)
      msg.finish()
      cRdy.bump()
    }

    should.equal(spy.callCount, 1)

    msg = conn.createMessage('8', Date.now(), 0, 'Message 8')
    msg.finish()
    cRdy.bump()

    should.equal(spy.callCount, 2)
    should.equal(spy.lastCall.args[0], 10)
  })
})

describe('ReaderRdy', () => {
  let readerRdy = null

  beforeEach(() => {
    readerRdy = new ReaderRdy(1, 128, 'topic/channel')
  })

  afterEach(() => readerRdy.close())

  it('should register listeners on a connection', () => {
    // Stub out creation of ConnectionRdy to ignore the events registered by
    // ConnectionRdy.
    sinon.stub(readerRdy, 'createConnectionRdy').callsFake(() => ({on() {}}))
    // Empty

    const conn = createNSQDConnection(1)
    const mock = sinon.mock(conn)
    mock.expects('on').withArgs(NSQDConnection.CLOSED)
    mock.expects('on').withArgs(NSQDConnection.FINISHED)
    mock.expects('on').withArgs(NSQDConnection.REQUEUED)
    mock.expects('on').withArgs(NSQDConnection.BACKOFF)

    readerRdy.addConnection(conn)
    mock.verify()
  })

  it('should be in the zero state until a new connection is READY', () => {
    const conn = createNSQDConnection(1)
    readerRdy.current_state_name.should.eql('ZERO')
    readerRdy.addConnection(conn)
    readerRdy.current_state_name.should.eql('ZERO')
    conn.emit(NSQDConnection.READY)
    readerRdy.current_state_name.should.eql('MAX')
  })

  it('should be in the zero state if it loses all connections', () => {
    const conn = createNSQDConnection(1)
    readerRdy.addConnection(conn)
    conn.emit(NSQDConnection.READY)
    conn.emit(NSQDConnection.CLOSED)
    readerRdy.current_state_name.should.eql('ZERO')
  })

  it('should evenly distribute RDY count across connections', () => {
    readerRdy = new ReaderRdy(100, 128, 'topic/channel')

    const conn1 = createNSQDConnection(1)
    const conn2 = createNSQDConnection(2)

    const setRdyStub1 = sinon.spy(conn1, 'setRdy')
    const setRdyStub2 = sinon.spy(conn2, 'setRdy')

    readerRdy.addConnection(conn1)
    conn1.emit(NSQDConnection.READY)

    setRdyStub1.lastCall.args[0].should.eql(100)

    readerRdy.addConnection(conn2)
    conn2.emit(NSQDConnection.READY)

    setRdyStub1.lastCall.args[0].should.eql(50)
    setRdyStub2.lastCall.args[0].should.eql(50)
  })

  describe('low RDY conditions', () => {
    const assertAlternatingRdyCounts = (conn1, conn2) => {
      const minSize = Math.min(conn1.rdyCounts.length, conn2.rdyCounts.length)

      const zippedCounts = _.zip(
        conn1.rdyCounts.slice(-minSize),
        conn2.rdyCounts.slice(-minSize)
      )

      // We expect the connection RDY counts to look like this:
      // conn 0: [1, 0, 1, 0]
      // conn 1: [0, 1, 0, 1]
      zippedCounts.forEach(([firstRdy, secondRdy]) => {
        should.ok(firstRdy + secondRdy === 1)
      })
    }

    it('should periodically redistribute RDY', (done) => {
      // Shortening the periodically `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01)

      const connections = [1, 2].map((i) => createNSQDConnection(i))

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      connections.forEach((conn) => {
        readerRdy.addConnection(conn)
        conn.emit(NSQDConnection.READY)
      })

      // Given the number of connections and the maxInFlight, we should be in low
      // RDY conditions.
      should.equal(readerRdy.isLowRdy(), true)

      const checkRdyCounts = () => {
        assertAlternatingRdyCounts(...connections)
        done()
      }

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      setTimeout(checkRdyCounts, 50)
    })

    it('should handle the transition from normal', (done) => {
      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01)

      const conn1 = createNSQDConnection(1)
      const conn2 = createNSQDConnection(2)

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      readerRdy.addConnection(conn1)
      conn1.emit(NSQDConnection.READY)

      should.equal(readerRdy.isLowRdy(), false)

      const addConnection = () => {
        readerRdy.addConnection(conn2)
        conn2.emit(NSQDConnection.READY)

        // Given the number of connections and the maxInFlight, we should be in
        // low RDY conditions.
        should.equal(readerRdy.isLowRdy(), true)
      }

      // Add the 2nd connections after some duration to simulate a new nsqd being
      // discovered and connected.
      setTimeout(addConnection, 20)

      const checkRdyCounts = () => {
        assertAlternatingRdyCounts(conn1, conn2)
        done()
      }

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      setTimeout(checkRdyCounts, 40)
    })

    it('should handle the transition to normal conditions', (done) => {
      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01)

      const connections = [1, 2].map((i) => createNSQDConnection(i))

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      connections.forEach((conn) => {
        readerRdy.addConnection(conn)
        conn.emit(NSQDConnection.READY)
      })

      should.equal(readerRdy.isLowRdy(), true)
      readerRdy.isLowRdy().should.eql(true)

      const checkNormal = () => {
        should.equal(readerRdy.isLowRdy(), false)
        should.equal(readerRdy.balanceId, null)
        should.equal(readerRdy.connections[0].lastRdySent, 1)
        done()
      }

      const removeConnection = () => {
        connections[1].emit(NSQDConnection.CLOSED)
        setTimeout(checkNormal, 20)
      }

      // Remove a connection after some period of time to get back to normal
      // conditions.
      setTimeout(removeConnection, 20)
    })

    it('should move to normal conditions with connections in backoff', (done) => {
      /*
      1. Create two nsqd connections
      2. Close the 2nd connection when the first connection is in the BACKOFF
          state.
      3. Check to see if the 1st connection does get it's RDY count.
      */

      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01)

      const connections = [1, 2].map((i) => createNSQDConnection(i))

      connections.forEach((conn) => {
        readerRdy.addConnection(conn)
        conn.emit(NSQDConnection.READY)
      })

      should.equal(readerRdy.isLowRdy(), true)

      const checkNormal = () => {
        should.equal(readerRdy.isLowRdy(), false)
        should.equal(readerRdy.balanceId, null)
        should.equal(readerRdy.connections[0].lastRdySent, 1)
        done()
      }

      const removeConnection = _.once(() => {
        connections[1].emit(NSQDConnection.CLOSED)
        setTimeout(checkNormal, 30)
      })

      const removeOnBackoff = () => {
        const connRdy1 = readerRdy.connections[0]
        connRdy1.on(ConnectionRdy.STATE_CHANGE, () => {
          if (connRdy1.statemachine.current_state_name === 'BACKOFF') {
            // If we don't do the connection CLOSED in the next tick, we remove
            // the connection immediately which leaves `@connections` within
            // `balance` in an inconsistent state which isn't possible normally.
            setTimeout(removeConnection, 0)
          }
        })
      }

      // Remove a connection after some period of time to get back to normal
      // conditions.
      setTimeout(removeOnBackoff, 20)
    })

    it('should not exceed maxInFlight for long running message.', (done) => {
      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01)

      const connections = [1, 2].map((i) => createNSQDConnection(i))

      connections.forEach((conn) => {
        readerRdy.addConnection(conn)
        conn.emit(NSQDConnection.READY)
      })

      // Handle the message but delay finishing the message so that several
      // balance calls happen and the check to ensure that RDY count is zero for
      // all connections.
      const handleMessage = (msg) => {
        const finish = () => {
          msg.finish()
          done()
        }

        setTimeout(finish, 40)
      }

      connections.forEach((conn) => {
        conn.on(NSQDConnection.MESSAGE, handleMessage)
      })

      // When the message is in-flight, balance cannot give a RDY count out to
      // any of the connections.
      const checkRdyCount = () => {
        should.equal(readerRdy.isLowRdy(), true)
        should.equal(readerRdy.connections[0].lastRdySent, 0)
        should.equal(readerRdy.connections[1].lastRdySent, 0)
      }

      const sendMessageOnce = _.once(() => {
        connections[1].createMessage('1', Date.now(), 0, Buffer.from('test'))
        setTimeout(checkRdyCount, 20)
      })

      // Send a message on the 2nd connection when we can. Only send the message
      // once so that we don't violate the maxInFlight count.
      const sendOnRdy = () => {
        const connRdy2 = readerRdy.connections[1]
        connRdy2.on(ConnectionRdy.STATE_CHANGE, () => {
          if (
            ['ONE', 'MAX'].includes(connRdy2.statemachine.current_state_name)
          ) {
            sendMessageOnce()
          }
        })
      }

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      setTimeout(sendOnRdy, 20)
    })

    it('should recover losing a connection with a message in-flight', (done) => {
      /*
      Detailed description:
      1. Connect to 5 nsqds and add them to the ReaderRdy
      2. When the 1st connection has the shared RDY count, it receives a
         message.
      3. On receipt of a message, the 1st connection will process the message
         for a long period of time.
      4. While the message is being processed, the 1st connection will close.
      5. Finally, check that the other connections are indeed now getting the
         RDY count.
      */

      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01)

      const connections = [1, 2, 3, 4, 5].map((i) => createNSQDConnection(i))

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      connections.forEach((conn) => {
        readerRdy.addConnection(conn)
        conn.emit(NSQDConnection.READY)
      })

      const closeConnection = _.once(() => {
        connections[0].emit(NSQDConnection.CLOSED)
      })

      // When the message is in-flight, balance cannot give a RDY count out to
      // any of the connections.
      const checkRdyCount = () => {
        should.equal(readerRdy.isLowRdy(), true)

        const rdyCounts = Array.from(readerRdy.connections).map(
          (connRdy) => connRdy.lastRdySent
        )

        should.equal(readerRdy.connections.length, 4)
        should.ok(Array.from(rdyCounts).includes(1))
      }

      const handleMessage = (msg) => {
        const delayFinish = () => {
          msg.finish()
          done()
        }

        setTimeout(closeConnection, 10)
        setTimeout(checkRdyCount, 30)
        setTimeout(delayFinish, 50)
      }

      connections.forEach((conn) => {
        conn.on(NSQDConnection.MESSAGE, handleMessage)
      })

      const sendMessageOnce = _.once(() => {
        connections[0].createMessage('1', Date.now(), 0, Buffer.from('test'))
      })

      // Send a message on the 2nd connection when we can. Only send the message
      // once so that we don't violate the maxInFlight count.
      const sendOnRdy = () => {
        const connRdy = readerRdy.connections[0]
        connRdy.on(ConnectionRdy.STATE_CHANGE, () => {
          if (
            ['ONE', 'MAX'].includes(connRdy.statemachine.current_state_name)
          ) {
            sendMessageOnce()
          }
        })
      }

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      setTimeout(sendOnRdy, 10)
    })
  })

  describe('try', () => {
    it('should on completion of backoff attempt a single connection', (done) => {
      /*
      Detailed description:
      1. Create ReaderRdy with connections to 5 nsqds.
      2. Generate a message from an nsqd that causes a backoff.
      3. Verify that all the nsqds are in backoff mode.
      4. At the end of the backoff period, verify that only one ConnectionRdy
         is in the try one state and the others are still in backoff.
      */

      // Shortening the periodic `balance` calls to every 10ms. Changing the
      // max backoff duration to 10 sec.
      readerRdy = new ReaderRdy(100, 10, 'topic/channel', 0.01)

      const connections = [1, 2, 3, 4, 5].map((i) => createNSQDConnection(i))

      connections.forEach((conn) => {
        readerRdy.addConnection(conn)
        conn.emit(NSQDConnection.READY)
      })

      connections[0]
        .createMessage('1', Date.now(), 0, 'Message causing a backoff')
        .requeue()

      const checkInBackoff = () => {
        readerRdy.connections.forEach((connRdy) => {
          connRdy.statemachine.current_state_name.should.eql('BACKOFF')
        })
      }

      checkInBackoff()

      const afterBackoff = () => {
        const states = readerRdy.connections.map(
          (connRdy) => connRdy.statemachine.current_state_name
        )

        const ones = states.filter((state) => state === 'ONE')
        const backoffs = states.filter((state) => state === 'BACKOFF')

        should.equal(ones.length, 1)
        should.equal(backoffs.length, 4)
        done()
      }

      // Add 50ms to the delay so that we're confident that the event fired.
      const delay = readerRdy.backoffTimer.getInterval() + 0.05

      setTimeout(afterBackoff, delay.valueOf() * 1000)
    })

    it('should after backoff with a successful message go to MAX', (done) => {
      /*
      Detailed description:
      1. Create ReaderRdy with connections to 5 nsqds.
      2. Generate a message from an nsqd that causes a backoff.
      3. At the end of backoff, generate a message that will succeed.
      4. Verify that ReaderRdy is in MAX and ConnectionRdy instances are in
         either ONE or MAX. At least on ConnectionRdy should be in MAX as well.
      */

      // Shortening the periodica `balance` calls to every 10ms. Changing the
      // max backoff duration to 1 sec.
      readerRdy = new ReaderRdy(100, 1, 'topic/channel', 0.01)

      const connections = [1, 2, 3, 4, 5].map((i) => createNSQDConnection(i))

      connections.forEach((conn) => {
        readerRdy.addConnection(conn)
        conn.emit(NSQDConnection.READY)
      })

      let msg = connections[0].createMessage(
        '1',
        Date.now(),
        0,
        'Message causing a backoff'
      )

      msg.requeue()

      const afterBackoff = () => {
        const [connRdy] = readerRdy.connections.filter(
          (conn) => conn.statemachine.current_state_name === 'ONE'
        )

        msg = connRdy.conn.createMessage('1', Date.now(), 0, 'Success')
        msg.finish()

        const verifyMax = () => {
          const states = readerRdy.connections.map(
            (conn) => conn.statemachine.current_state_name
          )

          const max = states.filter((s) => ['ONE', 'MAX'].includes(s))

          max.length.should.eql(5)
          should.equal(max.length, 5)
          should.ok(states.includes('MAX'))
          done()
        }

        setTimeout(verifyMax, 0)
      }

      const delay = readerRdy.backoffTimer.getInterval() + 100
      setTimeout(afterBackoff, delay)
    })
  })

  describe('backoff', () => {
    it('should not increase interval with more failures during backoff', () => {
      readerRdy = new ReaderRdy(100, 1, 'topic/channel', 0.01)

      // Create a connection and make it ready.
      const c = createNSQDConnection(0)
      readerRdy.addConnection(c)
      c.emit(NSQDConnection.READY)

      readerRdy.raise('backoff')
      const interval = readerRdy.backoffTimer.getInterval()

      readerRdy.raise('backoff')
      readerRdy.backoffTimer.getInterval().should.eql(interval)
    })
  })

  describe('pause / unpause', () => {
    beforeEach(() => {
      // Shortening the periodic `balance` calls to every 10ms. Changing the
      // max backoff duration to 1 sec.
      readerRdy = new ReaderRdy(100, 1, 'topic/channel', 0.01)

      const connections = [1, 2, 3, 4, 5].map((i) => createNSQDConnection(i))

      connections.forEach((conn) => {
        readerRdy.addConnection(conn)
        conn.emit(NSQDConnection.READY)
      })
    })

    it('should drop ready count to zero on all connections when paused', () => {
      readerRdy.pause()
      should.equal(readerRdy.current_state_name, 'PAUSE')
      readerRdy.connections.forEach((conn) => should.equal(conn.lastRdySent, 0))
    })

    it('should unpause by trying one', () => {
      readerRdy.pause()
      readerRdy.unpause()
      should.equal(readerRdy.current_state_name, 'TRY_ONE')
    })

    it('should update the value of @isPaused when paused', () => {
      readerRdy.pause()
      should.equal(readerRdy.isPaused(), true)
      readerRdy.unpause()
      should.equal(readerRdy.isPaused(), false)
    })
  })
})
