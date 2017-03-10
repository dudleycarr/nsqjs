import _ from 'underscore';
import should from 'should';
import sinon from 'sinon';

import { EventEmitter } from 'events';
import { NSQDConnection } from '../src/nsqdconnection';
import Message from '../src/message';
import { ReaderRdy, ConnectionRdy } from '../src/readerrdy';


class StubNSQDConnection extends EventEmitter {
  constructor(nsqdHost, nsqdPort, topic, channel, requeueDelay,
    heartbeatInterval) {
    super(...arguments);
    this.nsqdHost = nsqdHost;
    this.nsqdPort = nsqdPort;
    this.topic = topic;
    this.channel = channel;
    this.requeueDelay = requeueDelay;
    this.heartbeatInterval = heartbeatInterval;
    this.conn =
      {localPort: 1};
    this.maxRdyCount = 2500;
    this.msgTimeout = 60 * 1000;
    this.maxMsgTimeout = 15 * 60 * 1000;
    this.rdyCounts = [];
  }

  id() {
    return `${this.nsqdHost}:${this.nsqdPort}`;
  }

  connect() {}
    // Empty
  destroy() {}
    // Empty
  setRdy(rdyCount) {
    return this.rdyCounts.push(rdyCount);
  }

  createMessage(msgId, msgTimestamp, attempts, msgBody) {

    let msgComponents = [msgId, msgTimestamp, attempts, msgBody];
    let msgArgs = msgComponents.concat([this.requeueDelay, this.msgTimeout, this.maxMsgTimeout]);
    let msg = new Message(...msgArgs);

    msg.on(Message.RESPOND, (responseType, wireData) => {
      if (responseType === Message.FINISH) {
        return this.emit(NSQDConnection.FINISHED);
      } else if (responseType === Message.REQUEUE) {
        return this.emit(NSQDConnection.REQUEUED);
      }
    }
    );
    msg.on(Message.BACKOFF, () => {
      return this.emit(NSQDConnection.BACKOFF);
    }
    );

    this.emit(NSQDConnection.MESSAGE, msg);
    return msg;
  }
}

let createNSQDConnection = function(id) {
  let conn = new StubNSQDConnection(`host${id}`, '4150', 'test', 'default', 60, 30);
  conn.conn.localPort = id;
  return conn;
};

describe('ConnectionRdy', function() {
  let [conn, spy, cRdy] = Array.from([null, null, null]);

  beforeEach(function() {
    conn = createNSQDConnection(1);
    spy = sinon.spy(conn, 'setRdy');
    cRdy = new ConnectionRdy(conn);
    return cRdy.start();
  });

  it('should register listeners on a connection', function() {
    conn = new NSQDConnection('localhost', 1234, 'test', 'test');
    let mock = sinon.mock(conn);
    mock.expects('on').withArgs(NSQDConnection.ERROR);
    mock.expects('on').withArgs(NSQDConnection.FINISHED);
    mock.expects('on').withArgs(NSQDConnection.MESSAGE);
    mock.expects('on').withArgs(NSQDConnection.REQUEUED);
    mock.expects('on').withArgs(NSQDConnection.READY);

    cRdy = new ConnectionRdy(conn);
    return mock.verify();
  });

  it('should have a connection RDY max of zero', () => cRdy.maxConnRdy.should.eql(0));

  it('should not increase RDY when connection RDY max has not been set', function() {
    // This bump should be a no-op
    cRdy.bump();
    cRdy.maxConnRdy.should.eql(0);
    return spy.called.should.not.be.ok;
  });

  it('should not allow RDY counts to be negative', function() {
    cRdy.setConnectionRdyMax(10);
    cRdy.setRdy(-1);

    return spy.notCalled.should.be.ok();
  });

  it('should not allow RDY counts to exceed the connection max', function() {
    cRdy.setConnectionRdyMax(10);
    cRdy.setRdy(9);
    cRdy.setRdy(10);
    cRdy.setRdy(20);

    spy.calledTwice.should.be.ok();
    spy.firstCall.args[0].should.eql(9);
    return spy.secondCall.args[0].should.eql(10);
  });

  it('should set RDY to max after initial bump', function() {
    cRdy.setConnectionRdyMax(3);
    cRdy.bump();

    return spy.firstCall.args[0].should.eql(3);
  });

  it('should keep RDY at max after 1+ bumps', function() {
    let i;
    cRdy.setConnectionRdyMax(3);
    for (i = 1; i <= 3; i++) {
      cRdy.bump();
    }

    cRdy.maxConnRdy.should.eql(3);
    return (() => {
      let result = [];
      for (i = 0, end = spy.callCount, asc = 0 <= end; asc ? i < end : i > end; asc ? i++ : i--) {
        var asc, end;
        result.push(should.ok(spy.getCall(i).args[0] <= 3));
      }
      return result;
    })();
  });

  it('should set RDY to zero from after first bump and then backoff', function() {
    cRdy.setConnectionRdyMax(3);
    cRdy.bump();
    cRdy.backoff();

    return spy.lastCall.args[0].should.eql(0);
  });

  it('should set RDY to zero after 1+ bumps and then a backoff', function() {
    cRdy.setConnectionRdyMax(3);
    cRdy.bump();
    cRdy.backoff();

    return spy.lastCall.args[0].should.eql(0);
  });

  it('should raise RDY when new connection RDY max is lower', function() {
    cRdy.setConnectionRdyMax(3);
    cRdy.bump();
    cRdy.setConnectionRdyMax(5);

    cRdy.maxConnRdy.should.eql(5);
    return spy.lastCall.args[0].should.eql(5);
  });

  it('should reduce RDY when new connection RDY max is higher', function() {
    cRdy.setConnectionRdyMax(3);
    cRdy.bump();
    cRdy.setConnectionRdyMax(2);

    cRdy.maxConnRdy.should.eql(2);
    return spy.lastCall.args[0].should.eql(2);
  });

  return it('should update RDY when 75% of previous RDY is consumed', function() {
    let msg;
    cRdy.setConnectionRdyMax(10);
    cRdy.bump();

    spy.firstCall.args[0].should.eql(10);

    for (let i = 1; i <= 7; i++) {
      msg = conn.createMessage(`${i}`, Date.now(), 0, `Message ${i}`);
      msg.finish();
      cRdy.bump();
    }

    spy.callCount.should.eql(1);

    msg = conn.createMessage('8', Date.now(), 0, 'Message 8');
    msg.finish();
    cRdy.bump();

    spy.callCount.should.eql(2);
    return spy.lastCall.args[0].should.eql(10);
  });
});


describe('ReaderRdy', function() {
  let readerRdy = null;

  beforeEach(() => readerRdy = new ReaderRdy(1, 128, 'topic/channel'));

  afterEach(() => readerRdy.close());

  it('should register listeners on a connection', function() {
    // Stub out creation of ConnectionRdy to ignore the events registered by
    // ConnectionRdy.
    sinon.stub(readerRdy, 'createConnectionRdy', () => ({on() {}}));
        // Empty

    let conn = createNSQDConnection(1);
    let mock = sinon.mock(conn);
    mock.expects('on').withArgs(NSQDConnection.CLOSED);
    mock.expects('on').withArgs(NSQDConnection.FINISHED);
    mock.expects('on').withArgs(NSQDConnection.REQUEUED);
    mock.expects('on').withArgs(NSQDConnection.BACKOFF);

    readerRdy.addConnection(conn);
    return mock.verify();
  });

  it('should be in the zero state until a new connection is READY', function() {
    let conn = createNSQDConnection(1);

    readerRdy.current_state_name.should.eql('ZERO');
    readerRdy.addConnection(conn);
    readerRdy.current_state_name.should.eql('ZERO');
    conn.emit(NSQDConnection.READY);
    return readerRdy.current_state_name.should.eql('MAX');
  });

  it('should be in the zero state if it loses all connections', function() {
    let conn = createNSQDConnection(1);

    readerRdy.addConnection(conn);
    conn.emit(NSQDConnection.READY);
    conn.emit(NSQDConnection.CLOSED);
    return readerRdy.current_state_name.should.eql('ZERO');
  });

  it('should evenly distribute RDY count across connections', function() {
    readerRdy = new ReaderRdy(100, 128, 'topic/channel');

    let conn1 = createNSQDConnection(1);
    let conn2 = createNSQDConnection(2);

    let setRdyStub1 = sinon.spy(conn1, 'setRdy');
    let setRdyStub2 = sinon.spy(conn2, 'setRdy');

    readerRdy.addConnection(conn1);
    conn1.emit(NSQDConnection.READY);

    setRdyStub1.lastCall.args[0].should.eql(100);

    readerRdy.addConnection(conn2);
    conn2.emit(NSQDConnection.READY);

    setRdyStub1.lastCall.args[0].should.eql(50);
    return setRdyStub2.lastCall.args[0].should.eql(50);
  });

  describe('low RDY conditions', function() {
    let assertAlternatingRdyCounts = function(conn1, conn2) {
      let minSize = Math.min(conn1.rdyCounts.length, conn2.rdyCounts.length);

      let zippedCounts = _.zip(conn1.rdyCounts.slice(-minSize),
        conn2.rdyCounts.slice(-minSize));

      // We expect the connection RDY counts to look like this:
      // conn 0: [1, 0, 1, 0]
      // conn 1: [0, 1, 0, 1]
      return (() => {
        let result = [];
        for (let [firstRdy, secondRdy] of Array.from(zippedCounts)) {
          result.push(should.ok((firstRdy + secondRdy) === 1));
        }
        return result;
      })();
    };

    it('should periodically redistribute RDY', function(done) {
      // Shortening the periodically `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      let connections = [1, 2].map((i) =>
        createNSQDConnection(i));

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      for (let conn of Array.from(connections)) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      }

      // Given the number of connections and the maxInFlight, we should be in low
      // RDY conditions.
      readerRdy.isLowRdy().should.eql(true);

      let checkRdyCounts = function() {
        assertAlternatingRdyCounts(...connections);
        return done();
      };

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      return setTimeout(checkRdyCounts, 50);
    });

    it('should handle the transition from normal', function(done) {
      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      let conn1 = createNSQDConnection(1);
      let conn2 = createNSQDConnection(2);

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      readerRdy.addConnection(conn1);
      conn1.emit(NSQDConnection.READY);

      readerRdy.isLowRdy().should.eql(false);

      let addConnection = function() {
        readerRdy.addConnection(conn2);
        conn2.emit(NSQDConnection.READY);

        // Given the number of connections and the maxInFlight, we should be in
        // low RDY conditions.
        return readerRdy.isLowRdy().should.eql(true);
      };

      // Add the 2nd connections after some duration to simulate a new nsqd being
      // discovered and connected.
      setTimeout(addConnection, 20);

      let checkRdyCounts = function() {
        assertAlternatingRdyCounts(conn1, conn2);
        return done();
      };

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      return setTimeout(checkRdyCounts, 40);
    });

    it('should handle the transition to normal conditions', function(done) {
      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      let connections = [1, 2].map((i) =>
        createNSQDConnection(i));

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      for (let conn of Array.from(connections)) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      }

      readerRdy.isLowRdy().should.eql(true);

      let removeConnection = function() {
        connections[1].emit(NSQDConnection.CLOSED);

        return setTimeout(checkNormal, 20);
      };

      var checkNormal = function() {
        readerRdy.isLowRdy().should.eql(false);
        should.ok(readerRdy.balanceId === null);

        readerRdy.connections[0].lastRdySent.should.eql(1);
        return done();
      };

      // Remove a connection after some period of time to get back to normal
      // conditions.
      return setTimeout(removeConnection, 20);
    });

    it('should move to normal conditions with connections in backoff', function(done) {
      /*
      1. Create two nsqd connections
      2. Close the 2nd connection when the first connection is in the BACKOFF
          state.
      3. Check to see if the 1st connection does get it's RDY count.
      */

      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      let connections = [1, 2].map((i) =>
        createNSQDConnection(i));

      for (let conn of Array.from(connections)) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      }

      readerRdy.isLowRdy().should.eql(true);

      let removeConnection = _.once(function() {
        connections[1].emit(NSQDConnection.CLOSED);
        return setTimeout(checkNormal, 30);
      });

      let removeOnBackoff = function() {
        let connRdy1 = readerRdy.connections[0];
        return connRdy1.on(ConnectionRdy.STATE_CHANGE, function() {
          if (connRdy1.statemachine.current_state_name === 'BACKOFF') {
            // If we don't do the connection CLOSED in the next tick, we remove
            // the connection immediately which leaves `@connections` within
            // `balance` in an inconsistent state which isn't possible normally.
            return setTimeout(removeConnection, 0);
          }
        });
      };

      var checkNormal = function() {
        readerRdy.isLowRdy().should.eql(false);
        should.ok(readerRdy.balanceId === null);
        readerRdy.connections[0].lastRdySent.should.eql(1);
        return done();
      };

      // Remove a connection after some period of time to get back to normal
      // conditions.
      return setTimeout(removeOnBackoff, 20);
    });


    it('should not exceed maxInFlight for long running message.', function(done) {
      // Shortening the periodica `balance` calls to every 10ms.
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      let connections = [1, 2].map((i) =>
        createNSQDConnection(i));

      for (var conn of Array.from(connections)) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      }

      // Handle the message but delay finishing the message so that several
      // balance calls happen and the check to ensure that RDY count is zero for
      // all connections.
      let handleMessage = function(msg) {
        let finish = function() {
          msg.finish();
          return done();
        };
        return setTimeout(finish, 40);
      };

      for (conn of Array.from(connections)) {
        conn.on(NSQDConnection.MESSAGE, handleMessage);
      }

      let sendMessageOnce = _.once(function() {
        connections[1].createMessage('1', Date.now(), new Buffer('test'));
        return setTimeout(checkRdyCount, 20);
      });

      // Send a message on the 2nd connection when we can. Only send the message
      // once so that we don't violate the maxInFlight count.
      let sendOnRdy = function() {
        let connRdy2 = readerRdy.connections[1];
        return connRdy2.on(ConnectionRdy.STATE_CHANGE, function() {
          if (['ONE', 'MAX'].includes(connRdy2.statemachine.current_state_name)) {
            return sendMessageOnce();
          }
        });
      };

      // When the message is in-flight, balance cannot give a RDY count out to
      // any of the connections.
      var checkRdyCount = function() {
        readerRdy.isLowRdy().should.eql(true);
        readerRdy.connections[0].lastRdySent.should.eql(0);
        return readerRdy.connections[1].lastRdySent.should.eql(0);
      };

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      return setTimeout(sendOnRdy, 20);
    });

    return it('should recover losing a connection with a message in-flight', function(done) {
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
      readerRdy = new ReaderRdy(1, 128, 'topic/channel', 0.01);

      let connections = [1, 2, 3, 4, 5].map((i) =>
        createNSQDConnection(i));

      // Add the connections and trigger the NSQDConnection event that tells
      // listeners that the connections are connected and ready for message flow.
      for (var conn of Array.from(connections)) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      }

      let handleMessage = function(msg) {
        let delayFinish = function() {
          msg.finish();
          return done();
        };

        setTimeout(closeConnection, 10);
        setTimeout(checkRdyCount, 30);
        return setTimeout(delayFinish, 50);
      };

      for (conn of Array.from(connections)) {
        conn.on(NSQDConnection.MESSAGE, handleMessage);
      }

      var closeConnection = _.once(() => connections[0].emit(NSQDConnection.CLOSED));

      let sendMessageOnce = _.once(() => connections[0].createMessage('1', Date.now(), new Buffer('test')));

      // Send a message on the 2nd connection when we can. Only send the message
      // once so that we don't violate the maxInFlight count.
      let sendOnRdy = function() {
        let connRdy = readerRdy.connections[0];
        return connRdy.on(ConnectionRdy.STATE_CHANGE, function() {
          if (['ONE', 'MAX'].includes(connRdy.statemachine.current_state_name)) {
            return sendMessageOnce();
          }
        });
      };

      // When the message is in-flight, balance cannot give a RDY count out to
      // any of the connections.
      var checkRdyCount = function() {
        readerRdy.isLowRdy().should.eql(true);

        let rdyCounts = Array.from(readerRdy.connections).map((connRdy) =>
          connRdy.lastRdySent);

        readerRdy.connections.length.should.eql(4);
        return should.ok(Array.from(rdyCounts).includes(1));
      };

      // We have to wait a small period of time for log events to occur since the
      // `balance` call is invoked perdiocally.
      return setTimeout(sendOnRdy, 10);
    });
  });

  describe('try', function() {
    it('should on completion of backoff attempt a single connection', function(done) {
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
      readerRdy = new ReaderRdy(100, 10, 'topic/channel', 0.01);

      let connections = [1, 2, 3, 4, 5].map((i) =>
        createNSQDConnection(i));

      for (let conn of Array.from(connections)) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      }

      let msg = connections[0].createMessage("1", Date.now(), 0,
        'Message causing a backoff');
      msg.requeue();

      let checkInBackoff = () =>
        Array.from(readerRdy.connections).map((connRdy) =>
          connRdy.statemachine.current_state_name.should.eql('BACKOFF'))
      ;

      setTimeout(checkInBackoff, 0);

      let afterBackoff = function() {
        let s;
        let states = Array.from(readerRdy.connections).map((connRdy) =>
          connRdy.statemachine.current_state_name);

        let ones = ((() => {
          let result = [];
          for (s of Array.from(states)) {             if (s === 'ONE') {
              result.push(s);
            }
          }
          return result;
        })());
        let backoffs = ((() => {
          let result1 = [];
          for (s of Array.from(states)) {             if (s === 'BACKOFF') {
              result1.push(s);
            }
          }
          return result1;
        })());

        ones.length.should.eql(1);
        backoffs.length.should.eql(4);
        return done();
      };

      // Add 50ms to the delay so that we're confident that the event fired.
      let delay = readerRdy.backoffTimer.getInterval().plus(0.05);
      return setTimeout(afterBackoff, new Number(delay.valueOf()) * 1000);
    });

    return it('should after backoff with a successful message go to MAX', function(done) {
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
      readerRdy = new ReaderRdy(100, 1, 'topic/channel', 0.01);

      let connections = [1, 2, 3, 4, 5].map((i) =>
        createNSQDConnection(i));

      for (let conn of Array.from(connections)) {
        readerRdy.addConnection(conn);
        conn.emit(NSQDConnection.READY);
      }

      let msg = connections[0].createMessage("1", Date.now(), 0,
        'Message causing a backoff');
      msg.requeue();

      let afterBackoff = function() {
        var [connRdy] = Array.from((() => {
          let result = [];
          for (connRdy of Array.from(readerRdy.connections)) {
            let item;
            if (connRdy.statemachine.current_state_name === 'ONE') {
              item = connRdy;
            }
            result.push(item);
          }
          return result;
        })());

        msg = connRdy.conn.createMessage("1", Date.now(), 0, 'Success');
        msg.finish();

        let verifyMax = function() {
          let states = (() => {
            let result1 = [];
            for (connRdy of Array.from(readerRdy.connections)) {
              result1.push(connRdy.statemachine.current_state_name);
            }
            return result1;
          })();

          let max = (Array.from(states).filter((s) => ['ONE', 'MAX'].includes(s)).map((s) => s));

          max.length.should.eql(5);
          should.ok(Array.from(states).includes('MAX'));
          return done();
        };

        return setTimeout(verifyMax, 0);
      };

      let delay = readerRdy.backoffTimer.getInterval() + 100;
      return setTimeout(afterBackoff, delay * 1000);
    });
  });

  return describe('pause / unpause', function() {
    beforeEach(function() {
      // Shortening the periodic `balance` calls to every 10ms. Changing the
      // max backoff duration to 1 sec.
      readerRdy = new ReaderRdy(100, 1, 'topic/channel', 0.01);

      let connections = [1, 2, 3, 4, 5].map((i) =>
        createNSQDConnection(i));

      return Array.from(connections).map((conn) =>
        (readerRdy.addConnection(conn),
        conn.emit(NSQDConnection.READY)));
    });

    it('should drop ready count to zero on all connections when paused', function() {
      readerRdy.pause();
      readerRdy.current_state_name.should.eql('PAUSE');

      return Array.from(readerRdy.connections).map((conn) =>
        conn.lastRdySent.should.eql(0));
    });

    it('should unpause by trying one', function() {
      readerRdy.pause();
      readerRdy.unpause();

      return readerRdy.current_state_name.should.eql('TRY_ONE');
    });

    return it('should update the value of @isPaused when paused', function() {
      readerRdy.pause();
      readerRdy.isPaused().should.ok;
      
      readerRdy.unpause();
      return readerRdy.isPaused().should.eql(false);
    });
  });
});
