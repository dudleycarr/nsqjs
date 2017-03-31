import BackoffTimer from './backofftimer';
import Debug from 'debug';
import NodeState from 'node-state';
import RoundRobinList from './roundrobinlist';
import _ from 'underscore';
import { EventEmitter } from 'events';
import { NSQDConnection } from './nsqdconnection';

/*
Maintains the RDY and in-flight counts for a nsqd connection. ConnectionRdy
ensures that the RDY count will not exceed the max set for this connection.
The max for the connection can be adjusted at any time.

Usage:

connRdy = ConnectionRdy conn
connRdy.setConnectionRdyMax 10

conn.on 'message', ->
  * On a successful message, bump up the RDY count for this connection.
  connRdy.raise 'bump'
conn.on 'requeue', ->
  * We're backing off when we encounter a requeue. Wait 5 seconds to try
  * again.
  connRdy.raise 'backoff'
  setTimeout (-> connRdy.raise 'bump'), 5000
*/
class ConnectionRdy extends EventEmitter {
  static initClass() {
    // Events emitted by ConnectionRdy
    this.READY = 'ready';
    this.STATE_CHANGE = 'statechange';
  }

  constructor(conn) {
    super(...arguments);
    this.conn = conn;
    const readerId = `${this.conn.topic}/${this.conn.channel}`;
    const connId = `${this.conn.id().replace(':', '/')}`;
    this.debug = Debug(`nsqjs:reader:${readerId}:rdy:conn:${connId}`);

    this.maxConnRdy = 0; // The absolutely maximum the RDY count can be per conn.
    this.inFlight = 0; // The num. messages currently in-flight for this conn.
    this.lastRdySent = 0; // The RDY value last sent to the server.
    this.availableRdy = 0; // The RDY count remaining on the server for this conn.
    this.statemachine = new ConnectionRdyState(this);

    this.conn.on(NSQDConnection.ERROR, err => this.log(err));
    this.conn.on(NSQDConnection.MESSAGE, () => {
      if (this.idleId != null) {
        clearTimeout(this.idleId);
      }
      this.idleId = null;
      this.inFlight += 1;
      this.availableRdy -= 1;
    });
    this.conn.on(NSQDConnection.FINISHED, () => this.inFlight--);
    this.conn.on(NSQDConnection.REQUEUED, () => this.inFlight--);
    this.conn.on(NSQDConnection.READY, () => this.start());
  }

  close() {
    return this.conn.destroy();
  }

  name() {
    return String(this.conn.conn.localPort);
  }

  start() {
    this.statemachine.start();
    return this.emit(ConnectionRdy.READY);
  }

  setConnectionRdyMax(maxConnRdy) {
    this.log(`setConnectionRdyMax ${maxConnRdy}`);
    // The RDY count for this connection should not exceed the max RDY count
    // configured for this nsqd connection.
    this.maxConnRdy = Math.min(maxConnRdy, this.conn.maxRdyCount);
    return this.statemachine.raise('adjustMax');
  }

  bump() {
    return this.statemachine.raise('bump');
  }

  backoff() {
    return this.statemachine.raise('backoff');
  }

  isStarved() {
    if (!(this.inFlight <= this.maxConnRdy)) {
      throw new Error('isStarved check is failing');
    }
    return this.inFlight === this.lastRdySent;
  }

  setRdy(rdyCount) {
    this.log(`RDY ${rdyCount}`);
    if (rdyCount < 0 || rdyCount > this.maxConnRdy) return;

    this.conn.setRdy(rdyCount);
    this.availableRdy = (this.lastRdySent = rdyCount);
  }

  log(message) {
    if (message) {
      return this.debug(message);
    }
  }
}
ConnectionRdy.initClass();

class ConnectionRdyState extends NodeState {
  static initClass() {
    this.prototype.states = {
      INIT: {
        // RDY is implicitly zero
        bump() {
          if (this.connRdy.maxConnRdy > 0) {
            return this.goto('MAX');
          }
        },
        backoff() {}, // No-op
        adjustMax() {},
      }, // No-op

      BACKOFF: {
        Enter() {
          return this.connRdy.setRdy(0);
        },
        bump() {
          if (this.connRdy.maxConnRdy > 0) return this.goto('ONE');
        },
        backoff() {}, // No-op
        adjustMax() {},
      }, // No-op

      ONE: {
        Enter() {
          return this.connRdy.setRdy(1);
        },
        bump() {
          return this.goto('MAX');
        },
        backoff() {
          return this.goto('BACKOFF');
        },
        adjustMax() {},
      }, // No-op

      MAX: {
        Enter() {
          return this.connRdy.setRdy(this.connRdy.maxConnRdy);
        },
        bump() {
          // No need to keep setting the RDY count for versions of NSQD >= 0.3.0.
          const version = this.connRdy.conn != null
            ? this.connRdy.conn.nsqdVersion
            : undefined;
          if (!version || version.split('.') < [0, 3, 0]) {
            if (this.connRdy.availableRdy <= this.connRdy.lastRdySent * 0.25) {
              return this.connRdy.setRdy(this.connRdy.maxConnRdy);
            }
          }
        },
        backoff() {
          return this.goto('BACKOFF');
        },
        adjustMax() {
          this.log(`adjustMax RDY ${this.connRdy.maxConnRdy}`);
          return this.connRdy.setRdy(this.connRdy.maxConnRdy);
        },
      },
    };

    this.prototype.transitions = {
      '*': {
        '*': function(data, callback) {
          this.log();
          callback(data);
          return this.connRdy.emit(ConnectionRdy.STATE_CHANGE);
        },
      },
    };
  }

  constructor(connRdy) {
    super({
      autostart: false,
      initial_state: 'INIT',
      sync_goto: true,
    });

    this.connRdy = connRdy;
  }

  log(message) {
    this.connRdy.debug(this.current_state_name);
    if (message) {
      return this.connRdy.debug(message);
    }
  }
}
ConnectionRdyState.initClass();

/*
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
*/

let READER_COUNT = 0;

class ReaderRdy extends NodeState {
  static initClass() {
    /*
    The following events results in transitions in the ReaderRdy state machine:
    1. Adding the first connection
    2. Remove the last connections
    3. Finish event from message handling
    4. Backoff event from message handling
    5. Backoff timeout
    */
    this.prototype.states = {
      ZERO: {
        Enter() {
          if (this.backoffId) {
            return clearTimeout(this.backoffId);
          }
        },
        backoff() {}, // No-op
        success() {}, // No-op
        try() {}, // No-op
        pause() {
          // No-op
          return this.goto('PAUSE');
        },
        unpause() {},
      }, // No-op

      PAUSE: {
        Enter() {
          return this.connections.map(conn => conn.backoff());
        },
        backoff() {}, // No-op
        success() {}, // No-op
        try() {}, // No-op
        pause() {}, // No-op
        unpause() {
          return this.goto('TRY_ONE');
        },
      },

      TRY_ONE: {
        Enter() {
          return this.try();
        },
        backoff() {
          return this.goto('BACKOFF');
        },
        success(connectionRdy) {
          this.backoffTimer.success();
          this.onMessageSuccess(connectionRdy);
          return this.goto('MAX');
        },
        try() {}, // No-op
        pause() {
          return this.goto('PAUSE');
        },
        unpause() {},
      }, // No-op

      MAX: {
        Enter() {
          this.balance();
          return this.bump();
        },
        backoff() {
          return this.goto('BACKOFF');
        },
        success(connectionRdy) {
          this.backoffTimer.success();
          return this.onMessageSuccess(connectionRdy);
        },
        try() {}, // No-op
        pause() {
          return this.goto('PAUSE');
        },
        unpause() {},
      }, // No-op

      BACKOFF: {
        Enter() {
          this.backoffTimer.failure();
          return this.backoff();
        },
        backoff() {
          this.backoffTimer.failure();
          return this.backoff();
        },
        success() {}, // No-op
        try() {
          return this.goto('TRY_ONE');
        },
        pause() {
          return this.goto('PAUSE');
        },
        unpause() {},
      }, // No-op
    };

    this.prototype.transitions = {
      '*': {
        '*': function(data, callback) {
          this.log();
          return callback(data);
        },
      },
    };
  }

  // Class method
  static getId() {
    READER_COUNT += 1;
    return READER_COUNT - 1;
  }

  /*
  Parameters:
  - maxInFlight        : Maximum number of messages in-flight across all
                           connections.
  - maxBackoffDuration : The longest amount of time (secs) for a backoff event.
  - readerId           : The descriptive id for the Reader
  - lowRdyTimeout      : Time (secs) to rebalance RDY count among connections
                           during low RDY conditions.
  */
  constructor(maxInFlight, maxBackoffDuration, readerId, lowRdyTimeout) {
    if (lowRdyTimeout == null) {
      lowRdyTimeout = 1.5;
    }

    super({
      autostart: true,
      initial_state: 'ZERO',
      sync_goto: true,
    });

    this.maxInFlight = maxInFlight;
    this.maxBackoffDuration = maxBackoffDuration;
    this.readerId = readerId;
    this.lowRdyTimeout = lowRdyTimeout;
    this.debug = Debug(`nsqjs:reader:${this.readerId}:rdy`);

    this.id = ReaderRdy.getId();
    this.backoffTimer = new BackoffTimer(0, this.maxBackoffDuration);
    this.backoffId = null;
    this.balanceId = null;
    this.connections = [];
    this.roundRobinConnections = new RoundRobinList([]);
  }

  close() {
    clearTimeout(this.backoffId);
    clearTimeout(this.balanceId);
    return this.connections.map(conn => conn.close());
  }

  pause() {
    return this.raise('pause');
  }

  unpause() {
    return this.raise('unpause');
  }

  isPaused() {
    return this.current_state_name === 'PAUSE';
  }

  log(message) {
    if (this.debug) {
      this.debug(this.current_state_name);

      if (message) return this.debug(message);
    }
  }

  isStarved() {
    if (_.isEmpty(this.connections)) {
      return false;
    }
    return !_.isEmpty(
      (() => {
        const result = [];
        for (const c of Array.from(this.connections)) {
          let item;
          if (c.isStarved()) {
            item = c;
          }
          result.push(item);
        }
        return result;
      })()
    );
  }

  createConnectionRdy(conn) {
    return new ConnectionRdy(conn);
  }

  isLowRdy() {
    return this.maxInFlight < this.connections.length;
  }

  onMessageSuccess(connectionRdy) {
    if (!this.isPaused()) {
      if (this.isLowRdy()) {
        // Balance the RDY count amoung existing connections given the low RDY
        // condition.
        return this.balance();
      }
      // Restore RDY count for connection to the connection max.
      return connectionRdy.bump();
    }
  }

  addConnection(conn) {
    const connectionRdy = this.createConnectionRdy(conn);

    conn.on(NSQDConnection.CLOSED, () => {
      this.removeConnection(connectionRdy);
      this.balance();
    });

    conn.on(NSQDConnection.FINISHED, () =>
      this.raise('success', connectionRdy));

    conn.on(NSQDConnection.REQUEUED, () => {
      // Since there isn't a guaranteed order for the REQUEUED and BACKOFF
      // events, handle the case when we handle BACKOFF and then REQUEUED.
      if (this.current_state_name !== 'BACKOFF' && !this.isPaused()) {
        return connectionRdy.bump();
      }
    });

    conn.on(NSQDConnection.BACKOFF, () => this.raise('backoff'));

    connectionRdy.on(ConnectionRdy.READY, () => {
      this.connections.push(connectionRdy);
      this.roundRobinConnections.add(connectionRdy);

      this.balance();
      if (this.current_state_name === 'ZERO') {
        return this.goto('MAX');
      } else if (['TRY_ONE', 'MAX'].includes(this.current_state_name)) {
        return connectionRdy.bump();
      }
    });
  }

  removeConnection(conn) {
    this.connections.splice(this.connections.indexOf(conn), 1);
    this.roundRobinConnections.remove(conn);

    if (this.connections.length === 0) {
      return this.goto('ZERO');
    }
  }

  bump() {
    return Array.from(this.connections).map(conn => conn.bump());
  }

  try() {
    return this.balance();
  }

  backoff() {
    this.connections.forEach(conn => conn.backoff());

    if (this.backoffId) {
      clearTimeout(this.backoffId);
    }

    const onTimeout = () => {
      this.log('Backoff done');
      return this.raise('try');
    };

    // Convert from the BigNumber representation to Number.
    const delay = Number(this.backoffTimer.getInterval().valueOf()) * 1000;
    this.backoffId = setTimeout(onTimeout, delay);
    return this.log(`Backoff for ${delay}`);
  }

  inFlight() {
    const add = (previous, conn) => previous + conn.inFlight;
    return this.connections.reduce(add, 0);
  }

  /*
  Evenly or fairly distributes RDY count based on the maxInFlight across
  all nsqd connections.
  */
  balance() {
    /*
    In the perverse situation where there are more connections than max in
    flight, we do the following:

    There is a sliding window where each of the connections gets a RDY count
    of 1. When the connection has processed it's single message, then the RDY
    count is distributed to the next waiting connection. If the connection
    does nothing with it's RDY count, then it should timeout and give it's
    RDY count to another connection.
    */

    this.log('balance');

    if (this.balanceId != null) {
      clearTimeout(this.balanceId);
      this.balanceId = null;
    }

    const max = (() => {
      switch (this.current_state_name) {
        case 'TRY_ONE':
          return 1;
        case 'PAUSE':
          return 0;
        default:
          return this.maxInFlight;
      }
    })();

    const perConnectionMax = Math.floor(max / this.connections.length);

    // Low RDY and try conditions
    if (perConnectionMax === 0) {
      // Backoff on all connections. In-flight messages from connections
      // will still be processed.
      this.connections.forEach(conn => conn.backoff());

      // Distribute available RDY count to the connections next in line.
      this.roundRobinConnections.next(max - this.inFlight()).forEach(conn => {
        conn.setConnectionRdyMax(1);
        conn.bump();
      });

      // Rebalance periodically. Needed when no messages are received.
      this.balanceId = setTimeout(
        () => {
          this.balance();
        },
        this.lowRdyTimeout * 1000
      );
    } else {
      let rdyRemainder = this.maxInFlight % this.connectionsLength;
      return (() => {
        const result = [];
        for (
          let i = 0, end = this.connections.length, asc = end >= 0;
          asc ? i < end : i > end;
          asc ? i++ : i--
        ) {
          let connMax = perConnectionMax;

          // Distribute the remainder RDY count evenly between the first
          // n connections.
          if (rdyRemainder > 0) {
            connMax += 1;
            rdyRemainder -= 1;
          }

          this.connections[i].setConnectionRdyMax(connMax);
          result.push(this.connections[i].bump());
        }
        return result;
      })();
    }
  }
}
ReaderRdy.initClass();

export { ReaderRdy, ConnectionRdy };
