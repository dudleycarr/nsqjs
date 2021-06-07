const {EventEmitter} = require('events')

const NodeState = require('node-state')
const _ = require('lodash')
const debug = require('./debug')

const BackoffTimer = require('./backofftimer')
const RoundRobinList = require('./roundrobinlist')
const {NSQDConnection} = require('./nsqdconnection')

/**
 * Maintains the RDY and in-flight counts for a nsqd connection. ConnectionRdy
 * ensures that the RDY count will not exceed the max set for this connection.
 * The max for the connection can be adjusted at any time.
 *
 * Usage:
 *   const connRdy = ConnectionRdy(conn);
 *   const connRdy.setConnectionRdyMax(10);
 *
 *   // On a successful message, bump up the RDY count for this connection.
 *   conn.on('message', () => connRdy.raise('bump'));
 *
 *   // We're backing off when we encounter a requeue. Wait 5 seconds to try
 *   // again.
 *   conn.on('requeue', () => connRdy.raise('backoff'));
 *   setTimeout(() => connRdy.raise (bump'), 5000);
 */
class ConnectionRdy extends EventEmitter {
  // Events emitted by ConnectionRdy
  static get READY() {
    return 'ready'
  }
  static get STATE_CHANGE() {
    return 'statechange'
  }

  /**
   * Instantiates a new ConnectionRdy event emitter.
   *
   * @param  {Object} conn
   * @constructor
   */
  constructor(conn, ...args) {
    super(conn, ...args)
    this.conn = conn
    const readerId = `${this.conn.topic}/${this.conn.channel}`
    const connId = `${this.conn.id().replace(':', '/')}`
    this.debug = debug(`nsqjs:reader:${readerId}:rdy:conn:${connId}`)

    this.maxConnRdy = 0 // The absolutely maximum the RDY count can be per conn.
    this.inFlight = 0 // The num. messages currently in-flight for this conn.
    this.lastRdySent = 0 // The RDY value last sent to the server.
    this.availableRdy = 0 // The RDY count remaining on the server for this conn.
    this.statemachine = new ConnectionRdyState(this)

    this.conn.on(NSQDConnection.ERROR, (err) => this.log(err))
    this.conn.on(NSQDConnection.MESSAGE, () => {
      if (this.idleId != null) {
        clearTimeout(this.idleId)
      }
      this.idleId = null
      this.inFlight += 1
      this.availableRdy -= 1
    })
    this.conn.on(NSQDConnection.FINISHED, () => this.inFlight--)
    this.conn.on(NSQDConnection.REQUEUED, () => this.inFlight--)
    this.conn.on(NSQDConnection.READY, () => this.start())
  }

  /**
   * Close the reader ready connection.
   */
  close() {
    this.conn.close()
  }

  /**
   * Return the name of the local port connection.
   *
   * @return {String}
   */
  name() {
    return String(this.conn.conn.localPort)
  }

  /**
   * Emit that the connection is ready.
   *
   * @return {Boolean} Returns true if the event had listeners, false otherwise.
   */
  start() {
    this.statemachine.start()
    return this.emit(ConnectionRdy.READY)
  }

  /**
   * Initialize the max number of connections ready.
   *
   * @param {Number} maxConnRdy
   */
  setConnectionRdyMax(maxConnRdy) {
    this.log(`setConnectionRdyMax ${maxConnRdy}`)
    // The RDY count for this connection should not exceed the max RDY count
    // configured for this nsqd connection.
    this.maxConnRdy = Math.min(maxConnRdy, this.conn.maxRdyCount)
    this.statemachine.raise('adjustMax')
  }

  /**
   * Raises a `BUMP` event.
   */
  bump() {
    this.statemachine.raise('bump')
  }

  /**
   * Raises a `BACKOFF` event.
   */
  backoff() {
    this.statemachine.raise('backoff')
  }

  /**
   * Used to identify when buffered messages should be processed
   * and responded to.
   *
   * @return {Boolean} [description]
   */
  isStarved() {
    if (!(this.inFlight <= this.maxConnRdy)) {
      throw new Error('isStarved check is failing')
    }
    return this.inFlight === this.lastRdySent
  }

  /**
   * Assign the number of readers available.
   *
   * @param {Number} rdyCount
   */
  setRdy(rdyCount) {
    this.log(`RDY ${rdyCount}`)
    if (rdyCount < 0 || rdyCount > this.maxConnRdy) return

    this.conn.setRdy(rdyCount)
    this.availableRdy = this.lastRdySent = rdyCount
  }

  /**
   * @param  {String} message
   * @return {String}
   */
  log(message) {
    if (message) return this.debug(message)
  }
}

/**
 * Internal statemachine used handle the various reader ready states.
 * @type {NodeState}
 */
class ConnectionRdyState extends NodeState {
  /**
   * Instantiates a new ConnectionRdyState.
   *
   * @param  {Object} connRdy reader connection
   * @constructor
   */
  constructor(connRdy) {
    super({
      autostart: false,
      initial_state: 'INIT',
      sync_goto: true,
    })

    this.connRdy = connRdy
  }

  /**
   * Utility function to log a message through debug.
   *
   * @param  {Message} message
   * @return {String}
   */
  log(message) {
    this.connRdy.debug(this.current_state_name)
    if (message) {
      return this.connRdy.debug(message)
    }
  }
}

ConnectionRdyState.prototype.states = {
  INIT: {
    // RDY is implicitly zero
    bump() {
      if (this.connRdy.maxConnRdy > 0) {
        return this.goto('MAX')
      }
    },
    backoff() {}, // No-op
    adjustMax() {},
  }, // No-op

  BACKOFF: {
    Enter() {
      return this.connRdy.setRdy(0)
    },
    bump() {
      if (this.connRdy.maxConnRdy > 0) return this.goto('ONE')
    },
    backoff() {}, // No-op
    adjustMax() {},
  }, // No-op

  ONE: {
    Enter() {
      return this.connRdy.setRdy(1)
    },
    bump() {
      return this.goto('MAX')
    },
    backoff() {
      return this.goto('BACKOFF')
    },
    adjustMax() {},
  }, // No-op

  MAX: {
    Enter() {
      return this.connRdy.setRdy(this.connRdy.maxConnRdy)
    },
    bump() {
      // No need to keep setting the RDY count for versions of NSQD >= 0.3.0.
      const version =
        this.connRdy.conn != null ? this.connRdy.conn.nsqdVersion : undefined
      if (!version || version.split('.') < [0, 3, 0]) {
        if (this.connRdy.availableRdy <= this.connRdy.lastRdySent * 0.25) {
          return this.connRdy.setRdy(this.connRdy.maxConnRdy)
        }
      }
    },
    backoff() {
      return this.goto('BACKOFF')
    },
    adjustMax() {
      this.log(`adjustMax RDY ${this.connRdy.maxConnRdy}`)
      return this.connRdy.setRdy(this.connRdy.maxConnRdy)
    },
  },
}

ConnectionRdyState.prototype.transitions = {
  '*': {
    '*': function (data, callback) {
      this.log()
      callback(data)
      return this.connRdy.emit(ConnectionRdy.STATE_CHANGE)
    },
  },
}

/**
 * Usage:
 *   const backoffTime = 90;
 *   const heartbeat = 30;
 *
 *   const [topic, channel] = ['sample', 'default'];
 *   const [host1, port1] = ['127.0.0.1', '4150'];
 *   const c1 = new NSQDConnection(host1, port1, topic, channel,
 *     backoffTime, heartbeat);
 *
 *   const readerRdy = new ReaderRdy(1, 128);
 *   readerRdy.addConnection(c1);
 *
 *   const message = (msg) => {
 *     console.log(`Callback [message]: ${msg.attempts}, ${msg.body.toString()}1);
 *     if (msg.attempts >= 5) {
 *       msg.finish();
 *       return;
 *     }
 *
 *     if (msg.body.toString() === 'requeue')
 *       msg.requeue();
 *     else
 *       msg.finish();
 *   }
 *
 *   const discard = (msg) => {
 *     console.log(`Giving up on this message: ${msg.id}`);
 *     msg.finish();
 *   }
 *
 *   c1.on(NSQDConnection.MESSAGE, message);
 *   c1.connect();
 */
let READER_COUNT = 0

/**
 * ReaderRdy statemachine.
 * @type {[type]}
 */
class ReaderRdy extends NodeState {
  /**
   * Generates a new ID for a reader connection.
   *
   * @return {Number}
   */
  static getId() {
    return READER_COUNT++
  }

  /**
   * @constructor
   * @param  {Number} maxInFlight Maximum number of messages in-flight
   *   across all connections.
   * @param  {Number} maxBackoffDuration  The longest amount of time (secs)
   *   for a backoff event.
   * @param  {Number} readerId            The descriptive id for the Reader
   * @param  {Number} [lowRdyTimeout=1.5] Time (milliseconds) to rebalance RDY
   *   count among connections
   */
  constructor(maxInFlight, maxBackoffDuration, readerId, lowRdyTimeout = 50) {
    super({
      autostart: true,
      initial_state: 'ZERO',
      sync_goto: true,
    })

    this.maxInFlight = maxInFlight
    this.maxBackoffDuration = maxBackoffDuration
    this.readerId = readerId
    this.lowRdyTimeout = lowRdyTimeout
    this.debug = debug(`nsqjs:reader:${this.readerId}:rdy`)

    this.id = ReaderRdy.getId()
    this.backoffTimer = new BackoffTimer(0, this.maxBackoffDuration)
    this.backoffId = null
    this.balanceId = null
    this.connections = []
    this.roundRobinConnections = new RoundRobinList([])
    this.isClosed = false
  }

  /**
   * Close all reader connections.
   *
   * @return {Array} The closed connections.
   */
  close() {
    this.isClosed = true
    clearTimeout(this.backoffId)
    clearTimeout(this.balanceId)
    return _.clone(this.connections).map((conn) => conn.close())
  }

  /**
   * Raise a `PAUSE` event.
   */
  pause() {
    this.raise('pause')
  }

  /**
   * Raise a `UNPAUSE` event.
   */
  unpause() {
    this.raise('unpause')
  }

  /**
   * Indicates if a the reader ready connection has been paused.
   *
   * @return {Boolean}
   */
  isPaused() {
    return this.current_state_name === 'PAUSE'
  }

  /**
   * @param  {String} message
   * @return {String}
   */
  log(message) {
    if (this.debug) {
      this.debug(this.current_state_name)

      if (message) return this.debug(message)
    }
  }

  /**
   * Used to identify when buffered messages should be processed
   * and responded to.
   *
   * @return {Boolean} [description]
   */
  isStarved() {
    if (_.isEmpty(this.connections)) return false

    return this.connections.filter((conn) => conn.isStarved()).length > 0
  }

  /**
   * Creates a new ConnectionRdy statemachine.
   * @param  {Object} conn
   * @return {ConnectionRdy}
   */
  createConnectionRdy(conn) {
    return new ConnectionRdy(conn)
  }

  /**
   * Indicates if a producer is in a state where RDY counts are re-distributed.
   * @return {Boolean}
   */
  isLowRdy() {
    return this.maxInFlight < this.connections.length
  }

  /**
   * Message success handler.
   *
   * @param  {ConnectionRdy} connectionRdy
   */
  onMessageSuccess(connectionRdy) {
    if (!this.isPaused()) {
      if (this.isLowRdy()) {
        // Balance the RDY count amoung existing connections given the
        // low RDY condition.
        this.balance()
      } else {
        // Restore RDY count for connection to the connection max.
        connectionRdy.bump()
      }
    }
  }

  /**
   * Add a new connection to the pool.
   *
   * @param {Object} conn
   */
  addConnection(conn) {
    const connectionRdy = this.createConnectionRdy(conn)

    conn.on(NSQDConnection.CLOSED, () => {
      this.removeConnection(connectionRdy)
      this.balance()
    })

    conn.on(NSQDConnection.FINISHED, () => this.raise('success', connectionRdy))

    conn.on(NSQDConnection.REQUEUED, () => {
      // Since there isn't a guaranteed order for the REQUEUED and BACKOFF
      // events, handle the case when we handle BACKOFF and then REQUEUED.
      if (this.current_state_name !== 'BACKOFF' && !this.isPaused()) {
        connectionRdy.bump()
      }
    })

    conn.on(NSQDConnection.BACKOFF, () => this.raise('backoff'))

    connectionRdy.on(ConnectionRdy.READY, () => {
      // Aborting the connection. ReaderRdy received a close while the
      //   nsqdConnection was still being established.
      if (this.isClosed) {
        conn.close()
        return
      }

      this.connections.push(connectionRdy)
      this.roundRobinConnections.add(connectionRdy)

      this.balance()
      if (this.current_state_name === 'ZERO') {
        this.goto('MAX')
      } else if (['TRY_ONE', 'MAX'].includes(this.current_state_name)) {
        connectionRdy.bump()
      }
    })
  }

  /**
   * Remove a connection from the pool.
   *
   * @param  {Object} conn
   */
  removeConnection(conn) {
    this.connections.splice(this.connections.indexOf(conn), 1)
    this.roundRobinConnections.remove(conn)

    if (this.connections.length === 0) {
      this.goto('ZERO')
    }
  }

  /**
   * Raise a `BUMP` event for each connection in the pool.
   *
   * @return {Array} The bumped connections
   */
  bump() {
    return this.connections.map((conn) => conn.bump())
  }

  /**
   * Try to balance the connection pool.
   */
  try() {
    this.balance()
  }

  /**
   * Raise a `BACKOFF` event for each connection in the pool.
   */
  backoff() {
    this.connections.forEach((conn) => conn.backoff())

    if (this.backoffId) {
      clearTimeout(this.backoffId)
    }

    const onTimeout = () => {
      this.log('Backoff done')
      this.raise('try')
    }

    const delay = this.backoffTimer.getInterval() * 1000
    this.backoffId = setTimeout(onTimeout, delay)
    this.log(`Backoff for ${delay}`)
  }

  /**
   * Return the number of connections inflight.
   *
   * @return {Number}
   */
  inFlight() {
    const add = (previous, conn) => previous + conn.inFlight
    return this.connections.reduce(add, 0)
  }

  /**
   * The max connections readily available.
   *
   * @return {Number}
   */
  maxConnectionsRdy() {
    switch (this.current_state_name) {
      case 'TRY_ONE':
        return 1
      case 'PAUSE':
        return 0
      default:
        return this.maxInFlight
    }
  }

  /**
   * Evenly or fairly distributes RDY count based on the maxInFlight across
   * all nsqd connections.
   *
   * In the perverse situation where there are more connections than max in
   * flight, we do the following:
   *
   * There is a sliding window where each of the connections gets a RDY count
   * of 1. When the connection has processed it's single message, then
   * the RDY count is distributed to the next waiting connection. If
   * the connection does nothing with it's RDY count, then it should
   * timeout and give it's RDY count to another connection.
   */
  balance() {
    this.log('balance')

    if (this.balanceId != null) {
      clearTimeout(this.balanceId)
      this.balanceId = null
    }

    const max = this.maxConnectionsRdy()
    const perConnectionMax = Math.floor(max / this.connections.length)

    // Low RDY and try conditions
    if (perConnectionMax === 0) {
      /**
       * Backoff on all connections. In-flight messages from
       * connections will still be processed.
       */
      this.connections.forEach((conn) => conn.backoff())

      // Distribute available RDY count to the connections next in line.
      this.roundRobinConnections.next(max - this.inFlight()).forEach((conn) => {
        conn.setConnectionRdyMax(1)
        conn.bump()
      })

      // Rebalance periodically. Needed when no messages are received.
      this.balanceId = setTimeout(() => {
        this.balance()
      }, this.lowRdyTimeout)
    } else {
      let rdyRemainder = this.maxInFlight % this.connectionsLength
      this.connections.forEach((c) => {
        let connMax = perConnectionMax

        /**
         * Distribute the remainder RDY count evenly between the first
         * n connections.
         */
        if (rdyRemainder > 0) {
          connMax += 1
          rdyRemainder -= 1
        }

        c.setConnectionRdyMax(connMax)
        c.bump()
      })
    }
  }
}

/**
 * The following events results in transitions in the ReaderRdy state machine:
 * 1. Adding the first connection
 * 2. Remove the last connections
 * 3. Finish event from message handling
 * 4. Backoff event from message handling
 * 5. Backoff timeout
 */
ReaderRdy.prototype.states = {
  ZERO: {
    Enter() {
      if (this.backoffId) {
        return clearTimeout(this.backoffId)
      }
    },
    backoff() {}, // No-op
    success() {}, // No-op
    try() {}, // No-op
    pause() {
      // No-op
      return this.goto('PAUSE')
    },
    unpause() {},
  }, // No-op

  PAUSE: {
    Enter() {
      return this.connections.map((conn) => conn.backoff())
    },
    backoff() {}, // No-op
    success() {}, // No-op
    try() {}, // No-op
    pause() {}, // No-op
    unpause() {
      return this.goto('TRY_ONE')
    },
  },

  TRY_ONE: {
    Enter() {
      return this.try()
    },
    backoff() {
      return this.goto('BACKOFF')
    },
    success(connectionRdy) {
      this.backoffTimer.success()
      this.onMessageSuccess(connectionRdy)
      return this.goto('MAX')
    },
    try() {}, // No-op
    pause() {
      return this.goto('PAUSE')
    },
    unpause() {},
  }, // No-op

  MAX: {
    Enter() {
      this.balance()
      return this.bump()
    },
    backoff() {
      return this.goto('BACKOFF')
    },
    success(connectionRdy) {
      this.backoffTimer.success()
      return this.onMessageSuccess(connectionRdy)
    },
    try() {}, // No-op
    pause() {
      return this.goto('PAUSE')
    },
    unpause() {},
  }, // No-op

  BACKOFF: {
    Enter() {
      this.backoffTimer.failure()
      return this.backoff()
    },
    backoff() {}, // No-op
    success() {}, // No-op
    try() {
      return this.goto('TRY_ONE')
    },
    pause() {
      return this.goto('PAUSE')
    },
    unpause() {},
  }, // No-op
}

ReaderRdy.prototype.transitions = {
  '*': {
    '*': function (data, callback) {
      this.log()
      return callback(data)
    },
  },
}

module.exports = {ReaderRdy, ConnectionRdy}
