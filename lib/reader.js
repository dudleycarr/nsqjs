const {EventEmitter} = require('events')

const debug = require('./debug')

const RoundRobinList = require('./roundrobinlist')
const lookup = require('./lookupd')
const {NSQDConnection} = require('./nsqdconnection')
const {ReaderConfig, splitHostPort, joinHostPort} = require('./config')
const {ReaderRdy} = require('./readerrdy')

/**
 * Reader provides high-level functionality for building robust NSQ
 * consumers. Reader is built upon the EventEmitter and thus supports various
 * hooks when different events occur.
 * @type {Reader}
 */
class Reader extends EventEmitter {
  static get ERROR() {
    return 'error'
  }
  static get MESSAGE() {
    return 'message'
  }
  static get READY() {
    return 'ready'
  }
  static get NOT_READY() {
    return 'not_ready'
  }
  static get DISCARD() {
    return 'discard'
  }
  static get NSQD_CONNECTED() {
    return 'nsqd_connected'
  }
  static get NSQD_CLOSED() {
    return 'nsqd_closed'
  }

  /**
   * @constructor
   * @param  {String} topic
   * @param  {String} channel
   * @param  {Object} options
   */
  constructor(topic, channel, options, ...args) {
    super(topic, channel, options, ...args)
    this.topic = topic
    this.channel = channel
    this.debug = debug(`nsqjs:reader:${this.topic}/${this.channel}`)
    this.config = new ReaderConfig(options)
    this.config.validate()

    this.debug('Configuration')
    this.debug(this.config)

    this.roundrobinLookupd = new RoundRobinList(
      this.config.lookupdHTTPAddresses
    )

    this.readerRdy = new ReaderRdy(
      this.config.maxInFlight,
      this.config.maxBackoffDuration,
      `${this.topic}/${this.channel}`,
      this.config.lowRdyTimeout
    )

    this.lookupdIntervalId = null
    this.directIntervalId = null
    this.connectionIds = []
    this.isClosed = false
  }

  /**
   * Adds a connection to nsqd at the configured address.
   *
   * @return {undefined}
   */
  connect() {
    this._connectTCPAddresses()
    this._connectLookupd()
  }

  _connectInterval() {
    return this.config.lookupdPollInterval * 1000
  }

  _connectTCPAddresses() {
    const directConnect = () => {
      // Don't establish new connections while the Reader is paused.
      if (this.isPaused()) return

      if (this.connectionIds.length < this.config.nsqdTCPAddresses.length) {
        return this.config.nsqdTCPAddresses.forEach((addr) => {
          const [address, port] = splitHostPort(addr)
          this.connectToNSQD(address, Number(port))
        })
      }
    }

    this.lookupdIntervalId = setInterval(() => {
      directConnect()
    }, this._connectInterval())

    // Connect immediately.
    directConnect()
  }

  _connectLookupd() {
    this.directIntervalId = setInterval(() => {
      this.queryLookupd()
    }, this._connectInterval())

    // Connect immediately.
    this.queryLookupd()
  }

  /**
   * Close all connections and prevent any periodic callbacks.
   * @return {Array} The closed connections.
   */
  close() {
    this.isClosed = true
    clearInterval(this.directIntervalId)
    clearInterval(this.lookupdIntervalId)
    return this.readerRdy.close()
  }

  /**
   * Pause all connections
   * @return {Array} The paused connections.
   */
  pause() {
    this.debug('pause')
    return this.readerRdy.pause()
  }

  /**
   * Unpause all connections
   * @return {Array} The unpaused connections.
   */
  unpause() {
    this.debug('unpause')
    return this.readerRdy.unpause()
  }

  /**
   * @return {Boolean}
   */
  isPaused() {
    return this.readerRdy.isPaused()
  }

  /**
   * Trigger a query of the configured nsq_lookupd_http_addresses.
   * @return {undefined}
   */
  async queryLookupd() {
    // Don't establish new connections while the Reader is paused.
    if (this.isPaused()) return

    // Trigger a query of the configured `lookupdHTTPAddresses`.
    const endpoint = this.roundrobinLookupd.next()
    const nodes = await lookup(endpoint, this.topic)

    for (const n of nodes) {
      this.connectToNSQD(n.broadcast_address || n.hostname, n.tcp_port)
    }
  }

  /**
   * Adds a connection to nsqd at the specified address.
   *
   * @param  {String} host
   * @param  {Number|String} port
   * @return {Object|undefined} The newly created nsqd connection.
   */
  connectToNSQD(host, port) {
    if (this.isClosed) {
      return
    }

    this.debug(`discovered ${joinHostPort(host, port)} for ${this.topic} topic`)
    const conn = new NSQDConnection(
      host,
      port,
      this.topic,
      this.channel,
      this.config
    )

    // Ensure a connection doesn't already exist to this nsqd instance.
    if (this.connectionIds.indexOf(conn.id()) !== -1) {
      return
    }

    this.debug(`connecting to ${joinHostPort(host, port)}`)
    this.connectionIds.push(conn.id())

    this.registerConnectionListeners(conn)
    this.readerRdy.addConnection(conn)

    return conn.connect()
  }

  /**
   * Registers event handlers for the nsqd connection.
   * @param  {Object} conn
   */
  registerConnectionListeners(conn) {
    conn.on(NSQDConnection.CONNECTED, () => {
      this.debug(Reader.NSQD_CONNECTED)
      this.emit(Reader.NSQD_CONNECTED, conn.nsqdHost, conn.nsqdPort)
    })

    conn.on(NSQDConnection.READY, () => {
      // Emit only if this is the first connection for this Reader.
      if (this.connectionIds.length === 1) {
        this.debug(Reader.READY)
        this.emit(Reader.READY)
      }
    })

    conn.on(NSQDConnection.ERROR, (err) => {
      this.debug(Reader.ERROR)
      this.debug(err)
      this.emit(Reader.ERROR, err)
    })

    conn.on(NSQDConnection.CONNECTION_ERROR, (err) => {
      this.debug(Reader.ERROR)
      this.debug(err)
      this.emit(Reader.ERROR, err)
    })

    // On close, remove the connection id from this reader.
    conn.on(NSQDConnection.CLOSED, () => {
      this.debug(Reader.NSQD_CLOSED)

      const index = this.connectionIds.indexOf(conn.id())
      if (index === -1) {
        return
      }
      this.connectionIds.splice(index, 1)

      this.emit(Reader.NSQD_CLOSED, conn.nsqdHost, conn.nsqdPort)

      if (this.connectionIds.length === 0) {
        this.debug(Reader.NOT_READY)
        this.emit(Reader.NOT_READY)
      }
    })

    /**
     * On message, send either a message or discard event depending on the
     * number of attempts.
     */
    conn.on(NSQDConnection.MESSAGE, (message) => {
      this.handleMessage(message)
    })
  }

  /**
   * Asynchronously handles an nsqd message.
   *
   * @param  {Object} message
   */
  handleMessage(message) {
    /**
     * Give the internal event listeners a chance at the events
     * before clients of the Reader.
     */
    process.nextTick(() => {
      const autoFinishMessage =
        this.config.maxAttempts > 0 &&
        this.config.maxAttempts < message.attempts
      const numDiscardListeners = this.listeners(Reader.DISCARD).length

      if (autoFinishMessage && numDiscardListeners > 0) {
        this.emit(Reader.DISCARD, message)
      } else {
        this.emit(Reader.MESSAGE, message)
      }

      if (autoFinishMessage) {
        message.finish()
      }
    })
  }
}

module.exports = Reader
