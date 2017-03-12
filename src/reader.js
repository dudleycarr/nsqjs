import Debug from 'debug'
import RoundRobinList from './roundrobinlist'
import lookup from './lookupd'
import { EventEmitter } from 'events'
import { NSQDConnection } from './nsqdconnection'
import { ReaderConfig } from './config'
import { ReaderRdy } from './readerrdy'

class Reader extends EventEmitter {
  static initClass () {
    // Responsibilities
    // 1. Responsible for periodically querying the nsqlookupds
    // 2. Connects and subscribes to discovered/configured nsqd
    // 3. Consumes messages and triggers MESSAGE events
    // 4. Starts the ReaderRdy instance
    // 5. Hands nsqd connections to the ReaderRdy instance
    // 6. Stores Reader configurations

    // Reader events
    this.ERROR = 'error'
    this.MESSAGE = 'message'
    this.DISCARD = 'discard'
    this.NSQD_CONNECTED = 'nsqd_connected'
    this.NSQD_CLOSED = 'nsqd_closed'
  }

  constructor (topic, channel, options) {
    super(...arguments)
    this.topic = topic
    this.channel = channel
    this.debug = Debug(`nsqjs:reader:${this.topic}/${this.channel}`)
    this.config = new ReaderConfig(options)
    this.config.validate()

    this.debug('Configuration')
    this.debug(this.config)

    this.roundrobinLookupd = new RoundRobinList(this.config.lookupdHTTPAddresses)
    this.readerRdy = new ReaderRdy(this.config.maxInFlight, this.config.maxBackoffDuration,
      `${this.topic}/${this.channel}`)
    this.connectIntervalId = null
    this.connectionIds = []
  }

  connect () {
    let delayedStart
    const interval = this.config.lookupdPollInterval * 1000
    const delay = Math.random() * this.config.lookupdPollJitter * interval

    // Connect to provided nsqds.
    if (this.config.nsqdTCPAddresses.length) {
      const directConnect = () => {
        // Don't establish new connections while the Reader is paused.
        if (this.isPaused()) return

        if (this.connectionIds.length < this.config.nsqdTCPAddresses.length) {
          return this.config.nsqdTCPAddresses.forEach(addr => {
            const [address, port] = addr.split(':')
            this.connectToNSQD(address, Number(port))
          })
        }
      }

      delayedStart = () => {
        this.connectIntervalId = setInterval(directConnect.bind(this), interval)
      }

      // Connect immediately.
      directConnect()

      // Start interval for connecting after delay.
      setTimeout(delayedStart, delay)
    }

    delayedStart = () => {
      this.connectIntervalId = setInterval(this.queryLookupd.bind(this), interval)
    }

      // Connect immediately.
    this.queryLookupd()

    // Start interval for querying lookupd after delay.
    setTimeout(delayedStart, delay)
  }

  // Caution: in-flight messages will not get a chance to finish.
  close () {
    clearInterval(this.connectIntervalId)
    return this.readerRdy.close()
  }

  pause () {
    this.debug('pause')
    return this.readerRdy.pause()
  }

  unpause () {
    this.debug('unpause')
    return this.readerRdy.unpause()
  }

  isPaused () {
    return this.readerRdy.isPaused()
  }

  queryLookupd () {
    // Don't establish new connections while the Reader is paused.
    if (this.isPaused()) { return }

    // Trigger a query of the configured ``lookupdHTTPAddresses``
    const endpoint = this.roundrobinLookupd.next()
    return lookup(endpoint, this.topic, (err, nodes) => (() => {
      const result = []
      for (const n of Array.from(nodes)) {
        let item
        if (!err) { item = this.connectToNSQD(n.broadcast_address, n.tcp_port) }
        result.push(item)
      }
      return result
    })(),
    )
  }

  connectToNSQD (host, port) {
    this.debug(`discovered ${host}:${port} for ${this.topic} topic`)
    const conn = new NSQDConnection(host, port, this.topic, this.channel, this.config)

    // Ensure a connection doesn't already exist to this nsqd instance.
    if (this.connectionIds.indexOf(conn.id()) !== -1) { return }
    this.debug(`connecting to ${host}:${port}`)
    this.connectionIds.push(conn.id())

    this.registerConnectionListeners(conn)
    this.readerRdy.addConnection(conn)

    return conn.connect()
  }

  registerConnectionListeners (conn) {
    conn.on(NSQDConnection.CONNECTED, () => {
      this.debug(Reader.NSQD_CONNECTED)
      return this.emit(Reader.NSQD_CONNECTED, conn.nsqdHost, conn.nsqdPort)
    },
    )

    conn.on(NSQDConnection.ERROR, (err) => {
      this.debug(Reader.ERROR)
      this.debug(err)
      return this.emit(Reader.ERROR, err)
    },
    )

    conn.on(NSQDConnection.CONNECTION_ERROR, (err) => {
      this.debug(Reader.ERROR)
      this.debug(err)
      return this.emit(Reader.ERROR, err)
    },
    )

    // On close, remove the connection id from this reader.
    conn.on(NSQDConnection.CLOSED, () => {
      this.debug(Reader.NSQD_CLOSED)

      const index = this.connectionIds.indexOf(conn.id())
      if (index === -1) { return }
      this.connectionIds.splice(index, 1)

      return this.emit(Reader.NSQD_CLOSED, conn.nsqdHost, conn.nsqdPort)
    },
    )

    // On message, send either a message or discard event depending on the
    // number of attempts.
    return conn.on(NSQDConnection.MESSAGE, message => this.handleMessage(message),
    )
  }

  handleMessage (message) {
    // Give the internal event listeners a chance at the events before clients
    // of the Reader.
    return process.nextTick(() => {
      const autoFinishMessage = this.config.maxAttempts > 0 && this.config.maxAttempts <= message.attempts
      const numDiscardListeners = this.listeners(Reader.DISCARD).length

      if (autoFinishMessage && (numDiscardListeners > 0)) {
        this.emit(Reader.DISCARD, message)
      } else {
        this.emit(Reader.MESSAGE, message)
      }

      if (autoFinishMessage) { return message.finish() }
    },
    )
  }
}
Reader.initClass()

export default Reader
