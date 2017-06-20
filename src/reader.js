import { EventEmitter } from 'events';

import debug from 'debug';

import RoundRobinList from './roundrobinlist';
import lookup from './lookupd';
import { NSQDConnection } from './nsqdconnection';
import { ReaderConfig } from './config';
import { ReaderRdy } from './readerrdy';

/**
 * Reader provides high-level functionality for building robust NSQ
 * consumers. Reader is built upon the EventEmitter and thus supports various
 * hooks when different events occur.
 * @type {Reader}
 */
class Reader extends EventEmitter {
  static ERROR = 'error';
  static MESSAGE = 'message';
  static DISCARD = 'discard';
  static NSQD_CONNECTED = 'nsqd_connected';
  static NSQD_CLOSED = 'nsqd_closed';

  /**
   * @constructor
   * @param  {String} topic
   * @param  {String} channel
   * @param  {Object} options
   */
  constructor(topic, channel, options, ...args) {
    super(topic, channel, options, ...args);
    this.topic = topic;
    this.channel = channel;
    this.debug = debug(`nsqjs:reader:${this.topic}/${this.channel}`);
    this.config = new ReaderConfig(options);
    this.config.validate();

    this.debug('Configuration');
    this.debug(this.config);

    this.roundrobinLookupd = new RoundRobinList(
      this.config.lookupdHTTPAddresses
    );

    this.readerRdy = new ReaderRdy(
      this.config.maxInFlight,
      this.config.maxBackoffDuration,
      `${this.topic}/${this.channel}`
    );

    this.connectIntervalId = null;
    this.connectionIds = [];
  }

  /**
   * Adds a connection to nsqd at the configured address.
   *
   * @return {undefined}
   */
  connect() {
    let delayedStart;
    const interval = this.config.lookupdPollInterval * 1000;
    const delay = Math.random() * this.config.lookupdPollJitter * interval;

    // Connect to provided nsqds.
    if (this.config.nsqdTCPAddresses.length) {
      const directConnect = () => {
        // Don't establish new connections while the Reader is paused.
        if (this.isPaused()) return;

        if (this.connectionIds.length < this.config.nsqdTCPAddresses.length) {
          return this.config.nsqdTCPAddresses.forEach(addr => {
            const [address, port] = addr.split(':');
            this.connectToNSQD(address, Number(port));
          });
        }
      };

      delayedStart = () => {
        this.connectIntervalId = setInterval(
          directConnect.bind(this),
          interval
        );
      };

      // Connect immediately.
      directConnect();

      // Start interval for connecting after delay.
      setTimeout(delayedStart, delay).unref();
    }

    delayedStart = () => {
      this.connectIntervalId = setInterval(
        this.queryLookupd.bind(this),
        interval
      );
    };

    // Connect immediately.
    this.queryLookupd();

    // Start interval for querying lookupd after delay.
    setTimeout(delayedStart, delay).unref();
  }

  /**
   * Close all connections and prevent any periodic callbacks.
   * @return {Array} The closed connections.
   */
  close() {
    clearInterval(this.connectIntervalId);
    return this.readerRdy.close();
  }

  /**
   * Pause all connections
   * @return {Array} The paused connections.
   */
  pause() {
    this.debug('pause');
    return this.readerRdy.pause();
  }

  /**
   * Unpause all connections
   * @return {Array} The unpaused connections.
   */
  unpause() {
    this.debug('unpause');
    return this.readerRdy.unpause();
  }

  /**
   * @return {Boolean}
   */
  isPaused() {
    return this.readerRdy.isPaused();
  }

  /**
   * Trigger a query of the configured nsq_lookupd_http_addresses.
   * @return {undefined}
   */
  queryLookupd() {
    // Don't establish new connections while the Reader is paused.
    if (this.isPaused()) return;

    // Trigger a query of the configured `lookupdHTTPAddresses`.
    const endpoint = this.roundrobinLookupd.next();
    lookup(endpoint, this.topic, (err, nodes = []) =>
      nodes.map(n => this.connectToNSQD(n.broadcast_address, n.tcp_port)));
  }

  /**
   * Adds a connection to nsqd at the specified address.
   *
   * @param  {String} host
   * @param  {Number|String} port
   * @return {Object|undefined} The newly created nsqd connection.
   */
  connectToNSQD(host, port) {
    this.debug(`discovered ${host}:${port} for ${this.topic} topic`);
    const conn = new NSQDConnection(
      host,
      port,
      this.topic,
      this.channel,
      this.config
    );

    // Ensure a connection doesn't already exist to this nsqd instance.
    if (this.connectionIds.indexOf(conn.id()) !== -1) {
      return;
    }

    this.debug(`connecting to ${host}:${port}`);
    this.connectionIds.push(conn.id());

    this.registerConnectionListeners(conn);
    this.readerRdy.addConnection(conn);

    return conn.connect();
  }

  /**
   * Registers event handlers for the nsqd connection.
   * @param  {Object} conn
   */
  registerConnectionListeners(conn) {
    conn.on(NSQDConnection.CONNECTED, () => {
      this.debug(Reader.NSQD_CONNECTED);
      this.emit(Reader.NSQD_CONNECTED, conn.nsqdHost, conn.nsqdPort);
    });

    conn.on(NSQDConnection.ERROR, err => {
      this.debug(Reader.ERROR);
      this.debug(err);
      this.emit(Reader.ERROR, err);
    });

    conn.on(NSQDConnection.CONNECTION_ERROR, err => {
      this.debug(Reader.ERROR);
      this.debug(err);
      this.emit(Reader.ERROR, err);
    });

    // On close, remove the connection id from this reader.
    conn.on(NSQDConnection.CLOSED, () => {
      this.debug(Reader.NSQD_CLOSED);

      const index = this.connectionIds.indexOf(conn.id());
      if (index === -1) {
        return;
      }
      this.connectionIds.splice(index, 1);

      this.emit(Reader.NSQD_CLOSED, conn.nsqdHost, conn.nsqdPort);
    });

    /**
     * On message, send either a message or discard event depending on the
     * number of attempts.
     */
    conn.on(NSQDConnection.MESSAGE, message => {
      this.handleMessage(message);
    });
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
      const autoFinishMessage = this.config.maxAttempts > 0 &&
        this.config.maxAttempts <= message.attempts;
      const numDiscardListeners = this.listeners(Reader.DISCARD).length;

      if (autoFinishMessage && numDiscardListeners > 0) {
        this.emit(Reader.DISCARD, message);
      } else {
        this.emit(Reader.MESSAGE, message);
      }

      if (autoFinishMessage) {
        message.finish();
      }
    });
  }
}

export default Reader;
