import url from 'url';

import _ from 'underscore';
import ArrayFrom from 'array.from';

/**
 * Responsible for configuring the official defaults for nsqd connections.
 * @type {ConnectionConfig}
 */
class ConnectionConfig {
  static DEFAULTS = {
    authSecret: null,
    clientId: null,
    deflate: false,
    deflateLevel: 6,
    heartbeatInterval: 30,
    maxInFlight: 1,
    messageTimeout: null,
    outputBufferSize: null,
    outputBufferTimeout: null,
    requeueDelay: 90,
    sampleRate: null,
    snappy: false,
    tls: false,
    tlsVerification: true,
    key: null,
    cert: null,
    ca: null,
  };

  /**
   * Indicates if an address has the host pair combo.
   *
   * @param  {String}  addr
   * @return {Boolean}
   */
  static isBareAddress(addr) {
    const [host, port] = addr.split(':');
    return host.length > 0 && port > 0;
  }

  /**
   * Instantiates a new ConnectionConfig.
   *
   * @constructor
   * @param  {Object} [options={}]
   */
  constructor(options = {}) {
    options = _.chain(options)
      .pick(_.keys(this.constructor.DEFAULTS))
      .defaults(this.constructor.DEFAULTS)
      .value();

    _.extend(this, options);
  }

  /**
   * Throws an error if the value is not a non empty string.
   *
   * @param  {String}  option
   * @param  {*}  value
   */
  isNonEmptyString(option, value) {
    if (!_.isString(value) || !(value.length > 0)) {
      throw new Error(`${option} must be a non-empty string`);
    }
  }

  /**
   * Throws an error if the value is not a number.
   *
   * @param  {String}  option
   * @param  {*}  value
   * @param  {*}  lower
   * @param  {*}  upper
   */
  isNumber(option, value, lower, upper) {
    if (_.isNaN(value) || !_.isNumber(value)) {
      throw new Error(`${option}(${value}) is not a number`);
    }

    if (upper) {
      if (!(lower <= value && value <= upper)) {
        throw new Error(`${lower} <= ${option}(${value}) <= ${upper}`);
      }
    } else if (!(lower <= value)) {
      throw new Error(`${lower} <= ${option}(${value})`);
    }
  }

  /**
   * Throws an error if the value is not exclusive.
   *
   * @param  {String}  option
   * @param  {*}  value
   * @param  {*}  lower
   * @param  {*}  upper
   */
  isNumberExclusive(option, value, lower, upper) {
    if (_.isNaN(value) || !_.isNumber(value)) {
      throw new Error(`${option}(${value}) is not a number`);
    }

    if (upper) {
      if (!(lower < value && value < upper)) {
        throw new Error(`${lower} < ${option}(${value}) < ${upper}`);
      }
    } else if (!(lower < value)) {
      throw new Error(`${lower} < ${option}(${value})`);
    }
  }

  /**
   * Throws an error if the option is not a Boolean.
   *
   * @param  {String}  option
   * @param  {*}  value
   */
  isBoolean(option, value) {
    if (!_.isBoolean(value)) {
      throw new Error(`${option} must be either true or false`);
    }
  }

  /**
   * Throws an error if the option is not a bare address.
   *
   * @param  {String}  option
   * @param  {*}  value
   */
  isBareAddresses(option, value) {
    if (!_.isArray(value) || !_.every(value, ConnectionConfig.isBareAddress)) {
      throw new Error(`${option} must be a list of addresses 'host:port'`);
    }
  }

  /**
   * Throws an error if the option is not a valid lookupd http address.
   *
   * @param  {String}  option
   * @param  {*}  value
   */
  isLookupdHTTPAddresses(option, value) {
    const isAddr = addr => {
      if (addr.indexOf('://') === -1) {
        return ConnectionConfig.isBareAddress(addr);
      }

      const parsedUrl = url.parse(addr);
      return ['http:', 'https:'].includes(parsedUrl.protocol) &&
        !!parsedUrl.host;
    };

    if (!_.isArray(value) || !_.every(value, isAddr)) {
      throw new Error(
        `${option} must be a list of addresses 'host:port' or \
HTTP/HTTPS URI`
      );
    }
  }

  /**
   * Throws an error if the option is not a buffer.
   *
   * @param  {String}  option
   * @param  {*}  value
   */
  isBuffer(option, value) {
    if (!Buffer.isBuffer(value)) {
      throw new Error(`${option} must be a buffer`);
    }
  }

  /**
   * Throws an error if the option is not an array.
   *
   * @param  {String}  option
   * @param  {*}  value
   */
  isArray(option, value) {
    if (!_.isArray(value)) {
      throw new Error(`${option} must be an array`);
    }
  }

  /**
   * Returns the validated client config. Throws an error if any values are
   * not correct.
   *
   * @return {Object}
   */
  conditions() {
    return {
      authSecret: [this.isNonEmptyString],
      clientId: [this.isNonEmptyString],
      deflate: [this.isBoolean],
      deflateLevel: [this.isNumber, 0, 9],
      heartbeatInterval: [this.isNumber, 1],
      maxInFlight: [this.isNumber, 1],
      messageTimeout: [this.isNumber, 1],
      outputBufferSize: [this.isNumber, 64],
      outputBufferTimeout: [this.isNumber, 1],
      requeueDelay: [this.isNumber, 0],
      sampleRate: [this.isNumber, 1, 99],
      snappy: [this.isBoolean],
      tls: [this.isBoolean],
      tlsVerification: [this.isBoolean],
      key: [this.isBuffer],
      cert: [this.isBuffer],
      ca: [this.isArray],
    };
  }

  /**
   * Helper function that will validate a condition with the given args.
   *
   * @param  {String} option
   * @param  {String} value
   * @return {Boolean}
   */
  validateOption(option, value) {
    const [fn, ...args] = this.conditions()[option];
    return fn(option, value, ...args);
  }

  /**
   * Validate the connection options.
   */
  validate() {
    const options = Object.keys(this);
    for (const option of options) {
      // dont validate our methods
      const value = this[option];

      if (_.isFunction(value)) {
        continue;
      }

      // Skip options that default to null
      if (_.isNull(value) && this.constructor.DEFAULTS[option] === null) {
        continue;
      }

      // Disabled via -1
      const keys = ['outputBufferSize', 'outputBufferTimeout'];
      if (keys.includes(option) && value === -1) {
        continue;
      }

      this.validateOption(option, value);
    }

    // Mutually exclusive options
    if (this.snappy && this.deflate) {
      throw new Error('Cannot use both deflate and snappy');
    }
  }
}

/**
 * Responsible for configuring the official defaults for Reader connections.
 * @type {[type]}
 */
class ReaderConfig extends ConnectionConfig {
  static DEFAULTS = _.extend({}, ConnectionConfig.DEFAULTS, {
    lookupdHTTPAddresses: [],
    lookupdPollInterval: 60,
    lookupdPollJitter: 0.3,
    name: null,
    nsqdTCPAddresses: [],
    maxAttempts: 0,
    maxBackoffDuration: 128,
  });

  /**
   * Returns the validated reader client config. Throws an error if any
   * values are not correct.
   *
   * @return {Object}
   */
  conditions() {
    return _.extend({}, super.conditions(), {
      lookupdHTTPAddresses: [this.isLookupdHTTPAddresses],
      lookupdPollInterval: [this.isNumber, 1],
      lookupdPollJitter: [this.isNumberExclusive, 0, 1],
      name: [this.isNonEmptyString],
      nsqdTCPAddresses: [this.isBareAddresses],
      maxAttempts: [this.isNumber, 0],
      maxBackoffDuration: [this.isNumber, 0],
    });
  }

  /**
   * Validate the connection options.
   */
  validate(...args) {
    const addresses = ['nsqdTCPAddresses', 'lookupdHTTPAddresses'];

    /**
     * Either a string or list of strings can be provided. Ensure list of
     * strings going forward.
     */
    for (const key of ArrayFrom(addresses)) {
      if (_.isString(this[key])) {
        this[key] = [this[key]];
      }
    }

    super.validate(...args);

    const pass = _.chain(addresses)
      .map(key => this[key].length)
      .any(_.identity)
      .value();

    if (!pass) {
      throw new Error(`Need to provide either ${addresses.join(' or ')}`);
    }
  }
}

export { ConnectionConfig, ReaderConfig };
