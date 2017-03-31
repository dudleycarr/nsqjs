import _ from 'underscore';
import url from 'url';

let isBareAddress;
class ConnectionConfig {
  static initClass() {
    this.DEFAULTS = {
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
    };

    isBareAddress = function(addr) {
      const [host, port] = Array.from(addr.split(':'));
      return host.length > 0 && port > 0;
    };
  }

  constructor(options) {
    if (options == null) {
      options = {};
    }
    options = _.chain(options)
      .pick(_.keys(this.constructor.DEFAULTS))
      .defaults(this.constructor.DEFAULTS)
      .value();
    _.extend(this, options);
  }

  isNonEmptyString(option, value) {
    if (!_.isString(value) || !(value.length > 0)) {
      throw new Error(`${option} must be a non-empty string`);
    }
  }

  isNumber(option, value, lower, upper) {
    if (upper == null) {
      upper = null;
    }
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

  isNumberExclusive(option, value, lower, upper) {
    if (upper == null) {
      upper = null;
    }
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

  isBoolean(option, value) {
    if (!_.isBoolean(value)) {
      throw new Error(`${option} must be either true or false`);
    }
  }

  isBareAddresses(option, value) {
    if (!_.isArray(value) || !_.every(value, isBareAddress)) {
      throw new Error(`${option} must be a list of addresses 'host:port'`);
    }
  }

  isLookupdHTTPAddresses(option, value) {
    const isAddr = function(addr) {
      if (addr.indexOf('://') === -1) {
        return isBareAddress(addr);
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
    };
  }

  validateOption(option, value) {
    const [fn, ...args] = Array.from(this.conditions()[option]);
    return fn(option, value, ...args);
  }

  validate() {
    for (const option in this) {
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
      if (Array.from(keys).includes(option) && value === -1) {
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
ConnectionConfig.initClass();

class ReaderConfig extends ConnectionConfig {
  static initClass() {
    this.DEFAULTS = _.extend({}, ConnectionConfig.DEFAULTS, {
      lookupdHTTPAddresses: [],
      lookupdPollInterval: 60,
      lookupdPollJitter: 0.3,
      name: null,
      nsqdTCPAddresses: [],
      maxAttempts: 0,
      maxBackoffDuration: 128,
    });
  }

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

  validate() {
    const addresses = ['nsqdTCPAddresses', 'lookupdHTTPAddresses'];

    // Either a string or list of strings can be provided. Ensure list of
    // strings going forward.
    for (const key of Array.from(addresses)) {
      if (_.isString(this[key])) {
        this[key] = [this[key]];
      }
    }

    super.validate(...arguments);

    const pass = _.chain(addresses)
      .map(key => this[key].length)
      .any(_.identity)
      .value();

    if (!pass) {
      throw new Error(`Need to provide either ${addresses.join(' or ')}`);
    }
  }
}
ReaderConfig.initClass();

export { ConnectionConfig, ReaderConfig };
