_ = require 'underscore'

class ConnectionConfig
  @DEFAULTS =
    authSecret: null
    clientId: null
    deflate: false
    deflateLevel: 6
    heartbeatInterval: 30
    maxInFlight: 1
    messageTimeout: null
    outputBufferSize: null
    outputBufferTimeout: null
    requeueDelay: 90
    sampleRate: null
    snappy: false
    tls: false
    tlsVerification: true

  constructor: (options={}) ->
    @options = _.chain(options)
      .pick(_.keys @constructor.DEFAULTS)
      .defaults(@constructor.DEFAULTS)
      .value()
    _.extend this, @options

  isNonEmptyString: (option, value) ->
    unless _.isString(value) and value.length > 0
      throw new Error "#{option} must be a non-empty string"

  isNumber: (option, value, lower, upper=null) ->
    if _.any (fn value for fn in [_.isNull, _.isNaN, _.isUndefined])
      throw new Error "#{option}(#{value}) is not a number"

    if upper
      unless lower <= value <= upper
        throw new Error "#{lower} <= #{option}(#{value}) <= #{upper}"
    else
      unless not _.isNumber(value) or lower <= value
        throw new Error "#{lower} <= #{option}(#{value})"

  isNumberExclusive: (option, value, lower, upper=null) ->
    if _.any (fn value for fn in [_.isNull, _.isNaN, _.isUndefined])
      throw new Error "#{option}(#{value}) is not a number"

    if upper
      unless lower < value < upper
        throw new Error "#{lower} < #{option}(#{value}) < #{upper}"
    else
      unless lower < value
        throw new Error "#{lower} < #{option}(#{value})"

  isBoolean: (option, value) ->
    unless _.isBoolean value
      throw new Error "#{option} must be either true or false"

  isAddressList: (option, value) ->
    isAddr = (addr) ->
      [host, port] = addr.split ':'
      host.length > 0 and port > 0

    unless _.isArray(value) and _.every value, isAddr
      throw new Error "#{option} must be a list of addresses 'host:port'"

  conditions: ->
    authSecret: [@isNonEmptyString]
    clientId: [@isNonEmptyString]
    deflate: [@isBoolean]
    deflateLevel: [@isNumber, 0, 9]
    heartbeatInterval: [@isNumber, 1]
    maxInFlight: [@isNumber, 1]
    messageTimeout: [@isNumber, 1]
    outputBufferSize: [@isNumber, 64]
    outputBufferTimeout: [@isNumber, 1]
    requeueDelay: [@isNumber, 0]
    sampleRate: [@isNumber, 1, 99]
    snappy: [@isBoolean]
    tls: [@isBoolean]
    tlsVerification: [@isBoolean]

  validateOption: (option, value) ->
    [fn, args...] = @conditions()[option]
    fn option, value, args...

  validate: ->
    conditions = @conditions()
    for option, value of @options
      # Skip options that default to null
      if _.isNull(value) and @constructor.DEFAULTS[option] is null
        continue

      # Disabled via -1
      keys = ['outputBufferSize', 'outputBufferTimeout']
      if option in keys and value is -1
        continue

      @validateOption option, value

    # Mutually exclusive options
    if @options.snappy and @options.deflate
      throw new Error 'Cannot use both deflate and snappy'


class ReaderConfig extends ConnectionConfig
  @DEFAULTS = _.extend {}, ConnectionConfig.DEFAULTS,
    lookupdHTTPAddresses: []
    lookupdPollInterval: 60
    lookupdPollJitter: 0.3
    name: null
    nsqdTCPAddresses: []
    maxAttempts: 5
    maxBackoffDuration: 128

  conditions: ->
    _.extend {}, super(),
      lookupdHTTPAddresses: [@isAddressList]
      lookupdPollInterval: [@isNumber, 1]
      lookupdPollJitter: [@isNumberExclusive, 0, 1]
      name: [@isNonEmptyString]
      nsqdTCPAddresses: [@isAddressList]
      maxAttempts: [@isNumber, 1]
      maxBackoffDuration: [@isNumber, 0]

  validate: ->
    addresses = ['nsqdTCPAddresses', 'lookupdHTTPAddresses']

    # Either a string or list of strings can be provided. Ensure list of
    # strings going forward.
    for key in addresses
      @options[key] = [@options[key]] if _.isString @options[key]

    super

    pass = _.chain(addresses)
      .map (key) =>
        @options[key].length
      .any(_.identity)
      .value()

    unless pass
      throw new Error "Need to provide either #{addresses.join ' or '}"

module.exports = {ConnectionConfig, ReaderConfig}
