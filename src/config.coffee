_ = require 'underscore'
url = require 'url'

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
    options = _.chain(options)
      .pick(_.keys @constructor.DEFAULTS)
      .defaults(@constructor.DEFAULTS)
      .value()
    _.extend this, options

  isNonEmptyString: (option, value) ->
    unless _.isString(value) and value.length > 0
      throw new Error "#{option} must be a non-empty string"

  isNumber: (option, value, lower, upper=null) ->
    if _.isNaN(value) or not _.isNumber value
      throw new Error "#{option}(#{value}) is not a number"

    if upper
      unless lower <= value <= upper
        throw new Error "#{lower} <= #{option}(#{value}) <= #{upper}"
    else
      unless lower <= value
        throw new Error "#{lower} <= #{option}(#{value})"

  isNumberExclusive: (option, value, lower, upper=null) ->
    if _.isNaN(value) or not _.isNumber value
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

  isBareAddress = (addr) ->
    [host, port] = addr.split ':'
    host.length > 0 and port > 0

  isBareAddresses: (option, value) ->
    unless _.isArray(value) and _.every value, isBareAddress
      throw new Error "#{option} must be a list of addresses 'host:port'"

  isLookupdHTTPAddresses: (option, value) ->
    isAddr = (addr) ->
      return isBareAddress(addr) if addr.indexOf('://') is -1
      parsedUrl = url.parse(addr)
      parsedUrl.protocol in ['http:', 'https:'] and !!parsedUrl.host

    unless _.isArray(value) and _.every value, isAddr
      throw new Error "#{option} must be a list of addresses 'host:port' or
        HTTP/HTTPS URI"

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
    for option, value of this
      # dont validate our methods
      continue if _.isFunction value

      # Skip options that default to null
      if _.isNull(value) and @constructor.DEFAULTS[option] is null
        continue

      # Disabled via -1
      keys = ['outputBufferSize', 'outputBufferTimeout']
      if option in keys and value is -1
        continue

      @validateOption option, value

    # Mutually exclusive options
    if @snappy and @deflate
      throw new Error 'Cannot use both deflate and snappy'


class ReaderConfig extends ConnectionConfig
  @DEFAULTS = _.extend {}, ConnectionConfig.DEFAULTS,
    lookupdHTTPAddresses: []
    lookupdPollInterval: 60
    lookupdPollJitter: 0.3
    name: null
    nsqdTCPAddresses: []
    maxAttempts: 0
    maxBackoffDuration: 128

  conditions: ->
    _.extend {}, super(),
      lookupdHTTPAddresses: [@isLookupdHTTPAddresses]
      lookupdPollInterval: [@isNumber, 1]
      lookupdPollJitter: [@isNumberExclusive, 0, 1]
      name: [@isNonEmptyString]
      nsqdTCPAddresses: [@isBareAddresses]
      maxAttempts: [@isNumber, 0]
      maxBackoffDuration: [@isNumber, 0]

  validate: ->
    addresses = ['nsqdTCPAddresses', 'lookupdHTTPAddresses']

    # Either a string or list of strings can be provided. Ensure list of
    # strings going forward.
    for key in addresses
      @[key] = [@[key]] if _.isString @[key]

    super

    pass = _.chain(addresses)
      .map (key) =>
        @[key].length
      .any(_.identity)
      .value()

    unless pass
      throw new Error "Need to provide either #{addresses.join ' or '}"

module.exports = {ConnectionConfig, ReaderConfig}
