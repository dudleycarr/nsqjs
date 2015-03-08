_ = require 'underscore'
Int64 = require 'node-int64'
BigNumber = require 'bignumber.js'

exports.MAGIC_V2 = '  V2'
exports.FRAME_TYPE_RESPONSE = 0
exports.FRAME_TYPE_ERROR = 1
exports.FRAME_TYPE_MESSAGE = 2

JSON_stringify = (obj, emit_unicode) ->
  json = JSON.stringify obj
  if emit_unicode
    json
  else
    json.replace /[\u007f-\uffff]/g, (c) ->
      '\\u' + ('0000' + c.charCodeAt(0).toString 16).slice -4

# Calculates the byte length for either a string or a Buffer.
byteLength = (msg) ->
  if _.isString msg then Buffer.byteLength msg else msg.length

exports.unpackMessage = (data) ->
  # Int64 to read the 64bit Int from the buffer
  timestamp = (new Int64 data, 0).toOctetString()
  # BigNumber to represent the timestamp in a workable way.
  timestamp = new BigNumber timestamp, 16

  attempts = data.readInt16BE 8
  id = data[10...26].toString()
  body = data[26..]
  [id, timestamp, attempts, body]

command = (cmd, body) ->
  buffers = []

  # Turn optional args into parameters for the command
  parameters = _.toArray(arguments)[2..]
  parameters.unshift('') if parameters.length > 0
  parametersStr = parameters.join ' '
  header = cmd + parametersStr + '\n'

  buffers.push new Buffer header

  # Body into output buffer it is not empty
  if body?
    # Write the size of the payload
    lengthBuffer = new Buffer 4
    lengthBuffer.writeInt32BE byteLength(body), 0
    buffers.push lengthBuffer

    if _.isString body
      buffers.push new Buffer(body)
    else
      buffers.push body

  Buffer.concat buffers

exports.subscribe = (topic, channel) ->
  throw new Error "Invalid topic: #{topic}" unless validTopicName topic
  throw new Error "Invalid channel: #{channel}" unless validChannelName channel
  command 'SUB', null, topic, channel

exports.identify = (data) ->
  validIdentifyKeys = [
    'client_id'
    'deflate'
    'deflate_level'
    'feature_negotiation',
    'heartbeat_interval',
    'long_id',
    'msg_timeout'
    'output_buffer_size',
    'output_buffer_timeout',
    'sample_rate'
    'short_id',
    'snappy'
    'tls_v1',
    'user_agent'
  ]
  # Make sure there are no unexpected keys
  unexpectedKeys = _.filter _.keys(data), (k) ->
    k not in validIdentifyKeys

  if unexpectedKeys.length
    throw new Error "Unexpected IDENTIFY keys: #{unexpectedKeys}"

  command 'IDENTIFY', JSON_stringify data

exports.ready = (count) ->
  throw new Error "RDY count (#{count}) is not a number" unless _.isNumber count
  throw new Error "RDY count (#{count}) is not positive" unless count >= 0
  command 'RDY', null, count.toString()

exports.finish = (id) ->
  throw new Error "FINISH invalid id (#{id})" unless Buffer.byteLength(id) <= 16
  command 'FIN', null, id

exports.requeue = (id, timeMs=0) ->
  unless Buffer.byteLength(id) <= 16
    throw new Error "REQUEUE invalid id (#{id})"
  unless _.isNumber timeMs
    throw new Error "REQUEUE delay time is invalid (#{timeMs})"

  parameters = ['REQ', null, id, timeMs]
  command.apply null, parameters

exports.touch = (id) ->
  command 'TOUCH', null, id

exports.nop = ->
  command 'NOP', null

exports.pub = (topic, data) ->
  command 'PUB', data, topic

exports.mpub = (topic, data) ->
  throw new Error "MPUB requires an array of message" unless _.isArray data
  messages = _.map data, (message) ->
    buffer = new Buffer 4 + byteLength message
    buffer.writeInt32BE byteLength(message), 0

    if _.isString message
      buffer.write message, 4
    else
      message.copy buffer, 4, 0, buffer.length

    buffer

  numMessagesBuffer = Buffer 4
  numMessagesBuffer.writeInt32BE messages.length, 0
  messages.unshift numMessagesBuffer

  command 'MPUB', Buffer.concat(messages), topic

exports.auth = (token) ->
  command 'AUTH', token

validTopicName = (topic) ->
  (0 < topic.length < 65) and topic.match(/^[\w._-]+(?:#ephemeral)?$/)?

validChannelName = (channel) ->
  channelRe = /^[\w._-]+(?:#ephemeral)?$/
  (0 < channel.length < 65) and channel.match(channelRe)?

