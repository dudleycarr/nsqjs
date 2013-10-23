_ = require 'underscore'
assert = require 'assert'
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

  buffers.push new Buffer(header)

  # Body into output buffer it is not empty
  if body?
    # Write the size of the payload
    lengthBuffer = new Buffer 4
    lengthBuffer.writeInt32BE body.length, 0
    buffers.push lengthBuffer

    if _.isString body
      buffers.push new Buffer(body)
    else
      buffers.push body

  Buffer.concat buffers

exports.subscribe = (topic, channel) ->
  assert validTopicName topic
  assert validChannelName channel
  command 'SUB', null, topic, channel

exports.identify = (data) ->
  validIdentifyKeys = [
    'short_id',
    'long_id',
    'feature_negotiation',
    'heartbeat_interval',
    'output_buffer_size',
    'output_buffer_timeout',
    'tls_v1',
    'snappy'
  ]
  # Make sure there are no unexpected keys
  unexpectedKeys = _.filter _.keys(data), (k) ->
    k not in validIdentifyKeys
  console.log unexpectedKeys if unexpectedKeys.length > 0
  assert unexpectedKeys.length is 0

  command 'IDENTIFY', JSON_stringify data

exports.ready = (count) ->
  assert _.isNumber count
  assert count >= 0
  command 'RDY', null, count.toString()

exports.finish = (id) ->
  assert Buffer.byteLength(id) <= 16
  command 'FIN', null, id

exports.requeue = (id, timeMs=0) ->
  assert Buffer.byteLength(id) <= 16
  assert _.isNumber timeMs

  parameters = ['REQ', null, id, timeMs]
  command.apply null, parameters

exports.touch = (id) ->
  command 'TOUCH', null, id

exports.nop = ->
  command 'NOP', null

exports.pub = (topic, data) ->
  command 'PUB', data, topic

exports.mpub = (topic, data) ->
  assert _.isArray data
  messages = _.map data, (message) ->
    buffer = new Buffer(4 + message.length)
    buffer.writeInt32BE message.length, 0
    buffer.write message, 4
    buffer

  numMessagesBuffer = Buffer 4
  numMessagesBuffer.writeInt32BE messages.length, 0
  messages.unshift numMessagesBuffer

  command 'MPUB', Buffer.concat(messages), topic

validTopicName = (topic) ->
  (0 < topic.length < 33) and topic.match(/^[\.a-zA-Z0-9_-]+$/)?

validChannelName = (channel) ->
  channelRe = /^[\.a-zA-Z0-9_-]+(#ephemeral)?$/
  (0 < channel.length < 33) and channel.match(channelRe)?

