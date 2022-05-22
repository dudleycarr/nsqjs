const _ = require('lodash')

const MAGIC_V2 = '  V2'
const FRAME_TYPE_RESPONSE = 0
const FRAME_TYPE_ERROR = 1
const FRAME_TYPE_MESSAGE = 2

/**
 * Stringifies an object. Supports unicode.
 *
 * @param  {Object} obj
 * @param  {Boolean} emitUnicode
 * @return {String}
 */
function jsonStringify(obj, emitUnicode) {
  const json = JSON.stringify(obj)
  if (emitUnicode) return json

  return json.replace(
    /[\u007f-\uffff]/g,
    (c) => `\\u${`0000${c.charCodeAt(0).toString(16)}`.slice(-4)}`
  )
}

/**
 * Compute the byte length of an nsq message.
 *
 * @param  {String|Array} msg
 * @return {Number}
 */
function byteLength(msg) {
  if (_.isString(msg)) return Buffer.byteLength(msg.toString())

  return msg.length
}

/**
 * Unpack a message payload. The message is returned as an array in the format
 * [id, timestamp, attempts, body].
 *
 * @param  {Object} data
 * @return {Array}
 */
function unpackMessage(data) {
  let timestamp = data.readBigInt64BE(0)

  const attempts = data.readInt16BE(8)
  const id = data.slice(10, 26).toString()
  const body = data.slice(26)
  return [id, timestamp, attempts, body]
}

function unpackMessageId(rawMessage) {
  return rawMessage.slice(10, 26).toString()
}

function unpackMessageTimestamp(rawMessage) {
  return rawMessage.readBigInt64BE(0)
}

function unpackMessageAttempts(rawMessage) {
  return rawMessage.readInt16BE(8)
}

function unpackMessageBody(rawMessage) {
  return rawMessage.slice(26)
}

/**
 * Performs the requested command on the buffer.
 *
 * @param  {String} cmd
 * @param  {String} body
 * @return {Buffer}
 */
function command(cmd, body, ...parameters) {
  const buffers = []

  if (parameters.length > 0) {
    parameters.unshift('')
  }

  const parametersStr = parameters.join(' ')
  const header = `${cmd + parametersStr}\n`

  buffers.push(Buffer.from(header))

  // Body into output buffer it is not empty
  if (body != null) {
    // Write the size of the payload
    const lengthBuffer = Buffer.alloc(4)
    lengthBuffer.writeInt32BE(byteLength(body), 0)
    buffers.push(lengthBuffer)

    if (_.isString(body)) {
      buffers.push(Buffer.from(body))
    } else {
      buffers.push(body)
    }
  }

  return Buffer.concat(buffers)
}

/**
 * Emit a `SUB` command.
 *
 * @param  {String} topic
 * @param  {String} channel
 * @return {Buffer}
 */
function subscribe(topic, channel) {
  if (!validTopicName(topic)) {
    throw new Error(`Invalid topic: ${topic}`)
  }

  if (!validChannelName(channel)) {
    throw new Error(`Invalid channel: ${channel}`)
  }

  return command('SUB', null, topic, channel)
}

/**
 * Emit an `INDENTIFY` command.
 *
 * @param  {Object} data
 * @return {Buffer}
 */
function identify(data) {
  const validIdentifyKeys = [
    'client_id',
    'deflate',
    'deflate_level',
    'feature_negotiation',
    'hostname',
    'heartbeat_interval',
    'long_id',
    'msg_timeout',
    'output_buffer_size',
    'output_buffer_timeout',
    'sample_rate',
    'short_id',
    'snappy',
    'tls_v1',
    'user_agent',
  ]

  // Make sure there are no unexpected keys
  const unexpectedKeys = _.filter(
    _.keys(data),
    (k) => !Array.from(validIdentifyKeys).includes(k)
  )

  if (unexpectedKeys.length) {
    throw new Error(`Unexpected IDENTIFY keys: ${unexpectedKeys}`)
  }

  return command('IDENTIFY', jsonStringify(data))
}

/**
 * Emit a `RDY` command.
 *
 * @param  {Number} count
 * @return {Buffer}
 */
function ready(count) {
  if (!_.isNumber(count)) {
    throw new Error(`RDY count (${count}) is not a number`)
  }

  if (!(count >= 0)) {
    throw new Error(`RDY count (${count}) is not positive`)
  }

  return command('RDY', null, count.toString())
}

/**
 * Emit a `FIN` command.
 *
 * @param  {String} id
 * @return {Buffer}
 */
function finish(id) {
  if (!(Buffer.byteLength(id) <= 16)) {
    throw new Error(`FINISH invalid id (${id})`)
  }
  return command('FIN', null, id)
}

function close() {
  return command('CLS', null)
}

/**
 * Emit a requeue command.
 *
 * @param  {String} id
 * @param  {Number} timeMs
 * @return {Buffer}
 */
function requeue(id, timeMs = 0) {
  if (!(Buffer.byteLength(id) <= 16)) {
    throw new Error(`REQUEUE invalid id (${id})`)
  }

  if (!_.isNumber(timeMs)) {
    throw new Error(`REQUEUE delay time is invalid (${timeMs})`)
  }

  const parameters = ['REQ', null, id, timeMs]
  return command(...parameters)
}

/**
 * Emit a `TOUCH` command.
 *
 * @param  {String} id
 * @return {Buffer}
 */
function touch(id) {
  return command('TOUCH', null, id)
}

/**
 * Emit a `NOP` command.
 *
 * @return {Buffer}
 */
function nop() {
  return command('NOP', null)
}

/**
 * Emit a `PUB` command.
 *
 * @param  {String} topic
 * @param  {Object} data
 * @return {Buffer}
 */
function pub(topic, data) {
  return command('PUB', data, topic)
}

/**
 * Emit an `MPUB` command.
 *
 * @param  {String} topic
 * @param  {Object} data
 * @return {Buffer}
 */
function mpub(topic, data) {
  if (!_.isArray(data)) {
    throw new Error('MPUB requires an array of message')
  }
  const messages = _.map(data, (message) => {
    const buffer = Buffer.alloc(4 + byteLength(message))
    buffer.writeInt32BE(byteLength(message), 0)

    if (_.isString(message)) {
      buffer.write(message, 4)
    } else {
      message.copy(buffer, 4, 0, buffer.length)
    }

    return buffer
  })

  const numMessagesBuffer = Buffer.alloc(4)
  numMessagesBuffer.writeInt32BE(messages.length, 0)
  messages.unshift(numMessagesBuffer)

  return command('MPUB', Buffer.concat(messages), topic)
}

/**
 * Emit a `DPUB` command.
 *
 * @param  {String} topic
 * @param  {Object} data
 * @param  {Number} timeMs
 * @return {Buffer}
 */
function dpub(topic, data, timeMs = 0) {
  return command('DPUB', data, topic, timeMs)
}

/**
 * Emit an `AUTH` command.
 *
 * @param  {String} token
 * @return {Buffer}
 */
function auth(token) {
  return command('AUTH', token)
}

/**
 * Validate topic names. Topic names must be no longer than
 * 65 characters.
 *
 * @param {String} topic
 * @return {Boolean}
 */
function validTopicName(topic) {
  return (
    topic &&
    topic.length > 0 &&
    topic.length < 65 &&
    topic.match(/^[\w._-]+(?:#ephemeral)?$/) != null
  )
}

/**
 * Validate channel names. Follows the same restriction as
 * topic names.
 *
 * @param {String} channel
 * @return {Boolean}
 */
function validChannelName(channel) {
  return (
    channel &&
    channel.length > 0 &&
    channel.length < 65 &&
    channel.match(/^[\w._-]+(?:#ephemeral)?$/) != null
  )
}

module.exports = {
  FRAME_TYPE_ERROR,
  FRAME_TYPE_MESSAGE,
  FRAME_TYPE_RESPONSE,
  MAGIC_V2,
  auth,
  close,
  dpub,
  finish,
  identify,
  mpub,
  nop,
  pub,
  ready,
  requeue,
  subscribe,
  touch,
  unpackMessage,
  unpackMessageId,
  unpackMessageTimestamp,
  unpackMessageAttempts,
  unpackMessageBody,
}
