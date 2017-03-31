import _ from 'underscore';
import Int64 from 'node-int64';
import BigNumber from 'bignumber.js';

export const MAGIC_V2 = '  V2';
export const FRAME_TYPE_RESPONSE = 0;
export const FRAME_TYPE_ERROR = 1;
export const FRAME_TYPE_MESSAGE = 2;

function jsonStringify(obj, emitUnicode) {
  const json = JSON.stringify(obj);
  if (emitUnicode) return json;

  return json.replace(
    /[\u007f-\uffff]/g,
    c => `\\u${`0000${c.charCodeAt(0).toString(16)}`.slice(-4)}`
  );
}

// Calculates the byte length for either a string or a Buffer.
function byteLength(msg) {
  if (_.isString(msg)) {
    return Buffer.byteLength(msg);
  }
  return msg.length;
}

export function unpackMessage(data) {
  // Int64 to read the 64bit Int from the buffer
  let timestamp = new Int64(data, 0).toOctetString();
  // BigNumber to represent the timestamp in a workable way.
  timestamp = new BigNumber(timestamp, 16);

  const attempts = data.readInt16BE(8);
  const id = data.slice(10, 26).toString();
  const body = data.slice(26);
  return [id, timestamp, attempts, body];
}

function command(cmd, body) {
  const buffers = [];

  // Turn optional args into parameters for the command
  const parameters = _.toArray(arguments).slice(2);
  if (parameters.length > 0) {
    parameters.unshift('');
  }
  const parametersStr = parameters.join(' ');
  const header = `${cmd + parametersStr}\n`;

  buffers.push(new Buffer(header));

  // Body into output buffer it is not empty
  if (body != null) {
    // Write the size of the payload
    const lengthBuffer = new Buffer(4);
    lengthBuffer.writeInt32BE(byteLength(body), 0);
    buffers.push(lengthBuffer);

    if (_.isString(body)) {
      buffers.push(new Buffer(body));
    } else {
      buffers.push(body);
    }
  }

  return Buffer.concat(buffers);
}

export function subscribe(topic, channel) {
  if (!validTopicName(topic)) {
    throw new Error(`Invalid topic: ${topic}`);
  }
  if (!validChannelName(channel)) {
    throw new Error(`Invalid channel: ${channel}`);
  }
  return command('SUB', null, topic, channel);
}

export function identify(data) {
  const validIdentifyKeys = [
    'client_id',
    'deflate',
    'deflate_level',
    'feature_negotiation',
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
  ];
  // Make sure there are no unexpected keys
  const unexpectedKeys = _.filter(
    _.keys(data),
    k => !Array.from(validIdentifyKeys).includes(k)
  );

  if (unexpectedKeys.length) {
    throw new Error(`Unexpected IDENTIFY keys: ${unexpectedKeys}`);
  }

  return command('IDENTIFY', jsonStringify(data));
}

export function ready(count) {
  if (!_.isNumber(count)) {
    throw new Error(`RDY count (${count}) is not a number`);
  }
  if (!(count >= 0)) {
    throw new Error(`RDY count (${count}) is not positive`);
  }
  return command('RDY', null, count.toString());
}

export function finish(id) {
  if (!(Buffer.byteLength(id) <= 16)) {
    throw new Error(`FINISH invalid id (${id})`);
  }
  return command('FIN', null, id);
}

export function requeue(id, timeMs) {
  if (timeMs == null) {
    timeMs = 0;
  }
  if (!(Buffer.byteLength(id) <= 16)) {
    throw new Error(`REQUEUE invalid id (${id})`);
  }
  if (!_.isNumber(timeMs)) {
    throw new Error(`REQUEUE delay time is invalid (${timeMs})`);
  }

  const parameters = ['REQ', null, id, timeMs];
  return command(...parameters);
}

export function touch(id) {
  return command('TOUCH', null, id);
}

export function nop() {
  return command('NOP', null);
}

export function pub(topic, data) {
  return command('PUB', data, topic);
}

export function mpub(topic, data) {
  if (!_.isArray(data)) {
    throw new Error('MPUB requires an array of message');
  }
  const messages = _.map(data, message => {
    const buffer = new Buffer(4 + byteLength(message));
    buffer.writeInt32BE(byteLength(message), 0);

    if (_.isString(message)) {
      buffer.write(message, 4);
    } else {
      message.copy(buffer, 4, 0, buffer.length);
    }

    return buffer;
  });

  const numMessagesBuffer = Buffer(4);
  numMessagesBuffer.writeInt32BE(messages.length, 0);
  messages.unshift(numMessagesBuffer);

  return command('MPUB', Buffer.concat(messages), topic);
}

export function auth(token) {
  return command('AUTH', token);
}

/**
 * Validate topic names. Topic names must be no longer than 
 * 65 characters.
 * 
 * @param {String} topic.
 */
function validTopicName(topic) {
  return topic &&
    topic.length > 0 &&
    topic.length < 65 &&
    topic.match(/^[\w._-]+(?:#ephemeral)?$/) != null;
}

/**
 * Validate channel names. Follows the same restriction as
 * topic names.
 * 
 * @param {String} topic.
 */
function validChannelName(channel) {
  return channel &&
    channel.length > 0 &&
    channel.length < 65 &&
    channel.match(/^[\w._-]+(?:#ephemeral)?$/) != null;
}
