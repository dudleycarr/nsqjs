const Int64 = require('node-int64')

function rawMessage (id, timestamp, attempts, body) {
  // Create the raw NSQ message
  id = Buffer.from(id)
  timestamp = (new Int64(timestamp)).toBuffer()
  body = Buffer.from(body)

  const b = Buffer.alloc(8 + 2 + 16 + body.length)
  b.copy(timestamp, 0, 0, 8)
  b.writeInt16BE(attempts, 8)
  b.copy(id, 10, 0, 16)
  b.copy(body, 26, 0, body.length)

  return b
}

module.exports = rawMessage
