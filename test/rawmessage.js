function rawMessage(id, timestamp, attempts, body) {
  // Create the raw NSQ message
  id = Buffer.from(id)
  body = Buffer.from(body)

  const b = Buffer.alloc(8 + 2 + 16 + body.length)
  b.writeBigInt64BE(BigInt(timestamp), 0)
  b.writeInt16BE(attempts, 8)
  b.copy(id, 10, 0, 16)
  b.copy(body, 26, 0, body.length)

  return b
}

module.exports = rawMessage
