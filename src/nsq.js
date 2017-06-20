// Necessary for node <= 0.10.
import { NSQDConnection, WriterNSQDConnection } from './nsqdconnection'
const Reader = require('./reader')
const Writer = require('./writer')

module.exports = {
  Reader,
  Writer,
  NSQDConnection,
  WriterNSQDConnection
}
