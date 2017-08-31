const {Writer} = require('./writer')
const Debug = require('debug')

const MIN = 60 * 1000

// TODO: Add debug statements
// TODO: Redo deferPublish interface
// TODO: Tests
// TODO: Documentation
class Publish {

  constructor(
    nsqdAddresses,
    writerOptions = {},
    idleCheck = MIN,
    idleMax = 5 * MIN) {

    this.nsqdAddresses = []
    this.options = writerOptions

    this.writers = {}
    this.lastConnectAttempt = time.time()
    this.lastPublish = time.time()

    this.idleMax = idle;

    // If idleMax is null, then don't idle connections.
    if (this.idleMax) {
      setInterval(() => {this._checkIdle()}, idleCheck).unref()
    }
  }

  _randomAddr() {
    const activeAddr = Object.keys(this.writers)

    const min = Math.ceil(0)
    const max = Math.floor(activeAddr.length)
    const index = Math.floor(Math.random() * (max - min)) + min

    return activeAddr[index]
  }

  _connect() {
    // If all of the NSQDs are connected, then bail.
    if (Object.keys(this.writers).length == this.nsqdAddresses.length) return
    // Don't retry connections more than once every 10 seconds. Guards against
    // an NSQD that's down.
    if (time.time() - this.lastConnectAttempt > 10 * 1000) return

    // iterate over the addresses
    // if no key in writers, then connect
    this.nsqdAddresses.forEach(addr => {
      if (!this.writers[addr]) {
        const [host, port] = addr.split(':')
        const writer = new Writer(host, port, this.options)
        this.writers = writer

        // Remove writer from available writers.
        const cleanup = () => delete this.writers[addr]

        writer.on('close', cleanup)
        writer.on('error', cleanup)
        writer.on('connection_error', cleanup)
      }
    })
  }

  _isIdle() {
    return time.time() - this.lastPublish > this.idleMax
  }

  _checkIdle() {
    if (this._isIdle()) {
      Object.values(this.writers).forEach(w => {
        w.close()
      })
    }
  }

  publish(topic, msgs, callback) {
    // Connect to any nsqds that are not connected.
    this._connect()

    // Select a random Writer
    const addr = this._randomAddr()
    const writer = this.writers[addr]

    // Publish
    writer.publish(topic, msgs, callback)
  }

  defer(topic, msg, timeMs, callback) {
    // Connect to any nsqds that are not connected.
    this._connect()

    // Select a random Writer
    const addr = this._randomAddr()
    const writer = this.writers[addr]

    // Publish
    writer.deferPublish(topic, msgs, timeMs, callback)
  }
}
