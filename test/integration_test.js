const temp = require('temp').track()
import _ from 'underscore'
import async from 'async'
import child_process from 'child_process' // eslint-disable-line camelcase
import nsq from '../src/nsq'
import request from 'request'
import should from 'should'

const TCP_PORT = 14150
const HTTP_PORT = 14151

const startNSQD = (dataPath, additionalOptions, callback) => {
  if (!additionalOptions) { additionalOptions = {} }
  let options = {
    'http-address': `127.0.0.1:${HTTP_PORT}`,
    'tcp-address': `127.0.0.1:${TCP_PORT}`,
    'data-path': dataPath,
    'tls-cert': './test/cert.pem',
    'tls-key': './test/key.pem'
  }

  _.extend(options, additionalOptions)

  // Convert to array for child_process.
  options = Object.keys(options).map(option => [`-${option}`, options[option]])

  const process = child_process.spawn('nsqd',
    _.flatten(options), {
      stdio: ['ignore', 'ignore', 'ignore']
    })

  setTimeout(() => callback(null, process), 500)
}

const topicOp = (op, topic, callback) => {
  const options = {
    method: 'POST',
    uri: `http://127.0.0.1:${HTTP_PORT}/${op}`,
    qs: {
      topic
    }
  }

  request(options, (err, res, body) => callback(err))
}

const createTopic = _.partial(topicOp, 'create_topic')
const deleteTopic = _.partial(topicOp, 'delete_topic')

// Publish a single message via HTTP
const publish = (topic, message, callback = () => {}) => {
  const options = {
    uri: `http://127.0.0.1:${HTTP_PORT}/pub`,
    method: 'POST',
    qs: {
      topic
    },
    body: message
  }

  request(options, (err, res, body) => callback(err))
}

describe('integration', () => {
  let nsqdProcess = null
  let reader = null

  before(done =>
    temp.mkdir('/nsq', (err, dirPath) => {
      if (err) return done(err)

      startNSQD(dirPath, {}, (err, process) => {
        nsqdProcess = process
        done(err)
      })
    })
  )

  after(done => {
    nsqdProcess.kill()
    // Give nsqd a chance to exit before it's data directory will be cleaned up.
    setTimeout(done, 500)
  })

  beforeEach(done => createTopic('test', done))

  afterEach(done => {
    reader.close()
    deleteTopic('test', done)
  })

  describe('stream compression and encryption', () => {
    const optionPermutations = [
      { deflate: true },
      { snappy: true },
      { tls: true, tlsVerification: false },
      { tls: true, tlsVerification: false, snappy: true },
      { tls: true, tlsVerification: false, deflate: true }
    ]

    optionPermutations.forEach(options => {
      const compression = ['deflate', 'snappy']
        .filter(key => key in options)
        .map(key => key)

      compression.push('none')

      // Figure out what compression is enabled
      let description = `reader with compression (${compression[0]}) and tls (${(options.tls != null)})`

      describe(description, () => {
        it('should send and receive a message', done => {
          const topic = 'test'
          const channel = 'default'
          const message = 'a message for our reader'

          publish(topic, message)

          reader = new nsq.Reader(topic, channel,
            _.extend({ nsqdTCPAddresses: [`127.0.0.1:${TCP_PORT}`] }, options))

          reader.on('message', msg => {
            should.equal(msg.body.toString(), message)
            msg.finish()
            done()
          })

          reader.on('error', () => {})

          reader.connect()
        })

        it('should send and receive a large message', done => {
          const topic = 'test'
          const channel = 'default'
          const message = (__range__(0, 100000, true).map(i => 'a')).join('')

          publish(topic, message)

          reader = new nsq.Reader(topic, channel,
            _.extend({ nsqdTCPAddresses: [`127.0.0.1:${TCP_PORT}`] }, options))

          reader.on('message', msg => {
            should.equal(msg.body.toString(), message)
            msg.finish()
            done()
          })

          reader.on('error', () => {})

          reader.connect()
        })
      })
    })
  })

  describe('end to end', () => {
    const topic = 'test'
    const channel = 'default'
    const tcpAddress = `127.0.0.1:${TCP_PORT}`
    let writer = null
    reader = null

    beforeEach((done) => {
      writer = new nsq.Writer('127.0.0.1', TCP_PORT)
      writer.on('ready', () => {
        reader = new nsq.Reader(topic, channel, { nsqdTCPAddresses: tcpAddress })
        reader.connect()
        done()
      })

      writer.on('error', () => {})

      writer.connect()
    })

    afterEach(() => writer.close())

    it('should send and receive a string', done => {
      const message = 'hello world'
      writer.publish(topic, message, err => {
        if (err) done(err)
      })

      reader.on('error', () => {})

      reader.on('message', (msg) => {
        msg.body.toString().should.eql(message)
        msg.finish()
        done()
      })
    })

    it('should send and receive a Buffer', done => {
      const message = new Buffer([0x11, 0x22, 0x33])
      writer.publish(topic, message)

      reader.on('error', () => {})

      reader.on('message', readMsg => {
        readMsg.body.forEach((readByte, i) => {
          should.equal(readByte, message[i])
        })
        readMsg.finish()
        done()
      })
    })

    // TODO (Dudley): The behavior of nsqd seems to have changed around this.
    //    This requires more investigation but not concerning at the moment.
    it.skip('should not receive messages when immediately paused', done => {
      let waitedLongEnough = false

      const timeout = setTimeout(() => {
        reader.unpause()
        waitedLongEnough = true
      }, 100)

      // Note: because NSQDConnection.connect() does most of it's work in
      // process.nextTick(), we're really pausing before the reader is
      // connected.
      //
      reader.pause()
      reader.on('message', msg => {
        msg.finish()
        clearTimeout(timeout)
        waitedLongEnough.should.be.true()
        done()
      })

      writer.publish(topic, 'pause test')
    })

    it('should not receive any new messages when paused', done => {
      writer.publish(topic, { messageShouldArrive: true })

      reader.on('message', msg => {
        // check the message
        msg.json().messageShouldArrive.should.be.true()
        msg.finish()

        if (reader.isPaused()) return done()

        reader.pause()

        process.nextTick(() => {
          // send it again, shouldn't get this one
          writer.publish(topic, { messageShouldArrive: false })
          setTimeout(done, 100)
        })
      })
    })

    it('should not receive any requeued messages when paused', done => {
      writer.publish(topic, 'requeue me')
      let id = ''

      reader.on('message', msg => {
        // this will fail if the msg comes through again
        id.should.equal('')
        id = msg.id

        if (reader.isPaused()) return done()
        reader.pause()

        process.nextTick(() => {
          // send it again, shouldn't get this one
          msg.requeue(0, false)
          setTimeout(done, 100)
        })
      })
    })

    it('should start receiving messages again after unpause', done => {
      let shouldReceive = true
      writer.publish(topic, { sentWhilePaused: false })

      reader.on('message', msg => {
        should.equal(shouldReceive, true)
        reader.pause()
        msg.requeue()

        if (msg.json().sentWhilePaused) return done()

        shouldReceive = false
        writer.publish(topic, { sentWhilePaused: true })
        setTimeout(() => {
          shouldReceive = true
          reader.unpause()
        }, 100)

        done()
      })
    })

    it('should successfully publish a message before fully connected', done => {
      writer = new nsq.Writer('127.0.0.1', TCP_PORT)
      writer.connect()

      // The writer is connecting, but it shouldn't be ready to publish.
      should.equal(writer.ready, false)

      writer.on('error', () => {})

      // Publish the message. It should succeed since the writer will queue up
      // the message while connecting.
      writer.publish('a_topic', 'a message', err => {
        should.not.exist(err)
        done()
      })
    })
  })
})

describe('failures', () => {
  let nsqdProcess = null

  before(done => {
    temp.mkdir('/nsq', (err, dirPath) => {
      if (err) return done(err)

      startNSQD(dirPath, {}, (err, process) => {
        nsqdProcess = process
        done(err)
      })
    })
  })

  describe('Writer', () => {
    describe('nsqd disconnect before publish', () => {
      it('should fail to publish a message', done => {
        const writer = new nsq.Writer('127.0.0.1', TCP_PORT)
        async.series([
          // Connect the writer to the nsqd.
          callback => {
            writer.connect()
            writer.on('ready', callback)
            writer.on('error', () => {}) // Ensure error message is handled.
          },

          // Stop the nsqd process.
          callback => {
            nsqdProcess.kill()
            setTimeout(callback, 200)
          },

          // Attempt to publish a message.
          callback => {
            writer.publish('test_topic', 'a message that should fail', err => {
              should.exist(err)
              callback()
            })
          }

        ], done)
      })
    })
  })
})

function __range__ (left, right, inclusive) {
  const range = []
  const ascending = left < right
  const end = !inclusive ? right : ascending ? right + 1 : right - 1
  for (let i = left; ascending ? i < end : i > end; ascending ? i++ : i--) {
    range.push(i)
  }
  return range
}