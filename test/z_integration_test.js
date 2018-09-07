const _ = require('lodash')
const async = require('async')
const child_process = require('child_process') // eslint-disable-line camelcase
const request = require('request')
const should = require('should')

const nsq = require('../lib/nsq')

const temp = require('temp').track()

let TCP_PORT = 4150
let HTTP_PORT = 4151

const startNSQD = (dataPath, additionalOptions = {}, callback) => {
  let options = {
    'http-address': `127.0.0.1:${HTTP_PORT}`,
    'tcp-address': `127.0.0.1:${TCP_PORT}`,
    'broadcast-address': '127.0.0.1',
    'data-path': dataPath,
    'tls-cert': './test/cert.pem',
    'tls-key': './test/key.pem'
  }

  _.extend(options, additionalOptions)

  // Convert to array for child_process.
  options = Object.keys(options).map(option => [`-${option}`, options[option]])

  const process = child_process.spawn('nsqd', _.flatten(options), {
    stdio: ['ignore', 'ignore', 'ignore']
  })

  process.on('error', err => {
    throw err
  })

  const retryOptions = { times: 10, interval: 50 }
  const liveliness = callback => {
    request(`http://localhost:${HTTP_PORT}/ping`, (err, res, body) => {
      if (err || res.statusCode != 200) {
        return callback(new Error('nsqd not ready'))
      }
      callback()
    })
  }

  async.retry(retryOptions, liveliness, err => {
    callback(err, process)
  })
}

const topicOp = (op, topic, callback) => {
  const options = {
    method: 'POST',
    uri: `http://127.0.0.1:${HTTP_PORT}/${op}`,
    qs: {
      topic
    }
  }

  request(options, err => callback(err))
}

const createTopic = _.partial(topicOp, 'topic/create')
const deleteTopic = _.partial(topicOp, 'topic/delete')

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

  request(options, err => callback(err))
}

describe('integration', () => {
  let nsqdProcess = null
  let reader = null

  beforeEach(done => {
    async.series(
      [
        // Start NSQD
        callback => {
          temp.mkdir('/nsq', (err, dirPath) => {
            if (err) return callback(err)

            startNSQD(dirPath, {}, (err, process) => {
              nsqdProcess = process
              callback(err)
            })
          })
        },
        // Create the test topic
        callback => {
          createTopic('test', callback)
        }
      ],
      done
    )
  })

  afterEach(done => {
    async.series(
      [
        callback => {
          reader.on('nsqd_closed', nsqdAddress => {
            callback()
          })
          reader.close()
        },
        callback => {
          deleteTopic('test', callback)
        },
        callback => {
          nsqdProcess.on('exit', err => {
            callback(err)
          })
          nsqdProcess.kill('SIGKILL')
        }
      ],
      err => {
        // After each start, increment the ports to prevent possible conflict the
        // next time an NSQD instance is started. Sometimes NSQD instances do not
        // exit cleanly causing odd behavior for tests and the test suite.
        TCP_PORT = TCP_PORT + 50
        HTTP_PORT = HTTP_PORT + 50

        reader = null
        done(err)
      }
    )
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
      const description = `reader with compression (${compression[0]}) and tls (${options.tls !=
        null})`

      describe(description, () => {
        it('should send and receive a message', done => {
          const topic = 'test'
          const channel = 'default'
          const message = 'a message for our reader'

          publish(topic, message)

          reader = new nsq.Reader(
            topic,
            channel,
            _.extend({ nsqdTCPAddresses: [`127.0.0.1:${TCP_PORT}`] }, options)
          )

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
          const message = _.range(0, 100000)
            .map(() => 'a')
            .join('')

          publish(topic, message)

          reader = new nsq.Reader(
            topic,
            channel,
            _.extend({ nsqdTCPAddresses: [`127.0.0.1:${TCP_PORT}`] }, options)
          )

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
    let writer = null
    reader = null

    beforeEach(done => {
      writer = new nsq.Writer('127.0.0.1', TCP_PORT)
      writer.on('ready', () => {
        reader = new nsq.Reader(topic, channel, {
          nsqdTCPAddresses: [`127.0.0.1:${TCP_PORT}`]
        })
        reader.on('nsqd_connected', addr => done())
        reader.connect()
      })

      writer.on('error', () => {})
      writer.connect()
    })

    afterEach(() => {
      writer.close()
    })

    it('should send and receive a string', done => {
      const message = 'hello world'
      writer.publish(topic, message, err => {
        if (err) done(err)
      })

      reader.on('error', err => {
        console.log(err)
      })

      reader.on('message', msg => {
        msg.body.toString().should.eql(message)
        msg.finish()
        done()
      })
    })

    it('should send and receive a Buffer', done => {
      const message = Buffer.from([0x11, 0x22, 0x33])
      writer.publish(topic, message)

      reader.on('error', () => {})

      reader.on('message', readMsg => {
        for (let i = 0; i < readMsg.body.length; i++) {
          should.equal(readMsg.body[i], message[i])
        }
        readMsg.finish()
        done()
      })
    })

    it('should not receive messages when immediately paused', done => {
      setTimeout(done, 50)

      // Note: because NSQDConnection.connect() does most of it's work in
      // process.nextTick(), we're really pausing before the reader is
      // connected.
      //
      reader.pause()
      reader.on('message', msg => {
        msg.finish()
        done(new Error('Should not have received a message while paused'))
      })

      writer.publish(topic, 'pause test')
    })

    it('should not receive any new messages when paused', done => {
      writer.publish(topic, { messageShouldArrive: true })

      reader.on('error', err => {
        console.log(err)
      })

      reader.on('message', msg => {
        // check the message
        msg.json().messageShouldArrive.should.be.true()
        msg.finish()

        if (reader.isPaused()) return done()

        reader.pause()

        process.nextTick(() => {
          // send it again, shouldn't get this one
          writer.publish(topic, { messageShouldArrive: false })
          setTimeout(done, 50)
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

        reader.pause()

        // send it again, shouldn't get this one
        msg.requeue(0, false)
        setTimeout(done, 50)
      })

      reader.on('error', err => {
        console.log(err)
      })
    })

    it('should start receiving messages again after unpause', done => {
      let paused = false
      let handlerFn = null
      let afterHandlerFn = null

      const firstMessage = msg => {
        reader.pause()
        paused = true
        msg.requeue()
      }

      const secondMessage = msg => {
        msg.finish()
      }

      reader.on('message', msg => {
        should.equal(paused, false)
        handlerFn(msg)

        if (afterHandlerFn) {
          afterHandlerFn()
        }
      })

      async.series(
        [
          // Publish and handle first message
          callback => {
            handlerFn = firstMessage
            afterHandlerFn = callback

            writer.publish(topic, 'not paused', err => {
              if (err) {
                callback(err)
              }
            })
          },
          // Publish second message
          callback => {
            afterHandlerFn = callback
            writer.publish(topic, 'paused', callback)
          },
          // Wait for 50ms
          callback => {
            setTimeout(callback, 50)
          },
          // Unpause. Processed queued message.
          callback => {
            handlerFn = secondMessage
            // Note: We know a message was processed after unpausing when this
            // callback is called. No need to explicitly note a 2nd message was
            // processed.
            afterHandlerFn = callback

            reader.unpause()
            paused = false
          }
        ],
        done
      )
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
        async.series(
          [
            // Connect the writer to the nsqd.
            callback => {
              writer.connect()
              writer.on('ready', callback)
              writer.on('error', () => {}) // Ensure error message is handled.
            },

            // Stop the nsqd process.
            callback => {
              nsqdProcess.on('exit', callback)
              nsqdProcess.kill('SIGKILL')
            },

            // Attempt to publish a message.
            callback => {
              writer.publish(
                'test_topic',
                'a message that should fail',
                err => {
                  should.exist(err)
                  callback()
                }
              )
            }
          ],
          done
        )
      })
    })
  })
})
