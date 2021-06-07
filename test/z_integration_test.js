const _ = require('lodash')
const child_process = require('child_process') // eslint-disable-line camelcase
const fetch = require('node-fetch')
const pEvent = require('p-event')
const retry = require('async-retry')
const should = require('should')
const temp = require('temp').track()
const url = require('url')
const util = require('util')

const nsq = require('../lib/nsq')
const EventEmitter = require('events')

let TCP_PORT = 4150
let HTTP_PORT = 4151

const startNSQD = async (dataPath, additionalOptions = {}) => {
  let options = {
    'http-address': `127.0.0.1:${HTTP_PORT}`,
    'tcp-address': `127.0.0.1:${TCP_PORT}`,
    'broadcast-address': '127.0.0.1',
    'data-path': dataPath,
    'tls-cert': './test/cert.pem',
    'tls-key': './test/key.pem',
  }

  _.extend(options, additionalOptions)

  // Convert to array for child_process.
  options = Object.keys(options).map((option) => [
    `-${option}`,
    options[option],
  ])

  const process = child_process.spawn('nsqd', _.flatten(options), {
    stdio: ['ignore', 'ignore', 'ignore'],
  })

  process.on('error', (err) => {
    throw err
  })

  await retry(
    async () => {
      const response = await fetch(`http://localhost:${HTTP_PORT}/ping`)
      if (!response.ok) {
        throw new Error('not ready')
      }
    },
    {retries: 10, minTimeout: 50}
  )

  return process
}

const topicOp = async (op, topic) => {
  const u = new url.URL(`http://127.0.0.1:${HTTP_PORT}/${op}`)
  u.searchParams.set('topic', topic)

  await fetch(u.toString(), {method: 'POST'})
}

const createTopic = async (topic) => topicOp('topic/create', topic)
const deleteTopic = async (topic) => topicOp('topic/delete', topic)

// Publish a single message via HTTP
const publish = async (topic, message) => {
  const u = new url.URL(`http://127.0.0.1:${HTTP_PORT}/pub`)
  u.searchParams.set('topic', topic)

  await fetch(u.toString(), {
    method: 'POST',
    body: message,
  })
}

describe('integration', () => {
  let nsqdProcess = null
  let reader = null

  beforeEach(async () => {
    nsqdProcess = await startNSQD(await temp.mkdir('/nsq'))
    await createTopic('test')
  })

  afterEach(async () => {
    const closeEvent = pEvent(reader, 'nsqd_closed')
    const exitEvent = pEvent(nsqdProcess, 'exit')

    reader.close()
    await closeEvent

    await deleteTopic('test')

    nsqdProcess.kill('SIGKILL')
    await exitEvent

    // After each start, increment the ports to prevent possible conflict the
    // next time an NSQD instance is started. Sometimes NSQD instances do not
    // exit cleanly causing odd behavior for tests and the test suite.
    TCP_PORT = TCP_PORT + 50
    HTTP_PORT = HTTP_PORT + 50

    reader = null
  })

  describe('stream compression and encryption', () => {
    const optionPermutations = [
      {deflate: true},
      {snappy: true},
      {tls: true, tlsVerification: false},
      {tls: true, tlsVerification: false, snappy: true},
      {tls: true, tlsVerification: false, deflate: true},
    ]

    optionPermutations.forEach((options) => {
      const compression = ['deflate', 'snappy']
        .filter((key) => key in options)
        .map((key) => key)

      compression.push('none')

      // Figure out what compression is enabled
      const description = `reader with compression (${
        compression[0]
      }) and tls (${options.tls != null})`

      describe(description, () => {
        it('should send and receive a message', async () => {
          const topic = 'test'
          const channel = 'default'
          const message = 'a message for our reader'

          await publish(topic, message)

          reader = new nsq.Reader(
            topic,
            channel,
            Object.assign(
              {nsqdTCPAddresses: [`127.0.0.1:${TCP_PORT}`]},
              options
            )
          )
          reader.on('error', () => {})

          const messageEvent = pEvent(reader, 'message')
          reader.connect()

          const msg = await messageEvent
          should.equal(msg.body.toString(), message)
          msg.finish()
        })

        it('should send and receive a large message', async () => {
          const topic = 'test'
          const channel = 'default'
          const message = _.range(0, 100000)
            .map(() => 'a')
            .join('')

          await publish(topic, message)

          reader = new nsq.Reader(
            topic,
            channel,
            Object.assign(
              {nsqdTCPAddresses: [`127.0.0.1:${TCP_PORT}`]},
              options
            )
          )
          reader.on('error', () => {})
          const messageEvent = pEvent(reader, 'message')
          reader.connect()

          const msg = await messageEvent
          should.equal(msg.body.toString(), message)
          msg.finish()
        })
      })
    })
  })

  describe('end to end', () => {
    const topic = 'test'
    const channel = 'default'
    let writer = null
    reader = null

    beforeEach((done) => {
      writer = new nsq.Writer('127.0.0.1', TCP_PORT)
      writer.on('ready', () => {
        reader = new nsq.Reader(topic, channel, {
          nsqdTCPAddresses: [`127.0.0.1:${TCP_PORT}`],
        })
        reader.on('nsqd_connected', () => done())
        reader.connect()
      })

      writer.on('error', () => {})
      writer.connect()
    })

    afterEach(() => {
      writer.close()
    })

    it('should send and receive a string', (done) => {
      const message = 'hello world'
      writer.publish(topic, message, (err) => {
        if (err) done(err)
      })

      reader.on('error', (err) => {
        console.log(err)
      })

      reader.on('message', (msg) => {
        msg.body.toString().should.eql(message)
        msg.finish()
        done()
      })
    })

    it('should send and receive a String object', (done) => {
      const message = new String('hello world')
      writer.publish(topic, message, (err) => {
        if (err) done(err)
      })

      reader.on('error', (err) => {
        console.log(err)
      })

      reader.on('message', (msg) => {
        msg.body.toString().should.eql(message.toString())
        msg.finish()
        done()
      })
    })

    it('should send and receive a Buffer', (done) => {
      const message = Buffer.from([0x11, 0x22, 0x33])
      writer.publish(topic, message)

      reader.on('error', () => {})

      reader.on('message', (readMsg) => {
        for (let i = 0; i < readMsg.body.length; i++) {
          should.equal(readMsg.body[i], message[i])
        }
        readMsg.finish()
        done()
      })
    })

    it('should not receive messages when immediately paused', (done) => {
      setTimeout(done, 50)

      // Note: because NSQDConnection.connect() does most of it's work in
      // process.nextTick(), we're really pausing before the reader is
      // connected.
      //
      reader.pause()
      reader.on('message', (msg) => {
        msg.finish()
        done(new Error('Should not have received a message while paused'))
      })

      writer.publish(topic, 'pause test')
    })

    it('should not receive any new messages when paused', (done) => {
      writer.publish(topic, {messageShouldArrive: true})

      reader.on('error', (err) => {
        console.log(err)
      })

      reader.on('message', (msg) => {
        // check the message
        msg.json().messageShouldArrive.should.be.true()
        msg.finish()

        if (reader.isPaused()) return done()

        reader.pause()

        process.nextTick(() => {
          // send it again, shouldn't get this one
          writer.publish(topic, {messageShouldArrive: false})
          setTimeout(done, 50)
        })
      })
    })

    it('should not receive any requeued messages when paused', (done) => {
      writer.publish(topic, 'requeue me')
      let id = ''

      reader.on('message', (msg) => {
        // this will fail if the msg comes through again
        id.should.equal('')
        id = msg.id

        reader.pause()

        // send it again, shouldn't get this one
        msg.requeue(0, false)
        setTimeout(done, 50)
      })

      reader.on('error', () => {})
    })

    it('should start receiving messages again after unpause async', async () => {
      const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

      const publish = util.promisify((topic, msg, cb) => {
        writer.publish(topic, msg, cb)
      })

      let paused = false
      const messageEvents = new EventEmitter()
      const firstEvent = pEvent(messageEvents, 'first')
      const secondEvent = pEvent(messageEvents, 'second')

      reader.on('message', (msg) => {
        should.equal(paused, false)
        messageEvents.emit(msg.body.toString(), msg)
      })

      // Pubish message
      publish(topic, 'first')

      // Handle first message
      let msg = await firstEvent
      paused.should.be.false()
      msg.finish()

      // Pause reader
      reader.pause()
      paused = true

      // Publish second message
      await publish(topic, 'second')

      // Unpause after delay
      await wait(50)
      reader.unpause()
      paused = false

      // Handle second message
      msg = await secondEvent
      msg.finish()
    })

    it('should successfully publish a message before fully connected', (done) => {
      writer = new nsq.Writer('127.0.0.1', TCP_PORT)
      writer.connect()

      // The writer is connecting, but it shouldn't be ready to publish.
      should.equal(writer.ready, false)

      writer.on('error', () => {})

      // Publish the message. It should succeed since the writer will queue up
      // the message while connecting.
      writer.publish('a_topic', 'a message', (err) => {
        should.not.exist(err)
        done()
      })
    })
  })
})

describe('failures', () => {
  let nsqdProcess = null

  before(async () => {
    nsqdProcess = await startNSQD(await temp.mkdir('/nsq'))
  })

  describe('Writer', () => {
    describe('nsqd disconnect before publish', () => {
      it('should fail to publish a message', async () => {
        const writer = new nsq.Writer('127.0.0.1', TCP_PORT)
        writer.on('error', () => {})

        const readyEvent = pEvent(writer, 'ready')
        const exitEvent = pEvent(nsqdProcess, 'exit')

        writer.connect()
        await readyEvent

        nsqdProcess.kill('SIGKILL')
        await exitEvent

        const publish = util.promisify((topic, msg, cb) =>
          writer.publish(topic, msg, cb)
        )
        try {
          await publish('test_topic', 'a failing message')
          should.fail()
        } catch (e) {
          should.exist(e)
        }
      })
    })
  })
})
