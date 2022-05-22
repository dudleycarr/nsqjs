const should = require('should')
const sinon = require('sinon')

const nsq = require('../lib/nsq')

describe('reader', () => {
  const readerWithAttempts = (attempts) =>
    new nsq.Reader('topic', 'default', {
      nsqdTCPAddresses: ['127.0.0.1:4150'],
      maxAttempts: attempts,
    })

  describe('max attempts', () =>
    describe('exceeded', () => {
      it('should process msg while attempts do not exceed max', (done) => {
        const maxAttempts = 1
        const reader = readerWithAttempts(maxAttempts)

        reader.on(nsq.Reader.DISCARD, () => {
          done(new Error('should not be discarded'))
        })
        reader.on(nsq.Reader.MESSAGE, () => done())
        reader.handleMessage({attempts: 1, finish: () => {}})
      })

      it('should finish after exceeding specified max attempts', (done) => {
        const maxAttempts = 2
        const reader = readerWithAttempts(maxAttempts)

        // Message that has exceed the maximum number of attempts
        const message = {
          attempts: maxAttempts + 1,
          finish: sinon.spy(),
        }

        reader.handleMessage(message)

        process.nextTick(() => {
          should.equal(message.finish.called, true)
          done()
        })
      })

      it('should call the DISCARD message hanlder if registered', (done) => {
        const maxAttempts = 2
        const reader = readerWithAttempts(maxAttempts)

        const message = {
          attempts: maxAttempts + 1,
          finish() {},
        }

        reader.on(nsq.Reader.DISCARD, () => done())
        reader.handleMessage(message)
      })

      it('should call the MESSAGE handler by default', (done) => {
        const maxAttempts = 2
        const reader = readerWithAttempts(maxAttempts)

        const message = {
          attempts: maxAttempts + 1,
          finish() {},
        }

        reader.on(nsq.Reader.MESSAGE, () => done())
        reader.handleMessage(message)
      })
    }))

  describe('off by default', () =>
    it('should not finish the message', (done) => {
      const reader = readerWithAttempts(0)

      const message = {
        attempts: 100,
        finish: sinon.spy(),
      }

      // Registering this to make sure that even if the listener is available,
      // it should not be getting called.
      reader.on(nsq.Reader.DISCARD, () => {
        done(new Error('Unexpected discard message'))
      })

      const messageHandlerSpy = sinon.spy()
      reader.on(nsq.Reader.MESSAGE, messageHandlerSpy)
      reader.handleMessage(message)

      process.nextTick(() => {
        should.equal(messageHandlerSpy.called, true)
        should.equal(message.finish.called, false)
        done()
      })
    }))
})
