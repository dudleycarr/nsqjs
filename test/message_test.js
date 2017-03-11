import should from 'should'
import sinon from 'sinon'
import Message from '../src/message'

const createMessage = (body, requeueDelay, timeout, maxTimeout) =>
  new Message('1', Date.now(), 0, new Buffer(body), requeueDelay, timeout,
    maxTimeout)

describe('Message', () =>
  describe('timeout', () => {
    it('should not allow finishing a message twice', (done) => {
      const msg = createMessage('body', 90, 50, 100)

      const firstFinish = () => msg.finish()

      const secondFinish = function () {
        msg.hasResponded.should.eql(true)
        return done()
      }

      setTimeout(firstFinish, 10)
      return setTimeout(secondFinish, 20)
    })

    it('should not allow requeue after finish', (done) => {
      const msg = createMessage('body', 90, 50, 100)

      const responseSpy = sinon.spy()
      msg.on(Message.RESPOND, responseSpy)

      const firstFinish = () => msg.finish()

      const secondRequeue = () => msg.requeue()

      const check = function () {
        responseSpy.calledOnce.should.be.true()
        return done()
      }

      setTimeout(firstFinish, 10)
      setTimeout(secondRequeue, 20)
      return setTimeout(check, 20)
    })

    it('should allow touch and then finish post first timeout', (done) => {
      const touchIn = 15
      const timeoutIn = 20
      const finishIn = 25
      const checkIn = 30

      const msg = createMessage('body', 90, timeoutIn, 100)
      const responseSpy = sinon.spy()
      msg.on(Message.RESPOND, responseSpy)

      const touch = () => msg.touch()

      const finish = function () {
        msg.timedOut.should.be.eql(false)
        return msg.finish()
      }

      const check = function () {
        responseSpy.calledTwice.should.be.true()
        return done()
      }

      setTimeout(touch, touchIn)
      setTimeout(finish, finishIn)
      return setTimeout(check, checkIn)
    })

    return it('should clear timeout on finish', (done) => {
      const msg = createMessage('body', 10, 60, 120)
      msg.finish()

      return process.nextTick(() => {
        should.not.exist(msg.trackTimeoutId)
        return done()
      })
    })
  }),
)
