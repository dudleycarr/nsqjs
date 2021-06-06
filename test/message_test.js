const should = require('should')
const sinon = require('sinon')
const Message = require('../lib/message')
const rawMessage = require('./rawmessage')

const createMessage = (body, requeueDelay, timeout, maxTimeout) => {
  return new Message(
    rawMessage('1', Date.now(), 0, body),
    requeueDelay,
    timeout,
    maxTimeout
  )
}

describe('Message', () =>
  describe('timeout', () => {
    it('should not allow finishing a message twice', (done) => {
      const msg = createMessage('body', 90, 50, 100)

      const firstFinish = () => msg.finish()
      const secondFinish = () => {
        msg.hasResponded.should.eql(true)
        done()
      }

      setTimeout(firstFinish, 10)
      setTimeout(secondFinish, 20)
    })

    it('should not allow requeue after finish', (done) => {
      const msg = createMessage('body', 90, 50, 100)

      const responseSpy = sinon.spy()
      msg.on(Message.RESPOND, responseSpy)

      const firstFinish = () => msg.finish()
      const secondRequeue = () => msg.requeue()

      const check = () => {
        responseSpy.calledOnce.should.be.true()
        done()
      }

      setTimeout(firstFinish, 10)
      setTimeout(secondRequeue, 20)
      setTimeout(check, 20)
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

      const finish = () => {
        msg.timedOut.should.be.eql(false)
        msg.finish()
      }

      const check = () => {
        responseSpy.calledTwice.should.be.true()
        done()
      }

      setTimeout(touch, touchIn)
      setTimeout(finish, finishIn)
      setTimeout(check, checkIn)
    })

    return it('should clear timeout on finish', (done) => {
      const msg = createMessage('body', 10, 60, 120)
      msg.finish()

      return process.nextTick(() => {
        should.not.exist(msg.trackTimeoutId)
        return done()
      })
    })
  }))
