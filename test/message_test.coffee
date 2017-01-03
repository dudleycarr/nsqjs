should = require 'should'
sinon = require 'sinon'


Message = require '../src/message'

createMessage = (body, requeueDelay, timeout, maxTimeout) ->
  new Message '1', Date.now(), 0, new Buffer(body), requeueDelay, timeout,
    maxTimeout

describe 'Message', ->
  describe 'timeout', ->
    it 'should not allow finishing a message twice', (done) ->
      msg = createMessage 'body', 90, 50, 100

      firstFinish = ->
        msg.finish()

      secondFinish = ->
        msg.hasResponded.should.eql true
        done()

      setTimeout firstFinish, 10
      setTimeout secondFinish, 20

    it 'should not allow requeue after finish', (done) ->
      msg = createMessage 'body', 90, 50, 100

      responseSpy = sinon.spy()
      msg.on Message.RESPOND, responseSpy

      firstFinish = ->
        msg.finish()

      secondRequeue = ->
        msg.requeue()

      check = ->
        responseSpy.calledOnce.should.be.true()
        done()

      setTimeout firstFinish, 10
      setTimeout secondRequeue, 20
      setTimeout check, 20

    it 'should allow touch and then finish post first timeout', (done) ->
      touchIn = 15
      timeoutIn = 20
      finishIn = 25
      checkIn = 30

      msg = createMessage 'body', 90, timeoutIn, 100
      responseSpy = sinon.spy()
      msg.on Message.RESPOND, responseSpy

      touch = ->
        msg.touch()

      finish = ->
        msg.timedOut.should.be.eql false
        msg.finish()

      check = ->
        responseSpy.calledTwice.should.be.true()
        done()

      setTimeout touch, touchIn
      setTimeout finish, finishIn
      setTimeout check, checkIn

    it 'should clear timeout on finish', (done) ->
      msg = createMessage 'body', 10, 60, 120
      msg.finish()

      process.nextTick ->
        should.not.exist msg.trackTimeoutId
        done()
