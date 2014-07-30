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
        (-> msg.finish()).should.throw()
        done()

      setTimeout firstFinish, 10
      setTimeout secondFinish, 20

    it 'should not allow requeue after finish', (done) ->
      msg = createMessage 'body', 90, 50, 100

      firstFinish = ->
        msg.finish()

      secondRequeue = ->
        (-> msg.requeue()).should.throw()
        done()

      setTimeout firstFinish, 10
      setTimeout secondRequeue, 20

    it 'should error when finish after timeout', (done) ->
      timeoutIn = 10
      finishIn = 20

      msg = createMessage 'body', 90, timeoutIn, 100

      finish = ->
        (-> msg.finish()).should.throw()
        done()

      setTimeout finish, finishIn

    it 'should error when requeue after timeout', (done) ->
      timeoutIn = 10
      requeueIn = 20

      msg = createMessage 'body', 90, timeoutIn, 100

      requeue = ->
        (-> msg.requeue()).should.throw()
        done()

      setTimeout requeue, requeueIn

    it 'should allow touch and then finish post first timeout', (done) ->
      touchIn = 15
      timeoutIn = 20
      finishIn = 25

      msg = createMessage 'body', 90, timeoutIn, 100

      touch = ->
        msg.touch()

      finish = ->
        (-> msg.finish()).should.not.throw()
        done()

      setTimeout touch, touchIn
      setTimeout finish, finishIn

    it 'should error when touch fails to extend far enough', (done) ->
      touchIn = 5
      timeoutIn = 10
      finishIn = 25

      msg = createMessage 'body', 90, timeoutIn, 100

      touch = ->
        msg.touch()

      finish = ->
        (-> msg.finish()).should.throw()
        done()

      setTimeout touch, touchIn
      setTimeout finish, finishIn

    it 'touch should fail after hard timeout', (done) ->
      msg = createMessage 'body', 90, 10, 20

      okTouch = ->
        msg.touch()

      badTouch = ->
        (-> msg.touch()).should.throw()
        done()

      setTimeout okTouch, 9
      setTimeout okTouch, 15
      setTimeout badTouch, 35
