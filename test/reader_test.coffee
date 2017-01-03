should = require 'should'
sinon = require 'sinon'

nsq = require '../src/nsq'

describe 'reader', ->

  readerWithAttempts = (attempts) ->
    new nsq.Reader 'topic', 'default',
      nsqdTCPAddresses: ['127.0.0.1:4150']
      maxAttempts: attempts

  describe 'max attempts', ->
    describe 'exceeded', ->
      it 'should finish after exceeding specified max attempts', (done) ->
        maxAttempts = 2
        reader = readerWithAttempts maxAttempts

        # Message that has exceed the maximum number of attempts
        message =
          attempts: maxAttempts
          finish: sinon.spy()

        reader.handleMessage message

        process.nextTick ->
          message.finish.called.should.be.true()
          done()

      it 'should call the DISCARD message hanlder if registered', (done) ->
        maxAttempts = 2
        reader = readerWithAttempts maxAttempts

        message =
          attempts: maxAttempts
          finish: ->

        reader.on nsq.Reader.DISCARD, (msg) ->
          done()

        reader.handleMessage message

      it 'should call the MESSAGE handler by default', (done) ->
        maxAttempts = 2
        reader = readerWithAttempts maxAttempts

        message =
          attempts: maxAttempts
          finish: ->

        reader.on nsq.Reader.MESSAGE, (msg) ->
          done()

        reader.handleMessage message

  describe 'off by default', ->
    it 'should not finish the message', (done) ->
      reader = readerWithAttempts 0

      message =
        attempts: 100
        finish: sinon.spy()

      # Registering this to make sure that even if the listener is available,
      # it should not be getting called.
      reader.on nsq.Reader.DISCARD, (msg) ->
        done new Error 'Unexpected discard message'

      messageHandlerSpy = sinon.spy()
      reader.on nsq.Reader.MESSAGE, messageHandlerSpy

      reader.handleMessage message

      process.nextTick ->
        messageHandlerSpy.called.should.be.true()
        message.finish.called.should.be.false()
        done()
