should = require 'should'
sinon = require 'sinon'

nsq = require '../src/nsq'

describe 'writer', ->
  writer = null

  beforeEach ->
    writer = new nsq.Writer '127.0.0.1', '4150'
    writer.conn =
      produceMessages: sinon.stub()

  afterEach ->
    writer = null

  describe 'publish', ->
    it 'should publish a string', ->
      topic = 'test_topic'
      msg = 'hello world!'

      writer.publish topic, msg
      writer.conn.produceMessages.calledOnce
      writer.conn.produceMessages.calledWith topic, msg
      
    it 'should publish a list of strings', ->
      topic = 'test_topic'
      msgs = ['hello world!', 'another message']

      writer.publish topic, msgs
      writer.conn.produceMessages.calledOnce
      writer.conn.produceMessages.calledWith topic, msgs

    it 'should publish a buffer', ->
      topic = 'test_topic'
      msg = new Buffer 'a buffer message'

      writer.publish topic, msg
      writer.conn.produceMessages.calledOnce
      writer.conn.produceMessages.calledWith topic, [msg]

    it 'should publish an object as JSON', ->
      topic = 'test_topic'
      msg = a: 1

      writer.publish topic, msg
      writer.conn.produceMessages.calledOnce
      writer.conn.produceMessages.calledWith topic, [JSON.stringify msg]

    it 'should publish a list of buffers', ->
      topic = 'test_topic'
      msgs = [new Buffer('a buffer message'), new Buffer('another msg')]

      writer.publish topic, msgs
      writer.conn.produceMessages.calledOnce
      writer.conn.produceMessages.calledWith topic, msgs

    it 'should publish a list of objects as JSON', ->
      topic = 'test_topic'
      msgs = [{a: 1}, {b: 2}]
      encodedMsgs = (JSON.stringify i for i in msgs)

      writer.publish topic, msgs
      writer.conn.produceMessages.calledOnce
      writer.conn.produceMessages.calledWith topic, encodedMsgs

    it 'should fail when publishing Null', (done) ->
      topic = 'test_topic'
      msg = null

      writer.publish topic, msg, (err) ->
        err.should.exist
        done()

    it 'should fail when publishing Undefined', (done) ->
      topic = 'test_topic'
      msg = undefined

      writer.publish topic, msg, (err) ->
        err.should.exist
        done()

    it 'should fail when publishing an empty string', (done) ->
      topic = 'test_topic'
      msg = ''

      writer.publish topic, msg, (err) ->
        err.should.exist
        done()

    it 'should fail when publishing an empty list', (done) ->
      topic = 'test_topic'
      msg = []

      writer.publish topic, msg, (err) ->
        err.should.exist
        done()

    it 'should fail when the Writer is not connected', (done) ->
      writer = new nsq.Writer '127.0.0.1', '4150'
      writer.publish 'test_topic', 'a briliant message', (err) ->
        err.should.exist
        done()
