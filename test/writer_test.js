import should from 'should'
import sinon from 'sinon'

import nsq from '../src/nsq'

describe('writer', () => {
  let writer = null

  beforeEach(() => {
    writer = new nsq.Writer('127.0.0.1', '4150')
    return writer.conn =
      { produceMessages: sinon.stub() }
  })

  afterEach(() => writer = null)

  return describe('publish', () => {
    it('should publish a string', () => {
      const topic = 'test_topic'
      const msg = 'hello world!'

      writer.publish(topic, msg)
      writer.conn.produceMessages.calledOnce
      return writer.conn.produceMessages.calledWith(topic, msg)
    })

    it('should publish a list of strings', () => {
      const topic = 'test_topic'
      const msgs = ['hello world!', 'another message']

      writer.publish(topic, msgs)
      writer.conn.produceMessages.calledOnce
      return writer.conn.produceMessages.calledWith(topic, msgs)
    })

    it('should publish a buffer', () => {
      const topic = 'test_topic'
      const msg = new Buffer('a buffer message')

      writer.publish(topic, msg)
      writer.conn.produceMessages.calledOnce
      return writer.conn.produceMessages.calledWith(topic, [msg])
    })

    it('should publish an object as JSON', () => {
      const topic = 'test_topic'
      const msg = { a: 1 }

      writer.publish(topic, msg)
      writer.conn.produceMessages.calledOnce
      return writer.conn.produceMessages.calledWith(topic, [JSON.stringify(msg)])
    })

    it('should publish a list of buffers', () => {
      const topic = 'test_topic'
      const msgs = [new Buffer('a buffer message'), new Buffer('another msg')]

      writer.publish(topic, msgs)
      writer.conn.produceMessages.calledOnce
      return writer.conn.produceMessages.calledWith(topic, msgs)
    })

    it('should publish a list of objects as JSON', () => {
      const topic = 'test_topic'
      const msgs = [{ a: 1 }, { b: 2 }]
      const encodedMsgs = (Array.from(msgs).map(i => JSON.stringify(i)))

      writer.publish(topic, msgs)
      writer.conn.produceMessages.calledOnce
      return writer.conn.produceMessages.calledWith(topic, encodedMsgs)
    })

    it('should fail when publishing Null', (done) => {
      const topic = 'test_topic'
      const msg = null

      return writer.publish(topic, msg, (err) => {
        err.should.exist
        return done()
      })
    })

    it('should fail when publishing Undefined', (done) => {
      const topic = 'test_topic'
      const msg = undefined

      return writer.publish(topic, msg, (err) => {
        err.should.exist
        return done()
      })
    })

    it('should fail when publishing an empty string', (done) => {
      const topic = 'test_topic'
      const msg = ''

      return writer.publish(topic, msg, (err) => {
        err.should.exist
        return done()
      })
    })

    it('should fail when publishing an empty list', (done) => {
      const topic = 'test_topic'
      const msg = []

      return writer.publish(topic, msg, (err) => {
        err.should.exist
        return done()
      })
    })

    return it('should fail when the Writer is not connected', (done) => {
      writer = new nsq.Writer('127.0.0.1', '4150')
      return writer.publish('test_topic', 'a briliant message', (err) => {
        err.should.exist
        return done()
      })
    })
  })
})
