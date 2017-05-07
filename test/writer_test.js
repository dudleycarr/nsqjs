import should from 'should';
import sinon from 'sinon';
import ArrayFrom from 'array.from';

const nsq = require('../src/nsq');

describe('writer', () => {
  let writer = null;

  beforeEach(() => {
    writer = new nsq.Writer('127.0.0.1', '4150');
    writer.conn = { produceMessages: sinon.stub() };
  });

  afterEach(() => {
    writer = null;
  });

  describe('publish', () => {
    it('should publish a string', () => {
      const topic = 'test_topic';
      const msg = 'hello world!';

      writer.publish(topic, msg, () => {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(
          writer.conn.produceMessages.calledWith(topic, [msg]),
          true
        );
      });
    });

    it('should publish a list of strings', () => {
      const topic = 'test_topic';
      const msgs = ['hello world!', 'another message'];

      writer.publish(topic, msgs, () => {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(writer.conn.produceMessages.calledWith(topic, msgs), true);
      });
    });

    it('should publish a buffer', () => {
      const topic = 'test_topic';
      const msg = new Buffer('a buffer message');

      writer.publish(topic, msg, () => {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(
          writer.conn.produceMessages.calledWith(topic, [msg]),
          true
        );
      });
    });

    it('should publish an object as JSON', () => {
      const topic = 'test_topic';
      const msg = { a: 1 };

      writer.publish(topic, msg, () => {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(
          writer.conn.produceMessages.calledWith(topic, [JSON.stringify(msg)]),
          true
        );
      });
    });

    it('should publish a list of buffers', () => {
      const topic = 'test_topic';
      const msgs = [new Buffer('a buffer message'), new Buffer('another msg')];

      writer.publish(topic, msgs, () => {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(writer.conn.produceMessages.calledWith(topic, msgs), true);
      });
    });

    it('should publish a list of objects as JSON', () => {
      const topic = 'test_topic';
      const msgs = [{ a: 1 }, { b: 2 }];
      const encodedMsgs = ArrayFrom(msgs).map(i => JSON.stringify(i));

      writer.publish(topic, msgs, () => {
        should.equal(writer.conn.produceMessages.calledOnce, true);
        should.equal(
          writer.conn.produceMessages.calledWith(topic, encodedMsgs),
          true
        );
      });
    });

    it('should fail when publishing Null', done => {
      const topic = 'test_topic';
      const msg = null;

      writer.publish(topic, msg, err => {
        should.exist(err);
        done();
      });
    });

    it('should fail when publishing Undefined', done => {
      const topic = 'test_topic';
      const msg = undefined;

      writer.publish(topic, msg, err => {
        should.exist(err);
        done();
      });
    });

    it('should fail when publishing an empty string', done => {
      const topic = 'test_topic';
      const msg = '';

      writer.publish(topic, msg, err => {
        should.exist(err);
        done();
      });
    });

    it('should fail when publishing an empty list', done => {
      const topic = 'test_topic';
      const msg = [];

      writer.publish(topic, msg, err => {
        should.exist(err);
        done();
      });
    });

    it('should fail when the Writer is not connected', done => {
      writer = new nsq.Writer('127.0.0.1', '4150');
      writer.publish('test_topic', 'a briliant message', err => {
        should.exist(err);
        done();
      });
    });
  });
});
