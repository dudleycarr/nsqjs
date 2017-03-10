import should from 'should';
import sinon from 'sinon';

import nsq from '../src/nsq';

describe('writer', function() {
  let writer = null;

  beforeEach(function() {
    writer = new nsq.Writer('127.0.0.1', '4150');
    return writer.conn =
      {produceMessages: sinon.stub()};
  });

  afterEach(() => writer = null);

  return describe('publish', function() {
    it('should publish a string', function() {
      let topic = 'test_topic';
      let msg = 'hello world!';

      writer.publish(topic, msg);
      writer.conn.produceMessages.calledOnce;
      return writer.conn.produceMessages.calledWith(topic, msg);
    });
      
    it('should publish a list of strings', function() {
      let topic = 'test_topic';
      let msgs = ['hello world!', 'another message'];

      writer.publish(topic, msgs);
      writer.conn.produceMessages.calledOnce;
      return writer.conn.produceMessages.calledWith(topic, msgs);
    });

    it('should publish a buffer', function() {
      let topic = 'test_topic';
      let msg = new Buffer('a buffer message');

      writer.publish(topic, msg);
      writer.conn.produceMessages.calledOnce;
      return writer.conn.produceMessages.calledWith(topic, [msg]);});

    it('should publish an object as JSON', function() {
      let topic = 'test_topic';
      let msg = {a: 1};

      writer.publish(topic, msg);
      writer.conn.produceMessages.calledOnce;
      return writer.conn.produceMessages.calledWith(topic, [JSON.stringify(msg)]);});

    it('should publish a list of buffers', function() {
      let topic = 'test_topic';
      let msgs = [new Buffer('a buffer message'), new Buffer('another msg')];

      writer.publish(topic, msgs);
      writer.conn.produceMessages.calledOnce;
      return writer.conn.produceMessages.calledWith(topic, msgs);
    });

    it('should publish a list of objects as JSON', function() {
      let topic = 'test_topic';
      let msgs = [{a: 1}, {b: 2}];
      let encodedMsgs = (Array.from(msgs).map((i) => JSON.stringify(i)));

      writer.publish(topic, msgs);
      writer.conn.produceMessages.calledOnce;
      return writer.conn.produceMessages.calledWith(topic, encodedMsgs);
    });

    it('should fail when publishing Null', function(done) {
      let topic = 'test_topic';
      let msg = null;

      return writer.publish(topic, msg, function(err) {
        err.should.exist;
        return done();
      });
    });

    it('should fail when publishing Undefined', function(done) {
      let topic = 'test_topic';
      let msg = undefined;

      return writer.publish(topic, msg, function(err) {
        err.should.exist;
        return done();
      });
    });

    it('should fail when publishing an empty string', function(done) {
      let topic = 'test_topic';
      let msg = '';

      return writer.publish(topic, msg, function(err) {
        err.should.exist;
        return done();
      });
    });

    it('should fail when publishing an empty list', function(done) {
      let topic = 'test_topic';
      let msg = [];

      return writer.publish(topic, msg, function(err) {
        err.should.exist;
        return done();
      });
    });

    return it('should fail when the Writer is not connected', function(done) {
      writer = new nsq.Writer('127.0.0.1', '4150');
      return writer.publish('test_topic', 'a briliant message', function(err) {
        err.should.exist;
        return done();
      });
    });
  });
});
