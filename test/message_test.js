import should from 'should';
import sinon from 'sinon';
import Message from '../src/message';

let createMessage = (body, requeueDelay, timeout, maxTimeout) =>
  new Message('1', Date.now(), 0, new Buffer(body), requeueDelay, timeout,
    maxTimeout)
;

describe('Message', () =>
  describe('timeout', function() {
    it('should not allow finishing a message twice', function(done) {
      let msg = createMessage('body', 90, 50, 100);

      let firstFinish = () => msg.finish();

      let secondFinish = function() {
        msg.hasResponded.should.eql(true);
        return done();
      };

      setTimeout(firstFinish, 10);
      return setTimeout(secondFinish, 20);
    });

    it('should not allow requeue after finish', function(done) {
      let msg = createMessage('body', 90, 50, 100);

      let responseSpy = sinon.spy();
      msg.on(Message.RESPOND, responseSpy);

      let firstFinish = () => msg.finish();

      let secondRequeue = () => msg.requeue();

      let check = function() {
        responseSpy.calledOnce.should.be.true();
        return done();
      };

      setTimeout(firstFinish, 10);
      setTimeout(secondRequeue, 20);
      return setTimeout(check, 20);
    });

    it('should allow touch and then finish post first timeout', function(done) {
      let touchIn = 15;
      let timeoutIn = 20;
      let finishIn = 25;
      let checkIn = 30;

      let msg = createMessage('body', 90, timeoutIn, 100);
      let responseSpy = sinon.spy();
      msg.on(Message.RESPOND, responseSpy);

      let touch = () => msg.touch();

      let finish = function() {
        msg.timedOut.should.be.eql(false);
        return msg.finish();
      };

      let check = function() {
        responseSpy.calledTwice.should.be.true();
        return done();
      };

      setTimeout(touch, touchIn);
      setTimeout(finish, finishIn);
      return setTimeout(check, checkIn);
    });

    return it('should clear timeout on finish', function(done) {
      let msg = createMessage('body', 10, 60, 120);
      msg.finish();

      return process.nextTick(function() {
        should.not.exist(msg.trackTimeoutId);
        return done();
      });
    });
  })
);
