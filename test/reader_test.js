import should from 'should';
import sinon from 'sinon';

import nsq from '../src/nsq';

describe('reader', function() {

  let readerWithAttempts = attempts =>
    new nsq.Reader('topic', 'default', {
        nsqdTCPAddresses: ['127.0.0.1:4150'],
        maxAttempts: attempts
      }
    )
  ;

  describe('max attempts', () =>
    describe('exceeded', function() {
      it('should finish after exceeding specified max attempts', function(done) {
        let maxAttempts = 2;
        let reader = readerWithAttempts(maxAttempts);

        // Message that has exceed the maximum number of attempts
        let message = {
          attempts: maxAttempts,
          finish: sinon.spy()
        };

        reader.handleMessage(message);

        return process.nextTick(function() {
          message.finish.called.should.be.true();
          return done();
        });
      });

      it('should call the DISCARD message hanlder if registered', function(done) {
        let maxAttempts = 2;
        let reader = readerWithAttempts(maxAttempts);

        let message = {
          attempts: maxAttempts,
          finish() {}
        };

        reader.on(nsq.Reader.DISCARD, msg => done());

        return reader.handleMessage(message);
      });

      return it('should call the MESSAGE handler by default', function(done) {
        let maxAttempts = 2;
        let reader = readerWithAttempts(maxAttempts);

        let message = {
          attempts: maxAttempts,
          finish() {}
        };

        reader.on(nsq.Reader.MESSAGE, msg => done());

        return reader.handleMessage(message);
      });
    })
  );

  return describe('off by default', () =>
    it('should not finish the message', function(done) {
      let reader = readerWithAttempts(0);

      let message = {
        attempts: 100,
        finish: sinon.spy()
      };

      // Registering this to make sure that even if the listener is available,
      // it should not be getting called.
      reader.on(nsq.Reader.DISCARD, msg => done(new Error('Unexpected discard message')));

      let messageHandlerSpy = sinon.spy();
      reader.on(nsq.Reader.MESSAGE, messageHandlerSpy);

      reader.handleMessage(message);

      return process.nextTick(function() {
        messageHandlerSpy.called.should.be.true();
        message.finish.called.should.be.false();
        return done();
      });
    })
  );
});
