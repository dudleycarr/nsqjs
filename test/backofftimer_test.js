import should from 'should';
import decimal from 'bignumber.js';
import BackoffTimer from '../src/backofftimer';

describe('backofftimer', function() {
  let timer = null;
  beforeEach(() => timer = new BackoffTimer(0, 128));
  describe('constructor', function() {
    it('should have @maxShortTimer eq 1', () => timer.maxShortTimer.toString().should.eql('32'));
    it('should have a @maxLongTimer eq 3', () => timer.maxLongTimer.toString().should.eql('96'));
    it('should have a @shortUnit equal to 0.1', () => timer.shortUnit.toString().should.eql('3.2'));
    return it('should have a @longUnit equal to 0.3', () => timer.longUnit.toString().should.eql('0.384'));
  });

  describe('success', function() {
    it('should adjust @shortInterval to 0', function() {
      timer.success();
      return timer.shortInterval.toString().should.eql('0');
    });
    return it('should adjust @longInterval to 0', function() {
      timer.success();
      return timer.longInterval.toString().should.eql('0');
    });
  });

  describe('failure', function() {
    it('should adjust @shortInterval to 3.2 after 1 failure', function() {
      timer.failure();
      return timer.shortInterval.toString().should.eql('3.2');
    });
    return it('should adjust @longInterval to .384 after 1 failure', function() {
      timer.failure();
      return timer.longInterval.toString().should.eql('0.384');
    });
  });

  return describe('getInterval', function() {
    it('should initially be 0', () => timer.getInterval().toString().should.eql('0'));
    it('should be 0 after 1 success', function() {
      timer.success();
      return timer.getInterval().toString().should.eql('0');
    });
    it('should be 0 after 2 successes', function() {
      timer.success();
      timer.success();
      return timer.getInterval().toString().should.eql('0');
    });
    it('should be 3.584 after 1 failure', function() {
      timer.failure();
      return timer.getInterval().toString().should.eql('3.584');
    });
    return it('should be 7.168 after 2 failure', function() {
      timer.failure();
      timer.failure();
      return timer.getInterval().toString().should.eql('7.168');
    });
  });
});
