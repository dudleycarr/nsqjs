import _ from 'underscore';
import assert from 'assert';
import should from 'should';
import RoundRobinList from '../src/roundrobinlist';

describe('roundrobinlist', function() {
  let lst = null;
  let rrl = null;

  beforeEach(function() {
    lst = [1, 2, 3];
    return rrl = new RoundRobinList(lst);
  });

  describe('constructor', function() {
    it('should have @lst eq to passed in list', () => assert(_.isEqual(rrl.lst, lst)));

    it('should have made a copy of the list argument', () => assert(rrl.lst !== lst));

    return it('should have @index eq to 0', () => rrl.index.should.eql(0));
  });

  describe('add', () =>
    it('@lst should include the item', function() {
      rrl.add(10);
      return should.ok(Array.from(rrl.lst).includes(10));
    })
  );

  describe('next', function() {
    it('should return a list of 1 item by default', function() {
      assert(_.isEqual(rrl.next(), lst.slice(0, 1)));
      return rrl.index.should.eql(1);
    });
    it('should return a list as large as the count provided', function() {
      assert(_.isEqual(rrl.next(2), lst.slice(0, 2)));
      return rrl.index.should.eql(2);
    });
    return it('should return all items and and then start over', function() {
      assert(_.isEqual(rrl.next(), [1]));
      assert(_.isEqual(rrl.next(), [2]));
      assert(_.isEqual(rrl.next(), [3]));
      return assert(_.isEqual(rrl.next(), [1]));});});

  return describe('remove', function() {
    it('should remove the item if it exists in the list', function() {
      rrl.remove(3);
      return should.ok(!Array.from(rrl.lst).includes(3));
    });

    it('should not affect the order of items returned', function() {
      rrl.remove(1);
      assert(_.isEqual(rrl.next(), [2]));
      assert(_.isEqual(rrl.next(), [3]));
      return assert(_.isEqual(rrl.next(), [2]));});

    it('should not affect the order of items returned with items consumed', function() {
      assert(_.isEqual(rrl.next(), [1]));
      assert(_.isEqual(rrl.next(), [2]));
      rrl.remove(2);
      assert(_.isEqual(rrl.next(), [3]));
      return assert(_.isEqual(rrl.next(), [1]));});

    return it('should silently fail when it does not have the item', function() {
      rrl.remove(10);
      assert(_.isEqual(rrl.lst, [1, 2, 3]));
      return rrl.index.should.eql(0);
    });
  });
});
