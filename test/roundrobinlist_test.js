const assert = require('assert')

const _ = require('lodash')
const should = require('should')

const RoundRobinList = require('../lib/roundrobinlist')

describe('roundrobinlist', () => {
  let list = null
  let rrl = null

  beforeEach(() => {
    list = [1, 2, 3]
    rrl = new RoundRobinList(list)
  })

  describe('constructor', () => {
    it('should have @list eq to passed in list', () =>
      assert(_.isEqual(rrl.list, list)))

    it('should have made a copy of the list argument', () =>
      assert(rrl.list !== list))

    it('should have @index eq to 0', () => rrl.index.should.eql(0))
  })

  describe('add', () =>
    it('@list should include the item', () => {
      rrl.add(10)
      should.ok(Array.from(rrl.list).includes(10))
    }))

  describe('next', () => {
    it('should return a list of 1 item by default', () => {
      assert(_.isEqual(rrl.next(), list.slice(0, 1)))
      rrl.index.should.eql(1)
    })

    it('should return a list as large as the count provided', () => {
      assert(_.isEqual(rrl.next(2), list.slice(0, 2)))
      rrl.index.should.eql(2)
    })

    it('should return all items and and then start over', () => {
      assert(_.isEqual(rrl.next(), [1]))
      assert(_.isEqual(rrl.next(), [2]))
      assert(_.isEqual(rrl.next(), [3]))
      assert(_.isEqual(rrl.next(), [1]))
    })
  })

  describe('remove', () => {
    it('should remove the item if it exists in the list', () => {
      rrl.remove(3)
      should.ok(!Array.from(rrl.list).includes(3))
    })

    it('should not affect the order of items returned', () => {
      rrl.remove(1)
      assert(_.isEqual(rrl.next(), [2]))
      assert(_.isEqual(rrl.next(), [3]))
      assert(_.isEqual(rrl.next(), [2]))
    })

    it('should not affect the order of items returned with items consumed', () => {
      assert(_.isEqual(rrl.next(), [1]))
      assert(_.isEqual(rrl.next(), [2]))
      rrl.remove(2)
      assert(_.isEqual(rrl.next(), [3]))
      assert(_.isEqual(rrl.next(), [1]))
    })

    it('should silently fail when it does not have the item', () => {
      rrl.remove(10)
      assert(_.isEqual(rrl.list, [1, 2, 3]))
      rrl.index.should.eql(0)
    })
  })
})
