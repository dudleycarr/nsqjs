import _ from 'underscore'
import assert from 'assert'
import should from 'should'
import RoundRobinList from '../src/roundrobinlist'

describe('roundrobinlist', () => {
  let lst = null
  let rrl = null

  beforeEach(() => {
    lst = [1, 2, 3]
    return rrl = new RoundRobinList(lst)
  })

  describe('constructor', () => {
    it('should have @lst eq to passed in list', () => assert(_.isEqual(rrl.lst, lst)))

    it('should have made a copy of the list argument', () => assert(rrl.lst !== lst))

    return it('should have @index eq to 0', () => rrl.index.should.eql(0))
  })

  describe('add', () =>
    it('@lst should include the item', () => {
      rrl.add(10)
      return should.ok(Array.from(rrl.lst).includes(10))
    }),
  )

  describe('next', () => {
    it('should return a list of 1 item by default', () => {
      assert(_.isEqual(rrl.next(), lst.slice(0, 1)))
      return rrl.index.should.eql(1)
    })
    it('should return a list as large as the count provided', () => {
      assert(_.isEqual(rrl.next(2), lst.slice(0, 2)))
      return rrl.index.should.eql(2)
    })
    return it('should return all items and and then start over', () => {
      assert(_.isEqual(rrl.next(), [1]))
      assert(_.isEqual(rrl.next(), [2]))
      assert(_.isEqual(rrl.next(), [3]))
      return assert(_.isEqual(rrl.next(), [1]))
    })
  })

  return describe('remove', () => {
    it('should remove the item if it exists in the list', () => {
      rrl.remove(3)
      return should.ok(!Array.from(rrl.lst).includes(3))
    })

    it('should not affect the order of items returned', () => {
      rrl.remove(1)
      assert(_.isEqual(rrl.next(), [2]))
      assert(_.isEqual(rrl.next(), [3]))
      return assert(_.isEqual(rrl.next(), [2]))
    })

    it('should not affect the order of items returned with items consumed', () => {
      assert(_.isEqual(rrl.next(), [1]))
      assert(_.isEqual(rrl.next(), [2]))
      rrl.remove(2)
      assert(_.isEqual(rrl.next(), [3]))
      return assert(_.isEqual(rrl.next(), [1]))
    })

    return it('should silently fail when it does not have the item', () => {
      rrl.remove(10)
      assert(_.isEqual(rrl.lst, [1, 2, 3]))
      return rrl.index.should.eql(0)
    })
  })
})
