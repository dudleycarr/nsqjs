/**
 * Takes a list and cycles through the elements in the list repeatedly and
 * in-order. Adding and removing to the list does not perturb the order.
 *
 * Usage:
 *   const list = RoundRobinList([1, 2, 3]);
 *   list.next() ==> [1]
 *   list.next(2) ==> [2, 3]
 *   list.next(2) ==> [1, 2]
 *   list.add(5) ==> 5
 *   list.next(2) ==> [3, 5]
 */
class RoundRobinList {
  /**
   * Instantiate a new RoundRobinList.
   *
   * @param  {Array} list
   */
  constructor(list) {
    this.list = list.slice()
    this.index = 0
  }

  /**
   * Returns the length of the list.
   *
   * @return {Number}
   */
  length() {
    return this.list.length
  }

  /**
   * Add an item to the list.
   *
   * @param {*} item
   * @return {*} The item added.
   */
  add(item) {
    return this.list.push(item)
  }

  /**
   * Remove an item from the list.
   *
   * @param  {*} item
   * @return {Array|undefined}
   */
  remove(item) {
    const itemIndex = this.list.indexOf(item)
    if (itemIndex === -1) return

    if (this.index > itemIndex) {
      this.index -= 1
    }

    return this.list.splice(itemIndex, 1)
  }

  /**
   * Get the next items in the list, round robin style.
   *
   * @param  {Number}   [count=1]
   * @return {Array}
   */
  next(count = 1) {
    const {index} = this
    this.index = (this.index + count) % this.list.length
    return this.list.slice(index, index + count)
  }
}

module.exports = RoundRobinList
