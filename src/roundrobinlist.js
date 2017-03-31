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
  constructor(lst) {
    this.lst = lst.slice();
    this.index = 0;
  }

  length() {
    return this.lst.length;
  }

  add(item) {
    return this.lst.push(item);
  }

  remove(item) {
    const itemIndex = this.lst.indexOf(item);
    if (itemIndex === -1) {
      return;
    }
    if (this.index > itemIndex) {
      this.index -= 1;
    }
    return this.lst.splice(itemIndex, 1);
  }

  next(count = 1) {
    const { index } = this;
    this.index = (this.index + count) % this.lst.length;
    return this.lst.slice(index, index + count);
  }
}

export default RoundRobinList;
