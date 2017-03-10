/*
Takes a list and cycles through the elements in the list repeatedly and
in-order. Adding and removing to the list does not perturb the order.

Usage:
  lst = RoundRobinList [1, 2, 3]
  lst.next()                      # Returns [1]
  lst.next 2                      # Returns [2, 3]
  lst.next 2                      # Returns [1, 2]
  lst.add 5
  lst.next 2                      # Retunrs [3, 5]
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
    let itemIndex = this.lst.indexOf(item);
    if (itemIndex === -1) { return; }
    if (this.index > itemIndex) { this.index -= 1; }
    return this.lst.splice(itemIndex, 1);
  }

  next(count) {
    if (count == null) { count = 1; }
    let { index } = this;
    this.index = (this.index + count) % this.lst.length;
    return this.lst.slice(index, index + count);
  }
}

export default RoundRobinList;
