###
Takes a list and cycles through the elements in the list repeatedly and
in-order. Adding and removing to the list does not perturb the order.

Usage:
  lst = RoundRobinList [1, 2, 3]
  lst.next()                      # Returns [1]
  lst.next 2                      # Returns [2, 3]
  lst.next 2                      # Returns [1, 2]
  lst.add 5
  lst.next 2                      # Retunrs [3, 5]
###
class RoundRobinList

  constructor: (lst) ->
    @lst = lst[..]
    @index = 0

  length: ->
    @lst.length

  add: (item) ->
    @lst.push item

  remove: (item) ->
    itemIndex = @lst.indexOf item
    return if itemIndex is -1
    @index -= 1 if @index > itemIndex
    @lst.splice itemIndex, 1

  next: (count = 1) ->
    index = @index
    @index = (@index + count) % @lst.length
    @lst[index...index + count]

module.exports = RoundRobinList
