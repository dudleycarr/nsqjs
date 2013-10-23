decimal = require 'bignumber.js'

min = (a, b) ->
  if a.lte b then a else b

max = (a, b) ->
  if a.gte b then a else b

class BackoffTimer

  constructor: (minInterval, maxInterval, ratio=.25, shortLength=10,
    longLength=250) ->

      @minInterval = decimal minInterval
      @maxInterval = decimal maxInterval

      ratio = decimal ratio
      intervalDelta = decimal(@maxInterval - @minInterval)
      # (maxInterval - minInterval) * ratio
      @maxShortTimer = intervalDelta.times ratio
      # (maxInterval - minInterval) * (1 - ratio)
      @maxLongTimer = intervalDelta.times  decimal(1).minus(ratio)

      @shortUnit = @maxShortTimer.dividedBy shortLength
      @longUnit = @maxLongTimer.dividedBy longLength

      @shortInterval = decimal 0
      @longInterval = decimal 0

  success: ->
    @shortInterval = @shortInterval.minus @shortUnit
    @longInterval = @longInterval.minus @longUnit
    @shortInterval = max(@shortInterval, decimal(0))
    @longInterval = max @longInterval, decimal(0)

  failure: ->
    @shortInterval = @shortInterval.plus @shortUnit
    @longInterval = @longInterval.plus @longUnit
    @shortInterval = min @shortInterval, @maxShortTimer
    @longInterval = min @longInterval, @maxLongTimer

  getInterval: ->
    @minInterval.plus @shortInterval.plus @longInterval

module.exports = BackoffTimer
