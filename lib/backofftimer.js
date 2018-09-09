const { BigNumber } = require('bignumber.js')

const min = (a, b) => (a.lte(b) ? a : b)
const max = (a, b) => (a.gte(b) ? a : b)

/**
 * This is a timer that is smart about backing off exponentially
 * when there are problems.
 *
 * Ported from pynsq:
 *   https://github.com/bitly/pynsq/blob/master/nsq/BackoffTimer.py
 */
class BackoffTimer {
  /**
   * Instantiates a new instance of BackoffTimer.
   *
   * @constructor
   * @param  {Number} minInterval
   * @param  {Number} maxInterval
   * @param  {Number} [ratio=0.25]
   * @param  {Number} [shortLength=10]
   * @param  {Number} [longLength=250]
   */
  constructor (
    minInterval,
    maxInterval,
    ratio = 0.25,
    shortLength = 10,
    longLength = 250
  ) {
    this.minInterval = new BigNumber(minInterval)
    this.maxInterval = new BigNumber(maxInterval)

    ratio = new BigNumber(ratio)
    const intervalDelta = new BigNumber(this.maxInterval - this.minInterval)

    // (maxInterval - minInterval) * ratio
    this.maxShortTimer = intervalDelta.times(ratio)

    // (maxInterval - minInterval) * (1 - ratio)
    this.maxLongTimer = intervalDelta.times(new BigNumber(1).minus(ratio))

    this.shortUnit = this.maxShortTimer.dividedBy(shortLength)
    this.longUnit = this.maxLongTimer.dividedBy(longLength)

    this.shortInterval = new BigNumber(0)
    this.longInterval = new BigNumber(0)

    this.interval = 0.0
  }

  /**
   * On success updates the backoff timers.
   */
  success () {
    if (this.interval === 0.0) return

    this.shortInterval = this.shortInterval.minus(this.shortUnit)
    this.longInterval = this.longInterval.minus(this.longUnit)
    this.shortInterval = max(this.shortInterval, new BigNumber(0))
    this.longInterval = max(this.longInterval, new BigNumber(0))

    this.updateInterval()
  }

  /**
   * On failure updates the backoff timers.
   */
  failure () {
    this.shortInterval = this.shortInterval.plus(this.shortUnit)
    this.longInterval = this.longInterval.plus(this.longUnit)
    this.shortInterval = min(this.shortInterval, this.maxShortTimer)
    this.longInterval = min(this.longInterval, this.maxLongTimer)

    this.updateInterval()
  }

  updateInterval () {
    const newInterval = this.minInterval
      .plus(this.shortInterval.plus(this.longInterval))
    this.interval = Number(newInterval.valueOf())
  }

  /**
   * Get the next backoff interval.
   *
   * @return {Number}
   */
  getInterval () {
    return this.interval
  }
}

module.exports = BackoffTimer
