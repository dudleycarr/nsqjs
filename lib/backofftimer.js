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
  constructor(
    minInterval,
    maxInterval,
    ratio = 0.25,
    shortLength = 10,
    longLength = 250
  ) {
    this.minInterval = minInterval
    this.maxInterval = maxInterval

    this.maxShortTimer = (maxInterval - minInterval) * ratio
    this.maxLongTimer = (maxInterval - minInterval) * (1 - ratio)

    this.shortUnit = this.maxShortTimer / shortLength
    this.longUnit = this.maxLongTimer / longLength

    this.shortInterval = 0
    this.longInterval = 0

    this.interval = 0.0
  }

  /**
   * On success updates the backoff timers.
   */
  success() {
    if (this.interval === 0.0) return

    this.shortInterval = Math.max(this.shortInterval - this.shortUnit, 0)
    this.longInterval = Math.max(this.longInterval - this.longUnit, 0)

    this.updateInterval()
  }

  /**
   * On failure updates the backoff timers.
   */
  failure() {
    this.shortInterval = Math.min(
      this.shortInterval + this.shortUnit,
      this.maxShortTimer
    )
    this.longInterval = Math.min(
      this.longInterval + this.longUnit,
      this.maxLongTimer
    )

    this.updateInterval()
  }

  updateInterval() {
    this.interval = this.minInterval + this.shortInterval + this.longInterval
  }

  /**
   * Get the next backoff interval in seconds.
   *
   * @return {Number}
   */
  getInterval() {
    return this.interval
  }
}

module.exports = BackoffTimer
