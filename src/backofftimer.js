import decimal from 'bignumber.js';

let min = function(a, b) {
  if (a.lte(b)) { return a; } else { return b; }
};

let max = function(a, b) {
  if (a.gte(b)) { return a; } else { return b; }
};

/*
This is a timer that is smart about backing off exponentially when there
are problems

Ported from pynsq:
  https://github.com/bitly/pynsq/blob/master/nsq/BackoffTimer.py
*/
class BackoffTimer {

  constructor(minInterval, maxInterval, ratio, shortLength,
    longLength) {

    if (ratio == null) { ratio = .25; }
    if (shortLength == null) { shortLength = 10; }
    if (longLength == null) { longLength = 250; }
    this.minInterval = decimal(minInterval);
    this.maxInterval = decimal(maxInterval);

    ratio = decimal(ratio);
    let intervalDelta = decimal(this.maxInterval - this.minInterval);
    // (maxInterval - minInterval) * ratio
    this.maxShortTimer = intervalDelta.times(ratio);
    // (maxInterval - minInterval) * (1 - ratio)
    this.maxLongTimer = intervalDelta.times(decimal(1).minus(ratio));

    this.shortUnit = this.maxShortTimer.dividedBy(shortLength);
    this.longUnit = this.maxLongTimer.dividedBy(longLength);

    this.shortInterval = decimal(0);
    this.longInterval = decimal(0);
  }

  success() {
    this.shortInterval = this.shortInterval.minus(this.shortUnit);
    this.longInterval = this.longInterval.minus(this.longUnit);
    this.shortInterval = max(this.shortInterval, decimal(0));
    return this.longInterval = max(this.longInterval, decimal(0));
  }

  failure() {
    this.shortInterval = this.shortInterval.plus(this.shortUnit);
    this.longInterval = this.longInterval.plus(this.longUnit);
    this.shortInterval = min(this.shortInterval, this.maxShortTimer);
    return this.longInterval = min(this.longInterval, this.maxLongTimer);
  }

  getInterval() {
    return this.minInterval.plus(this.shortInterval.plus(this.longInterval));
  }
}

export default BackoffTimer;
