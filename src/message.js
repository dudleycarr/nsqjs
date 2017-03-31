import * as wire from './wire';
import { EventEmitter } from 'events';

class Message extends EventEmitter {
  static initClass() {
    // Event types
    this.BACKOFF = 'backoff';
    this.RESPOND = 'respond';

    // Response types
    this.FINISH = 0;
    this.REQUEUE = 1;
    this.TOUCH = 2;
  }

  constructor(
    id,
    timestamp,
    attempts,
    body,
    requeueDelay,
    msgTimeout,
    maxMsgTimeout
  ) {
    let trackTimeout;
    super(...arguments);
    this.id = id;
    this.timestamp = timestamp;
    this.attempts = attempts;
    this.body = body;
    this.requeueDelay = requeueDelay;
    this.msgTimeout = msgTimeout;
    this.maxMsgTimeout = maxMsgTimeout;
    this.hasResponded = false;
    this.receivedOn = Date.now();
    this.lastTouched = this.receivedOn;
    this.touchCount = 0;
    this.trackTimeoutId = null;

    // Keep track of when this message actually times out.
    this.timedOut = false;
    (trackTimeout = () => {
      if (this.hasResponded) {
        return;
      }

      const soft = this.timeUntilTimeout();
      const hard = this.timeUntilTimeout(true);

      // Both values have to be not null otherwise we've timedout.
      this.timedOut = !soft || !hard;
      if (!this.timedOut) {
        clearTimeout(this.trackTimeoutId);
        this.trackTimeoutId = setTimeout(trackTimeout, Math.min(soft, hard));
      }
    })();
  }

  json() {
    if (this.parsed == null) {
      try {
        this.parsed = JSON.parse(this.body);
      } catch (err) {
        throw new Error('Invalid JSON in Message');
      }
    }
    return this.parsed;
  }

  // Returns in milliseconds the time until this message expires. Returns
  // null if that time has already ellapsed. There are two different timeouts
  // for a message. There are the soft timeouts that can be extended by touching
  // the message. There is the hard timeout that cannot be exceeded without
  // reconfiguring the nsqd.
  timeUntilTimeout(hard) {
    if (hard == null) {
      hard = false;
    }
    if (this.hasResponded) {
      return null;
    }

    const delta = hard
      ? this.receivedOn + this.maxMsgTimeout - Date.now()
      : this.lastTouched + this.msgTimeout - Date.now();

    if (delta > 0) {
      return delta;
    }
    return null;
  }

  finish() {
    return this.respond(Message.FINISH, wire.finish(this.id));
  }

  requeue(delay, backoff) {
    if (delay == null) {
      delay = this.requeueDelay;
    }
    if (backoff == null) {
      backoff = true;
    }
    this.respond(Message.REQUEUE, wire.requeue(this.id, delay));
    if (backoff) {
      return this.emit(Message.BACKOFF);
    }
  }

  touch() {
    this.touchCount += 1;
    this.lastTouched = Date.now();
    return this.respond(Message.TOUCH, wire.touch(this.id));
  }

  respond(responseType, wireData) {
    // TODO: Add a debug/warn when we moved to debug.js
    if (this.hasResponded) {
      return;
    }

    return process.nextTick(() => {
      if (responseType !== Message.TOUCH) {
        this.hasResponded = true;
        clearTimeout(this.trackTimeoutId);
        this.trackTimeoutId = null;
      } else {
        this.lastTouched = Date.now();
      }

      return this.emit(Message.RESPOND, responseType, wireData);
    });
  }
}
Message.initClass();

export default Message;
