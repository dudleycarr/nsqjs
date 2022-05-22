const {EventEmitter} = require('events')
const wire = require('./wire')

/**
 * Message - a high-level message object, which exposes stateful methods
 * for responding to nsqd (FIN, REQ, TOUCH, etc.) as well as metadata
 * such as attempts and timestamp.
 * @type {Message}
 */
class Message extends EventEmitter {
  // Event types
  static get BACKOFF() {
    return 'backoff'
  }
  static get RESPOND() {
    return 'respond'
  }

  // Response types
  static get FINISH() {
    return 0
  }
  static get REQUEUE() {
    return 1
  }
  static get TOUCH() {
    return 2
  }

  /**
   * Instantiates a new instance of a Message.
   * @constructor
   * @param  {String} id
   * @param  {String|Number} timestamp
   * @param  {Number} attempts
   * @param  {String} body
   * @param  {Number} requeueDelay
   * @param  {Number} msgTimeout
   * @param  {Number} maxMsgTimeout
   */
  constructor(rawMessage, requeueDelay, msgTimeout, maxMsgTimeout) {
    super(...arguments) // eslint-disable-line prefer-rest-params
    this.rawMessage = rawMessage
    this.requeueDelay = requeueDelay
    this.msgTimeout = msgTimeout
    this.maxMsgTimeout = maxMsgTimeout
    this.hasResponded = false
    this.receivedOn = Date.now()
    this.lastTouched = this.receivedOn
    this.touchCount = 0
    this.trackTimeoutId = null

    // Keep track of when this message actually times out.
    this.timedOut = false
    this.trackTimeout()
  }

  get id() {
    return wire.unpackMessageId(this.rawMessage)
  }

  get timestamp() {
    return wire.unpackMessageTimestamp(this.rawMessage)
  }

  get attempts() {
    return wire.unpackMessageAttempts(this.rawMessage)
  }

  get body() {
    return wire.unpackMessageBody(this.rawMessage)
  }

  /**
   * track whether or not a message has timed out.
   */
  trackTimeout() {
    if (this.hasResponded) return

    const soft = this.timeUntilTimeout()
    const hard = this.timeUntilTimeout(true)

    // Both values have to be not null otherwise we've timedout.
    this.timedOut = !soft || !hard
    if (!this.timedOut) {
      clearTimeout(this.trackTimeoutId)
      this.trackTimeoutId = setTimeout(
        () => this.trackTimeout(),
        Math.min(soft, hard)
      ).unref()
    }
  }

  /**
   * Safely parse the body into JSON.
   *
   * @return {Object}
   */
  json() {
    if (this.parsed == null) {
      try {
        this.parsed = JSON.parse(this.body)
      } catch (err) {
        throw new Error('Invalid JSON in Message')
      }
    }

    return this.parsed
  }

  /**
   * Returns in milliseconds the time until this message expires. Returns
   * null if that time has already ellapsed. There are two different timeouts
   * for a message. There are the soft timeouts that can be extended by touching
   * the message. There is the hard timeout that cannot be exceeded without
   * reconfiguring the nsqd.
   *
   * @param  {Boolean} [hard=false]
   * @return {Number|null}
   */
  timeUntilTimeout(hard = false) {
    if (this.hasResponded) return null

    let delta
    if (hard) {
      delta = this.receivedOn + this.maxMsgTimeout - Date.now()
    } else {
      delta = this.lastTouched + this.msgTimeout - Date.now()
    }

    if (delta > 0) {
      return delta
    }

    return null
  }

  /**
   * Respond with a `FINISH` event.
   */
  finish() {
    this.respond(Message.FINISH, wire.finish(this.id))
  }

  /**
   * Requeue the message with the specified amount of delay. If backoff is
   * specifed, then the subscribed Readers will backoff.
   *
   * @param  {Number}  [delay=this.requeueDelay]
   * @param  {Boolean} [backoff=true]            [description]
   */
  requeue(delay = this.requeueDelay, backoff = true) {
    this.respond(Message.REQUEUE, wire.requeue(this.id, delay))
    if (backoff) {
      this.emit(Message.BACKOFF)
    }
  }

  /**
   * Emit a `TOUCH` command. `TOUCH` command can be used to reset the timer
   * on the nsqd side. This can be done repeatedly until the message
   * is either FIN or REQ, up to the sending nsqd’s configured max_msg_timeout.
   */
  touch() {
    this.touchCount += 1
    this.lastTouched = Date.now()
    this.respond(Message.TOUCH, wire.touch(this.id))
  }

  /**
   * Emit a `RESPOND` event.
   *
   * @param  {Number} responseType
   * @param  {Buffer} wireData
   * @return {undefined}
   */
  respond(responseType, wireData) {
    // TODO: Add a debug/warn when we moved to debug.js
    if (this.hasResponded) return

    process.nextTick(() => {
      if (responseType !== Message.TOUCH) {
        this.hasResponded = true
        clearTimeout(this.trackTimeoutId)
        this.trackTimeoutId = null
      } else {
        this.lastTouched = Date.now()
      }

      this.emit(Message.RESPOND, responseType, wireData)
    })
  }
}

module.exports = Message
