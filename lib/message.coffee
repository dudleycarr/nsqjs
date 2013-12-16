_ = require 'underscore'
assert = require 'assert'
wire = require './wire'
{EventEmitter} = require 'events'

class Message extends EventEmitter
  # Event types
  @BACKOFF: 'backoff'
  @RESPOND: 'respond'

  # Response types
  @FINISH = 0
  @REQUEUE = 1
  @TOUCH = 2

  constructor: (@id, @timestamp, @attempts, @body, @requeueDelay, @msgTimeout,
    @maxMsgTimeout) ->
    @hasResponded = false
    @receivedOn = Date.now()
    @lastTouched = @receivedOn

    # The worker is not allowed to stall longer than this configured
    # timeout.
    noDelayRequeue = =>
      @requeue 0, false
    @maxMsgTimeoutId = setTimeout noDelayRequeue, @maxMsgTimeout

  json: ->
    unless @parsed?
      try
        @parsed = JSON.parse @body
      catch err
        throw new Error "Invalid JSON in Message"
    @parsed


  # Returns in milliseconds the time until this message expires. Returns
  # null if that time has already ellapsed. There are two different timeouts
  # for a message. There are the soft timeouts that can be extended by touching
  # the message. There is the hard timeout that cannot be exceeded without
  # reconfiguring the nsqd.
  timeUntilTimeout: (hard = false) ->
    return null if @hasResponded

    delta = if hard
      @receivedOn + @maxMsgTimeout - Date.now()
    else
      @lastTouched + @msgTimeout - Date.now()

    if delta > 0 then delta else null

  finish: ->
    @respond Message.FINISH, wire.finish @id

  requeue: (delay = @requeueDelay, backoff = true) ->
    @respond Message.REQUEUE, wire.requeue @id, delay
    @emit Message.BACKOFF if backoff

  touch: ->
    @lastTouched = Date.now()
    @respond Message.TOUCH, wire.touch @id

  respond: (responseType, wireData) ->
    process.nextTick =>
      assert not @hasResponded

      if responseType isnt Message.TOUCH
        @hasResponded = true
        clearTimeout @maxMsgTimeoutId
      else
        @lastTouched = Date.now()

      @emit Message.RESPOND, responseType, wireData


module.exports = Message
