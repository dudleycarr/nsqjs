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

  constructor: (@id, @timestamp, @attempts, @body, @msgTimeout,
    @maxMsgTimeout) ->
    @hasResponded = false
    @receivedOn = Date.now()

    # The worker is not allowed to stall longer than this configured
    # timeout.
    noDelayRequeue = =>
      @requeue 0
    @maxMsgTimeoutId = setTimeout noDelayRequeue, @maxMsgTimeout

  # Returns in milliseconds the time until this message expires. Returns
  # null if that time has already ellapsed.
  timeUntilTimeout: ->
    return null if @hasResponded

    delta = @receivedOn + @msgTimeout - Date.now()
    if delta > 0 then delta else null

  finish: ->
    @respond Message.FINISH, wire.finish @id

  requeue: (delay, backoff=true) ->
    @respond Message.REQUEUE, wire.requeue @id, delay
    @emit Message.BACKOFF if backoff

  touch: ->
    @respond TOUCH, wire.requeue @id

  respond: (responseType, wireData) ->
    assert not @hasResponded
    @hasResponded = true
    clearTimeout @maxMsgTimeoutId
    @emit Message.RESPOND, responseType, wireData


module.exports = Message
