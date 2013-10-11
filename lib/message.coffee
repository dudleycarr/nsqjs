_ = require 'underscore'
assert = require 'assert'
wire = require './wire'
{EventEmitter} = require 'events'

class Message extends EventEmitter
  constructor: (@id, @timestamp, @attempts, @body, @msgTimeout,
    @maxMsgTimeout) ->
    @hasResponded = false
    @receivedOn = Date.now()

    # The worker is not allowed to stall longer than this configured
    # timeout.
    @maxMsgTimeoutId = setTimeout (=> @requeue 0), @maxMsgTimeout

  # Returns in milliseconds the time until this message expires. Returns
  # null if that time has already ellapsed.
  timeUntilTimeout: ->
    return true if @hasResponded

    delta = (@receivedOn + @msgTimeout) - Date.now()
    if delta > 0 then delta else null

  finish: ->
    @respond wire.finish @id
    @emit 'finish'

  requeue: (delay, backoff=true) ->
    @respond wire.requeue @id, delay
    if backoff
      @emit 'backoff'

  touch: ->
    @respond wire.requeue @id

  respond: (wireData) ->
    assert not @hasResponded
    @hasResponded = true
    clearTimeout @maxMsgTimeoutId
    @emit 'respond', wireData


module.exports = Message
