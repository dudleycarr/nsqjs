_ = require 'underscore'
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

    # Keep track of when this message actually times out.
    @timedOut = false
    do trackTimeout = =>
      return if @hasResponded

      soft = @timeUntilTimeout()
      hard = @timeUntilTimeout true

      # Both values have to be not null otherwise we've timedout.
      @timedOut = not soft or not hard
      setTimeout trackTimeout, Math.min soft, hard unless @timedOut

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
    throw new Error 'Message timed out. Cannot finish message' if @timedOut
    @respond Message.FINISH, wire.finish @id

  requeue: (delay = @requeueDelay, backoff = true) ->
    throw new Error 'Message timed out. Cannot requeue message.' if @timedOut
    @respond Message.REQUEUE, wire.requeue @id, delay
    @emit Message.BACKOFF if backoff

  touch: ->
    throw new Error 'Message timed out. Cannot touch message.' if @timedOut
    @lastTouched = Date.now()
    @respond Message.TOUCH, wire.touch @id

  respond: (responseType, wireData) ->
    throw new Error "Already responded to message (#{@id})" if @hasResponded

    process.nextTick =>
      if responseType isnt Message.TOUCH
        @hasResponded = true
      else
        @lastTouched = Date.now()

      @emit Message.RESPOND, responseType, wireData


module.exports = Message
