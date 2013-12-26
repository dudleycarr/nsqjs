_ = require 'underscore'
moment = require 'moment'

# Stores and/or prints state transitions for the various state machines. Useful
# for debugging and testing.
class StateChangeLogger
  constructor: (@storeLogs = false, @debug = false) ->
    @logs = []

  log: (component, state, id, message) ->
    args =
      timestamp: Date.now()
      component: component
      state: state
      id: id
      message: message

    if @storeLogs
      @logs.push args
    if @debug
      console.log @format args

  format: (logEntry) ->
    ts = moment logEntry.timestamp
    time = ts.format 'YYYY/MM/DD HH:mm:ss'

    if logEntry.id?
      prefix = "#{time} #{logEntry.component}(#{logEntry.id})"
    else
      prefix = "#{time} #{logEntry.component}"

    if logEntry.state?
      "#{prefix} #{logEntry.state}: #{logEntry.message}"
    else
      "#{prefix}: #{logEntry.message}"

  print: ->
    for entry in @logs
      console.log @format entry

# Expose singleton instance
module.exports = new StateChangeLogger()
