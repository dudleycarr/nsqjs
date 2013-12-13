_ = require 'underscore'

# Stores and/or prints state transitions for the various state machines. Useful
# for debugging and testing.
class StateChangeLogger
  constructor: () ->
    @storeLogs = false
    @debug = false
    @logs = []

  log: (component, id, description) ->
    timestamp = Date.now()
    args = [timestamp, component, id, description]

    if @storeLogs
      @logs.push args
    if @debug
      console.log @format args...

  format: (timestamp, component, id, description) ->
    ts = new Date(timestamp)
    time = "#{ts.getFullYear()}/#{ts.getMonth()+1}/#{ts.getDate()}"
    time += " #{ts.getHours()}:#{ts.getMinutes()}:#{ts.getSeconds()}"

    if id?
      "#{time} #{component}(#{id}): #{description}"
    else
      "#{time} #{component}: #{description}"

# Expose singleton instance
module.exports = new StateChangeLogger()
