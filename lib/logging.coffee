_ = require 'underscore'
moment = require 'moment'

# Stores and/or prints state transitions for the various state machines. Useful
# for debugging and testing.
class StateChangeLogger
  constructor: (@storeLogs = false, @debug = false) ->
    @logs = []

  log: (component, id, description) ->
    timestamp = Date.now()
    args = [timestamp, component, id, description]

    if @storeLogs
      @logs.push args
    if @debug
      console.log @format args...

  format: (timestamp, component, id, description) ->
    ts = moment timestamp
    time = ts.format 'YYYY/MM/DD HH:mm:ss'

    if id?
      "#{time} #{component}(#{id}): #{description}"
    else
      "#{time} #{component}: #{description}"

# Expose singleton instance
module.exports = new StateChangeLogger()
