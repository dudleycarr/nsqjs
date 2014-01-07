_ = require 'underscore'
async = require 'async'
request = require 'request'

lookupdRequest = (url, callback) ->
  # All responses are JSON
  options =
    url: url
    method: 'GET'
    json: true
    timeout: 2000

  request options, (err, response, data) ->
    if err
      callback null, []
      return

    # Unpack JSON response
    try
      {status_code: status_code, data: {producers: producers}} = data
    catch error
      callback null, []
      return

    if status_code isnt 200
      callback null, []
      return

    callback null, producers

###
Takes a list of responses from lookupds and dedupes the nsqd hosts based on
host / port pair.

Arguments:
  results: list of lists of nsqd node objects.
###
dedupeOnHostPort = (results) ->
  _.chain(results)
    # Flatten list of lists of objects
    .flatten()
    # De-dupe nodes by hostname / port
    .groupBy (item) ->
      "#{item.hostname}:#{item.tcp_port}"
    # Get a list of lists
    .values()
    # Take the first item from each of the grouped list of nodes
    .map(_.first)
    .value()

dedupedRequests = (lookupdEndpoints, urlFn, callback) ->
  # Ensure we have a list of endpoints for lookupds.
  lookupdEndpoints = [lookupdEndpoints] if _.isString lookupdEndpoints

  # URLs for querying `nodes` on each of the lookupds.
  urls = (urlFn endpoint for endpoint in lookupdEndpoints)

  # List of functions for querying lookupds for nodes.
  requestFns = for url in urls
    do (url) ->
      (cb) ->
        lookupdRequest url, cb

  async.parallel requestFns, (err, results) ->
    if err
      callback err, null
    else
      callback null, dedupeOnHostPort results

###
Queries lookupds for known nsqd nodes and returns a deduped list.

Arguments:
  lookupdEndpoints: a string or a list of strings of lookupd HTTP endpoints. eg.
    ['127.0.0.1:4161']
  callback: with signature `(err, nodes) ->`. `nodes` is a list of objects
    return by lookupds and deduped.
###
nodes = (lookupdEndpoints, callback) ->
  endpointURL = (endpoint) ->
    "http://#{endpoint}/nodes"
  dedupedRequests lookupdEndpoints, endpointURL, callback

###
Queries lookupds for known nsqd nodes given a topic and returns a deduped list.

Arguments:
  lookupdEndpoints: a string or a list of strings of lookupd HTTP endpoints. eg.
    ['127.0.0.1:4161']
  topic: a string of the topic name.
  callback: with signature `(err, nodes) ->`. `nodes` is a list of objects
    return by lookupds and deduped.
###
lookup = (lookupdEndpoints, topic, callback) ->
  endpointURL = (endpoint) ->
    "http://#{endpoint}/lookup?topic=#{topic}"
  dedupedRequests lookupdEndpoints, endpointURL, callback

module.exports =
  nodes: nodes
  lookup: lookup
