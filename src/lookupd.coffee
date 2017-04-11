_ = require 'underscore'
async = require 'async'
request = require 'request'
url = require 'url'

###
lookupdRequest returns the list of producers from a lookupd given a URL to
query.

The callback will not return an error since it's assumed that there might
be transient issues with lookupds.
###
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
      {statusCode: status_code} = response
      {producers:producers} = data
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
    .indexBy (item) ->
      "#{item.broadcast_address}:#{item.tcp_port}"
    .values()
    .value()

dedupedRequests = (lookupdEndpoints, urlFn, callback) ->
  # Ensure we have a list of endpoints for lookupds.
  lookupdEndpoints = [lookupdEndpoints] if _.isString lookupdEndpoints

  # URLs for querying `nodes` on each of the lookupds.
  urls = (urlFn endpoint for endpoint in lookupdEndpoints)

  async.map urls, lookupdRequest, (err, results) ->
    if err
      callback err, null
    else
      callback null, dedupeOnHostPort results

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
    endpoint = "http://#{endpoint}" if endpoint.indexOf('://') is -1
    parsedUrl = url.parse endpoint, true

    if (not parsedUrl.pathname) or (parsedUrl.pathname is '/')
      parsedUrl.pathname = "/lookup"
    parsedUrl.query.topic = topic
    delete parsedUrl.search
    url.format(parsedUrl)
  dedupedRequests lookupdEndpoints, endpointURL, callback

module.exports = lookup
