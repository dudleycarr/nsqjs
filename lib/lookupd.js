const _ = require('lodash')
const async = require('async')
const debug = require('debug')
const request = require('request')
const url = require('url')

const log = debug('nsqjs:lookup')
/**
 * lookupdRequest returns the list of producers from a lookupd given a
 * URL to query.
 *
 * The callback will not return an error since it's assumed that there might
 * be transient issues with lookupds.
 *
 * @param {String} url
 * @param {Function} callback
 */
function lookupdRequest (url, callback) {
  // All responses are JSON
  const options = {
    url,
    method: 'GET',
    json: true,
    timeout: 2000
  }

  const requestWithRetry = cb => request(options, cb)
  const retryOptions = { times: 3, interval: 500 }

  async.retry(retryOptions, requestWithRetry, (err, response, data = {}) => {
    if (err) {
      log(`lookup failed for ${url}`)
      return callback(null, [])
    }

    const statusCode = (data ? data.status_code : null) || response.statusCode
    if (statusCode !== 200) {
      log(`lookup failed for ${url}. Returned status code: ${statusCode}.`)
      return callback(null, [])
    }

    try {
      let { producers } = data

      // Support pre version 1.x lookupd response.
      if (!_.isEmpty(data.data)) {
        producers = data.data.producers
      }

      callback(null, producers)
    } catch (err) {
      log(`lookup failed. Getting unsupported JSON back!`)
      callback(null, [])
    }
  })
}

/**
 * Takes a list of responses from lookupds and dedupes the nsqd
 * hosts based on host / port pair.
 *
 * @param {Array} results - list of lists of nsqd node objects.
 * @return {Array}
 */
function dedupeOnHostPort (results) {
  return (
    _.chain(results)
      // Flatten list of lists of objects
      .flatten()
      // De-dupe nodes by broadcast address / port
      .keyBy(item => {
        if (item.broadcast_address) {
          return `${item.broadcast_address}:${item.tcp_port}`
        } else {
          return `${item.hostname}:${item.tcp_port}`
        }
      })
      .values()
      .value()
  )
}

const dedupedRequests = function (lookupdEndpoints, urlFn, callback) {
  // Ensure we have a list of endpoints for lookupds.
  if (_.isString(lookupdEndpoints)) {
    lookupdEndpoints = [lookupdEndpoints]
  }

  // URLs for querying `nodes` on each of the lookupds.
  const urls = Array.from(lookupdEndpoints).map(endpoint => urlFn(endpoint))

  return async.map(urls, lookupdRequest, (err, results) => {
    if (err) {
      // This should be very unlikely since lookupdRequest *shouldn't* be
      // returning errors.
      return callback(err, [])
    }
    return callback(null, dedupeOnHostPort(results))
  })
}

/**
 * Queries lookupds for known nsqd nodes given a topic and returns
 * a deduped list.
 *
 * @param {String} lookupdEndpoints - a string or a list of strings of
 *   lookupd HTTP endpoints. eg. ['127.0.0.1:4161']
 * @param {String} topic - a string of the topic name
 * @param {Function} callback - with signature `(err, nodes) ->`. `nodes`
 *   is a list of objects return by lookupds and deduped.
 */
function lookup (lookupdEndpoints, topic, callback) {
  const endpointURL = endpoint => {
    if (endpoint.indexOf('://') === -1) {
      endpoint = `http://${endpoint}`
    }
    const parsedUrl = url.parse(endpoint, true)

    if (!parsedUrl.pathname || parsedUrl.pathname === '/') {
      parsedUrl.pathname = '/lookup'
    }
    parsedUrl.query.topic = topic
    delete parsedUrl.search
    return url.format(parsedUrl)
  }

  dedupedRequests(lookupdEndpoints, endpointURL, callback)
}

module.exports = lookup
