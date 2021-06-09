const debug = require('./debug')
const fetch = require('node-fetch')
const url = require('url')
const {joinHostPort} = require('./config')

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
async function lookupdRequest(url) {
  try {
    log(`Query: ${url}`)
    const response = await fetch(url)
    if (!response.ok) {
      log(`Request to nsqlookupd failed. Response code = ${response.status}`)
      return []
    }

    // Pre nsq 1.x contained producers within data
    const {producers, data} = await response.json()
    return producers || data.producers
  } catch (e) {
    log(`Request to nslookupd failed without a response`)
    return []
  }
}

/**
 * Takes a list of responses from lookupds and dedupes the nsqd
 * hosts based on host / port pair.
 *
 * @param {Array} results - list of lists of nsqd node objects.
 * @return {Array}
 */
function dedupeOnHostPort(results) {
  const uniqueNodes = {}
  for (const lookupdResult of results) {
    for (const item of lookupdResult) {
      const key = item.broadcast_address
        ? joinHostPort(item.broadcast_address, item.tcp_port)
        : joinHostPort(item.hostname, item.tcp_port)
      uniqueNodes[key] = item
    }
  }

  return Object.values(uniqueNodes)
}

/**
 * Construct a lookupd URL to query for a particular topic.
 *
 * @param {String} endpoint - host/port pair or a URL
 * @param {String} topic - nsq topic
 * @returns {String} lookupd URL
 */
function lookupdURL(endpoint, topic) {
  endpoint = endpoint.indexOf('://') !== -1 ? endpoint : `http://${endpoint}`

  const parsedUrl = new url.URL(endpoint)
  const pathname = parsedUrl.pathname
  parsedUrl.pathname = pathname && pathname !== '/' ? pathname : '/lookup'
  parsedUrl.searchParams.set('topic', topic)
  return parsedUrl.toString()
}

/**
 * Queries lookupds for known nsqd nodes given a topic and returns
 * a deduped list.
 *
 * @param {String} lookupdEndpoints - a string or a list of strings of
 *   lookupd HTTP endpoints. eg. ['127.0.0.1:4161']
 * @param {String} topic - a string of the topic name
 */
async function lookup(lookupdEndpoints, topic) {
  // Ensure we have a list of endpoints for lookupds.
  if (!Array.isArray(lookupdEndpoints)) {
    lookupdEndpoints = [lookupdEndpoints]
  }

  // URLs for querying `nodes` on each of the lookupds.
  const urls = Array.from(lookupdEndpoints).map((e) => lookupdURL(e, topic))

  const results = await Promise.all(urls.map(lookupdRequest))
  return dedupeOnHostPort(results)
}

module.exports = lookup
