import url from 'url';

import _ from 'underscore';
import async from 'async';
import request from 'request';

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
function lookupdRequest(url, callback) {
  // All responses are JSON
  const options = {
    url,
    method: 'GET',
    json: true,
    timeout: 2000,
  };

  request(options, (err, response, data = {}) => {
    if (err || data.status_code !== 200) {
      return callback(err, []);
    }

    callback(null, data.data.producers);
  });
}

/**
 * Takes a list of responses from lookupds and dedupes the nsqd
 * hosts based on host / port pair.
 *
 * @param {Array} results - list of lists of nsqd node objects.
 * @return {Array}
 */
function dedupeOnHostPort(results) {
  return (
    _.chain(results)
      // Flatten list of lists of objects
      .flatten()
      // De-dupe nodes by hostname / port
      .indexBy(item => `${item.hostname}:${item.tcp_port}`)
      .values()
      .value()
  );
}

const dedupedRequests = function(lookupdEndpoints, urlFn, callback) {
  // Ensure we have a list of endpoints for lookupds.
  if (_.isString(lookupdEndpoints)) {
    lookupdEndpoints = [lookupdEndpoints];
  }

  // URLs for querying `nodes` on each of the lookupds.
  const urls = Array.from(lookupdEndpoints).map(endpoint => urlFn(endpoint));

  return async.map(urls, lookupdRequest, (err, results) => {
    if (err) {
      return callback(err, null);
    }
    return callback(null, dedupeOnHostPort(results));
  });
};

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
function lookup(lookupdEndpoints, topic, callback) {
  const endpointURL = endpoint => {
    if (endpoint.indexOf('://') === -1) {
      endpoint = `http://${endpoint}`;
    }
    const parsedUrl = url.parse(endpoint, true);

    if (!parsedUrl.pathname || parsedUrl.pathname === '/') {
      parsedUrl.pathname = '/lookup';
    }
    parsedUrl.query.topic = topic;
    delete parsedUrl.search;
    return url.format(parsedUrl);
  };

  dedupedRequests(lookupdEndpoints, endpointURL, callback);
}

export default lookup;
