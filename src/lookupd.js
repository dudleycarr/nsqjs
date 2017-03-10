import _ from 'underscore';
import async from 'async';
import request from 'request';
import url from 'url';

/*
lookupdRequest returns the list of producers from a lookupd given a URL to
query.

The callback will not return an error since it's assumed that there might
be transient issues with lookupds.
*/
let lookupdRequest = function(url, callback) {
  // All responses are JSON
  let options = {
    url,
    method: 'GET',
    json: true,
    timeout: 2000
  };

  return request(options, function(err, response, data) {
    let producers, status_code;
    if (err) {
      callback(null, []);
      return;
    }

    // Unpack JSON response
    try {
      ({status_code, data: {producers}} = data);
    } catch (error) {
      callback(null, []);
      return;
    }

    if (status_code !== 200) {
      callback(null, []);
      return;
    }

    return callback(null, producers);
  });
};

/*
Takes a list of responses from lookupds and dedupes the nsqd hosts based on
host / port pair.

Arguments:
  results: list of lists of nsqd node objects.
*/
let dedupeOnHostPort = results =>
  _.chain(results)
    // Flatten list of lists of objects
    .flatten()
    // De-dupe nodes by hostname / port
    .indexBy(item => `${item.hostname}:${item.tcp_port}`).values()
    .value()
;

let dedupedRequests = function(lookupdEndpoints, urlFn, callback) {
  // Ensure we have a list of endpoints for lookupds.
  if (_.isString(lookupdEndpoints)) { lookupdEndpoints = [lookupdEndpoints]; }

  // URLs for querying `nodes` on each of the lookupds.
  let urls = (Array.from(lookupdEndpoints).map((endpoint) => urlFn(endpoint)));

  return async.map(urls, lookupdRequest, function(err, results) {
    if (err) {
      return callback(err, null);
    } else {
      return callback(null, dedupeOnHostPort(results));
    }
  });
};

/*
Queries lookupds for known nsqd nodes given a topic and returns a deduped list.

Arguments:
  lookupdEndpoints: a string or a list of strings of lookupd HTTP endpoints. eg.
    ['127.0.0.1:4161']
  topic: a string of the topic name.
  callback: with signature `(err, nodes) ->`. `nodes` is a list of objects
    return by lookupds and deduped.
*/
let lookup = function(lookupdEndpoints, topic, callback) {
  let endpointURL = function(endpoint) {
    if (endpoint.indexOf('://') === -1) { endpoint = `http://${endpoint}`; }
    let parsedUrl = url.parse(endpoint, true);

    if ((!parsedUrl.pathname) || (parsedUrl.pathname === '/')) {
      parsedUrl.pathname = "/lookup";
    }
    parsedUrl.query.topic = topic;
    delete parsedUrl.search;
    return url.format(parsedUrl);
  };
  return dedupedRequests(lookupdEndpoints, endpointURL, callback);
};

export default lookup;
