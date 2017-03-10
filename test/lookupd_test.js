import _ from 'underscore';

import nock from 'nock';
import should from 'should';
import url from 'url';

import lookup from '../src/lookupd';

let NSQD_1 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 4151,
  remote_address: 'localhost:12345',
  tcp_port: 4150,
  topics: ['sample_topic'],
  version: '0.2.23'
};
let NSQD_2 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 5151,
  remote_address: 'localhost:56789',
  tcp_port: 5150,
  topics: ['sample_topic'],
  version: '0.2.23'
};
let NSQD_3 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 6151,
  remote_address: 'localhost:23456',
  tcp_port: 6150,
  topics: ['sample_topic'],
  version: '0.2.23'
};
let NSQD_4 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 7151,
  remote_address: 'localhost:34567',
  tcp_port: 7150,
  topics: ['sample_topic'],
  version: '0.2.23'
};

let LOOKUPD_1 = '127.0.0.1:4161';
let LOOKUPD_2 = '127.0.0.1:5161';
let LOOKUPD_3 = 'http://127.0.0.1:6161/';
let LOOKUPD_4 = 'http://127.0.0.1:7161/path/lookup';

let nockUrlSplit = function(url) {
  let match = url.match(/^(https?:\/\/[^\/]+)(\/.*$)/i);
  if (match) {
    return {
      baseUrl: match[1],
      path: match[2]
    };
  }
};

let registerWithLookupd = function(lookupdAddress, nsqd) {
  let producers = (nsqd != null) ? [nsqd] : [];

  if (nsqd != null) {
    return (() => {
      let result = [];
      for (let topic of Array.from(nsqd.topics)) {
        if (lookupdAddress.indexOf('://') === -1) {
          result.push(nock(`http://${lookupdAddress}`)
            .get(`/lookup?topic=${topic}`)
            .reply(200, {
              status_code: 200,
              status_txt: 'OK',
              data: {
                producers
              }
            }
          ));
        } else {
          let {baseUrl, path} = nockUrlSplit(lookupdAddress);
          if (!path || (path === '/')) {
            path = '/lookup';
          }
          result.push(nock(baseUrl)
            .get(`${path}?topic=${topic}`)
            .reply(200, {
              status_code: 200,
              status_txt: 'OK',
              data: {
                producers
              }
            }
          ));
        }
      }
      return result;
    })();
  }
};

let setFailedTopicReply = (lookupdAddress, topic) =>
  nock(`http://${lookupdAddress}`)
    .get(`/lookup?topic=${topic}`)
    .reply(200, {
      status_code: 500,
      status_txt: 'INVALID_ARG_TOPIC',
      data: null
    }
  )
;


describe('lookupd.lookup', function() {
  afterEach(() => nock.cleanAll());

  describe('querying a single lookupd for a topic', function() {
    it('should return an empty list if no nsqd nodes', function(done) {
      setFailedTopicReply(LOOKUPD_1, 'sample_topic');

      return lookup(LOOKUPD_1, 'sample_topic', function(err, nodes) {
        nodes.should.be.empty;
        return done();
      });
    });

    return it('should return a list of nsqd nodes for a success reply', function(done) {
      registerWithLookupd(LOOKUPD_1, NSQD_1);

      return lookup(LOOKUPD_1, 'sample_topic', function(err, nodes) {
        nodes.should.have.length(1);
        for (let key of ['address', 'broadcast_address', 'tcp_port', 'http_port']) {
          should.ok(Array.from(_.keys(nodes[0])).includes(key));
        }
        return done();
      });
    });
  });

  return describe('querying a multiple lookupd', function() {
    it('should combine results from multiple lookupds', function(done) {
      registerWithLookupd(LOOKUPD_1, NSQD_1);
      registerWithLookupd(LOOKUPD_2, NSQD_2);
      registerWithLookupd(LOOKUPD_3, NSQD_3);
      registerWithLookupd(LOOKUPD_4, NSQD_4);

      let lookupdAddresses = [LOOKUPD_1, LOOKUPD_2, LOOKUPD_3, LOOKUPD_4];
      return lookup(lookupdAddresses, 'sample_topic', function(err, nodes) {
        nodes.should.have.length(4);
        _.chain(nodes)
          .pluck('tcp_port')
          .sort()
          .value().should.be.eql([4150, 5150, 6150, 7150]);
        return done();
      });
    });

    it('should dedupe combined results', function(done) {
      registerWithLookupd(LOOKUPD_1, NSQD_1);
      registerWithLookupd(LOOKUPD_2, NSQD_1);
      registerWithLookupd(LOOKUPD_3, NSQD_1);
      registerWithLookupd(LOOKUPD_4, NSQD_1);

      let lookupdAddresses = [LOOKUPD_1, LOOKUPD_2, LOOKUPD_3, LOOKUPD_4];
      return lookup(lookupdAddresses, 'sample_topic', function(err, nodes) {
        nodes.should.have.length(1);
        return done();
      });
    });

    return it('should succeed inspite of failures to query a lookupd', function(done) {
      registerWithLookupd(LOOKUPD_1, NSQD_1);
      nock(`http://${LOOKUPD_2}`)
        .get('/lookup?topic=sample_topic')
        .reply(500);

      let lookupdAddresses = [LOOKUPD_1, LOOKUPD_2];
      return lookup(lookupdAddresses, 'sample_topic', function(err, nodes) {
        nodes.should.have.length(1);
        return done();
      });
    });
  });
});
