import _ from 'underscore'

import nock from 'nock'
import should from 'should'
import url from 'url'

import lookup from '../src/lookupd'

const NSQD_1 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 4151,
  remote_address: 'localhost:12345',
  tcp_port: 4150,
  topics: ['sample_topic'],
  version: '0.2.23'
}
const NSQD_2 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 5151,
  remote_address: 'localhost:56789',
  tcp_port: 5150,
  topics: ['sample_topic'],
  version: '0.2.23'
}
const NSQD_3 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 6151,
  remote_address: 'localhost:23456',
  tcp_port: 6150,
  topics: ['sample_topic'],
  version: '0.2.23'
}
const NSQD_4 = {
  address: 'localhost',
  broadcast_address: 'localhost',
  hostname: 'localhost',
  http_port: 7151,
  remote_address: 'localhost:34567',
  tcp_port: 7150,
  topics: ['sample_topic'],
  version: '0.2.23'
}

const LOOKUPD_1 = '127.0.0.1:4161'
const LOOKUPD_2 = '127.0.0.1:5161'
const LOOKUPD_3 = 'http://127.0.0.1:6161/'
const LOOKUPD_4 = 'http://127.0.0.1:7161/path/lookup'

const nockUrlSplit = function (url) {
  const match = url.match(/^(https?:\/\/[^\/]+)(\/.*$)/i)
  if (match) {
    return {
      baseUrl: match[1],
      path: match[2]
    }
  }
}

const registerWithLookupd = function (lookupdAddress, nsqd) {
  const producers = (nsqd != null) ? [nsqd] : []

  if (nsqd != null) {
    return (() => {
      const result = []
      for (const topic of Array.from(nsqd.topics)) {
        if (lookupdAddress.indexOf('://') === -1) {
          result.push(nock(`http://${lookupdAddress}`)
            .get(`/lookup?topic=${topic}`)
            .reply(200, {
              status_code: 200,
              status_txt: 'OK',
              data: {
                producers
              }
            },
          ))
        } else {
          let { baseUrl, path } = nockUrlSplit(lookupdAddress)
          if (!path || (path === '/')) {
            path = '/lookup'
          }
          result.push(nock(baseUrl)
            .get(`${path}?topic=${topic}`)
            .reply(200, {
              status_code: 200,
              status_txt: 'OK',
              data: {
                producers
              }
            },
          ))
        }
      }
      return result
    })()
  }
}

const setFailedTopicReply = (lookupdAddress, topic) =>
  nock(`http://${lookupdAddress}`)
    .get(`/lookup?topic=${topic}`)
    .reply(200, {
      status_code: 500,
      status_txt: 'INVALID_ARG_TOPIC',
      data: null
    },
  )

describe('lookupd.lookup', () => {
  afterEach(() => nock.cleanAll())

  describe('querying a single lookupd for a topic', () => {
    it('should return an empty list if no nsqd nodes', (done) => {
      setFailedTopicReply(LOOKUPD_1, 'sample_topic')

      return lookup(LOOKUPD_1, 'sample_topic', (err, nodes) => {
        nodes.should.be.empty
        return done()
      })
    })

    return it('should return a list of nsqd nodes for a success reply', (done) => {
      registerWithLookupd(LOOKUPD_1, NSQD_1)

      return lookup(LOOKUPD_1, 'sample_topic', (err, nodes) => {
        nodes.should.have.length(1)
        for (const key of ['address', 'broadcast_address', 'tcp_port', 'http_port']) {
          should.ok(Array.from(_.keys(nodes[0])).includes(key))
        }
        return done()
      })
    })
  })

  return describe('querying a multiple lookupd', () => {
    it('should combine results from multiple lookupds', (done) => {
      registerWithLookupd(LOOKUPD_1, NSQD_1)
      registerWithLookupd(LOOKUPD_2, NSQD_2)
      registerWithLookupd(LOOKUPD_3, NSQD_3)
      registerWithLookupd(LOOKUPD_4, NSQD_4)

      const lookupdAddresses = [LOOKUPD_1, LOOKUPD_2, LOOKUPD_3, LOOKUPD_4]
      return lookup(lookupdAddresses, 'sample_topic', (err, nodes) => {
        nodes.should.have.length(4)
        _.chain(nodes)
          .pluck('tcp_port')
          .sort()
          .value().should.be.eql([4150, 5150, 6150, 7150])
        return done()
      })
    })

    it('should dedupe combined results', (done) => {
      registerWithLookupd(LOOKUPD_1, NSQD_1)
      registerWithLookupd(LOOKUPD_2, NSQD_1)
      registerWithLookupd(LOOKUPD_3, NSQD_1)
      registerWithLookupd(LOOKUPD_4, NSQD_1)

      const lookupdAddresses = [LOOKUPD_1, LOOKUPD_2, LOOKUPD_3, LOOKUPD_4]
      return lookup(lookupdAddresses, 'sample_topic', (err, nodes) => {
        nodes.should.have.length(1)
        return done()
      })
    })

    return it('should succeed inspite of failures to query a lookupd', (done) => {
      registerWithLookupd(LOOKUPD_1, NSQD_1)
      nock(`http://${LOOKUPD_2}`)
        .get('/lookup?topic=sample_topic')
        .reply(500)

      const lookupdAddresses = [LOOKUPD_1, LOOKUPD_2]
      return lookup(lookupdAddresses, 'sample_topic', (err, nodes) => {
        nodes.should.have.length(1)
        return done()
      })
    })
  })
})
