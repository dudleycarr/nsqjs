_ = require 'underscore'

chai      = require 'chai'
expect    = chai.expect
nock      = require 'nock'
should    = chai.should()
sinon     = require 'sinon'
sinonChai = require 'sinon-chai'

chai.use sinonChai

{nodes, lookup} = require '../lib/lookupd'

NSQD_1 =
  address: 'localhost'
  broadcast_address: 'localhost'
  hostname: 'localhost'
  http_port: 4151
  remote_address: 'localhost:12345'
  tcp_port: 4150
  topics: ['sample_topic']
  version: '0.2.23'
NSQD_2 =
  address: 'localhost'
  broadcast_address: 'localhost'
  hostname: 'localhost'
  http_port: 5151
  remote_address: 'localhost:56789'
  tcp_port: 5150
  topics: ['sample_topic']
  version: '0.2.23'

LOOKUPD_1 = '127.0.0.1:4161'
LOOKUPD_2 = '127.0.0.1:5161'

registerWithLookupd = (lookupdAddress, nsqd) ->
  producers = if nsqd? then [nsqd] else []

  nock("http://#{lookupdAddress}")
    .get('/nodes')
    .reply 200,
      status_code: 200
      status_txt: 'OK'
      data:
        producers: producers

  if nsqd?
    for topic in nsqd.topics
      nock("http://#{lookupdAddress}")
        .get("/lookup?topic=#{topic}")
        .reply 200,
          status_code: 200
          status_txt: 'OK'
          data:
            producers: producers

setFailedTopicReply = (lookupdAddress, topic) ->
  nock("http://#{lookupdAddress}")
    .get("/lookup?topic=#{topic}")
    .reply 200,
      status_code: 500
      status_txt: 'INVALID_ARG_TOPIC'
      data: null

describe 'lookupd.nodes', ->

  afterEach ->
    nock.cleanAll()

  describe 'querying a single lookupd', ->
    it 'should return an empty list if no nsqd nodes', (done) ->
      registerWithLookupd LOOKUPD_1, null

      nodes LOOKUPD_1, (err, nodes) ->
        nodes.should.be.empty
        done()

    it 'should return a list of nsqd nodes for a success reply', (done) ->
      registerWithLookupd LOOKUPD_1, NSQD_1

      nodes LOOKUPD_1, (err, nodes) ->
        nodes.should.have.length 1
        for key in ['address', 'broadcast_address', 'tcp_port', 'http_port']
          _.keys(nodes[0]).should.contain key
        done()

  describe 'querying a multiple lookupd', ->
    it 'should combine results from multiple lookupds', (done) ->
      registerWithLookupd LOOKUPD_1, NSQD_1
      registerWithLookupd LOOKUPD_2, NSQD_2

      nodes [LOOKUPD_1, LOOKUPD_2], (err, nodes) ->
        nodes.should.have.length 2
        _.chain(nodes)
          .pluck('tcp_port')
          .sort()
          .value().should.be.eql [4150, 5150]
        done()

    it 'should dedupe combined results', (done) ->
      registerWithLookupd LOOKUPD_1, NSQD_1
      registerWithLookupd LOOKUPD_2, NSQD_1

      nodes [LOOKUPD_1, LOOKUPD_2], (err, nodes) ->
        nodes.should.have.length 1
        done()

    it 'should succeed inspite of failures to query a lookupd', (done) ->
      registerWithLookupd LOOKUPD_1, NSQD_1
      nock("http://#{LOOKUPD_2}")
        .get('/nodes')
        .reply 500

      nodes [LOOKUPD_1, LOOKUPD_2], (err, nodes) ->
        nodes.should.have.length 1
        done()

describe 'lookupd.lookup', ->
  describe 'querying a single lookupd for a topic', ->
    it 'should return an empty list if no nsqd nodes', (done) ->
      setFailedTopicReply LOOKUPD_1, 'sample_topic'

      lookup LOOKUPD_1, 'sample_topic', (err, nodes) ->
        nodes.should.be.empty
        done()

    it 'should return a list of nsqd nodes for a success reply', (done) ->
      registerWithLookupd LOOKUPD_1, NSQD_1

      lookup LOOKUPD_1, 'sample_topic', (err, nodes) ->
        nodes.should.have.length 1
        for key in ['address', 'broadcast_address', 'tcp_port', 'http_port']
          _.keys(nodes[0]).should.contain key
        done()

  describe 'querying a multiple lookupd', ->
    it 'should combine results from multiple lookupds', (done) ->
      registerWithLookupd LOOKUPD_1, NSQD_1
      registerWithLookupd LOOKUPD_2, NSQD_2

      lookupdAddresses = [LOOKUPD_1, LOOKUPD_2]
      lookup lookupdAddresses, 'sample_topic', (err, nodes) ->
        nodes.should.have.length 2
        _.chain(nodes)
          .pluck('tcp_port')
          .sort()
          .value().should.be.eql [4150, 5150]
        done()

    it 'should dedupe combined results', (done) ->
      registerWithLookupd LOOKUPD_1, NSQD_1
      registerWithLookupd LOOKUPD_2, NSQD_1

      lookupdAddresses = [LOOKUPD_1, LOOKUPD_2]
      lookup lookupdAddresses, 'sample_topic', (err, nodes) ->
        nodes.should.have.length 1
        done()

    it 'should succeed inspite of failures to query a lookupd', (done) ->
      registerWithLookupd LOOKUPD_1, NSQD_1
      nock("http://#{LOOKUPD_2}")
        .get('/lookup?topic=sample_topic')
        .reply 500

      lookupdAddresses = [LOOKUPD_1, LOOKUPD_2]
      lookup lookupdAddresses, 'sample_topic', (err, nodes) ->
        nodes.should.have.length 1
        done()
