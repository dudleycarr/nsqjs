_ = require 'underscore'

chai      = require 'chai'
expect    = chai.expect
nock      = require 'nock'
should    = chai.should()
sinon     = require 'sinon'
sinonChai = require 'sinon-chai'

chai.use sinonChai

{nodes, lookup} = require '../lib/lookupd'

describe 'lookupd.nodes', ->
  describe 'querying a single lookupd', ->
    it 'should return an empty list if no nsqd nodes', (done) ->
      nock('http://127.0.0.1:4161')
        .get('/nodes')
        .reply 200,
          status_code: 200
          status_txt: 'OK'
          data:
            producers: []

      nodes '127.0.0.1:4161', (err, nodes) ->
        nodes.should.be.empty
        done()

    it 'should return a list of nsqd nodes for a success reply', (done) ->
      nock('http://127.0.0.1:4161')
        .get('/nodes')
        .reply 200,
          status_code: 200
          status_txt: 'OK'
          data:
            producers:
              address: 'localhost'
              broadcast_address: 'localhost'
              hostname: 'localhost'
              http_port: 4151
              remote_address: 'localhost:12345'
              tcp_port: 4150
              topics: ['sample_topic']
              version: '0.2.23'

      nodes '127.0.0.1:4161', (err, nodes) ->
        nodes.should.have.length 1
        for key in ['address', 'broadcast_address', 'tcp_port', 'http_port']
          _.keys(nodes[0]).should.contain key
        done()

  describe 'querying a multiple lookupd', ->
    it 'should combine results from multiple lookupds', (done) ->
      nock('http://127.0.0.1:4161')
        .get('/nodes')
        .reply 200,
          status_code: 200
          status_txt: 'OK'
          data:
            producers:
              address: 'localhost'
              broadcast_address: 'localhost'
              hostname: 'localhost'
              http_port: 4151
              remote_address: 'localhost:12345'
              tcp_port: 4150
              topics: ['sample_topic']
              version: '0.2.23'
      nock('http://127.0.0.1:5161')
        .get('/nodes')
        .reply 200,
          status_code: 200
          status_txt: 'OK'
          data:
            producers:
              address: 'localhost'
              broadcast_address: 'localhost'
              hostname: 'localhost'
              http_port: 5151
              remote_address: 'localhost:56789'
              tcp_port: 5150
              topics: ['sample_topic']
              version: '0.2.23'

      nodes ['127.0.0.1:4161', '127.0.0.1:5161'], (err, nodes) ->
        nodes.should.have.length 2
        _.chain(nodes)
          .pluck('tcp_port')
          .sort()
          .value().should.be.eql [4150, 5150]
        done()

    it 'should dedupe combined results', (done) ->
      nock('http://127.0.0.1:4161')
        .get('/nodes')
        .reply 200,
          status_code: 200
          status_txt: 'OK'
          data:
            producers:
              address: 'localhost'
              broadcast_address: 'localhost'
              hostname: 'localhost'
              http_port: 4151
              remote_address: 'localhost:12345'
              tcp_port: 4150
              topics: ['sample_topic']
              version: '0.2.23'
      nock('http://127.0.0.1:5161')
        .get('/nodes')
        .reply 200,
          status_code: 200
          status_txt: 'OK'
          data:
            producers:
              address: 'localhost'
              broadcast_address: 'localhost'
              hostname: 'localhost'
              http_port: 4151
              remote_address: 'localhost:12345'
              tcp_port: 4150
              topics: ['sample_topic']
              version: '0.2.23'

      nodes ['127.0.0.1:4161', '127.0.0.1:5161'], (err, nodes) ->
        nodes.should.have.length 1
        done()

    it 'should succeed inspite of failures to query a lookupd', (done) ->
      nock('http://127.0.0.1:4161')
        .get('/nodes')
        .reply 200,
          status_code: 200
          status_txt: 'OK'
          data:
            producers:
              address: 'localhost'
              broadcast_address: 'localhost'
              hostname: 'localhost'
              http_port: 4151
              remote_address: 'localhost:12345'
              tcp_port: 4150
              topics: ['sample_topic']
              version: '0.2.23'
      nock('http://127.0.0.1:5161')
        .get('/nodes')
        .reply 500

      nodes ['127.0.0.1:4161', '127.0.0.1:5161'], (err, nodes) ->
        nodes.should.have.length 1
        done()
