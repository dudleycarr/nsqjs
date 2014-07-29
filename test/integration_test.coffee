_ = require 'underscore'
child_process = require 'child_process'
request = require 'request'

chai = require 'chai'
should = chai.should
sinon = require 'sinon'
temp = require('temp').track()

nsq = require '../src/nsq'

TCP_PORT = 14150
HTTP_PORT = 14151

startNSQD = (dataPath, additionalOptions, callback) ->
  additionalOptions or= {}
  options =
    'http-address': "127.0.0.1:#{HTTP_PORT}"
    'tcp-address': "127.0.0.1:#{TCP_PORT}"
    'data-path': dataPath
    'tls-cert': './test/cert.pem'
    'tls-key': './test/key.pem'

  _.extend options, additionalOptions
  options = _.flatten (["-#{key}", value] for key, value of options)
  process = child_process.spawn 'nsqd', options.concat additionalOptions

  # Give nsqd a chance to startup successfully.
  setTimeout _.partial(callback, null, process), 500

topicOp = (op, topic, callback) ->
  options =
    method: 'POST'
    uri: "http://127.0.0.1:#{HTTP_PORT}/#{op}"
    qs:
      topic: topic

  request options, (err, res, body) ->
    callback err

createTopic = _.partial topicOp, 'create_topic'
deleteTopic = _.partial topicOp, 'delete_topic'

# Publish a single message via HTTP
publish = (topic, message, done) ->
  options =
    uri: "http://127.0.0.1:#{HTTP_PORT}/pub"
    method: 'POST'
    qs:
      topic: topic
    body: message

  request options, (err, res, body) ->
    done? err

describe 'integration', ->
  nsqdProcess = null
  reader = null

  before (done) ->
    temp.mkdir '/nsq', (err, dirPath) ->
      startNSQD dirPath, {}, (err, process) ->
        nsqdProcess = process
        done err

  after (done) ->
    nsqdProcess.kill()
    # Give nsqd a chance to exit before it's data directory will be cleaned up.
    setTimeout done, 500

  beforeEach (done) ->
      createTopic 'test', (err) ->
        done err

  afterEach (done) ->
    reader.close()
    deleteTopic 'test', (err) ->
      done err

  describe 'stream compression and encryption', ->
    optionPermutations = [
      {deflate: true}
      {snappy: true}
      {tls: true, tlsVerification: false}
      {tls: true, tlsVerification: false, snappy: true}
      {tls: true, tlsVerification: false, deflate: true}
    ]
    for options in optionPermutations
      # Figure out what compression is enabled
      compression = (key for key in ['deflate', 'snappy'] when key of options)
      compression.push 'none'

      description =
        "reader with compression (#{compression[0]}) and tls (#{options.tls?})"

      describe description, ->
        it 'should send and receive a message', (done) ->

          topic = 'test'
          channel = 'default'
          message = "a message for our reader"

          publish topic, message

          reader = new nsq.Reader topic, channel,
            _.extend {nsqdTCPAddresses: ["127.0.0.1:#{TCP_PORT}"]}, options

          reader.on 'message', (msg) ->
            msg.body.toString().should.eql message
            msg.finish()
            done()

          reader.connect()

        it 'should send and receive a large message', (done) ->
          topic = 'test'
          channel = 'default'
          message = ('a' for i in [0..100000]).join ''

          publish topic, message

          reader = new nsq.Reader topic, channel,
            _.extend {nsqdTCPAddresses: ["127.0.0.1:#{TCP_PORT}"]}, options

          reader.on 'message', (msg) ->
            msg.body.toString().should.eql message
            msg.finish()
            done()

          reader.connect()
