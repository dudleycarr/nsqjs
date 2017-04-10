_ = require 'underscore'
async = require 'async'
child_process = require 'child_process'
request = require 'request'

should = require 'should'
temp = require('temp').track()

nsq = require '../src/nsq'

TCP_PORT = 4150
HTTP_PORT = 4151

startNSQD = (dataPath, additionalOptions, callback) ->
  additionalOptions or= {}
  options =
    'http-address': "127.0.0.1:#{HTTP_PORT}"
    'tcp-address': "127.0.0.1:#{TCP_PORT}"
    'broadcast-address': '127.0.0.1'
    'data-path': dataPath
    'tls-cert': './test/cert.pem'
    'tls-key': './test/key.pem'

  _.extend options, additionalOptions
  options = _.flatten (["-#{key}", value] for key, value of options)
  process = child_process.spawn 'nsqd', options.concat(additionalOptions),
    stdio: ['ignore', 'ignore', 'ignore']

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

createTopic = _.partial topicOp, 'topic/create'
deleteTopic = _.partial topicOp, 'topic/delete'

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
    createTopic 'test', done

  afterEach (done) ->
    reader.close()
    deleteTopic 'test', done

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

  describe 'end to end', ->
    topic = 'test'
    channel = 'default'
    tcpAddress = "127.0.0.1:#{TCP_PORT}"
    writer = null
    reader = null

    beforeEach (done) ->
      writer = new nsq.Writer '127.0.0.1', TCP_PORT
      writer.on 'ready', ->
        reader = new nsq.Reader topic, channel, nsqdTCPAddresses: tcpAddress
        reader.connect()
        done()

      writer.connect()

    afterEach ->
      writer.close()

    it 'should send and receive a string', (done) ->
      message = 'hello world'
      writer.publish topic, message, (err) ->
        done err if err

      reader.on 'message', (msg) ->
        msg.body.toString().should.eql message
        msg.finish()
        done()

    it 'should send and receive a Buffer', (done) ->
      message = new Buffer [0x11, 0x22, 0x33]
      writer.publish topic, message

      reader.on 'message', (readMsg) ->
        readByte.should.equal message[i] for readByte, i in readMsg.body
        readMsg.finish()
        done()

    # TODO (Dudley): The behavior of nsqd seems to have changed around this.
    #    This requires more investigation but not concerning at the moment.
    it.skip 'should not receive messages when immediately paused', (done) ->
      waitedLongEnough = false

      timeout = setTimeout ->
        reader.unpause()
        waitedLongEnough = true
      , 100

      # Note: because NSQDConnection.connect() does most of it's work in
      # process.nextTick(), we're really pausing before the reader is
      # connected.
      #
      reader.pause()
      reader.on 'message', (msg) ->
        msg.finish()
        clearTimeout timeout
        waitedLongEnough.should.be.true()
        done()

      writer.publish topic, 'pause test'

    it 'should not receive any new messages when paused', (done) ->
      writer.publish topic, messageShouldArrive: true

      reader.on 'message', (msg) ->
        # check the message
        msg.json().messageShouldArrive.should.be.true()
        msg.finish()

        if reader.isPaused() then return done()

        reader.pause()

        process.nextTick ->
          # send it again, shouldn't get this one
          writer.publish topic, messageShouldArrive: false
          setTimeout done, 100


    it 'should not receive any requeued messages when paused', (done) ->
      writer.publish topic, 'requeue me'
      id = ''

      reader.on 'message', (msg) ->
        # this will fail if the msg comes through again
        id.should.equal ''
        id = msg.id

        if reader.isPaused() then return done()
        reader.pause()

        process.nextTick ->
          # send it again, shouldn't get this one
          msg.requeue 0, false
          setTimeout done, 100

    it 'should start receiving messages again after unpause', (done) ->
      shouldReceive = true
      writer.publish topic, sentWhilePaused: false

      reader.on 'message', (msg) ->
        shouldReceive.should.be.true()

        reader.pause()
        msg.requeue()

        if msg.json().sentWhilePaused then return done()

        shouldReceive = false
        writer.publish topic, sentWhilePaused: true
        setTimeout ->
          shouldReceive = true
          reader.unpause()
        , 100

        done()

    it 'should successfully publish a message before fully connected', (done) ->
      writer = new nsq.Writer '127.0.0.1', TCP_PORT
      writer.connect()

      # The writer is connecting, but it shouldn't be ready to publish.
      writer.ready.should.eql false

      # Publish the message. It should succeed since the writer will queue up
      # the message while connecting.
      writer.publish 'a_topic', 'a message', (err) ->
        should.not.exist err
        done()

describe 'failures', ->
  before (done) ->
    temp.mkdir '/nsq', (err, dirPath) =>
      startNSQD dirPath, {}, (err, process) =>
        @nsqdProcess = process
        done err

  after (done) ->
    @nsqdProcess.kill()
    # Give nsqd a chance to exit before it's data directory will be cleaned up.
    setTimeout done, 500

  describe 'Writer', ->
    describe 'nsqd disconnect before publish', ->
      it 'should fail to publish a message', (done) ->
        writer = new nsq.Writer '127.0.0.1', TCP_PORT
        async.series [
          # Connect the writer to the nsqd.
          (callback) ->
            writer.connect()
            writer.on 'ready', ->
              callback()
          # Stop the nsqd process.
          (callback) =>
            @nsqdProcess.kill()
            setTimeout callback, 200
          # Attempt to publish a message.
          (callback) ->
            writer.publish 'test_topic', 'a message that should fail', (err) ->
              should.exist err
              callback()
        ], (err) ->
          done err
