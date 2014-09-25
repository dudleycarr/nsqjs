nsq = require './src/nsq'
async = require 'async'

TOPIC = 'test_topic'

writer = new nsq.Writer '127.0.0.1', 4150,
  deflate: true
reader = new nsq.Reader TOPIC, 'test_channel',
  nsqdTCPAddresses: ['127.0.0.1:4150']
  snappy: true


# Connect both the reader and the writer
connect = (callback) ->
  async.parallel [
    (callback) ->
      writer.connect()
      writer.on 'ready', ->
        callback()
    (callback) ->
      reader.connect()
      reader.on 'nsqd_connected', ->
        callback()
    ], callback

# Consume a message
consume = (callback) ->
  reader.on 'message', (msg) ->
    console.log "message: #{msg.body.toString()}"
    msg.finish()
    callback()

  writer.publish TOPIC, 'hello, world!'

async.series [connect, consume], (err) ->
  console.log 'all done!'
  process.exit 0
