nsq = require './src/nsq'

writer = new nsq.Writer '127.0.0.1', 4150
writer.on 'ready', ->
  writer.publish 'test', 'empty message'
  writer.close()
writer.connect()

reader = new nsq.Reader 'test', 'default',
  nsqdTCPAddresses: ['127.0.0.1:4150']

reader.on 'message', (msg) ->
  console.log 'got a message!'
  msg.finish()

reader.connect()
