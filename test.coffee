nsq = require './src/nsq'

writer = new nsq.Writer '127.0.0.1', 4150
writer.connect()

reader = new nsq.Reader 'sample', 'default',
  nsqdTCPAddresses: ['127.0.0.1:4150']

writer.on nsq.Writer.READY, ->
  writer.publish 'sample', 'sample message', ->
    console.log 'Sent message'
    reader.connect()

reader.on nsq.Reader.MESSAGE, (msg) ->
  console.log 'Got a message'

reader.on nsq.Reader.NSQD_CONNECTED, (host, port) ->
  console.log "Connected to nsqd (#{host}, #{port})"

reader.on nsq.Reader.NSQD_CLOSED, (host, port) ->
  console.log "Disconnected from nsqd (#{host}, #{port})"

