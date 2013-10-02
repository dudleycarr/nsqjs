chai      = require('chai')
expect    = chai.expect
should    = chai.should()
sinon     = require('sinon')
sinonChai = require('sinon-chai')

chai.use(sinonChai)

wire = require '../lib/wire.coffee'

matchCommand = (commandFn, args, expected) ->
  commandOut = commandFn.apply null, args
  commandOut.toString().should.eq expected

describe "nsq wire", ->

  it "should construct an identity message", ->
    matchCommand wire.identify, [{'short_id': 1, 'long_id': 2}],
      'IDENTIFY\n\u0000\u0000\u0000\u001a{"short_id":1,"long_id":2}'

  it 'should construct an identity message with unicode', ->
    matchCommand wire.identify,
      [{"long_id": "w\u00c3\u00a5\u00e2\u0080\u00a0"}],
      'IDENTIFY\n\u0000\u0000\u0000-{"long_id":"w\\u00c3\\u00a5\\u00e2\\u0080\\u00a0"}'

  it "should subscribe to a topic and channel", ->
    matchCommand wire.subscribe, ['test_topic', 'test_channel'],
      'SUB test_topic test_channel\n'

  it "should finish a message", ->
    matchCommand wire.finish, ['test'], 'FIN test\n'

  it 'should finish a message with a unicode id', ->
    matchCommand wire.finish,
      ['\u00fcn\u00ee\u00e7\u00f8\u2202\u00e9'],
      'FIN \u00fcn\u00ee\u00e7\u00f8\u2202\u00e9\n'

  it "should requeue a message", ->
    matchCommand wire.requeue, ['test'], 'REQ test\n'

  it "should requeue a message with timeout", ->
    matchCommand wire.requeue, ['test', 60], 'REQ test 60\n'

  it "should touch a message", ->
    matchCommand wire.touch, ['test'], 'TOUCH test\n'

  it "should construct a ready message", ->
    matchCommand wire.ready, [100], 'RDY 100\n'

  it 'should construct a no-op message', ->
    matchCommand wire.nop, [], 'NOP\n'

  it 'should publish a message', ->
    matchCommand wire.pub, ['test_topic', 'abcd'],
      'PUB test_topic\n\u0000\u0000\u0000\u0004abcd'

  it 'should publish multiple messages', ->
    matchCommand wire.mpub, ['test_topic', ['abcd', 'efgh', 'ijkl']],
      ['MPUB test_topic\n\u0000\u0000\u0000\u001c\u0000\u0000\u0000\u0003'
       '\u0000\u0000\u0000\u0004abcd', '\u0000\u0000\u0000\u0004efgh',
       '\u0000\u0000\u0000\u0004ijkl'].join ''
