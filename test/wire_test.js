const should = require('should')
const wire = require('../lib/wire')

const matchCommand = (commandFn, args, expected) => {
  const commandOut = commandFn(...args)
  should.equal(commandOut.toString(), expected)
}

describe('nsq wire', () => {
  it('should construct an identity message', () => {
    matchCommand(
      wire.identify,
      [{short_id: 1, long_id: 2}],
      'IDENTIFY\n\u0000\u0000\u0000\u001a{"short_id":1,"long_id":2}'
    )
  })

  it('should construct an identity message with unicode', () =>
    matchCommand(
      wire.identify,
      [{long_id: 'w\u00c3\u00a5\u00e2\u0080\u00a0'}],
      'IDENTIFY\n\u0000\u0000\u0000-{"long_id":"w\\u00c3\\u00a5\\u00e2' +
        '\\u0080\\u00a0"}'
    ))

  it('should subscribe to a topic and channel', () =>
    matchCommand(
      wire.subscribe,
      ['test_topic', 'test_channel'],
      'SUB test_topic test_channel\n'
    ))

  it('should finish a message', () =>
    matchCommand(wire.finish, ['test'], 'FIN test\n'))

  it('should finish a message with a unicode id', () =>
    matchCommand(
      wire.finish,
      ['\u00fcn\u00ee\u00e7\u00f8\u2202\u00e9'],
      'FIN \u00fcn\u00ee\u00e7\u00f8\u2202\u00e9\n'
    ))

  it('should requeue a message', () =>
    matchCommand(wire.requeue, ['test'], 'REQ test 0\n'))

  it('should requeue a message with timeout', () =>
    matchCommand(wire.requeue, ['test', 60], 'REQ test 60\n'))

  it('should touch a message', () =>
    matchCommand(wire.touch, ['test'], 'TOUCH test\n'))

  it('should construct a ready message', () =>
    matchCommand(wire.ready, [100], 'RDY 100\n'))

  it('should construct a no-op message', () =>
    matchCommand(wire.nop, [], 'NOP\n'))

  it('should publish a message', () =>
    matchCommand(
      wire.pub,
      ['test_topic', 'abcd'],
      'PUB test_topic\n\u0000\u0000\u0000\u0004abcd'
    ))

  it('should publish a multi-byte string message', () =>
    matchCommand(
      wire.pub,
      ['test_topic', 'こんにちは'],
      'PUB test_topic\n\u0000\u0000\u0000\u000fこんにちは'
    ))

  it('should publish multiple string messages', () =>
    matchCommand(
      wire.mpub,
      ['test_topic', ['abcd', 'efgh', 'ijkl']],
      [
        'MPUB test_topic\n\u0000\u0000\u0000\u001c\u0000\u0000\u0000\u0003',
        '\u0000\u0000\u0000\u0004abcd',
        '\u0000\u0000\u0000\u0004efgh',
        '\u0000\u0000\u0000\u0004ijkl',
      ].join('')
    ))

  it('should publish multiple buffer messages', () =>
    matchCommand(
      wire.mpub,
      ['test_topic', [Buffer.from('abcd'), Buffer.from('efgh')]],
      [
        'MPUB test_topic\n\u0000\u0000\u0000\u0014\u0000\u0000\u0000\u0002',
        '\u0000\u0000\u0000\u0004abcd',
        '\u0000\u0000\u0000\u0004efgh',
      ].join('')
    ))

  return it('should unpack a received message', () => {
    const msgPayload = [
      '132cb60626e9fd7a00013035356335626531636534333330323769747265616c6c7974',
      '696564746865726f6f6d746f676574686572',
    ]
    const msgParts = wire.unpackMessage(Buffer.from(msgPayload.join(''), 'hex'))

    const [id, timestamp, attempts] = Array.from(msgParts)
    timestamp.toString(10).should.eql('1381679323234827642')
    id.should.eql('055c5be1ce433027')
    return attempts.should.eql(1)
  })
})
