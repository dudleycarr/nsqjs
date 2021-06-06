const _ = require('lodash')
const should = require('should')
const sinon = require('sinon')
const rawMessage = require('./rawmessage')

const wire = require('../lib/wire')
const {
  ConnectionState,
  NSQDConnection,
  WriterNSQDConnection,
  WriterConnectionState,
} = require('../lib/nsqdconnection')

describe('Reader ConnectionState', () => {
  const state = {
    sent: [],
    connection: null,
    statemachine: null,
  }

  beforeEach(() => {
    const sent = []

    const connection = new NSQDConnection(
      '127.0.0.1',
      4150,
      'topic_test',
      'channel_test'
    )
    sinon
      .stub(connection, 'write')
      .callsFake((data) => sent.push(data.toString()))
    sinon.stub(connection, 'close').callsFake(() => {})
    sinon.stub(connection, 'destroy').callsFake(() => {})

    const statemachine = new ConnectionState(connection)

    return _.extend(state, {
      sent,
      connection,
      statemachine,
    })
  })

  it('handle initial handshake', () => {
    const {statemachine, sent} = state
    statemachine.raise('connecting')
    statemachine.raise('connected')
    sent[0].should.match(/^ {2}V2$/)
    sent[1].should.match(/^IDENTIFY/)
  })

  it('handle OK identify response', () => {
    const {statemachine, connection} = state
    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', Buffer.from('OK'))

    should.equal(connection.maxRdyCount, 2500)
    should.equal(connection.maxMsgTimeout, 900000)
    should.equal(connection.msgTimeout, 60000)
  })

  it('handle identify response', () => {
    const {statemachine, connection} = state
    statemachine.raise('connecting')
    statemachine.raise('connected')

    statemachine.raise(
      'response',
      JSON.stringify({
        max_rdy_count: 1000,
        max_msg_timeout: 10 * 60 * 1000,
        msg_timeout: 2 * 60 * 1000,
      })
    )

    should.equal(connection.maxRdyCount, 1000)
    should.equal(connection.maxMsgTimeout, 600000)
    should.equal(connection.msgTimeout, 120000)
  })

  it('create a subscription', (done) => {
    const {sent, statemachine, connection} = state

    // Subscribe notification
    connection.on(NSQDConnection.READY, () => done())

    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK') // Identify response

    sent[2].should.match(/^SUB topic_test channel_test\n$/)
    statemachine.raise('response', 'OK')
  })

  it('handle a message', (done) => {
    const {statemachine, connection} = state
    connection.on(NSQDConnection.MESSAGE, () => done())

    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK') // Identify response
    statemachine.raise('response', 'OK') // Subscribe response

    should.equal(statemachine.current_state_name, 'READY_RECV')

    statemachine.raise('consumeMessage', {})
    should.equal(statemachine.current_state_name, 'READY_RECV')
  })

  it('handle a message finish after a disconnect', (done) => {
    const {statemachine, connection} = state
    sinon
      .stub(wire, 'unpackMessage')
      .callsFake(() => ['1', 0, 0, Buffer.from(''), 60, 60, 120])

    connection.on(NSQDConnection.MESSAGE, (msg) => {
      const fin = () => {
        msg.finish()
        done()
      }
      setTimeout(fin, 10)
    })

    // Advance the connection to the READY state.
    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK') // Identify response
    statemachine.raise('response', 'OK') // Subscribe response

    // Receive message
    const msg = connection.createMessage(rawMessage('1', Date.now(), 0, 'msg'))
    statemachine.raise('consumeMessage', msg)

    // Close the connection before the message has been processed.
    connection.destroy()
    statemachine.goto('CLOSED')

    // Undo stub
    wire.unpackMessage.restore()
  })

  it('handles non-fatal errors', (done) => {
    const {connection, statemachine} = state

    // Note: we still want an error event raised, just not a closed connection
    connection.on(NSQDConnection.ERROR, () => done())

    // Yields an error if the connection actually closes
    connection.on(NSQDConnection.CLOSED, () => {
      done(new Error('Should not have closed!'))
    })

    statemachine.goto('ERROR', new Error('E_REQ_FAILED'))
  })
})

describe('WriterConnectionState', () => {
  const state = {
    sent: [],
    connection: null,
    statemachine: null,
  }

  beforeEach(() => {
    const sent = []
    const connection = new WriterNSQDConnection('127.0.0.1', 4150)
    sinon.stub(connection, 'destroy')

    sinon.stub(connection, 'write').callsFake((data) => {
      sent.push(data.toString())
    })

    const statemachine = new WriterConnectionState(connection)
    connection.statemachine = statemachine

    _.extend(state, {
      sent,
      connection,
      statemachine,
    })
  })

  it('should generate a READY event after IDENTIFY', (done) => {
    const {statemachine, connection} = state

    connection.on(WriterNSQDConnection.READY, () => {
      should.equal(statemachine.current_state_name, 'READY_SEND')
      done()
    })

    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK')
  })

  it('should use PUB when sending a single message', (done) => {
    const {statemachine, connection, sent} = state

    connection.on(WriterNSQDConnection.READY, () => {
      connection.produceMessages('test', ['one'])
      sent[sent.length - 1].should.match(/^PUB/)
      done()
    })

    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK')
  })

  it('should use MPUB when sending multiplie messages', (done) => {
    const {statemachine, connection, sent} = state

    connection.on(WriterNSQDConnection.READY, () => {
      connection.produceMessages('test', ['one', 'two'])
      sent[sent.length - 1].should.match(/^MPUB/)
      done()
    })

    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK')
  })

  it('should call the callback when supplied on publishing a message', (done) => {
    const {statemachine, connection} = state

    connection.on(WriterNSQDConnection.READY, () => {
      connection.produceMessages('test', ['one'], undefined, () => done())
      statemachine.raise('response', 'OK')
    })

    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK')
  })

  it('should call the the right callback on several messages', (done) => {
    const {statemachine, connection} = state

    connection.on(WriterNSQDConnection.READY, () => {
      connection.produceMessages('test', ['one'], undefined)
      connection.produceMessages('test', ['two'], undefined, () => {
        // There should be no more callbacks
        should.equal(connection.messageCallbacks.length, 0)
        done()
      })

      statemachine.raise('response', 'OK')
      statemachine.raise('response', 'OK')
    })

    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK')
  })

  it('should call all callbacks on nsqd disconnect', (done) => {
    const {statemachine, connection} = state

    const firstCb = sinon.spy()
    const secondCb = sinon.spy()

    connection.on(WriterNSQDConnection.ERROR, () => {})

    connection.on(WriterNSQDConnection.READY, () => {
      connection.produceMessages('test', ['one'], undefined, firstCb)
      connection.produceMessages('test', ['two'], undefined, secondCb)
      statemachine.goto('ERROR', 'lost connection')
    })

    connection.on(WriterNSQDConnection.CLOSED, () => {
      firstCb.calledOnce.should.be.ok()
      secondCb.calledOnce.should.be.ok()
      done()
    })

    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK')
  })
})
