import _ from 'underscore'
import should from 'should'
import sinon from 'sinon'

import { ConnectionState, NSQDConnection, WriterNSQDConnection, WriterConnectionState } from '../src/nsqdconnection'
import * as wire from '../src/wire'

describe('Reader ConnectionState', () => {
  const state = {
    sent: [],
    connection: null,
    statemachine: null
  }

  beforeEach(() => {
    const sent = []

    const connection = new NSQDConnection('127.0.0.1', 4150, 'topic_test',
      'channel_test')
    sinon.stub(connection, 'write', data => sent.push(data.toString()))
    sinon.stub(connection, 'destroy', () => {})

    const statemachine = new ConnectionState(connection)

    return _.extend(state, {
      sent,
      connection,
      statemachine
    },
    )
  })

  it('handle initial handshake', () => {
    const { statemachine, sent } = state
    statemachine.raise('connecting')
    statemachine.raise('connected')

    sent[0].should.match(/^ {2}V2$/)
    return sent[1].should.match(/^IDENTIFY/)
  })

  it('handle OK identify response', () => {
    const { statemachine, connection } = state
    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', new Buffer('OK'))

    connection.maxRdyCount.should.eql(2500)
    connection.maxMsgTimeout.should.eql(900000) // 15 minutes
    return connection.msgTimeout.should.eql(60000)
  })     // 1 minute

  it('handle identify response', () => {
    const { statemachine, connection } = state
    statemachine.raise('connecting')
    statemachine.raise('connected')

    statemachine.raise('response', JSON.stringify({
      max_rdy_count: 1000,
      max_msg_timeout: 10 * 60 * 1000,      // 10 minutes
      msg_timeout: 2 * 60 * 1000
    }),
    )           //  2 minutes

    connection.maxRdyCount.should.eql(1000)
    connection.maxMsgTimeout.should.eql(600000)  // 10 minutes
    return connection.msgTimeout.should.eql(120000)
  })     //  2 minute

  it('create a subscription', (done) => {
    const { sent, statemachine, connection } = state
    connection.on(NSQDConnection.READY, () =>
      // Subscribe notification
      done(),
    )

    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK') // Identify response

    sent[2].should.match(/^SUB topic_test channel_test\n$/)
    return statemachine.raise('response', 'OK')
  }) // Subscribe response

  it('handle a message', (done) => {
    const { statemachine, connection } = state
    connection.on(NSQDConnection.MESSAGE, msg => done())

    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK') // Identify response
    statemachine.raise('response', 'OK') // Subscribe response

    statemachine.current_state_name.should.eql('READY_RECV')

    statemachine.raise('consumeMessage', {})
    return statemachine.current_state_name.should.eql('READY_RECV')
  })

  it('handle a message finish after a disconnect', (done) => {
    const { statemachine, connection } = state
    sinon.stub(wire, 'unpackMessage', () => ['1', 0, 0, new Buffer(''), 60, 60, 120])

    connection.on(NSQDConnection.MESSAGE, (msg) => {
      const fin = function () {
        msg.finish()
        return done()
      }
      return setTimeout(fin, 10)
    })

    // Advance the connection to the READY state.
    statemachine.raise('connecting')
    statemachine.raise('connected')
    statemachine.raise('response', 'OK') // Identify response
    statemachine.raise('response', 'OK') // Subscribe response

    // Receive message
    const msg = connection.createMessage('')
    statemachine.raise('consumeMessage', msg)

    // Close the connection before the message has been processed.
    connection.destroy()
    statemachine.goto('CLOSED')

    // Undo stub
    return wire.unpackMessage.restore()
  })

  return it('handles non-fatal errors', (done) => {
    const { connection, statemachine } = state

    // Note: we still want an error event raised, just not a closed connection
    connection.on(NSQDConnection.ERROR, err => done())

    // Yields an error if the connection actually closes
    connection.on(NSQDConnection.CLOSED, () => done(new Error('Should not have closed!')))

    return statemachine.goto('ERROR', new Error('E_REQ_FAILED'))
  })
})

describe('WriterConnectionState', () => {
  const state = {
    sent: [],
    connection: null,
    statemachine: null
  }

  beforeEach(() => {
    const sent = []
    const connection = new WriterNSQDConnection('127.0.0.1', 4150)
    sinon.stub(connection, 'destroy')

    const write = sinon.stub(connection, 'write', data => sent.push(data.toString()))

    const statemachine = new WriterConnectionState(connection)
    connection.statemachine = statemachine

    return _.extend(state, {
      sent,
      connection,
      statemachine
    },
    )
  })

  it('should generate a READY event after IDENTIFY', (done) => {
    const { statemachine, connection } = state

    connection.on(WriterNSQDConnection.READY, () => {
      statemachine.current_state_name.should.eql('READY_SEND')
      return done()
    })

    statemachine.raise('connecting')
    statemachine.raise('connected')
    return statemachine.raise('response', 'OK')
  }) // Identify response

  it('should use PUB when sending a single message', (done) => {
    const { statemachine, connection, sent } = state

    connection.on(WriterNSQDConnection.READY, () => {
      connection.produceMessages('test', ['one'])
      sent[sent.length - 1].should.match(/^PUB/)
      return done()
    })

    statemachine.raise('connecting')
    statemachine.raise('connected')
    return statemachine.raise('response', 'OK')
  }) // Identify response

  it('should use MPUB when sending multiplie messages', (done) => {
    const { statemachine, connection, sent } = state

    connection.on(WriterNSQDConnection.READY, () => {
      connection.produceMessages('test', ['one', 'two'])
      sent[sent.length - 1].should.match(/^MPUB/)
      return done()
    })

    statemachine.raise('connecting')
    statemachine.raise('connected')
    return statemachine.raise('response', 'OK')
  }) // Identify response

  it('should call the callback when supplied on publishing a message', (done) => {
    const { statemachine, connection, sent } = state

    connection.on(WriterNSQDConnection.READY, () => {
      connection.produceMessages('test', ['one'], () => done())

      return statemachine.raise('response', 'OK')
    }) // Message response

    statemachine.raise('connecting')
    statemachine.raise('connected')
    return statemachine.raise('response', 'OK')
  }) // Identify response

  it('should call the the right callback on several messages', (done) => {
    const { statemachine, connection, sent } = state

    connection.on(WriterNSQDConnection.READY, () => {
      connection.produceMessages('test', ['one'])
      connection.produceMessages('test', ['two'], () => {
        // There should be no more callbacks
        connection.messageCallbacks.length.should.be.eql(0)
        return done()
      })

      statemachine.raise('response', 'OK') // Message response
      return statemachine.raise('response', 'OK')
    }) // Message response

    statemachine.raise('connecting')
    statemachine.raise('connected')
    return statemachine.raise('response', 'OK')
  }) // Identify response

  return it('should call all callbacks on nsqd disconnect', (done) => {
    const { statemachine, connection, sent } = state

    const firstCb = sinon.spy()
    const secondCb = sinon.spy()

    connection.on(WriterNSQDConnection.ERROR, () => {})
      // Nothing to do on error.

    connection.on(WriterNSQDConnection.READY, () => {
      connection.produceMessages('test', ['one'], firstCb)
      connection.produceMessages('test', ['two'], secondCb)
      return statemachine.goto('ERROR', 'lost connection')
    })

    connection.on(WriterNSQDConnection.CLOSED, () => {
      firstCb.calledOnce.should.be.ok()
      secondCb.calledOnce.should.be.ok()
      return done()
    })

    statemachine.raise('connecting')
    statemachine.raise('connected')
    return statemachine.raise('response', 'OK')
  })
}) // Identify response
