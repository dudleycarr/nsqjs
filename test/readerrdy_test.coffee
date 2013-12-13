chai = require 'chai'
expect = chai.expect
should = chai.should
sinon = require 'sinon'
sinonChai = require 'sinon-chai'
chai.use sinonChai

{NSQDConnection} = require '../lib/nsqdconnection'
{ReaderRdy, ConnectionRdy} = require '../lib/readerrdy'

describe 'ConnectionRdy', ->
  [conn, spy, cRdy] = [null, null, null]

  beforeEach ->
    conn =
      conn:
        localPort: 1
      on: (eventName, f) ->
      setRdy: (count) ->
      maxRdyCount: 100
    spy = sinon.spy conn, 'setRdy'
    cRdy = new ConnectionRdy conn
    cRdy.start()

  it 'should register listeners on a connection', ->
    conn = new NSQDConnection 'localhost', 1234, 'test', 'test'
    mock = sinon.mock conn
    mock.expects('on').withArgs(NSQDConnection.FINISHED)
    mock.expects('on').withArgs(NSQDConnection.MESSAGE)
    mock.expects('on').withArgs(NSQDConnection.REQUEUED)
    mock.expects('on').withArgs(NSQDConnection.SUBSCRIBED)

    cRdy = new ConnectionRdy conn
    mock.verify()

  it 'should have a connection RDY max of zero', ->
    expect(cRdy.maxConnRdy).is.eql 0

  it 'should not increase RDY when connection RDY max has not been set', ->
    # This bump should be a no-op
    cRdy.bump()
    expect(cRdy.maxConnRdy).is.eql 0
    expect(spy.called).is.not.ok

  it 'should not allow RDY counts to be negative', ->
    cRdy.setConnectionRdyMax 10
    cRdy.setRdy -1

    expect(spy.notCalled).is.ok

  it 'should not allow RDY counts to exceed the connection max', ->
    cRdy.setConnectionRdyMax 10
    cRdy.setRdy 9
    cRdy.setRdy 10
    cRdy.setRdy 20

    expect(spy.calledTwice).is.ok
    expect(spy.firstCall.args[0]).is.eql 9
    expect(spy.secondCall.args[0]).is.eql 10

  it 'should set RDY to max after initial bump', ->
    cRdy.setConnectionRdyMax 3
    cRdy.bump()

    expect(spy.firstCall.args[0]).is.eql 3

  it 'should keep RDY at max after 1+ bumps', ->
    cRdy.setConnectionRdyMax 3
    for i in [1..3]
      cRdy.bump()

    expect(cRdy.maxConnRdy).is.eql 3
    for i in [0...spy.callCount]
      expect(spy.getCall(i).args[0]).is.at.most 3

  it 'should set RDY to zero from after first bump and then backoff', ->
    cRdy.setConnectionRdyMax 3
    cRdy.bump()
    cRdy.backoff()

    expect(spy.lastCall.args[0]).is.eql 0

  it 'should set RDY to zero after 1+ bumps and then a backoff', ->
    cRdy.setConnectionRdyMax 3
    cRdy.bump()
    cRdy.backoff()

    expect(spy.lastCall.args[0]).is.eql 0

  it 'should reduce RDY when new connection RDY max is lower', ->
    cRdy.setConnectionRdyMax 3
    cRdy.bump()
    cRdy.setConnectionRdyMax 5

    expect(cRdy.maxConnRdy).is.eql 5
    expect(spy.lastCall.args[0]).is.eql 5

  it 'should raise RDY when new connection RDY max is higher', ->
    cRdy.setConnectionRdyMax 3
    cRdy.bump()
    cRdy.setConnectionRdyMax 2

    expect(cRdy.maxConnRdy).is.eql 2
    expect(spy.lastCall.args[0]).is.eql 2

