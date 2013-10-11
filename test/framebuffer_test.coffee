chai      = require 'chai'
expect    = chai.expect
should    = chai.should()
sinon     = require 'sinon'
sinonChai = require 'sinon-chai'

chai.use sinonChai 

FrameBuffer = require '../lib/framebuffer.coffee'
message = require '../lib/message'

createFrame = (frameId, payload) ->
  frame = new Buffer(4 + 4 + payload.length)
  frame.writeInt32BE(payload.length, 0)
  frame.writeInt32BE(frameId, 4)
  frame.write(payload, 8)
  frame

describe 'FrameBuffer', ->

  it 'should parse a single, full frame', ->
    frameBuffer = new FrameBuffer()
    data = createFrame message.FRAME_TYPE_RESPONSE, 'OK'
    frames = frameBuffer.consume data

    [frameId, payload] = frames.pop()
    frameId.should.eq message.FRAME_TYPE_RESPONSE
    payload.toString().should.eq 'OK'

  it 'should parse two full frames', ->
    frameBuffer = new FrameBuffer()

    firstFrame = createFrame message.FRAME_TYPE_RESPONSE, 'OK'
    secondFrame = createFrame message.FRAME_TYPE_ERROR,
      JSON.stringify {shortname: 'localhost'}

    frames = frameBuffer.consume Buffer.concat [firstFrame, secondFrame]
    frames.length.should.eq 2

    [frameId, data] = frames.shift()
    frameId.should.eq message.FRAME_TYPE_RESPONSE
    data.toString().should.eq 'OK'

    [frameId, data] = frames.shift()
    frameId.should.eq message.FRAME_TYPE_ERROR
    data.toString().should.eq JSON.stringify {shortname: 'localhost'}

  it 'should parse frame delivered in partials', ->
    frameBuffer = new FrameBuffer()
    data = createFrame message.FRAME_TYPE_RESPONSE, 'OK'

    # First frame is 10 bytes long. Don't expect to get anything back.
    frames = frameBuffer.consume data[0...3]
    frames.length.should.eq 0

    # Yup, still haven't received the whole frame.
    frames = frameBuffer.consume data[3...8]
    frames.length.should.eq 0

    # Got the whole first frame.
    frames = frameBuffer.consume data[8..]
    frames.length.should.eq 1

  it 'should parse multiple frames delivered in partials', ->
    frameBuffer = new FrameBuffer()
    first = createFrame message.FRAME_TYPE_RESPONSE, 'OK'
    second = createFrame message.FRAME_TYPE_RESPONSE, '{}'
    data = Buffer.concat [first, second]

    # First frame is 10 bytes long. Don't expect to get anything back.
    frames = frameBuffer.consume data[0...3]
    frames.length.should.eq 0

    # Yup, still haven't received the whole frame.
    frames = frameBuffer.consume data[3...8]
    frames.length.should.eq 0

    # Got the whole first frame and part of the 2nd frame.
    frames = frameBuffer.consume data[8...12]
    frames.length.should.eq 1

    # Got the 2nd frame.
    frames = frameBuffer.consume data[12..]
    frames.length.should.eq 1

  it 'empty internal buffer when all frames are consumed', ->
    frameBuffer = new FrameBuffer()
    data = createFrame message.FRAME_TYPE_RESPONSE, 'OK'
    
    frame = frameBuffer.consume data
    expect(frameBuffer._buffer).to.be.null
