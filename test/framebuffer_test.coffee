should = require 'should'

FrameBuffer = require '../src/framebuffer.coffee'
wire = require '../src/wire'

createFrame = (frameId, payload) ->
  frame = new Buffer(4 + 4 + payload.length)
  frame.writeInt32BE(payload.length + 4, 0)
  frame.writeInt32BE(frameId, 4)
  frame.write(payload, 8)
  frame

describe 'FrameBuffer', ->

  it 'should parse a single, full frame', ->
    frameBuffer = new FrameBuffer()
    data = createFrame wire.FRAME_TYPE_RESPONSE, 'OK'
    frameBuffer.consume data

    [frameId, payload] = frameBuffer.nextFrame()
    frameId.should.eql wire.FRAME_TYPE_RESPONSE
    payload.toString().should.eql 'OK'

  it 'should parse two full frames', ->
    frameBuffer = new FrameBuffer()

    firstFrame = createFrame wire.FRAME_TYPE_RESPONSE, 'OK'
    secondFrame = createFrame wire.FRAME_TYPE_ERROR,
      JSON.stringify {shortname: 'localhost'}

    frameBuffer.consume Buffer.concat [firstFrame, secondFrame]
    frames = [frameBuffer.nextFrame(), frameBuffer.nextFrame()]
    frames.length.should.eql 2

    [frameId, data] = frames.shift()
    frameId.should.eql wire.FRAME_TYPE_RESPONSE
    data.toString().should.eql 'OK'

    [frameId, data] = frames.shift()
    frameId.should.eql wire.FRAME_TYPE_ERROR
    data.toString().should.eql JSON.stringify {shortname: 'localhost'}

  it 'should parse frame delivered in partials', ->
    frameBuffer = new FrameBuffer()
    data = createFrame wire.FRAME_TYPE_RESPONSE, 'OK'

    # First frame is 10 bytes long. Don't expect to get anything back.
    frameBuffer.consume data[0...3]
    should.not.exist frameBuffer.nextFrame()

    # Yup, still haven't received the whole frame.
    frameBuffer.consume data[3...8]
    should.not.exist frameBuffer.nextFrame()

    # Got the whole first frame.
    frameBuffer.consume data[8..]
    should.exist frameBuffer.nextFrame()

  it 'should parse multiple frames delivered in partials', ->
    frameBuffer = new FrameBuffer()
    first = createFrame wire.FRAME_TYPE_RESPONSE, 'OK'
    second = createFrame wire.FRAME_TYPE_RESPONSE, '{}'
    data = Buffer.concat [first, second]

    # First frame is 10 bytes long. Don't expect to get anything back.
    frameBuffer.consume data[0...3]
    should.not.exist frameBuffer.nextFrame()

    # Yup, still haven't received the whole frame.
    frameBuffer.consume data[3...8]
    should.not.exist frameBuffer.nextFrame()

    # Got the whole first frame and part of the 2nd frame.
    frameBuffer.consume data[8...12]
    should.exist frameBuffer.nextFrame()

    # Got the 2nd frame.
    frameBuffer.consume data[12..]
    should.exist frameBuffer.nextFrame()

  it 'empty internal buffer when all frames are consumed', ->
    frameBuffer = new FrameBuffer()
    data = createFrame wire.FRAME_TYPE_RESPONSE, 'OK'

    frameBuffer.consume data
    'foo' while frameBuffer.nextFrame()

    should.not.exist frameBuffer.buffer
