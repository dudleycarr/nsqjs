const should = require('should')

const wire = require('../lib/wire')
const FrameBuffer = require('../lib/framebuffer')

const createFrame = (frameId, payload) => {
  const frame = Buffer.alloc(4 + 4 + payload.length)
  frame.writeInt32BE(payload.length + 4, 0)
  frame.writeInt32BE(frameId, 4)
  frame.write(payload, 8)
  return frame
}

describe('FrameBuffer', () => {
  it('should parse a single, full frame', () => {
    const frameBuffer = new FrameBuffer()
    const data = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK')
    frameBuffer.consume(data)

    const [frameId, payload] = Array.from(frameBuffer.nextFrame())
    frameId.should.eql(wire.FRAME_TYPE_RESPONSE)
    payload.toString().should.eql('OK')
  })

  it('should parse two full frames', () => {
    const frameBuffer = new FrameBuffer()

    const firstFrame = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK')
    const secondFrame = createFrame(
      wire.FRAME_TYPE_ERROR,
      JSON.stringify({shortname: 'localhost'})
    )

    frameBuffer.consume(Buffer.concat([firstFrame, secondFrame]))
    const frames = [frameBuffer.nextFrame(), frameBuffer.nextFrame()]
    frames.length.should.eql(2)

    let [frameId, data] = Array.from(frames.shift())
    frameId.should.eql(wire.FRAME_TYPE_RESPONSE)
    data.toString().should.eql('OK')
    ;[frameId, data] = Array.from(frames.shift())
    frameId.should.eql(wire.FRAME_TYPE_ERROR)
    data.toString().should.eql(JSON.stringify({shortname: 'localhost'}))
  })

  it('should parse frame delivered in partials', () => {
    const frameBuffer = new FrameBuffer()
    const data = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK')

    // First frame is 10 bytes long. Don't expect to get anything back.
    frameBuffer.consume(data.slice(0, 3))
    should.not.exist(frameBuffer.nextFrame())

    // Yup, still haven't received the whole frame.
    frameBuffer.consume(data.slice(3, 8))
    should.not.exist(frameBuffer.nextFrame())

    // Got the whole first frame.
    frameBuffer.consume(data.slice(8))
    should.exist(frameBuffer.nextFrame())
  })

  it('should parse multiple frames delivered in partials', () => {
    const frameBuffer = new FrameBuffer()
    const first = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK')
    const second = createFrame(wire.FRAME_TYPE_RESPONSE, '{}')
    const data = Buffer.concat([first, second])

    // First frame is 10 bytes long. Don't expect to get anything back.
    frameBuffer.consume(data.slice(0, 3))
    should.not.exist(frameBuffer.nextFrame())

    // Yup, still haven't received the whole frame.
    frameBuffer.consume(data.slice(3, 8))
    should.not.exist(frameBuffer.nextFrame())

    // Got the whole first frame and part of the 2nd frame.
    frameBuffer.consume(data.slice(8, 12))
    should.exist(frameBuffer.nextFrame())

    // Got the 2nd frame.
    frameBuffer.consume(data.slice(12))
    should.exist(frameBuffer.nextFrame())
  })

  return it('empty internal buffer when all frames are consumed', () => {
    const frameBuffer = new FrameBuffer()
    const data = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK')

    frameBuffer.consume(data)
    while (frameBuffer.nextFrame());

    should.not.exist(frameBuffer.buffer)
  })
})
