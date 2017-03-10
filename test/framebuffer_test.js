import should from 'should';

import FrameBuffer from '../src/framebuffer';
import * as wire from '../src/wire';

let createFrame = function(frameId, payload) {
  let frame = new Buffer(4 + 4 + payload.length);
  frame.writeInt32BE(payload.length + 4, 0);
  frame.writeInt32BE(frameId, 4);
  frame.write(payload, 8);
  return frame;
};

describe('FrameBuffer', function() {

  it('should parse a single, full frame', function() {
    let frameBuffer = new FrameBuffer();
    let data = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK');
    frameBuffer.consume(data);

    let [frameId, payload] = Array.from(frameBuffer.nextFrame());
    frameId.should.eql(wire.FRAME_TYPE_RESPONSE);
    return payload.toString().should.eql('OK');
  });

  it('should parse two full frames', function() {
    let frameBuffer = new FrameBuffer();

    let firstFrame = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK');
    let secondFrame = createFrame(wire.FRAME_TYPE_ERROR,
      JSON.stringify({shortname: 'localhost'}));

    frameBuffer.consume(Buffer.concat([firstFrame, secondFrame]));
    let frames = [frameBuffer.nextFrame(), frameBuffer.nextFrame()];
    frames.length.should.eql(2);

    let [frameId, data] = Array.from(frames.shift());
    frameId.should.eql(wire.FRAME_TYPE_RESPONSE);
    data.toString().should.eql('OK');

    [frameId, data] = Array.from(frames.shift());
    frameId.should.eql(wire.FRAME_TYPE_ERROR);
    return data.toString().should.eql(JSON.stringify({shortname: 'localhost'}));});

  it('should parse frame delivered in partials', function() {
    let frameBuffer = new FrameBuffer();
    let data = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK');

    // First frame is 10 bytes long. Don't expect to get anything back.
    frameBuffer.consume(data.slice(0, 3));
    should.not.exist(frameBuffer.nextFrame());

    // Yup, still haven't received the whole frame.
    frameBuffer.consume(data.slice(3, 8));
    should.not.exist(frameBuffer.nextFrame());

    // Got the whole first frame.
    frameBuffer.consume(data.slice(8));
    return should.exist(frameBuffer.nextFrame());
  });

  it('should parse multiple frames delivered in partials', function() {
    let frameBuffer = new FrameBuffer();
    let first = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK');
    let second = createFrame(wire.FRAME_TYPE_RESPONSE, '{}');
    let data = Buffer.concat([first, second]);

    // First frame is 10 bytes long. Don't expect to get anything back.
    frameBuffer.consume(data.slice(0, 3));
    should.not.exist(frameBuffer.nextFrame());

    // Yup, still haven't received the whole frame.
    frameBuffer.consume(data.slice(3, 8));
    should.not.exist(frameBuffer.nextFrame());

    // Got the whole first frame and part of the 2nd frame.
    frameBuffer.consume(data.slice(8, 12));
    should.exist(frameBuffer.nextFrame());

    // Got the 2nd frame.
    frameBuffer.consume(data.slice(12));
    return should.exist(frameBuffer.nextFrame());
  });

  return it('empty internal buffer when all frames are consumed', function() {
    let frameBuffer = new FrameBuffer();
    let data = createFrame(wire.FRAME_TYPE_RESPONSE, 'OK');

    frameBuffer.consume(data);
    while (frameBuffer.nextFrame()) { 'foo'; }

    return should.not.exist(frameBuffer.buffer);
  });
});
