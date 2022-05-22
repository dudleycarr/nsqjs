const _ = require('lodash')

/**
 * From the NSQ protocol documentation:
 *   http://bitly.github.io/nsq/clients/tcp_protocol_spec.html
 *
 * The Frame format:
 *
 *   [x][x][x][x][x][x][x][x][x][x][x][x]...
 *   | (int32) ||  (int32) || (binary)
 *   |  4-byte  ||  4-byte  || N-byte
 *   ------------------------------------...
 *       size      frame ID     data
 */
class FrameBuffer {
  /**
   * Consume a raw message into the buffer.
   *
   * @param  {String} raw
   */
  consume(raw) {
    this.buffer = Buffer.concat(_.compact([this.buffer, raw]))
  }

  /**
   * Advance the buffer and return the current slice.
   *
   * @return {String}
   */
  nextFrame() {
    if (!this.buffer) return

    if (!this.frameSize(0) || !(this.frameSize(0) <= this.buffer.length)) {
      return
    }

    const frame = this.pluckFrame()
    const nextOffset = this.nextOffset()
    this.buffer = this.buffer.slice(nextOffset)

    if (!this.buffer.length) {
      delete this.buffer
    }

    return frame
  }

  /**
   * Given an offset into a buffer, get the frame ID and data tuple.
   *
   * @param  {Number} [offset=0]
   * @return {Array}
   */
  pluckFrame(offset = 0) {
    const frame = this.buffer.slice(offset, offset + this.frameSize(offset))
    const frameId = frame.readInt32BE(4)
    return [frameId, frame.slice(8)]
  }

  /**
   * Given the offset of the current frame in the buffer, find the offset
   * of the next buffer.
   *
   * @param {Number} [offset=0]
   * @return {Number}
   */
  nextOffset(offset = 0) {
    const size = this.frameSize(offset)
    if (size) {
      return offset + size
    }
  }

  /**
   * Given the frame offset, return the frame size.
   *
   * @param {Number} offset
   * @return {Number}
   */
  frameSize(offset) {
    if (!this.buffer || !(this.buffer.length > 4)) return

    if (offset + 4 <= this.buffer.length) {
      return 4 + this.buffer.readInt32BE(offset)
    }
  }
}

module.exports = FrameBuffer
