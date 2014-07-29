_ = require 'underscore'

# From the NSQ protocol documentation:
# http://bitly.github.io/nsq/clients/tcp_protocol_spec.html
#
# The Frame format:
#
#   [x][x][x][x][x][x][x][x][x][x][x][x]...
#   |  (int32) ||  (int32) || (binary)
#   |  4-byte  ||  4-byte  || N-byte
#   ------------------------------------...
#       size      frame ID     data

class FrameBuffer

  consume: (raw) ->
    @buffer = Buffer.concat _.compact [@buffer, raw]

  nextFrame: ->
    return unless @buffer
    return unless @frameSize(0) and @frameSize(0) <= @buffer.length
    frame = @pluckFrame()

    nextOffset = @nextOffset()
    @buffer = @buffer[nextOffset..]
    delete @buffer unless @buffer.length

    frame

  # Given an offset into a buffer, get the frame ID and data tuple.
  pluckFrame: (offset = 0) ->
    frame = @buffer[offset...offset + @frameSize offset]
    frameId = frame.readInt32BE 4
    [frameId, frame[8..]]

  # Given the offset of the current frame in the buffer, find the offset
  # of the next buffer.
  nextOffset: (offset=0) ->
    size = @frameSize offset
    offset + size if size

  # Given the frame offset, return the frame size.
  frameSize: (offset) ->
    return unless @buffer and @buffer.length > 4
    4 + @buffer.readInt32BE offset if offset + 4 <= @buffer.length

module.exports = FrameBuffer
