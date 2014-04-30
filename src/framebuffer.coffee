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

  # Consume the raw data (Buffers) received from an NSQD connection. It returns
  # a list of frames.
  consume: (raw) ->
    @buffer = Buffer.concat _.compact [@buffer, raw]

  nextFrame: ->
    nextOffset = @nextOffset()
    return null unless nextOffset and nextOffset <= @buffer.length

    frame = @pluckFrame()
    @buffer = @buffer[nextOffset..]
    delete @buffer unless @buffer.length

    frame

  # Given an offset into a buffer, get the frame ID and data tuple.
  pluckFrame: (offset=0) ->
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
    return unless @buffer
    4 + @buffer.readInt32BE offset if offset + 4 <= @buffer.length

module.exports = FrameBuffer
