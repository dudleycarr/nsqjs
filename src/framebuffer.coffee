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

    # Return parsed frames
    @parseFrames()

  parseFrames: ->
    # Find all frame offsets within the buffer.
    frameOffsets = []
    offset = 0
    while offset < @buffer.length
      frameOffsets.push offset
      offset = @nextOffset offset

    # Get all but the last frame out of the buffer.
    frames = (@pluckFrame offset for offset in frameOffsets[0...-1])

    # Get the last frame if it's not a partial frame.
    consumedOffset = lastOffset = frameOffsets.pop()
    if lastOffset + @frameSize(lastOffset) <= @buffer.length
      # Parse out the last frame since it's a whole frame
      frames.push @pluckFrame lastOffset
      # Advance the consumed pointer to the end of the last frame
      consumedOffset = @nextOffset lastOffset

    # Remove the parsed out frames from the received buffer.
    @buffer = @buffer[consumedOffset...]

    # Slicing doesn't free up the underlying memory in a Buffer object. The
    # actual underlying memory is larger than the slice due to the concat
    # earlier. Drop the reference to the Buffer object when we've consumed
    # all frames.
    delete @buffer unless @buffer.length

    frames

  # Given an offset into a buffer, get the frame ID and data tuple.
  pluckFrame: (offset) ->
    frame = @buffer[offset...offset + @frameSize offset]
    frameId = frame.readInt32BE 4
    [frameId, frame[8..]]

  # Given the offset of the current frame in the buffer, find the offset
  # of the next buffer.
  nextOffset: (offset) ->
    offset + @frameSize offset

  # Given the frame offset, return the frame size.
  frameSize: (offset) ->
    4 + (@buffer.readInt32BE offset if offset + 4 <= @buffer.length)

module.exports = FrameBuffer
