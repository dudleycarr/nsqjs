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

# Given the frame offset, return the frame size.
frameSize = (buffer, offset) ->
  4 + dataSize(buffer, offset)

dataSize = (buffer, offset) ->
  buffer.readInt32BE(offset) if offset + 4 <= buffer.length

# Given the offset of the current frame in the buffer, find the offset
# of the next buffer.
nextFrameOffset = (buffer, offset=0) ->
  nextFrame = offset + frameSize(buffer, offset)
  if nextFrame? and nextFrame < buffer.length then nextFrame else null

# Given a buffer that contains a whole frame, return the tuple of frame ID and
# data.
unpackFrame = (frame) ->
  frameId = frame.readInt32BE 4
  [frameId, frame[8..]]

# Given an offset into a buffer, get the frame ID and data tuple.
pluckFrame = (buffer, offset) ->
  unpackFrame buffer[offset...offset + frameSize(buffer, offset)]

class FrameBuffer

  # Consume the raw data (Buffers) received from an NSQD connection. It returns
  # a list of frames.
  consume: (raw) ->
    buffers = if @buffer? then [@buffer, raw] else [raw]
    @buffer = Buffer.concat buffers

    # Return parsed frames
    @parseFrames()

  parseFrames: ->
    # Find all frame offsets within the buffer.
    [frameOffsets, offset] = [[], 0]
    while not _.isNull offset
      frameOffsets.push offset
      offset = nextFrameOffset @buffer, offset

    # Get all but the last frame out of the buffer.
    frames = for offset in frameOffsets[0...-1]
      pluckFrame @buffer, offset

    # Get the last frame if it's not a partial frame.
    consumedOffset = lastOffset = frameOffsets.pop()
    if lastOffset + frameSize(@buffer, lastOffset) <= @_buffer.length
      # Parse out the last frame since it's a whole frame
      frames.push pluckFrame(@buffer, lastOffset)
      # Advance the consumed pointer to the end of the last frame
      consumedOffset = lastOffset + frameSize(@buffer, lastOffset)

    # Remove the parsed out frames from the received buffer.
    @buffer = @buffer[consumedOffset...]
    if @buffer.length is 0
      # Slicing doesn't free up the underlying memory in a Buffer object. The
      # actual underlying memory is larger than the slice due to the concat
      # earlier. Drop the reference to the Buffer object when we've consumed
      # all frames.
      @buffer = null

    frames

module.exports = FrameBuffer
