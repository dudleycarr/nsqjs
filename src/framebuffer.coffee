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
    frames = []

    # Initialize the offset, next offset, and distance from buffer end to zero.
    offset = nextOffset = distance = 0

    loop
      offset = nextOffset
      nextOffset = offset + @frameSize offset

      # Calculate the current distance from the end of the buffer.
      distance = @buffer.length - nextOffset

      # We found at least a whole frame. Push it.
      frames.push @pluckFrame offset, nextOffset if distance >= 0

      # We are are the end of the buffer. Stop looping.
      break if distance <= 0

    # If we recieved a partial frame (i.e. we didn't exactly end up exactly at
    # the end of the buffer) then slice the buffer down so it starts at the
    # beginning of the partial frame. Otherwise, intentionally lose the
    # reference to the buffer so its memory gets freed.
    @buffer = if distance then @buffer[offset...]

    frames

  # Given an offset into a buffer, get the frame ID and data tuple.
  pluckFrame: (offset, nextOffset) ->
    frame = @buffer[offset...nextOffset]
    frameId = frame.readInt32BE 4
    [frameId, frame[8..]]

  # Given the frame offset, return the frame size.
  frameSize: (offset) ->
    4 + (if offset + 4 <= @buffer.length then @buffer.readInt32BE offset else 0)

module.exports = FrameBuffer
