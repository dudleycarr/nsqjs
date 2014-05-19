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

  # Consume the raw data (Buffers) received from an NSQD connection. Returns
  # a list of frames.
  consume: (raw) ->
    buffer = if @buffer? then Buffer.concat [@buffer, raw] else raw
    @parseFrames buffer

  parseFrames: (buffer) ->
    # We'll be returning this at the end.
    frames = []

    # Initialize the offset to the beginning of the buffer.
    start = 0

    # Initialize the distance from the end offset to the end of the buffer
    # to the buffer length and save off the buffer length.
    distance = length = buffer.length

    # Chunk through the buffer frame-by-frame. Push frames as we find
    # them. Stop once we've gotten to or past the end of the buffer.
    while distance > 0
      frameSize = (if start + 4 <= length then buffer.readInt32BE start else 0) + 4
      end = start + frameSize
      distance -= frameSize

      # We found a whole frame. Save off its [id, data] tuple.
      if distance >= 0
        frame = buffer[start...end]
        frameId = frame.readInt32BE 4
        frames.push [frameId, frame[8..]]

        # If distance is a positive number, move the frame start.
        start = end if distance

    # If we recieved a partial frame (i.e. we didn't exactly end up exactly at
    # the end of the buffer) then slice the buffer down so it starts at the
    # beginning of the partial frame. Otherwise, intentionally lose the
    # reference to the buffer so its memory gets freed.
    @buffer = if distance then buffer[start...]

    frames

module.exports = FrameBuffer
