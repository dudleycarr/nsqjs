# nsqjs

The official NodeJS client for the [nsq](http://nsq.io/) client protocol. This implementation attempts to be
fully compliant and maintain feature parity with the official Go ([go-nsq](https://github.com/nsqio/go-nsq)) and Python ([pynsq](https://github.com/nsqio/pynsq)) clients.


## Usage

### new Reader(topic, channel, options)

The topic and channel arguments are strings and must be specified. The options
argument is optional. Below are the parameters that can be specified in the
options object.

* ```maxInFlight: 1``` <br/>
  The maximum number of messages to process at once. This value is shared between nsqd connections. It's highly recommended that this value is greater than the number of nsqd connections.
* ```heartbeatInterval: 30``` <br/>
  The frequency in seconds at which the nsqd will send heartbeats to this Reader.
* ```maxBackoffDuration: 128``` <br/>
  The maximum amount of time (seconds) the Reader will backoff for any single backoff
  event.
* ```maxAttempts: 0``` <br/>
  The number of times a given message will be attempted (given to MESSAGE handler) before it will be handed to the DISCARD handler and then automatically finished. 0 means that there is **no limit.** If no DISCARD handler is specified and `maxAttempts > 0`, then the message will be finished automatically when the number of attempts has been exhausted.
* ```requeueDelay: 90,000 (90secs)``` <br/>
  The default amount of time (milliseconds) a message requeued should be delayed by before being dispatched by nsqd.
* ```nsqdTCPAddresses``` <br/>
  A string or an array of strings representing the host/port pair for nsqd instances.
  <br/> For example: `['localhost:4150']`
* ```lookupdHTTPAddresses``` <br/>
  A string or an array of strings representing the host/port pair of nsqlookupd instaces or the full HTTP/HTTPS URIs of the nsqlookupd instances.
  <br/> For example: `['localhost:4161']`, `['http://localhost/lookup']`, `['http://localhost/path/lookup?extra_param=true']`
* ```lookupdPollInterval: 60``` <br/>
  The frequency in seconds for querying lookupd instances.
* ```lookupdPollJitter: 0.3``` <br/>
  The jitter applied to the start of querying lookupd instances periodically.
* ```lowRdyTimeout: 50``` <br/>
  The timeout in milliseconds for switching between connections when the Reader
  maxInFlight is less than the number of connected NSQDs. 
* ```tls: false``` <br/>
  Use TLS if nsqd has TLS support enabled.
* ```tlsVerification: true``` <br/>
  Require verification of the TLS cert. This needs to be false if you're using
  a self signed cert.
* ```deflate: false``` <br/>
  Use zlib Deflate compression.
* ```deflateLevel: 6``` <br/>
  Use zlib Deflate compression level.
* ```snappy: false``` <br/>
  Use Snappy compression.
* ```authSecret: null```<br/>
  Authenticate using the provided auth secret.
* ```outputBufferSize: null```<br/>
  The size in bytes of the buffer nsqd will use when writing to this client. -1
  disables buffering. ```outputBufferSize >= 64```
* ```outputBufferTimeout: null```<br/>
  The timeout after which any data that nsqd has buffered will be flushed to this client. Value is in milliseconds. ```outputBufferTimeout >= 1```. A value of ```-1``` disables timeouts.
* ```messageTimeout: null```<br/>
  Sets the server-side message timeout in milliseconds for messages delivered to this client.
* ```sampleRate: null```<br/>
  Deliver a percentage of all messages received to this connection. ```1 <=
  sampleRate <= 99```
* ```clientId: null```<br/>
  An identifier used to disambiguate this client.
* ```idleTimeout: 0``` <br/>
  Socket timeout after idling for the duration in second (default to 0 means disabled).

Reader events are:

* `Reader.READY` or `ready`
* `Reader.NOT_READY` or `not_ready`
* `Reader.MESSAGE` or `message`
* `Reader.DISCARD` or `discard`
* `Reader.ERROR` or `error`
* `Reader.NSQD_CONNECTED` or `nsqd_connected`
* `Reader.NSQD_CLOSED` or `nsqd_closed`

`Reader.MESSAGE` and `Reader.DISCARD` both produce `Message` objects.
`Reader.NSQD_CONNECTED` and `Reader.NSQD_CLOSED` events both provide the host
and port of the nsqd to which the event pertains.

These methods are available on a Reader object:
* `connect()` <br/>
  Connect to the nsqds specified or connect to nsqds discovered via
  lookupd.
* `close()` <br/>
  Disconnect from all nsqds. Does not wait for in-flight messages to complete.
* `pause()` <br/>
  Pause the Reader by stopping message flow. Does not affect in-flight
  messages.
* `unpause()` <br/>
  Unpauses the Reader by resuming normal message flow.
* `isPaused()` <br/>
  `true` if paused, `false` otherwise.

### Message

The following properties and methods are available on Message objects produced by a Reader
instance.

* `timestamp` <br/>
  Numeric timestamp for the Message provided by nsqd.
* `attempts` <br/>
  The number of attempts that have been made to process this message.
* `id` <br/>
  The opaque string id for the Message provided by nsqd.
* `hasResponded` <br/>
  Boolean for whether or not a response has been sent.
* `body` <br/>
  The message payload as a Buffer object.
* `json()` <br/>
  Parses message payload as JSON and caches the result.
* `timeUntilTimeout(hard=false)`: <br/>
  Returns the amount of time until the message times out. If the hard argument
  is provided, then it calculates the time until the hard timeout when nsqd
  will requeue inspite of touch events.
* `finish()` <br/>
  Finish the message as successful.
* `requeue(delay=null, backoff=true)`
  The delay is in milliseconds. This is how long nsqd will hold on the message
  before attempting it again. The backoff parameter indicates that we should
  treat this as an error within this process and we need to backoff to recover.
* `touch()` <br/>
  Tell nsqd that you want extra time to process the message. It extends the
  soft timeout by the normal timeout amount.

### new Writer(nsqdHost, nsqdPort, options)

Allows messages to be sent to an nsqd.

Available Writer options:

* ```tls: false``` <br/>
  Use TLS if nsqd has TLS support enabled.
* ```tlsVerification: true``` <br/>
  Require verification of the TLS cert. This needs to be false if you're using
  a self signed cert.
* ```deflate: false``` <br/>
  Use zlib Deflate compression.
* ```deflateLevel: 6``` <br/>
  Use zlib Deflate compression level.
* ```snappy: false``` <br/>
  Use Snappy compression.
* ```clientId: null```<br/>
  An identifier used to disambiguate this client.

Writer events are:

* `Writer.READY` or `ready`
* `Writer.CLOSED` or `closed`
* `Writer.ERROR` or `error`

These methods are available on a Writer object:

* `connect()` <br/>
  Connect to the nsqd specified.
* `close()` <br/>
  Disconnect from nsqd.
* `publish(topic, msgs, [callback])` <br/>
  `topic` is a string. `msgs` is either a string, a `Buffer`, JSON serializable
  object, a list of strings / `Buffers` / JSON serializable objects. `callback` takes a single `error` argument.
* `deferPublish(topic, msg, timeMs, [callback])` <br/>
  `topic` is a string. `msg` is either a string, a `Buffer`, JSON serializable object. `timeMs` is the delay by which the message should be delivered. `callback` takes a single `error` argument.

### Simple example

Start [nsqd](http://nsq.io/components/nsqd.html) and
[nsqdlookupd](http://nsq.io/components/nsqlookupd.html)

```bash
# nsqdLookupd Listens on 4161 for HTTP requests and 4160 for TCP requests
$ nsqlookupd &
$ nsqd -lookupd-tcp-address=127.0.0.1:4160 -broadcast-address=127.0.0.1 &
```

```js
const nsq = require('nsqjs')

const reader = new nsq.Reader('sample_topic', 'test_channel', {
  lookupdHTTPAddresses: '127.0.0.1:4161'
})

reader.connect()

reader.on('message', msg => {
  console.log('Received message [%s]: %s', msg.id, msg.body.toString())
  msg.finish()
})
```

Publish a message to nsqd to be consumed by the sample client:

```bash
$ curl -d "it really tied the room together" http://localhost:4151/pub?topic=sample_topic
```

### Example with message timeouts

This script simulates a message that takes a long time to process or at least
longer than the default message timeout. To ensure that the message doesn't
timeout while being processed, touch events are sent to keep it alive.

```js
const nsq = require('nsqjs')

const reader = new nsq.Reader('sample_topic', 'test_channel', {
  lookupdHTTPAddresses: '127.0.0.1:4161'
})

reader.connect()

reader.on('message', msg => {
  console.log('Received message [%s]', msg.id)

  const touch = () => {
    if (!msg.hasResponded) {
      console.log('Touch [%s]', msg.id)
      msg.touch()

      // Touch the message again a second before the next timeout.
      setTimeout(touch, msg.timeUntilTimeout() - 1000)
    }
  }

  const finish = () => {
    console.log('Finished message [%s]: %s', msg.id, msg.body.toString())
    msg.finish()
  }

  console.log('Message timeout is %f secs.', msg.timeUntilTimeout() / 1000)
  setTimeout(touch, msg.timeUntilTimeout() - 1000)

  // Finish the message after 2 timeout periods and 1 second.
  setTimeout(finish, msg.timeUntilTimeout() * 2 + 1000)
})
```

### Enable nsqjs debugging

nsqjs uses [debug](https://github.com/visionmedia/debug) to log debug output.

To see all nsqjs events:
```
$ DEBUG=nsqjs:* node my_nsqjs_script.js
```

To see all reader events:
```
$ DEBUG=nsqjs:reader:* node my_nsqjs_script.js
```

To see a specific reader's events:
```
$ DEBUG=nsqjs:reader:<topic>/<channel>:* node my_nsqjs_script.js
```
> Replace `<topic>` and `<channel>`

To see all writer events:
```
$ DEBUG=nsqjs:writer:* node my_nsqjs_script.js
```

### A Writer Example

The writer sends a single message and then a list of messages.

```js
const nsq = require('nsqjs')

const w = new nsq.Writer('127.0.0.1', 4150)

w.connect()

w.on('ready', () => {
  w.publish('sample_topic', 'it really tied the room together')
  w.deferPublish('sample_topic', ['This message gonna arrive 1 sec later.'], 1000)
  w.publish('sample_topic', [
    'Uh, excuse me. Mark it zero. Next frame.', 
    'Smokey, this is not \'Nam. This is bowling. There are rules.'
  ])
  w.publish('sample_topic', 'Wu?',  err => {
    if (err) { return console.error(err.message) }
    console.log('Message sent successfully')
    w.close()
  })
})

w.on('closed', () => {
  console.log('Writer closed')
})
```

## Changes

* **0.13.0**
  * Fix: SnappyStream initialization race condition. (#353)
  * Fix: IPv6 address support (#352)
  * Fix: Support JavaScript String objects as messages (#341)
  * Deprecated: Node versions < 12 (#346)
  * Change: Debug is now a dev dependency (#349)
  * Change: Use SnappyStream 2.0 (#354)
  * Change: Replaced BigNumber.js with built-in BigInt (#337)
  * Change: Replaced request.js with node-fetch (#347)
  * Change: Removed dependency on async.js (#347)
* **0.12.0**
  * Expose `lowRdyTimeout` parameter for Readers.
  * Change the default value for `lowRdyTimeout` from 1.5s to 50ms.
* **0.11.0**
  * Support NodeJS 10
  * Fix Snappy issues with NSQD 1.0 and 1.1
  * Fix `close` behavior for Readers
  * Added `"ready"` and `"not_ready"` events for Reader.
  * Fix short timeout for connection IDENTIFY requests. (Thanks @emaincourt)
  * Lazy deserialization of messages
  * Cached Backoff Timer calculations
* **0.10.1**
  * Fix debug.js memory leak when destroying NSQDConnection objects.
* **0.10.0**
  * Fix off by one error for Message maxAttempts. (Thanks @tomc974)
  * Fix requeueDelay default to be 90secs instead of 90msec. Updated docs.
	(Thanks @tomc974)
* **0.9.2**
  * Fix `Reader.close` to cleanup all intervals to allow node to exit cleanly.
  * Upraded Sinon
  * Removed .eslintrc
* **0.9.1**
  * Fixed Reader close exceptions. (Thanks @ekristen)
  * Bump Sinon version
  * Bump bignumber.js version
* **0.9**
  * **Breaking change:** Node versions 6 and greater supported from now on.
  * Support for deferred message publishing! (thanks @spruce)
  * Added idleTimeout for Reader and Writer (thanks @abbshr)
  * Snappy support is now optional. (thanks @bcoe)
  * Snappy support is now fixed and working with 1.0.0-compat!
  * Fixed backoff behavior if mulitiple messages fail at the same time. Should recover much faster.
  * Use TCP NO_DELAY for nsqd connections.
  * Chores:
    * Replaced underscore with lodash
    * Move `src` to `lib`
    * Dropped Babel support
    * Use Standard JS style
    * Less flakey tests
* **0.8.4**
  * Move to ES6 using Babel.
* **0.7.12**
  * Bug: Fix issue introduced by not sending RDY count to main max-in-flight.
    Readers connected to mulitple nsqds do not set RDY count for connections
    made after the first nsqd connection.
* **0.7.11**
  * Improvement: Keep track of touch count in Message instances.
  * Improvement: Stop sending RDY count to main max-in-flight for newer
    versions of nsqd.
  * Improvement: Make the connect debug message more accurate in Reader.
    Previously lookupd poll results suggested new connections were being made
    when they were not.
  * Bug: Non-fatal nsqd errors would cause RDY count to decrease and never
    return to normal. This will happen for example when finishing messages
    that have exceeded their amount of time to process a message.
  * 
* **0.7.10**
  * Properly handles non-string errors
* **0.7.9**
  * Treat non-fatal errors appropriately
* **0.7.7**
  * Build with Node v4
* **0.7.6**
  * Fix npm install by adding .npmignore.
* **0.7.3**
  * Slightly better invalid topic and channel error messages.
  * Handle more conditions for failing to publish a message.
* **0.7.2**
  * Fix build for iojs and node v0.12
  * Bumped snappystream version.
* **0.7.1**
  * Fix connection returning to max connection RDY after backoff
  * Fix backoff ignored when `message.finish` is called after backoff event.
* **0.7.0**
  * Fixes for configuration breakages
  * Fix for AUTH
  * Fix for pause / unpause
  * Automatically finish messages when maxAttempts have been exceeded.
  * `maxAttempts` is now by default 0. [ Breaking Change! ]
  * discarded messages will now be sent to the `MESSAGE` listener if there's no
    `DISCARD` listener.
  * Support for emphemeral topics.
  * Support for 64 char topic / channel names.
  * Support for Lookupd URLs
  * Deprecate StateChangeLogger infavor of `debug` [ Breaking Change! ]
* **0.6.0**
  * Added support for authentication
  * Added support for sample rate
  * Added support for specifying outputBufferSize and outputBufferTimeout
  * Added support for specifying msg_timeout
  * Refactored configuration checks
  * Breaking change for NSQDConnection constructor. NSQDConnection takes an
    options argument instead of each option as a parameter.
* **0.5.1**
  * Fix for not failing when the nsqd isn't available on start.
* **0.5.0**
  * Reworked FrameBuffer
  * Added TLS support for Reader and Writer
  * Added Deflate support
  * Added Snappy support
* **0.4.1**
  * Fixed a logging issue on NSQConnection disconnected
* **0.4.0**
  * Added `close`, `pause`, and `unpause` to Reader
  * Added callback for Writer publish
  * Expose error events on Reader/Writer
  * Expose nsqd connect / disconnect events
  * Fix crash when Message `finish`, `requeue`, etc after nsqd disconnect
  * Fix lookupd only queried on startup.
* **0.3.1**
  * Fixed sending an array of Buffers
  * Fixed sending a message with multi-byte characters
* **0.3.0**
  * Added Writer implementation
* **0.2.1**
  * Added prepublish compilation to JavaScript.
* **0.2.0**
  * `ReaderRdy`, `ConnectionRdy` implementation
  * `Reader` implementation
  * Initial documentation
  * `NSQDConnection`
    * Moved defaults to `Reader`
    * Support protocol / state logging
    * `connect()` now happens on next tick so that it can be called before event
      handlers are registered.
  * `Message`
    * Correctly support `TOUCH` events
    * Support soft and hard timeout timings
    * JSON parsing of message body
* **0.1.0**
  * `NSQDConnection` implementation
  * `wire` implementation
  * `Message` implementation
