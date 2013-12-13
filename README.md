nsqjs
=====
A NodeJS client for the [nsq](http://bitly.github.io/nsq/) client protocol. This implementation attempts to be
fully compliant and maintain feature parity with the official Go ([go-nsq](https://github.com/bitly/go-nsq)) and Python ([pynsq](https://github.com/bitly/pynsq)) clients.

Usage
-----

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
* ```maxRetries: 5``` <br/>
  The number of times to a message can be requeued before it will be handed to the DISCARD handler and then automatically finished.
* ```requeueDelay: 90``` <br/>
  The default amount of time (seconds) a message requeued should be delayed by before being dispatched by nsqd.
* ```nsqdTCPAddresses``` <br/>
  A string or an array of string representing the host/port pair for nsqd instances.
  <br/> For example: `['localhost:4151']`
* ```lookupdHTTPAddresses``` <br/>
  A string or an array of strings representing the host/port pair of nsqlookupd instaces.
  <br/> For example: `['localhost:4161']`
* ```lookupdPollInterval: 60``` <br/>
  The frequency in seconds for querying lookupd instances.
* ```lookupdPollJitter: 0.3``` <br/>
  The jitter applied to the start of querying lookupd instances periodically.

Reader events are:

* `Reader.MESSAGE`
* `Reader.DISCARD`

Both events produce a Message object.

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
  The delay is in seconds. This is how long nsqd will hold on the message
  before attempting it again. The backoff parameter indicates that we should
  treat this as an error within this process and we need to backoff to recover.
* `touch()` <br/>
  Tell nsqd that you want extra time to process the message. It extends the
  soft timeout by the normal timeout amount.


### Simple example

Start [nsqd](http://bitly.github.io/nsq/components/nsqd.html) and
[nsqdlookupd](http://bitly.github.io/nsq/components/nsqlookupd.html)
```bash
# nsqdLookupd Listens on 4161 for HTTP requests
$ nsqlookupd &
$ nsqd &
```

Sample CoffeeScript client:
```coffee-script
nsq = require 'nsqjs'

topic = 'sample'
channel = 'test_channel'
options =
  lookupdHTTPAddresses: '127.0.0.1:4161'

reader = new nsq.Reader topic, channel, options
reader.connect()

reader.on nsq.Reader.MESSAGE, (msg) ->
  console.log "Received message [#{msg.id}]: #{msg.body.toString()}"
  msg.finish()
```

Publish a message to nsqd to be consumed by the sample client:
```bash
$ curl -d "it really tied the room together" http://localhost:4151/pub?topic=sample
```

### Example with message timeouts

This script simulates a message that takes a long time to process or at least
longer than the default message timeout. To ensure that the message doesn't
timeout while being processed, touch events are sent to keep it alive.

```coffee-script
{Reader} = require 'nsqjs'

topic = 'sample'
channel = 'test_channel'
options =
  lookupdHTTPAddresses: '127.0.0.1:4161'

reader = new Reader topic, channel, options
reader.connect()

reader.on Reader.MESSAGE, (msg) ->
  console.log "Received message [#{msg.id}]"

  touch = ->
    if not msg.hasResponded
      console.log "Touch [#{msg.id}]"
      msg.touch()
      # Touch the message again a second before the next timeout.
      setTimeout touch, msg.timeUntilTimeout() - 1000

  finish = ->
    console.log "Finished message [#{msg.id}]: #{msg.body.toString()}"
    msg.finish()

  console.log "Message timeout is #{msg.timeUntilTimeout() / 1000} secs."
  setTimeout touch, msg.timeUntilTimeout() - 1000

  # Finish the message after 2 timeout periods and 1 second.
  setTimeout finish, msg.timeUntilTimeout() * 2 + 1000
```

Changes
-------
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
