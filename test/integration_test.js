import _ from 'underscore';
import async from 'async';
import child_process from 'child_process';
import request from 'request';

import should from 'should';
let temp = require('temp').track();

import nsq from '../src/nsq';

let TCP_PORT = 14150;
let HTTP_PORT = 14151;

let startNSQD = function(dataPath, additionalOptions, callback) {
  if (!additionalOptions) { additionalOptions = {}; }
  let options = {
    'http-address': `127.0.0.1:${HTTP_PORT}`,
    'tcp-address': `127.0.0.1:${TCP_PORT}`,
    'data-path': dataPath,
    'tls-cert': './test/cert.pem',
    'tls-key': './test/key.pem'
  };

  _.extend(options, additionalOptions);
  options = _.flatten((() => {
    let result = [];
    for (let key in options) {
      let value = options[key];
      result.push([`-${key}`, value]);
    }
    return result;
  })());
  let process = child_process.spawn('nsqd', options.concat(additionalOptions),
    {stdio: ['ignore', 'ignore', 'ignore']});

  // Give nsqd a chance to startup successfully.
  return setTimeout(() => callback(null, process), 500);
};

let topicOp = function(op, topic, callback) {
  let options = {
    method: 'POST',
    uri: `http://127.0.0.1:${HTTP_PORT}/${op}`,
    qs: {
      topic
    }
  };

  return request(options, (err, res, body) => callback(err));
};

let createTopic = _.partial(topicOp, 'create_topic');
let deleteTopic = _.partial(topicOp, 'delete_topic');

// Publish a single message via HTTP
let publish = function(topic, message, done) {
  let options = {
    uri: `http://127.0.0.1:${HTTP_PORT}/pub`,
    method: 'POST',
    qs: {
      topic
    },
    body: message
  };

  return request(options, (err, res, body) => typeof done === 'function' ? done(err) : undefined);
};

describe('integration', () => {
  let nsqdProcess = null;
  let reader = null;

  before(done =>
    temp.mkdir('/nsq', (err, dirPath) =>
      startNSQD(dirPath, {}, function(err, process) {
        nsqdProcess = process;
        return done(err);
      })
    )
  );

  after((done) => {
    nsqdProcess.kill();
    // Give nsqd a chance to exit before it's data directory will be cleaned up.
    return setTimeout(done, 500);
  });

  beforeEach(done => createTopic('test', done));

  afterEach((done) => {
    reader.close();
    return deleteTopic('test', done);
  });

  describe('stream compression and encryption', () => {
    let compression, description;
    let optionPermutations = [
      {deflate: true},
      {snappy: true},
      {tls: true, tlsVerification: false},
      {tls: true, tlsVerification: false, snappy: true},
      {tls: true, tlsVerification: false, deflate: true}
    ];

    return optionPermutations.map((options) =>
      // Figure out what compression is enabled
      (compression = (['deflate', 'snappy'].filter((key) => key in options).map((key) => key)),
      compression.push('none'),

      description =
        `reader with compression (${compression[0]}) and tls (${(options.tls != null)})`,

      describe(description, () => {
        it('should send and receive a message', (done) => {

          let topic = 'test';
          let channel = 'default';
          let message = "a message for our reader";

          publish(topic, message);

          reader = new nsq.Reader(topic, channel,
            _.extend({nsqdTCPAddresses: [`127.0.0.1:${TCP_PORT}`]}, options));

          reader.on('message', (msg) => {
            msg.body.toString().should.eql(message);
            msg.finish();
            return done();
          });

          reader.on('error', () => {});

          return reader.connect();
        });

        it('should send and receive a large message', (done) => {
          let topic = 'test';
          let channel = 'default';
          let message = (__range__(0, 100000, true).map((i) => 'a')).join('');

          publish(topic, message);

          reader = new nsq.Reader(topic, channel,
            _.extend({nsqdTCPAddresses: [`127.0.0.1:${TCP_PORT}`]}, options));

          reader.on('message', (msg) => {
            msg.body.toString().should.eql(message);
            msg.finish();
            return done();
          });

          reader.on('error', () => {});

          return reader.connect();
        });
      })));
  });

  return describe('end to end', () => {
    let topic = 'test';
    let channel = 'default';
    let tcpAddress = `127.0.0.1:${TCP_PORT}`;
    let writer = null;
    reader = null;

    beforeEach((done) => {
      writer = new nsq.Writer('127.0.0.1', TCP_PORT);
      writer.on('ready', () => {
        reader = new nsq.Reader(topic, channel, {nsqdTCPAddresses: tcpAddress});
        reader.connect();
        return done();
      });

      writer.on('error', () => {});

      return writer.connect();
    });

    afterEach(() => writer.close());

    it('should send and receive a string', (done) => {
      let message = 'hello world';
      writer.publish(topic, message, (err) => {
        if (err) { return done(err); }
      });

      reader.on('error', () => {});

      return reader.on('message', (msg) => {
        msg.body.toString().should.eql(message);
        msg.finish();
        return done();
      });
    });

    it('should send and receive a Buffer', (done) => {
      let message = new Buffer([0x11, 0x22, 0x33]);
      writer.publish(topic, message);

      reader.on('error', () => {});

      return reader.on('message', (readMsg) => {
        for (let i = 0; i < readMsg.body.length; i++) { let readByte = readMsg.body[i]; readByte.should.equal(message[i]); }
        readMsg.finish();
        return done();
      });
    });

    // TODO (Dudley): The behavior of nsqd seems to have changed around this.
    //    This requires more investigation but not concerning at the moment.
    it.skip('should not receive messages when immediately paused', (done) => {
      let waitedLongEnough = false;

      let timeout = setTimeout(() => {
        reader.unpause();
        return waitedLongEnough = true;
      }
      , 100);

      // Note: because NSQDConnection.connect() does most of it's work in
      // process.nextTick(), we're really pausing before the reader is
      // connected.
      //
      reader.pause();
      reader.on('message', (msg) => {
        msg.finish();
        clearTimeout(timeout);
        waitedLongEnough.should.be.true();
        return done();
      });

      return writer.publish(topic, 'pause test');
    });

    it('should not receive any new messages when paused', (done) => {
      writer.publish(topic, {messageShouldArrive: true});

      return reader.on('message', function(msg) {
        // check the message
        msg.json().messageShouldArrive.should.be.true();
        msg.finish();

        if (reader.isPaused()) { return done(); }

        reader.pause();

        return process.nextTick(() => {
          // send it again, shouldn't get this one
          writer.publish(topic, {messageShouldArrive: false});
          return setTimeout(done, 100);
        });
      });
    });


    it('should not receive any requeued messages when paused', (done) => {
      writer.publish(topic, 'requeue me');
      let id = '';

      return reader.on('message', (msg) => {
        // this will fail if the msg comes through again
        id.should.equal('');
        ({ id } = msg);

        if (reader.isPaused()) { return done(); }
        reader.pause();

        return process.nextTick(() => {
          // send it again, shouldn't get this one
          msg.requeue(0, false);
          return setTimeout(done, 100);
        });
      });
    });

    it('should start receiving messages again after unpause', (done) => {
      let shouldReceive = true;
      writer.publish(topic, {sentWhilePaused: false});

      return reader.on('message', (msg) => {
        shouldReceive.should.be.true();

        reader.pause();
        msg.requeue();

        if (msg.json().sentWhilePaused) { return done(); }

        shouldReceive = false;
        writer.publish(topic, {sentWhilePaused: true});
        setTimeout(function() {
          shouldReceive = true;
          return reader.unpause();
        }
        , 100);

        return done();
      });
    });

    return it('should successfully publish a message before fully connected', (done) => {
      writer = new nsq.Writer('127.0.0.1', TCP_PORT);
      writer.connect();

      // The writer is connecting, but it shouldn't be ready to publish.
      writer.ready.should.eql(false);

      writer.on('error', () => {})

      // Publish the message. It should succeed since the writer will queue up
      // the message while connecting.
      return writer.publish('a_topic', 'a message', (err) => {
        should.not.exist(err);
        return done();
      });
    });
  });
});

describe('failures', () => {
  let nsqdProcess = null;
  let reader = null;

  before(done => {
    return temp.mkdir('/nsq', (err, dirPath) => {
      return startNSQD(dirPath, {}, (err, process) => {
        nsqdProcess = process;
        return done(err);
      });
    });
  });

  // after((done) => {
  //   nsqdProcess.kill();
  //   // Give nsqd a chance to exit before it's data directory will be cleaned up.
  //   return setTimeout(done, 500);
  // });

  return describe('Writer', () =>
    describe('nsqd disconnect before publish', () =>
      it('should fail to publish a message', (done) => {
        let writer = new nsq.Writer('127.0.0.1', TCP_PORT);
        return async.series([
          // Connect the writer to the nsqd.
          callback => {
            writer.connect();
            writer.on('ready', callback);
            writer.on('error', () => {}); // Ensure error message is handled.
          },

          // Stop the nsqd process.
          callback => {
            nsqdProcess.kill();
            setTimeout(callback, 200);
          },

          // Attempt to publish a message.
          callback => {
            writer.publish('test_topic', 'a message that should fail', err => {
              should.exist(err);
              return callback();
            });
          }

        ], done);
      })
    )
  );
});

function __range__(left, right, inclusive) {
  let range = [];
  let ascending = left < right;
  let end = !inclusive ? right : ascending ? right + 1 : right - 1;
  for (let i = left; ascending ? i < end : i > end; ascending ? i++ : i--) {
    range.push(i);
  }
  return range;
}
