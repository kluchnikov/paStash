var base_output = require('@pastash/pastash').base_output,
  util = require('util'),
  logger = require('@pastash/pastash').logger;

const { Kafka } = require('kafkajs')
var Logger;

function OutputKafka() {
  base_output.BaseOutput.call(this);
  this.mergeConfig(this.serializer_config('json_logstash'));
  this.mergeConfig({
    name: 'Kafka',
    optional_params: ['kafkaHost', 'topic', 'partition', 'acks', 'threshold_down', 'debug', 'ssl', 'sasl_mech', 'sasl_user', 'sasl_pass', 'key_field', 'timestamp_field'],
    default_values: {
      debug: false,
      threshold_down: 10,
      topic: false,
      partition: false,
      acks: -1,
      ssl: false,
      sasl_user: false,
      sasl_mech: 'scram-sha-256',
      key_field: false,
      timestamp_field: false
    },
    start_hook: this.start,
  });
}

util.inherits(OutputKafka, base_output.BaseOutput);

OutputKafka.prototype.start = async function(callback) {

  if(!this.kafkaHost) return;

  var kconf = {
  	clientId: 'paStash',
  	brokers: [this.kafkaHost],
  	ssl: this.ssl,
  	acks: this.acks
  }

  if (this.sasl_user && this.sasl_pass){
      kconf.sasl = {
	    mechanism: this.sasl_mech,
	    username: this.sasl_user,
	    password: this.sasl_pass,
      }
  }
  var kafka = new Kafka(kconf);
  this.producer = kafka.producer()
  await this.producer.connect()

  this.on_alarm = false;
  this.error_count = 0;

  logger.info('Creating Kafka Output to', this.kafkaHost);
  callback();
};

OutputKafka.prototype.check = function() {
  if (this.on_alarm) {
    if (this.threshold_down && this.error_count < this.threshold_down) {
      logger.warning('Kafka socket end of alarm', this.kafkaHost);
      this.on_alarm = false;
      this.emit('alarm', false, this.kafkaHost);
      this.error_count = 0;
    }
    else {
      logger.info('Kafka socket still in alarm : errors : ', this.error_count );
    }
  }
};

OutputKafka.prototype.process = async function(data) {
  var msgs = [{value: JSON.stringify(data)}]
  if (this.key_field) {
    msgs[0].key = data[this.key_field]
  }
  if (this.timestamp_field) {
    msgs[0].timestamp = data[this.timestamp_field]
  }
  var d = { topic: this.topic, messages: msgs };

  if (this.debug) logger.info("Preparing to send to Kafka\n", d, "\n\n" );
  try {
    await this.producer.send(d)
  } catch (e) {
    logger.error(e);
  }

};

OutputKafka.prototype.close = async function(callback) {
  logger.info('Closing Kafka Output to', this.kafkaHost);
  await producer.disconnect()
  callback();
};

exports.create = function() {
  return new OutputKafka();
};
