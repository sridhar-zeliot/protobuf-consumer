'use strict';

require('dotenv').config();

const { Kafka } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');

// ✅ Read from .env
const kafka = new Kafka({
  clientId: 'protobuf-consumer',
  brokers: ["my-cluster-kafka-bootstrap.kafka:9092"],
  sasl: {
    mechanism: 'scram-sha-512',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

// ✅ Consumer + Producer
const consumer = kafka.consumer({
  groupId: 'protobuf-consumer-group',
});

const producer = kafka.producer(); // ✅ NEW

// ✅ Schema Registry
const registry = new SchemaRegistry({
  host: process.env.SCHEMA_REGISTRY_URL,
  auth: {
    username: process.env.SCHEMA_REGISTRY_USERNAME,
    password: process.env.SCHEMA_REGISTRY_PASSWORD,
  },
});

// ✅ Topics from .env
const PROTOBUF_TOPIC = process.env.PROTOBUF_TOPIC;
const JSON_TOPIC = process.env.JSON_TOPIC;

// 🚀 Run consumer
async function run() {
  try {
    await consumer.connect();
    await producer.connect(); // ✅ CONNECT PRODUCER

    console.log('✅ Kafka Connected');

    await consumer.subscribe({ topic: PROTOBUF_TOPIC, fromBeginning: true });
    console.log(`📡 Subscribed to topic: ${PROTOBUF_TOPIC}`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          // ✅ Decode Protobuf → JSON
          const decoded = await registry.decode(message.value);

          console.log('===================================================\n');
          console.log(`📦 Topic: ${topic}`);
          console.log(`📍 Partition: ${partition}`);
          console.log(`🔑 Key: ${message.key?.toString()}`);
          console.log('✅ Decoded Data:', decoded);

          // ✅ Send to JSON topic
          await producer.send({
            topic: JSON_TOPIC,
            messages: [
              {
                key: message.key?.toString(),
                value: JSON.stringify(decoded),
              },
            ],
          });

          console.log(`🚀 Sent to topic: ${JSON_TOPIC}`);
          console.log('===================================================\n');
        } catch (err) {
          console.error('❌ Decode error:', err.message);
        }
      },
    });

  } catch (err) {
    console.error('❌ Consumer error:', err.message);
  }
}

run();