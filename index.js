'use strict';

require('dotenv').config();

const { Kafka } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');

// ✅ Read from .env
const kafka = new Kafka({
  clientId: 'protobuf-consumer',
  brokers: ["my-cluster-kafka-bootstrap.kafka:9092"],
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  },
});

const consumer = kafka.consumer({
  groupId: 'protobuf-consumer-group',
});

const registry = new SchemaRegistry({
  host: process.env.SCHEMA_REGISTRY_URL,
  auth: {
    username: process.env.SCHEMA_REGISTRY_USERNAME,
    password: process.env.SCHEMA_REGISTRY_PASSWORD,
  },
});

const TOPIC = process.env.PROTOBUF_TOPIC;

// 🚀 Run consumer
async function run() {
  try {
    await consumer.connect();
    console.log('✅ Kafka Connected');

    await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
    console.log(`📡 Subscribed to topic: ${TOPIC}`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          // 🔥 Decode Protobuf → readable
          const decoded = await registry.decode(message.value);

          console.log('-----------------------------');
          console.log(`📦 Topic: ${topic}`);
          console.log(`📍 Partition: ${partition}`);
          console.log(`🔑 Key: ${message.key?.toString()}`);
          console.log('✅ Data:', decoded);

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