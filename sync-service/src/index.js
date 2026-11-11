import amqp from 'amqplib';
import { MongoClient, ObjectId } from 'mongodb';
import { createClient } from 'redis';

const rabbitUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const queueName = process.env.SYNC_QUEUE || 'sync.events';
const houseIds = JSON.parse(process.env.DATA_HOUSES || '["house-1","house-2"]');
const mongoUser = process.env.MONGO_USER || 'root';
const mongoPassword = process.env.MONGO_PASSWORD || 'labpassword';
const clients = new Map();
const redis = createClient({ url: process.env.REDIS_URL || 'redis://redis:6379' });
redis.on('error', (error) => console.error('Redis error:', error.message));

function mongoUri(houseId) {
  const user = encodeURIComponent(mongoUser);
  const password = encodeURIComponent(mongoPassword);
  return `mongodb://${user}:${password}@mongo-${houseId}:27017/books?authSource=admin`;
}

async function connectMongo(houseId) {
  const client = new MongoClient(mongoUri(houseId), { serverSelectionTimeoutMS: 5000 });
  await client.connect();
  clients.set(houseId, client.db('books').collection('books'));
}

function compareVersion(incoming, current) {
  const incomingVersion = Number(incoming?._sync?.version || 0);
  const currentVersion = Number(current?._sync?.version || 0);
  if (incomingVersion !== currentVersion) return incomingVersion > currentVersion;
  return String(incoming?._sync?.origin || '') > String(current?._sync?.origin || '');
}

async function applyEvent(event) {
  const id = new ObjectId(event.document._id);
  const incoming = { ...event.document, _id: id };
  let winner = incoming;
  for (const [houseId, collection] of clients) {
    const current = await collection.findOne({ _id: id });
    if (current && compareVersion(current, winner)) winner = current;
  }

  for (const [houseId, collection] of clients) {
    const current = await collection.findOne({ _id: id });
    if (!current || compareVersion(winner, current)) {
      await collection.replaceOne({ _id: id }, winner, { upsert: true });
    }
    await redis.del(`${houseId}:books:${String(id)}`, `${houseId}:books:list`);
  }
}

async function start() {
  for (;;) {
    try {
      if (!redis.isOpen) await redis.connect();
      break;
    } catch (error) {
      console.error(`Redis is not ready: ${error.message}`);
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }

  for (const houseId of houseIds) {
    for (;;) {
      try { await connectMongo(houseId); break; }
      catch (error) {
        console.error(`MongoDB ${houseId} is not ready: ${error.message}`);
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }
  }

  for (;;) {
    try {
      const connection = await amqp.connect(rabbitUrl);
      const channel = await connection.createChannel();
      await channel.assertQueue(queueName, { durable: true, arguments: { 'x-max-priority': 10 } });
      channel.prefetch(1);
      await channel.consume(queueName, async (message) => {
        if (!message) return;
        try {
          await applyEvent(JSON.parse(message.content.toString()));
          channel.ack(message);
        } catch (error) {
          console.error('Could not synchronize event:', error);
          channel.nack(message, false, true);
        }
      });
      console.log(`Sync service listening on ${queueName} for ${houseIds.join(', ')}`);
      return;
    } catch (error) {
      console.error(`RabbitMQ is not ready: ${error.message}`);
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
}

start().catch((error) => { console.error(error); process.exit(1); });
