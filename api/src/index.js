import express from 'express';
import amqp from 'amqplib';
import { XMLBuilder, XMLParser } from 'fast-xml-parser';
import { MongoClient, ObjectId } from 'mongodb';
import { createClient } from 'redis';

const port = Number(process.env.PORT || 3000);
const houseId = process.env.HOUSE_ID || 'house-1';
const mongoHost = process.env.MONGO_HOST || 'localhost';
const mongoUser = process.env.MONGO_USER || 'root';
const mongoPassword = process.env.MONGO_PASSWORD || 'labpassword';
const mongoUri = `mongodb://${encodeURIComponent(mongoUser)}:${encodeURIComponent(mongoPassword)}@${mongoHost}:27017/books?authSource=admin`;
const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
const rabbitUrl = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const queueName = process.env.SYNC_QUEUE || 'sync.events';

const mongo = new MongoClient(mongoUri, { serverSelectionTimeoutMS: 3000 });
const redis = createClient({ url: redisUrl });
redis.on('error', (error) => console.error('Redis error:', error.message));

let books;
let channel;
let ready = false;

async function connectWithRetry(label, connect) {
  for (;;) {
    try {
      return await connect();
    } catch (error) {
      console.error(`${label} is not ready: ${error.message}`);
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }
}

function parseRequestBody(req) {
  if (req.is('application/xml') || req.is('text/xml')) {
    const parser = new XMLParser({ ignoreAttributes: false });
    const parsed = parser.parse(req.body.toString('utf8'));
    return parsed.book ?? parsed;
  }
  if (req.is('application/json')) return req.body;
  const error = new Error('Content-Type must be application/json or application/xml');
  error.status = 415;
  throw error;
}

function sendResponse(req, res, status, value) {
  const accepted = req.accepts(['json', 'xml']);
  if (!accepted) return res.status(406).json({ error: 'Accept must be application/json or application/xml' });
  if (accepted === 'xml') {
    const builder = new XMLBuilder({ format: true, ignoreAttributes: false });
    res.status(status).type('application/xml').send(builder.build({ response: value }));
    return;
  }
  res.status(status).json(value);
}

function cacheKey(suffix) {
  return `${houseId}:books:${suffix}`;
}

async function clearBookCache(id) {
  await redis.del(cacheKey(id));
  await redis.del(cacheKey('list'));
}

function asPublicBook(document) {
  if (!document) return document;
  const { _sync, _deleted, ...book } = document;
  return { ...book, id: String(book._id) };
}

function priorityValue(req) {
  return String(req.get('X-Priority') || 'normal').toLowerCase() === 'high' ? 10 : 5;
}

function publishChange(req, operation, document) {
  const event = {
    eventId: new ObjectId().toHexString(),
    operation,
    source: houseId,
    document: { ...document, _id: String(document._id) },
  };
  return new Promise((resolve, reject) => {
    channel.sendToQueue(queueName, Buffer.from(JSON.stringify(event)), {
      persistent: true,
      priority: priorityValue(req),
      contentType: 'application/json',
    }, (error) => error ? reject(error) : resolve());
  });
}

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(express.raw({ type: ['application/xml', 'text/xml'], limit: '1mb' }));

app.get('/healthz', (_req, res) => res.status(ready ? 200 : 503).json({ ready, houseId }));

app.get('/books', async (req, res, next) => {
  try {
    const cached = await redis.get(cacheKey('list'));
    if (cached) return sendResponse(req, res, 200, { books: JSON.parse(cached) });
    const documents = await books.find({ _deleted: { $ne: true } }).toArray();
    const result = documents.map(asPublicBook);
    await redis.set(cacheKey('list'), JSON.stringify(result), { EX: 30 });
    return sendResponse(req, res, 200, { books: result });
  } catch (error) { return next(error); }
});

app.get('/books/:id', async (req, res, next) => {
  try {
    if (!ObjectId.isValid(req.params.id)) return sendResponse(req, res, 400, { error: 'Invalid book id' });
    const key = cacheKey(req.params.id);
    const cached = await redis.get(key);
    if (cached) return sendResponse(req, res, 200, JSON.parse(cached));
    const document = await books.findOne({ _id: new ObjectId(req.params.id), _deleted: { $ne: true } });
    if (!document) return sendResponse(req, res, 404, { error: 'Book not found' });
    const result = asPublicBook(document);
    await redis.set(key, JSON.stringify(result), { EX: 60 });
    return sendResponse(req, res, 200, result);
  } catch (error) { return next(error); }
});

app.post('/books', async (req, res, next) => {
  try {
    const body = parseRequestBody(req);
    if (!body?.title || !body?.author) return sendResponse(req, res, 400, { error: 'title and author are required' });
    const now = Date.now();
    const document = {
      _id: new ObjectId(), title: String(body.title), author: String(body.author),
      ...(body.isbn ? { isbn: String(body.isbn) } : {}),
      createdAt: new Date(now).toISOString(), updatedAt: new Date(now).toISOString(),
      _sync: { version: now, origin: houseId }, _deleted: false,
    };
    await books.insertOne(document);
    await publishChange(req, 'upsert', document);
    await clearBookCache(String(document._id));
    return sendResponse(req, res, 201, asPublicBook(document));
  } catch (error) { return next(error); }
});

async function writeBook(req, res, next, partial) {
  try {
    if (!ObjectId.isValid(req.params.id)) return sendResponse(req, res, 400, { error: 'Invalid book id' });
    const body = parseRequestBody(req);
    const current = await books.findOne({ _id: new ObjectId(req.params.id) });
    if ((!current || current._deleted) && partial) return sendResponse(req, res, 404, { error: 'Book not found' });
    const now = Math.max(Date.now(), Number(current?._sync?.version || 0) + 1);
    const document = {
      ...(current || {}),
      ...(partial ? body : { title: body?.title, author: body?.author, ...(body?.isbn ? { isbn: body.isbn } : {}) }),
      _id: new ObjectId(req.params.id),
      createdAt: current?.createdAt || new Date(now).toISOString(),
      updatedAt: new Date(now).toISOString(),
      _sync: { version: now, origin: houseId },
      _deleted: false,
    };
    if (!document.title || !document.author) return sendResponse(req, res, 400, { error: 'title and author are required' });
    await books.replaceOne({ _id: document._id }, document, { upsert: true });
    await publishChange(req, 'upsert', document);
    await clearBookCache(req.params.id);
    return sendResponse(req, res, 200, asPublicBook(document));
  } catch (error) { return next(error); }
}

app.put('/books/:id', (req, res, next) => writeBook(req, res, next, false));
app.patch('/books/:id', (req, res, next) => writeBook(req, res, next, true));

app.delete('/books/:id', async (req, res, next) => {
  try {
    if (!ObjectId.isValid(req.params.id)) return sendResponse(req, res, 400, { error: 'Invalid book id' });
    const current = await books.findOne({ _id: new ObjectId(req.params.id) });
    if (!current || current._deleted) return sendResponse(req, res, 404, { error: 'Book not found' });
    const now = Math.max(Date.now(), Number(current._sync?.version || 0) + 1);
    const document = { ...current, updatedAt: new Date(now).toISOString(), _deleted: true, _sync: { version: now, origin: houseId } };
    await books.replaceOne({ _id: current._id }, document);
    await publishChange(req, 'upsert', document);
    await clearBookCache(req.params.id);
    return sendResponse(req, res, 200, { deleted: true, id: req.params.id });
  } catch (error) { return next(error); }
});

app.use((error, _req, res, _next) => {
  console.error(error);
  res.status(error.status || 500).json({ error: error.status ? error.message : 'Internal server error' });
});

async function start() {
  await connectWithRetry('MongoDB', async () => { await mongo.connect(); books = mongo.db('books').collection('books'); });
  await connectWithRetry('Redis', async () => { if (!redis.isOpen) await redis.connect(); });
  await connectWithRetry('RabbitMQ', async () => {
    const connection = await amqp.connect(rabbitUrl);
    channel = await connection.createConfirmChannel();
    await channel.assertQueue(queueName, { durable: true, arguments: { 'x-max-priority': 10 } });
  });
  ready = true;
  app.listen(port, '0.0.0.0', () => console.log(`API ${houseId} listening on ${port}`));
}

start().catch((error) => { console.error(error); process.exit(1); });
