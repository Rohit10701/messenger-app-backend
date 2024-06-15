import dotenv from "dotenv";
import express from 'express';
import http from 'http';
import mongoose from 'mongoose';
import { Kafka } from 'kafkajs';
import { createClient } from 'redis';
import { io } from 'socket.io-client';
import { v4 as uuidv4 } from 'uuid';
import cron from 'node-cron';

dotenv.config();

const app = express();
const server = http.createServer(app);
const socket = io('ws://localhost:3000');

mongoose.connect(`${process.env.MONGO_DB_URL}`, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});
console.log(process.env.MONGO_DB_URL)
const messageSchema = new mongoose.Schema({
  messageId: String,
  senderId: String,
  receiverId: String,
  message: String,
  timestamp: { type: Date, default: Date.now },
});

const Message = mongoose.model('Message', messageSchema);

const redisClient = createClient({ url: 'redis://localhost:6379' });
redisClient.on('error', (err) => console.log('Redis Client Error', err));
redisClient.connect().then(() => console.log('Redis Client connected'));

const kafka = new Kafka({
  clientId: 'messaging-app',
  brokers: [ 'localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'message-consumers' });

const saveMessageToDB = async (messages) => {
  try {
    await Message.insertMany(messages);
  } catch (error) {
    console.error('Error saving message to MongoDB:', error);
  }
};

const processMessage = async ({ topic, partition, message }) => {
  const messageContent = JSON.parse(message.value.toString());
  const messageId = uuidv4(); // Generate unique ID for the message
  console.log(`Received message from topic ${topic}:`, messageContent);

  const { senderId, receiverId, message: msg } = messageContent;

  const completeMessage = {
    messageId,
    senderId,
    receiverId,
    message: msg,
    timestamp: new Date(),
  };

  socket.emit('messageToRoom', completeMessage);

  await redisClient.set(
    `message:${messageId}`,
    JSON.stringify(completeMessage),
    'EX',
    960 // Expire in 15 minutes
  );
};

const connectConsumer = async () => {
  try {
    await consumer.connect();
    console.log("kafka consumer connected successfully!")
  } catch (error) {
    console.log("Error while creating the kafka quwuw",error)
  }
  console.log('Kafka Consumer connected');
  await consumer.subscribe({ topic: 'messages', fromBeginning: true });

  await consumer.run({ eachMessage: processMessage });
};

const flushMessagesToDB = async () => {
  try {
    const keys = await redisClient.keys('message:*');
    if (keys.length === 0) return;

    const messages = [];
    for (const key of keys) {
      const message = await redisClient.get(key);
      messages.push(JSON.parse(message));
      await redisClient.del(key);
    }

    await saveMessageToDB(messages);
    console.log('Flushed messages to DB:', messages);
  } catch (error) {
    console.error('Error flushing messages to DB:', error);
  }
};

cron.schedule('*/3 * * * *', flushMessagesToDB);

app.use(express.json());

app.put('/message/:id', async (req, res) => {
  const { id } = req.params;
  const { message } = req.body;
  try {
    const messageKey = `message:${id}`;
    const existingMessage = await redisClient.get(messageKey);
    if (!existingMessage) {
      return res.status(404).json({ error: 'Message not found or already saved' });
    }

    const updatedMessage = { ...JSON.parse(existingMessage), message };
    await redisClient.set(messageKey, JSON.stringify(updatedMessage), 'EX', 900);

    res.json(updatedMessage);
  } catch (error) {
    res.status(500).json({ error: 'Error updating message' });
  }
});

app.delete('/message/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const messageKey = `message:${id}`;
    const existingMessage = await redisClient.get(messageKey);
    if (!existingMessage) {
      return res.status(404).json({ error: 'Message not found or already saved' });
    }

    await redisClient.del(messageKey);

    res.json({ message: 'Message deleted' });
  } catch (error) {
    res.status(500).json({ error: 'Error deleting message' });
  }
});

server.listen(3001, async () => {
  console.log('Listening on *:3001');
  await connectConsumer();
});

process.on('SIGINT', async () => {
  await consumer.disconnect();
  process.exit();
});
