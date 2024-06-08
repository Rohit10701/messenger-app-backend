import express from 'express';
import http from 'http';
import { Kafka } from 'kafkajs';
import { io } from "socket.io-client";
import { createClient } from 'redis';

const app = express();
const server = http.createServer(app);
const socket = io("ws://localhost:3000"); // Changed protocol to http
const connectRedis = async () => {
  try {
    const client = await createClient({
      url : 'redis://localhost:6379'
    });
    client.on('error', err => console.log('Redis Client Error', err));
    await client.connect();
    console.log('Redis Client connected');
    return client;
  } catch (err) {
    console.error('Error connecting Redis client', err);
    throw err; // Rethrow the error to handle it where this function is called
  }
};



app.get('/', (req, res) => {
  res.send('<h1>Hello world</h1>');
});

const kafka = new Kafka({
  clientId: 'messaging-app',
  brokers: ['localhost:9092', 'localhost:9093'],
});

const consumer = kafka.consumer({ groupId: 'message-consumers' });
const fetchValue = async () => {
  // Start the Redis connection
const redisClient = await connectRedis().catch(err => {
  console.error('Error starting Redis connection', err);
  process.exit(1); // Exit with error status if Redis connection fails
});

  const value = await redisClient.get('19BEE0036');
  console.log("19BEE0036 this is the value : ", value)
}
fetchValue()
const connectConsumer = async () => {
  try {
    await consumer.connect();
    console.log('Kafka Consumer connected');
    await consumer.subscribe({ topic: "messages", fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const messageContent = JSON.parse(message.value.toString());
        console.log(`Received message from topic ${topic}:`, messageContent);
        const { senderId, receiverId, message: msg } = messageContent;

        // need to check if the reciever is online or not ? could be done by looking into redis
        //if not online just save it into db else emit the message
        socket.emit('messageToRoom', { msg, receiverId, senderId });
      },
    });
  } catch (err) {
    console.error('Error connecting Kafka consumer', err);
    throw err; // Rethrow the error to handle it where this function is called
  }
};

const disconnectConsumer = async () => {
  try {
    await consumer.disconnect();
    console.log('Kafka Consumer disconnected');
  } catch (err) {
    console.error('Error disconnecting Kafka consumer', err);
    throw err; // Rethrow the error to handle it where this function is called
  }
};

server.listen(3001, async () => {
  console.log('Listening on *:3001');
  await connectConsumer(); // Wait for consumer to connect before starting to consume messages
});

process.on('SIGINT', async () => {
  try {
    await disconnectConsumer();
    process.exit();
  } catch (err) {
    console.error('Error handling SIGINT', err);
    process.exit(1); // Exit with error status
  }
});

