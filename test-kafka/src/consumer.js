import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import { Kafka } from 'kafkajs';

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.get('/', (req, res) => {
  res.send('<h1>Hello world</h1>');
});

const kafka = new Kafka({
  clientId: 'messaging-app',
  brokers: ['localhost:9092', 'localhost:9093'],
});

const consumer = kafka.consumer({ groupId: 'message-consumers' });


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
        io.to(receiverId).emit('message', { senderId, message: msg });
      },
    });
  } catch (err) {
    console.error('Error connecting Kafka consumer', err);
  }
};

const disconnectConsumer = async () => {
  try {
    await consumer.disconnect();
    console.log('Kafka Consumer disconnected');
  } catch (err) {
    console.error('Error disconnecting Kafka consumer', err);
  }
};


server.listen(3001, () => {
  console.log('Listening on *:3001');
  connectConsumer();
});

process.on('SIGINT', async () => {
  await disconnectConsumer();
  process.exit();
});
