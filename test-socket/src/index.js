import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import { Kafka } from 'kafkajs';
import { createClient } from 'redis';

const app = express();
const server = http.createServer(app);
const io = new Server(server);
const client = await createClient({
  url : 'redis://localhost:6379'
})
  .on('error', err => console.log('Redis Client Error', err))
  .connect();

const serverInstanceId = 1


app.get('/', (req, res) => {
  res.send('<h1>Hello world</h1>');
});



const kafka = new Kafka({
	clientId: 'messaging-app',
	brokers: ['localhost:9092'],
  });
const producer = kafka.producer();

const connectProducer = async () => {
  try {
    await producer.connect();
    console.log('Kafka Producer connected');
  } catch (err) {
    console.error('Error connecting Kafka producer', err);
  }
};

const disconnectProducer = async () => {
  try {
    await producer.disconnect();
    console.log('Kafka Producer disconnected');
  } catch (err) {
    console.error('Error disconnecting Kafka producer', err);
  }
};

const sendMessageToKafkaQueue = async (payload) => {
  const { message, senderId, receiverId, messageId, timestamp } = payload;
  const roomId = [senderId, receiverId].sort().join('-');
  const kafkaTopic = `messages`;

  try {
    await producer.send({
      topic: kafkaTopic,
      messages: [{ value: JSON.stringify({ message, senderId, receiverId, messageId, timestamp }) }],
    });
    console.log('Message Sent successfully!');
  } catch (err) {
    console.error('Error sending message to Kafka', err);
  }
};

io.on('connection', async (socket) => {
  console.log('A user connected', socket.handshake.query);

  socket.on('message', async (mEvent) => {
    console.log('Message event', mEvent);

    const { message, senderId, receiverId, messageId } = mEvent;
    await sendMessageToKafkaQueue({
      message,
      senderId,
      receiverId,
      messageId,
      timestamp: new Date().toISOString(),
    });
    // Optionally emit the message to the receiver
    // socket.to(receiverId).emit('message', { message, senderId });
    //  console.log("socket sent!!!")
    
  });

  socket.on('messageToRoom', async (mEvent) => {
    console.log('Message event Room', mEvent);

    const { msg, receiverId, senderId } = mEvent;
    // Optionally emit the message to the receiver
    socket.to(receiverId).emit('message', mEvent);
     console.log("socket sent!!!")
    
  });

  socket.on('disconnect', () => {
    console.log('User disconnected');
  });

  const userId = socket.handshake.query.uid;
  socket.join(userId);
  const socketId = socket.id
  // not saving socketId cause roomId = userId for a specific port
  if(userId && serverInstanceId){
    await client.set(userId, JSON.stringify({  serverInstanceId }));
    console.log(userId, serverInstanceId)
  }
  console.log(socket.rooms);
});

server.listen(3000, () => {
  console.log('Listening on *:3000');
  connectProducer();
});

process.on('SIGINT', async () => {
  await disconnectProducer();
  process.exit();
});
