import express from 'express'
import { Kafka } from 'kafkajs'

const app = express()
app.use(express.json())

const kafka = new Kafka({
	clientId: 'messaging-app',
	brokers: ['http://localhost:9092', 'http://localhost:9092']
})

const producer = kafka.producer()

app.post('/messages', async (req, res) => {
	try {
		const { message, senderId, receiverId, messageId, timestamp } = req.body
		const roomId = [senderId, receiverId].sort().join('-')
		const kafkaTopic = `messages-${roomId}`
		console.log(roomId)
		await producer.connect()
		await producer.send({
			topic: kafkaTopic,
			messages: JSON.stringify({ message, senderId, receiverId, messageId, timestamp })
		})
		await producer.disconnect()
		res.sendStatus(200) // Send success response
	} catch (error) {
		console.error('Error sending message to Kafka:', error)
		res.sendStatus(500) // Send internal server error
	}
})



app.listen(6000, () => {
	console.log('Kafka microservice listening on port 5000')
})
