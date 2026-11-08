
package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Connection failed:", err)
	}
	defer conn.Close()

	// Create a channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Channel creation failed:", err)
	}
	defer ch.Close()

	// Declare the same queue as the producer
	q, err := ch.QueueDeclare(
		"homework",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Queue declaration failed:", err)
	}

	// Limit unacknowledged messages
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Fatal("QoS configuration failed:", err)
	}

	// Start consuming messages
	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer name
		false, // auto-ack (false = manual acknowledgment)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		log.Fatal("Consumer registration failed:", err)
	}

	log.Println("Waiting for messages...")

	// Process incoming messages
	for msg := range msgs {
		log.Printf(
			"Received message: ID=%s Body=%s",
			msg.MessageId,
			string(msg.Body),
		)

		// Acknowledge successful processing
		err := msg.Ack(false)
		if err != nil {
			log.Println("Acknowledgment failed:", err)
			continue
		}

		log.Printf("Message %s acknowledged", msg.MessageId)
	}
}