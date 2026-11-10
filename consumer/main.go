
package main

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var nodes = []string{
	"amqp://admin:admin@localhost:5673/", // Node 2: primary
	"amqp://admin:admin@localhost:5674/", // Node 3: fallback
}

func consumeFrom(url string) error {
	log.Printf("Connecting to %s", url)

	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("connection failed: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel creation failed: %w", err)
	}
	defer ch.Close()

	// Declare the Quorum Queue
	q, err := ch.QueueDeclare(
		"homework",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-queue-type": "quorum",
		},
	)
	if err != nil {
		return fmt.Errorf("queue declaration failed: %w", err)
	}

	// Process one unacknowledged message at a time
	err = ch.Qos(1, 0, false)
	if err != nil {
		return fmt.Errorf("QoS configuration failed: %w", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		false, // manual acknowledgment
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("consumer registration failed: %w", err)
	}

	log.Printf("Successfully connected. Waiting for messages...")

	for msg := range msgs {
		log.Printf(
			"Received: ID=%s Body=%s",
			msg.MessageId,
			string(msg.Body),
		)

		// Acknowledge only after processing
		err := msg.Ack(false)
		if err != nil {
			return fmt.Errorf("acknowledgment failed: %w", err)
		}

		log.Printf("Message %s acknowledged", msg.MessageId)
	}

	return fmt.Errorf("message channel closed")
}


func main() {
	for {
		for _, node := range nodes {
			err := consumeFrom(node)

			if err != nil {
				log.Printf("Connection to node failed/lost: %v", err)
			}

			// If Node 2 fails, immediately try Node 3.
			// If Node 3 disconnects, start again with Node 2.
		}

		log.Println("All nodes tried. Retrying in 3 seconds...")
		time.Sleep(3 * time.Second)
	}
}
