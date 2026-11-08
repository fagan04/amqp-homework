
package main

import (
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Connect to RabbitMQ
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal("Connection failed:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Channel creation failed:", err)
	}
	defer ch.Close()

	// Declare a durable queue
	q, err := ch.QueueDeclare(
		"homework",
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		log.Fatal("Queue declaration failed:", err)
	}

	// Enable publisher confirmations
	err = ch.Confirm(false)
	if err != nil {
		log.Fatal("Publisher confirm failed:", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	// Publish messages
	for i := 1; i <= 10; i++ {
		body := fmt.Sprintf("Message number %d", i)

		err = ch.Publish(
			"",     // default exchange
			q.Name, // routing key
			false,
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				MessageId:    fmt.Sprintf("%d", i),
				Timestamp:    time.Now(),
				ContentType:  "text/plain",
				Body:         []byte(body),
			},
		)

		if err != nil {
			log.Fatal("Publishing failed:", err)
		}

		confirmation := <-confirms
		if !confirmation.Ack {
			log.Fatal("Message was not acknowledged by broker")
		}

		log.Printf("Published and confirmed: %s", body)
	}
}
