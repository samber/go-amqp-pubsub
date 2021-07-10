package main

import (
	"flag"
	"fmt"
	"time"

	pubsub "github.com/samber/go-amqp-pubsub"
)

var rabbitmqURI = flag.String("rabbitmq-uri", "amqp://dev:dev@localhost:5672", "RabbitMQ URI")

func main() {
	flag.Parse()

	if rabbitmqURI == nil {
		panic(fmt.Errorf("missing --rabbitmiq-uri parameter"))
	}

	// You should better create a separate connection for consumers and producers
	conn, err := pubsub.NewAMQPConnection("connection-1", pubsub.AMQPConnectionOptions{
		URI: *rabbitmqURI,
	})
	if err != nil {
		// We ignore error, since it will reconnect automatically when available.
		// panic(err)
	}

	go RunPublisher(conn, "producer-1", "product.event")
	go RunPublisher(conn, "producer-2", "product.legacy-event")

	RunConsumersBasic(conn, "consumer-1")
	RunConsumersWithWorkers(conn, "consumer-2")

	time.Sleep(10 * time.Minute)

	conn.Close()

	fmt.Println("***** Closed! ***** ")
}
