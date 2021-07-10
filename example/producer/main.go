package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"time"

	pubsub "github.com/samber/go-amqp-pubsub"
	"github.com/streadway/amqp"
)

var rabbitmqURI = flag.String("rabbitmq-uri", "amqp://dev:dev@localhost:5672", "RabbitMQ URI")

const (
	routingKeyProductCreated pubsub.RoutingKey = "product.created"
	routingKeyProductUpdated pubsub.RoutingKey = "product.updated"
	routingKeyProductRemoved pubsub.RoutingKey = "product.removed"
)

var productRk = []pubsub.RoutingKey{routingKeyProductCreated, routingKeyProductUpdated, routingKeyProductRemoved}

func main() {
	flag.Parse()

	if rabbitmqURI == nil {
		panic(fmt.Errorf("missing --rabbitmiq-uri parameter"))
	}

	conn, err := pubsub.NewAMQPConnection("example-connection-1", pubsub.AMQPConnectionOptions{
		URI: *rabbitmqURI,
	})
	if err != nil {
		// We ignore error, since it will reconnect automatically when available.
		// panic(err)
	}

	producer, err := pubsub.NewAMQPProducer("example-producer-1", conn, pubsub.AMQPProducerOptions{
		ExchangeName: "product.event",
		ExchangeKind: pubsub.ExchangeKindTopic,
	})
	if err != nil {
		// We ignore error, since it will reconnect automatically when available.
		// panic(err)
	}

	fmt.Println("***** Let's go! ***** ")

	publishMessages(producer)

	fmt.Println("***** Finished! ***** ")

	producer.Close()
	conn.Close()

	fmt.Println("***** Closed! ***** ")
}

func publishMessages(producer *pubsub.AMQPProducer) {
	// Feel free to kill RabbitMQ and restart it, to see what happens ;)
	//		- docker-compose kill rabbitmq
	//		- docker-compose up rabbitmq

	for i := 0; i < 1000; i++ {
		time.Sleep(100 * time.Millisecond)

		routingKey := productRk[i%len(productRk)]

		body, _ := json.Marshal(map[string]interface{}{
			"id":   i,
			"name": "tomatoes",
		})

		err := producer.Publish(routingKey, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		})
		if err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Printf("Published message [RK=%s] %s\n", routingKey, string(body))
		}
	}
}
