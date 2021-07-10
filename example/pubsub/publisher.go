package main

import (
	"encoding/json"
	"fmt"
	"time"

	pubsub "github.com/samber/go-amqp-pubsub"
	"github.com/streadway/amqp"
)

var productRk = []pubsub.RoutingKey{routingKeyProductCreated, routingKeyProductUpdated, routingKeyProductRemoved}

func RunPublisher(conn *pubsub.AMQPConnection, name string, exchange string) {
	producer, err := pubsub.NewAMQPProducer(name, conn, pubsub.AMQPProducerOptions{
		ExchangeName: exchange,
		ExchangeKind: pubsub.ExchangeKindTopic,
	})
	if err != nil {
		// We ignore error, since it will reconnect automatically when available.
		// panic(err)
	}

	publishMessages(producer)

	producer.Close()
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
