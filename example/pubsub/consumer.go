package main

import (
	"fmt"
	"math/rand"
	"time"

	pubsub "github.com/samber/go-amqp-pubsub"
	"github.com/streadway/amqp"
)

func RunConsumersBasic(conn *pubsub.AMQPConnection, name string) {
	bindings := []pubsub.AMQPConsumerBinding{
		{ExchangeName: "product.event", Key: "product.*"},
	}

	consumer, _ := pubsub.NewAMQPConsumer("consumer-1", conn, pubsub.AMQPConsumerOptions{
		QueueName:        "product.on-event",
		QueueBindings:    bindings,
		MessagePrefetch:  10,
		EnableDeadLetter: true,
		LazyConnection:   true,
	})

	worker := func(id int) {
		delivery := consumer.Consume()

		for msg := range delivery {
			consumeMessage(id, msg)
		}
	}

	for i := 0; i < 10; i++ {
		go worker(10 + i)
	}
}

func RunConsumersWithWorkers(conn *pubsub.AMQPConnection, name string) {
	bindings := []pubsub.AMQPConsumerBinding{
		{ExchangeName: "product.event", Key: "product.created"},
		{ExchangeName: "product.legacy-event", Key: "product.created"},
	}

	consumer, _ := pubsub.NewAMQPConsumer("consumer-1", conn, pubsub.AMQPConsumerOptions{
		QueueName:        "product.on-created",
		QueueBindings:    bindings,
		MessagePrefetch:  10,
		EnableDeadLetter: false,
		LazyConnection:   true,
	})

	consumer.AddWorkers(true, 10, consumeMessage)
}

func consumeMessage(id int, msg amqp.Delivery) {
	time.Sleep(time.Duration(rand.Intn(4)) * time.Second) // long running job - between 0 and 4 seconds

	fmt.Printf("[ID=%d, TIME=%s, EX=%s, RK=%s] %s\n", id, msg.Timestamp.String(), msg.Exchange, msg.RoutingKey, string(msg.Body))

	if id%10 == 0 {
		msg.Reject(false)
	} else {
		msg.Ack(false)
	}
}
