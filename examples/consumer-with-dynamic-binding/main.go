package main

import (
	"flag"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	pubsub "github.com/samber/go-amqp-pubsub"
	"github.com/samber/lo"
	"github.com/samber/mo"
	"github.com/sirupsen/logrus"
)

var rabbitmqURI = flag.String("rabbitmq-uri", "amqp://dev:dev@localhost:5672", "RabbitMQ URI")

const (
	queueName string = "product.onEdit"

	routingKeyProductCreated string = "product.created"
	routingKeyProductUpdated string = "product.updated"
	routingKeyProductRemoved string = "product.removed"
)

var productRk = []string{routingKeyProductCreated, routingKeyProductUpdated, routingKeyProductRemoved}

func main() {
	flag.Parse()

	if rabbitmqURI == nil {
		logrus.Fatal("missing --rabbitmiq-uri parameter")
	}

	pubsub.SetLogger(logrus.Errorf)

	conn, err := pubsub.NewConnection("example-connection-1", pubsub.ConnectionOptions{
		URI: *rabbitmqURI,
		Config: amqp.Config{
			Dial:      amqp.DefaultDial(time.Second),
			Heartbeat: time.Second,
		},
		LazyConnection: mo.Some(false),
	})
	if err != nil {
		// We ignore error, since it will reconnect automatically when available.
		// panic(err)
	}

	consumer := pubsub.NewConsumer(conn, "example-consumer-1", pubsub.ConsumerOptions{
		Queue: pubsub.ConsumerOptionsQueue{
			Name: queueName,
		},
		Bindings: []pubsub.ConsumerOptionsBinding{
			// crud
			{ExchangeName: "product.event", RoutingKey: "product.created"},
			{ExchangeName: "product.event", RoutingKey: "product.updated"},
		},
		Message: pubsub.ConsumerOptionsMessage{
			PrefetchCount: mo.Some(1000),
		},
		EnableDeadLetter: mo.Some(true),
	})

	logrus.Info("***** Let's go! ***** ")

	go fuzzyConcurrentQueueBinding(consumer)
	consumeMessages(consumer)

	logrus.Info("***** Finished! ***** ")

	consumer.Close()
	conn.Close()

	logrus.Info("***** Closed! ***** ")
}

func consumeMessages(consumer *pubsub.Consumer) {
	// Feel free to kill RabbitMQ and restart it, to see what happens ;)
	//		- docker-compose kill rabbitmq
	//		- docker-compose up rabbitmq

	channel := consumer.Consume()

	i := 0
	for msg := range channel {
		lo.Try0(func() { // handle exceptions
			consumeMessage(i, msg)
		})

		i++
	}
}

func consumeMessage(index int, msg *amqp.Delivery) {
	logrus.Infof("Consumed message [ID=%d, EX=%s, RK=%s] %s", index, msg.Exchange, msg.RoutingKey, string(msg.Body))

	if index%10 == 0 {
		msg.Reject(false)
	} else {
		msg.Ack(false)
	}
}

func fuzzyConcurrentQueueBinding(consumer *pubsub.Consumer) {
	for {
		go func() {
			exchangeName := "product.event"
			routingKey := "fuzzy." + lo.RandomString(5, lo.AlphanumericCharset)

			consumer.AddBinding(exchangeName, routingKey, mo.None[amqp.Table]())
			time.Sleep(10 * time.Millisecond)
			consumer.RemoveBinding(exchangeName, routingKey, mo.None[amqp.Table]())

			fmt.Printf("fuzzy: binding/unbinding exchange '%s' to queue '%s' using routing key '%s'\n", exchangeName, queueName, routingKey)
		}()

		time.Sleep(10 * time.Millisecond)
	}
}
