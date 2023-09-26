package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
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

func main() {
	flag.Parse()

	if rabbitmqURI == nil {
		logrus.Fatal("missing --rabbitmiq-uri parameter")
	}

	conn, err := pubsub.NewConnection("example-connection-1", pubsub.ConnectionOptions{
		URI: *rabbitmqURI,
		Config: amqp.Config{
			Dial:      amqp.DefaultDial(time.Second),
			Heartbeat: time.Second,
		},
		LazyConnection: mo.Some(true),
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
			PrefetchCount: mo.Some(0),
		},
		EnableDeadLetter: mo.Some(true),
	})

	// handle ctrl+c
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		logrus.Info("***** Handled signal ***** ")
		consumer.Close()
		conn.Close()
	}()

	logrus.Info("***** Let's go! ***** ")

	consumeMessages(consumer)

	logrus.Info("***** Finished! ***** ")

	// close again, just in case a segfault would be hidden in the lib
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

	// simulate timeout
	// time.Sleep(60 * time.Second)

	if index%10 == 0 {
		msg.Reject(false)
	} else {
		msg.Ack(false)
	}
}
