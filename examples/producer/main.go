package main

import (
	"encoding/json"
	"flag"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	pubsub "github.com/samber/go-amqp-pubsub"
	"github.com/samber/mo"
	"github.com/sirupsen/logrus"
)

var rabbitmqURI = flag.String("rabbitmq-uri", "amqp://dev:dev@localhost:5672", "RabbitMQ URI")

const (
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
		LazyConnection: mo.Some(true),
	})
	if err != nil {
		// We ignore error, since it will reconnect automatically when available.
		// panic(err)
	}

	producer := pubsub.NewProducer(conn, "example-producer-1", pubsub.ProducerOptions{
		Exchange: pubsub.ProducerOptionsExchange{
			Name: "product.event",
			Kind: pubsub.ExchangeKindTopic,
		},
	})

	logrus.Info("***** Let's go! ***** ")

	publishMessages(producer)

	logrus.Info("***** Finished! ***** ")

	producer.Close()
	conn.Close()

	logrus.Info("***** Closed! ***** ")
}

func publishMessages(producer *pubsub.Producer) {
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

		err := producer.Publish(routingKey, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		})
		if err != nil {
			logrus.Error(err)
		} else {
			logrus.Infof("Published message [RK=%s] %s", routingKey, string(body))
		}
	}
}
