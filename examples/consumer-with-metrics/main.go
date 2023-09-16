package main

import (
	"flag"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	pubsub "github.com/samber/go-amqp-pubsub"
	"github.com/samber/lo"
	"github.com/samber/mo"
	"github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
		Metrics: pubsub.ConsumerOptionsMetrics{
			// triggers a warning when queue size is greater than 100_000 messages
			QueueMessagesThreshold: pubsub.NewConsumerOptionsMetricsThresholdWarning(100_000),
			// triggers an error when retry rate is greater than 10% except the weekend
			RetryQueueMessageRateThreshold: pubsub.NewConsumerOptionsMetricsThresholdErrorFunc(func() float64 {
				weekday := time.Now().Weekday()
				if weekday == time.Saturday || weekday == time.Sunday {
					return 0.8
				}

				return 0.1
			}),
		},
		Message: pubsub.ConsumerOptionsMessage{
			PrefetchCount: mo.Some(1000),
		},
		EnableDeadLetter: mo.Some(true),
	})

	// register consumer metrics
	prometheus.DefaultRegisterer.MustRegister(consumer)

	// expose metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":9000", nil)
	}()

	logrus.Info("***** Let's go! ***** ")

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

	// simulate timeout
	// n := rand.Intn(20)
	// time.Sleep(time.Duration(n) * time.Second)

	if index%10 == 0 {
		msg.Reject(false)
	} else {
		msg.Ack(false)
	}
}
