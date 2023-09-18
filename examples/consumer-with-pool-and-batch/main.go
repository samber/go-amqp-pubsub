package main

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	pubsub "github.com/samber/go-amqp-pubsub"
	"github.com/samber/lo"
	"github.com/samber/mo"
	"github.com/sirupsen/logrus"
)

var rabbitmqURI = flag.String("rabbitmq-uri", "amqp://dev:dev@localhost:5672", "RabbitMQ URI")

const (
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
		LazyConnection: mo.Some(false),
	})
	if err != nil {
		// We ignore error, since it will reconnect automatically when available.
		// panic(err)
	}

	consumer := pubsub.NewConsumer(conn, "example-consumer-1", pubsub.ConsumerOptions{
		Queue: pubsub.ConsumerOptionsQueue{
			Name: "product.onEdit",
		},
		Bindings: []pubsub.ConsumerOptionsBinding{
			// crud
			{ExchangeName: "product.event", RoutingKey: "product.created"},
			{ExchangeName: "product.event", RoutingKey: "product.updated"},
		},
		Message: pubsub.ConsumerOptionsMessage{
			PrefetchCount: mo.Some(100),
		},
		EnableDeadLetter: mo.Some(true),
	})

	logrus.Info("***** Let's go! ***** ")

	consumeMessages(5, consumer)

	logrus.Info("***** Finished! ***** ")

	consumer.Close()
	conn.Close()

	logrus.Info("***** Closed! ***** ")
}

func consumeMessages(workers int, consumer *pubsub.Consumer) {
	// Feel free to kill RabbitMQ and restart it, to see what happens ;)
	//		- docker-compose kill rabbitmq
	//		- docker-compose up rabbitmq

	wg := new(sync.WaitGroup)
	wg.Add(workers)

	channel := consumer.Consume()
	channels := lo.ChannelDispatcher(channel, workers, 42, lo.DispatchingStrategyRoundRobin[*amqp.Delivery])

	for i := range channels {
		go func(index int) {
			worker(index, channels[index])

			wg.Done()
		}(i)
	}

	wg.Wait()
}

func worker(workerID int, channel <-chan *amqp.Delivery) {
	batchSize := 10
	batchTime := time.Second

	for {
		buffer, length, _, ok := lo.BufferWithTimeout(channel, batchSize, batchTime)
		if !ok {
			break
		} else if length == 0 {
			continue
		}

		lo.Try0(func() { // handle exceptions
			consumeMessage(workerID, buffer)
		})
	}
}

func consumeMessage(workerID int, messages []*amqp.Delivery) {
	text := []string{fmt.Sprintf("WORKER %d - BATCH:", workerID)}

	for i, message := range messages {
		text = append(text, fmt.Sprintf("Consumed message [ID=%d, EX=%s, RK=%s] %s", workerID, message.Exchange, message.RoutingKey, string(message.Body)))

		if (workerID+i)%10 == 0 {
			message.Reject(false)
		} else {
			message.Ack(false)
		}
	}

	logrus.Info(strings.Join(text, "\n") + "\n\n")
}
