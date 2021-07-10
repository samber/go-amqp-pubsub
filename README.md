
# Pub/Sub framework for RabbitMQ and Go

- Based on github.com/streadway/amqp driver
- Resilient to network failure
- Auto reconnect: recreate channels, bindings, producers, consumers...

## How to

### Connect

```go
conn, err := pubsub.NewAMQPConnection("connection-1", pubsub.AMQPConnectionOptions{
    URI: *rabbitmqURI,
})

// ...

conn.Close()
```

### Producer

```go
conn, err := pubsub.NewAMQPConnection("connection-1", pubsub.AMQPConnectionOptions{
    URI: *rabbitmqURI,
})

producer, err := pubsub.NewAMQPProducer("producer-1", conn, pubsub.AMQPProducerOptions{
    ExchangeName:  "product.event",
    ExchangeKind: pubsub.ExchangeKindTopic,
})

err := producer.Publish("product.added", amqp.Publishing{
    ContentType:  "application/json",
    DeliveryMode: amqp.Persistent,
    Body:         []byte(`{"hello": "world"}`),
})

producer.Close()
conn.Close()
```

### Consumer

```go
conn, err := pubsub.NewAMQPConnection("connection-1", pubsub.AMQPConnectionOptions{
    URI: *rabbitmqURI,
})

bindings := []pubsub.AMQPConsumerBinding{
    {ExchangeName: "product.event", Key: "product.added"},
}

consumer, _ := pubsub.NewAMQPConsumer("consumer-1", conn, pubsub.AMQPConsumerOptions{
    QueueName:        "product.onAdded",
    QueueBindings:    bindings,
    MessagePrefetch:  10,
    EnableDeadLetter: true,
    LazyConnection:   true,
})

// Run 10 workers using the `delivery` channel
for i := 0; i < 10; i++ {
    go func(id int) {
        delivery := consumer.Consume()

        for msg := range delivery {
            // ...
    		msg.Ack(false)
        }
    }()
}

// Run 10 workers using a callback
consumer.AddWorkers(true, 10, func (id int, msg amqp.Delivery) {
    // ...
    msg.Ack(false)
})

consumer.Close()
conn.Close()
```

## Run example

```sh
docker-compose up -d rabbitmq
make dev
```

Then trigger network failure, by restarting rabbitmq:

```sh
docker-compose restart rabbitmq

# or

docker-compose kill rabbitmq
docker-compose up -d rabbitmq
```

## Todo

- Connection pooling (eg: 10 connections, 100 channels per connections)
- Support for advanced features from github.com/streadway/amqp
- Better documentation
- Testing + CI
- See `@TODO` in code
