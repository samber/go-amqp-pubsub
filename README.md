
# Pub/Sub framework for RabbitMQ and Go

- Based on github.com/rabbitmq/amqp091-go driver
- Resilient to network failure
- Auto reconnect: recreate channels, bindings, producers, consumers...
- Dead letter queue on message rejection

## How to

During your tests, feel free to restart Rabbitmq. This library will reconnect automatically.

### Connect

```go
import pubsub "github.com/samber/go-amqp-pubsub"

conn, err := pubsub.NewConnection("connection-1", pubsub.ConnectionOptions{
    URI: "amqp://dev:dev@localhost:5672",
    Config: amqp.Config{
        Dial:      amqp.DefaultDial(time.Second),
    },
})

// ...

conn.Close()
```

### Producer

```go
import (
    pubsub "github.com/samber/go-amqp-pubsub"
    "github.com/samber/lo"
    "github.com/samber/mo"
)

conn, err := pubsub.NewConnection("connection-1", pubsub.ConnectionOptions{
    URI: "amqp://dev:dev@localhost:5672",
    LazyConnection: mo.Some(true),
})

producer := pubsub.NewProducer(conn, "producer-1", pubsub.ProducerOptions{
    Exchange: pubsub.ProducerOptionsExchange{
        Name: "product.event",
        Kind: pubsub.ExchangeKindTopic,
    },
})

err := producer.Publish(routingKey, false, false, amqp.Publishing{
    ContentType:  "application/json",
    DeliveryMode: amqp.Persistent,
    Body:         []byte(`{"hello": "world"}`),
})

producer.Close()
conn.Close()
```

### Consumer

```go
import (
    pubsub "github.com/samber/go-amqp-pubsub"
    "github.com/samber/lo"
    "github.com/samber/mo"
)

conn, err := pubsub.NewConnection("connection-1", pubsub.ConnectionOptions{
    URI: "amqp://dev:dev@localhost:5672",
    LazyConnection: mo.Some(true),
})

consumer := pubsub.NewConsumer(conn, "consumer-1", pubsub.ConsumerOptions{
    Queue: pubsub.ConsumerOptionsQueue{
        Name: "product.onEdit",
    },
    Bindings: []pubsub.ConsumerOptionsBinding{
        {ExchangeName: "product.event", RoutingKey: "product.created"},
        {ExchangeName: "product.event", RoutingKey: "product.updated"},
    },
    Message: pubsub.ConsumerOptionsMessage{
        PrefetchCount: mo.Some(100),
    },
    EnableDeadLetter: mo.Some(true),     // will create a "product.onEdit.deadLetter" DL queue
})

for msg := range consumer.Consume() {
    lo.Try0(func() { // handle exceptions
        // ...
        msg.Ack(false)
    })
}

consumer.Close()
conn.Close()
```

### Consumer with pooling and batching

See [examples/consumer-with-pool-and-batch.md](examples/consumer-with-pool-and-batch.md).

## Run examples

```sh
# run rabbitmq
docker-compose up rabbitmq

# run producer
go run examples/producer/main.go --rabbitmq-uri amqp://dev:dev@localhost:5672

# run consumer
go run examples/consumer/main.go --rabbitmq-uri amqp://dev:dev@localhost:5672
```

Then trigger network failure, by restarting rabbitmq:

```sh
docker-compose restart rabbitmq
```

## Todo

- Connection pooling (eg: 10 connections, 100 channels per connections)
- Better documentation
- Testing + CI
