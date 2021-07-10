package main

import pubsub "github.com/samber/go-amqp-pubsub"

const (
	routingKeyProductCreated pubsub.RoutingKey = "product.created"
	routingKeyProductUpdated pubsub.RoutingKey = "product.updated"
	routingKeyProductRemoved pubsub.RoutingKey = "product.removed"
)
