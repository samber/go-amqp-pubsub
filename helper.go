package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

func GetMessageHeader[T any](msg *amqp.Delivery, key string) (value T, ok bool) {
	if msg == nil {
		return
	}

	if msg.Headers == nil {
		return
	}

	anyValue, ok := msg.Headers[key]
	if !ok {
		return
	}

	value, ok = anyValue.(T)
	return value, ok
}
