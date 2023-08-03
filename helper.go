package pubsub

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SetMessageHeader(msg *amqp.Delivery, key string, value interface{}) {
	if msg == nil {
		return
	}

	if msg.Headers == nil {
		msg.Headers = make(amqp.Table)
	}

	msg.Headers[key] = value
}

func GetMessageHeader[T any](msg *amqp.Delivery, key string) (value T, ok bool) {
	if msg == nil {
		return
	}

	if msg.Headers == nil {
		return
	}

	value, ok = msg.Headers[key].(T)

	return value, ok
}

func GetNumberOfAttempts(msg *amqp.Delivery) (int, bool) {
	return GetMessageHeader[int](msg, "x-retry-attempts")
}

func RejectWithBackOff(msg *amqp.Delivery, ttl time.Duration, requeue bool) error {
	acknowledger, ok := msg.Acknowledger.(*retryAcknowledger)
	if !ok || ttl < 0 {
		return msg.Reject(requeue)
	}

	attempts := getAttemptsFromHeaders(msg)
	_, retryOk := acknowledger.retryer.NextBackOff(msg, attempts)

	if retryOk {
		return acknowledger.retry(msg.DeliveryTag, attempts, ttl)
	}

	return acknowledger.parent.Reject(msg.DeliveryTag, requeue)
}
