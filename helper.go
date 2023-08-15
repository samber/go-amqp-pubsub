package pubsub

import (
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

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

func GetNumberOfAttempts(msg *amqp.Delivery) string {
	return strconv.FormatInt(int64(getAttemptsFromHeaders(msg)), 10)
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
