package pubsub

import "log"

type Scope string

const (
	ScopeConnection Scope = "connection"
	ScopeChannel    Scope = "channel"
	ScopeExchange   Scope = "exchange"
	ScopeQueue      Scope = "queue"
	ScopeConsumer   Scope = "consumer"
	ScopeProducer   Scope = "producer"
)

var logger func(scope Scope, name string, msg string, attributes map[string]any) = DefaultLogger

func SetLogger(cb func(scope Scope, name string, msg string, attributes map[string]any)) {
	logger = cb
}

func DefaultLogger(scope Scope, name string, msg string, attributes map[string]any) {
	log.Printf("AMQP %s '%s': %s", scope, name, msg)

	// if attributes == nil {
	// 	attributes = map[string]any{}
	// }

	// attrs := lo.MapToSlice(attributes, func(key string, value any) any {
	// 	return slog.Any(key, value)
	// })

	// msg = fmt.Sprintf("AMQP %s '%s': %s", scope, name, msg)
	// slog.Error(msg, attrs...)
}
