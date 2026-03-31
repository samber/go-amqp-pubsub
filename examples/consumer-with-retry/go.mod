module consumer

go 1.19

require (
	github.com/rabbitmq/amqp091-go v1.10.0
	github.com/samber/go-amqp-pubsub v0.0.0-20210710222824-c4781d5ae30d
	github.com/samber/lo v1.52.0
	github.com/samber/mo v1.16.0
	github.com/sirupsen/logrus v1.9.1
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_golang v1.20.5 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	golang.org/x/sys v0.22.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)

replace github.com/samber/go-amqp-pubsub => ../..
