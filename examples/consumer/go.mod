module consumer

go 1.19

require (
	github.com/rabbitmq/amqp091-go v1.8.1
	github.com/samber/go-amqp-pubsub v0.0.0-20210710222824-c4781d5ae30d
	github.com/samber/lo v1.35.0
	github.com/samber/mo v1.5.1
	github.com/sirupsen/logrus v1.9.0
)

require (
	github.com/google/uuid v1.3.0 // indirect
	golang.org/x/exp v0.0.0-20220303212507-bbda1eaf7a17 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
)

replace github.com/samber/go-amqp-pubsub => ../..
