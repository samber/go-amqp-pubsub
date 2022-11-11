package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/samber/lo"
	"github.com/samber/mo"
)

type ConsumerOptionsQueue struct {
	Name string

	// optional arguments
	Durable           mo.Option[bool]       // default true
	AutoDelete        mo.Option[bool]       // default false
	ExclusiveConsumer mo.Option[bool]       // default false
	NoWait            mo.Option[bool]       // default false
	Args              mo.Option[amqp.Table] // default nil
}

type ConsumerOptionsBinding struct {
	ExchangeName string
	RoutingKey   string

	// optional arguments
	Args mo.Option[amqp.Table] // default nil
}

type ConsumerOptionsMessage struct {
	// optional arguments
	AutoAck       mo.Option[bool] // default false
	PrefetchCount mo.Option[int]  // default 0
	PrefetchSize  mo.Option[int]  // default 0
}

type ConsumerOptions struct {
	Queue    ConsumerOptionsQueue
	Bindings []ConsumerOptionsBinding
	Message  ConsumerOptionsMessage

	// optional arguments
	EnableDeadLetter mo.Option[bool]       // default false
	ConsumeArgs      mo.Option[amqp.Table] // default nil
}

type Consumer struct {
	conn    *Connection
	name    string
	options ConsumerOptions

	delivery chan *amqp.Delivery
	done     chan struct{}
}

func NewConsumer(conn *Connection, name string, opt ConsumerOptions) *Consumer {
	c := Consumer{
		conn:    conn,
		name:    name,
		options: opt,

		delivery: make(chan *amqp.Delivery),
		done:     make(chan struct{}, 1),
	}

	go c.lifecycle()

	return &c
}

func (c *Consumer) lifecycle() {
	cancel, ch := c.conn.ListenConnection()
	onConnect := make(chan *amqp.Connection, 42)
	onDisconnect := make(chan struct{}, 42)

	for {
		select {
		case conn := <-ch:
			if conn != nil {
				onConnect <- conn
			} else {
				onDisconnect <- struct{}{}
			}

		case conn := <-onConnect:
			err := c.setupConsumer(conn)
			if err != nil {
				logger("AMQP consumer '%s': %s", c.name, err.Error())
				onDisconnect <- struct{}{}
			}

		case <-onDisconnect:

		case <-c.done:
			cancel()
			lo.Try0(func() { close(c.delivery) })

			return
		}
	}
}

func (c *Consumer) Close() error {
	// @TODO: should be blocking
	c.done <- struct{}{}
	return nil
}

func (c *Consumer) setupConsumer(conn *amqp.Connection) error {
	// create a channel dedicated to this consumer
	channel, err := conn.Channel()
	if err != nil {
		return err
	}

	// create dead-letter queue if necessary
	queueArgs := c.options.Queue.Args.OrElse(nil)

	if c.options.EnableDeadLetter.OrElse(false) {
		deadLetterArgs, err2 := c.setupDeadLetter(channel)
		if err2 != nil {
			_ = channel.Close()
			return err2
		}

		queueArgs = lo.Assign(queueArgs, deadLetterArgs)
	}

	// create queue if not exist
	_, err = channel.QueueDeclare(
		c.options.Queue.Name,
		c.options.Queue.Durable.OrElse(true),
		c.options.Queue.AutoDelete.OrElse(false),
		c.options.Queue.ExclusiveConsumer.OrElse(false),
		c.options.Queue.NoWait.OrElse(false),
		queueArgs,
	)
	if err != nil {
		_ = channel.Close()
		return err
	}

	// binding exchange->queue
	for _, b := range c.options.Bindings {
		err = channel.QueueBind(
			c.options.Queue.Name,
			b.RoutingKey,
			b.ExchangeName,
			false,
			b.Args.OrElse(nil),
		)
		if err != nil {
			_ = channel.Close()
			return err
		}
	}

	err = channel.Qos(
		c.options.Message.PrefetchCount.OrElse(0),
		c.options.Message.PrefetchSize.OrElse(0),
		false,
	)
	if err != nil {
		_ = channel.Close()
		return err
	}

	go c.handleCancel(conn, channel)

	return c.onMessage(channel)
}

func (c *Consumer) setupDeadLetter(channel *amqp.Channel) (map[string]any, error) {
	deadLetterExchange := "amq.direct"
	deadLetterQueueName := c.options.Queue.Name + ".deadLetter"
	deadLetterArgs := map[string]any{
		"x-dead-letter-exchange":    deadLetterExchange,
		"x-dead-letter-routing-key": deadLetterQueueName,
	}

	_, err := channel.QueueDeclare(
		deadLetterQueueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	// binding dead-letter exchange->queue
	err = channel.QueueBind(
		deadLetterQueueName,
		deadLetterQueueName,
		deadLetterExchange,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return deadLetterArgs, nil
}

func (c *Consumer) handleCancel(conn *amqp.Connection, channel *amqp.Channel) {
	onClose := channel.NotifyClose(make(chan *amqp.Error))
	onCancel := channel.NotifyCancel(make(chan string))

	select {
	case err := <-onClose:
		if err != nil {
			logger("AMQP channel '%s': %s", c.name, err.Error())
		}
	case msg := <-onCancel:
		logger("AMQP channel '%s': %v", c.name, msg)

		err := c.setupConsumer(conn)
		if err != nil {
			logger("AMQP consumer '%s': %s", c.name, err.Error())
		}
	}

	lo.Try0(func() { channel.Close() })
}

func (c *Consumer) onMessage(channel *amqp.Channel) error {
	delivery, err := channel.Consume(
		c.options.Queue.Name,
		c.name,
		c.options.Message.AutoAck.OrElse(false),
		c.options.Queue.ExclusiveConsumer.OrElse(false),
		false,
		false,
		c.options.ConsumeArgs.OrElse(nil),
	)
	if err != nil {
		return err
	}

	go func() {
		for raw := range delivery {
			c.delivery <- lo.ToPtr(raw)
		}

		// It may reach this line on consumer timeout or channel closing.
		// We let the c.delivery channel consumable.
	}()

	return nil
}

/**
 * API
 */

func (c *Consumer) Consume() <-chan *amqp.Delivery {
	return c.delivery
}
