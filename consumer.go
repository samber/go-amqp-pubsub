package pubsub

import (
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/samber/lo"
	"github.com/samber/mo"
)

const (
	// @TODO: Using a different exchange would be a breaking change.
	// deadLetterExchange     = "internal.dlx"
	// retryExchange          = "internal.retry"

	deadLetterExchange     = "amq.direct"
	deadLetterExchangeKind = amqp.ExchangeDirect
	retryExchange          = "amq.direct"
	retryExchangeKind      = amqp.ExchangeDirect
	deferExchange          = "amq.direct"
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
	EnableDeadLetter mo.Option[bool]             // default false
	Defer            mo.Option[time.Duration]    // default no Defer
	ConsumeArgs      mo.Option[amqp.Table]       // default nil
	RetryStrategy    mo.Option[RetryStrategy]    // default no retry
	RetryConsistency mo.Option[RetryConsistency] // default eventually consistent
}

type QueueSetupExchangeOptions struct {
	name       string
	kind       mo.Option[string]
	durable    mo.Option[bool]
	autoDelete mo.Option[bool]
	internal   mo.Option[bool]
	noWait     mo.Option[bool]
}

type QueueSetupQueueOptions struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
}

type QueueSetupDeadLetterOptions struct {
	name       string
	routingKey string
}

type QueueSetupOptions struct {
	Exchange   QueueSetupExchangeOptions
	Queue      QueueSetupQueueOptions
	DeadLetter QueueSetupDeadLetterOptions
}

type Consumer struct {
	conn    *Connection
	name    string
	options ConsumerOptions

	mu       sync.Mutex
	delivery chan *amqp.Delivery
	done     *rpc[struct{}, struct{}]

	bindingUpdates *rpc[lo.Tuple2[bool, ConsumerOptionsBinding], error]

	retryProducer *Producer
}

func NewConsumer(conn *Connection, name string, opt ConsumerOptions) *Consumer {
	doneCh := make(chan struct{})
	bindingUpdatesCh := make(chan<- lo.Tuple2[bool, ConsumerOptionsBinding], 10)

	c := Consumer{
		conn:    conn,
		name:    name,
		options: opt,

		mu:       sync.Mutex{},
		delivery: make(chan *amqp.Delivery),
		done:     newRPC[struct{}, struct{}](doneCh),

		bindingUpdates: newRPC[lo.Tuple2[bool, ConsumerOptionsBinding], error](bindingUpdatesCh),

		retryProducer: nil,
	}

	if opt.RetryStrategy.IsPresent() {
		c.retryProducer = NewProducer(
			conn,
			name+".retry",
			ProducerOptions{
				Exchange: ProducerOptionsExchange{
					Name: retryExchange,
					Kind: retryExchangeKind,
				},
			},
		)
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

		case req := <-c.done.C:
			cancel()
			safeClose(c.bindingUpdates.C)
			safeClose(c.delivery)

			req.B(struct{}{})
			return
		}
	}
}

func (c *Consumer) Close() error {
	_ = c.done.Send(struct{}{})
	safeClose(c.done.C)

	if c.retryProducer != nil {
		_ = c.retryProducer.Close()
	}

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

	err = channel.Qos(
		c.options.Message.PrefetchCount.OrElse(0),
		c.options.Message.PrefetchSize.OrElse(0),
		false,
	)
	if err != nil {
		_ = channel.Close()
		return err
	}

	queueToBind := c.options.Queue.Name

	// create defer queue if necessary
	if c.options.Defer.IsPresent() {
		err = c.setupDefer(channel, c.options.Defer.MustGet())
		if err != nil {
			_ = channel.Close()
			return err
		}

		queueToBind = c.options.Queue.Name + ".defer"
	}

	// binding exchange->queue
	for _, b := range c.options.Bindings {
		err = channel.QueueBind(
			queueToBind,
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

	// create retry queue if necessary
	if c.options.RetryStrategy.IsPresent() {
		err = c.setupRetry(channel)
		if err != nil {
			_ = channel.Close()
			return err
		}
	}

	err = c.onMessage(channel)
	if err != nil {
		_ = channel.Close()
		return err
	}

	go c.onChannelEvent(conn, channel)

	return nil
}

func (c *Consumer) setupQueue(channel *amqp.Channel, opts QueueSetupOptions, perMessageExpiration *time.Duration, declareExchange bool, bindQueueToConsummer bool) (map[string]any, error) {
	args := map[string]any{
		"x-dead-letter-exchange":    opts.DeadLetter.name,
		"x-dead-letter-routing-key": opts.DeadLetter.routingKey,
	}

	if perMessageExpiration != nil {
		args["x-message-ttl"] = perMessageExpiration.Milliseconds()

	}

	if declareExchange {
		err := channel.ExchangeDeclare(
			opts.Exchange.name,
			opts.Exchange.kind.MustGet(),
			opts.Exchange.durable.OrElse(true),
			opts.Exchange.autoDelete.OrElse(false),
			opts.Exchange.internal.OrElse(false),
			opts.Exchange.noWait.OrElse(false),
			nil,
		)
		if err != nil {
			return nil, err
		}
	}

	_, err := channel.QueueDeclare(
		opts.Queue.name,
		opts.Queue.durable,
		opts.Queue.autoDelete,
		opts.Queue.exclusive,
		opts.Queue.noWait,
		args,
	)
	if err != nil {
		return nil, err
	}

	// binding exchange->queue
	err = channel.QueueBind(
		opts.Queue.name,
		opts.Queue.name,
		opts.Exchange.name,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	if bindQueueToConsummer {
		err = channel.QueueBind(
			c.options.Queue.Name,
			c.options.Queue.Name,
			opts.Exchange.name,
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}

	return args, nil
}

func (c *Consumer) setupDeadLetter(channel *amqp.Channel) (map[string]any, error) {
	deadLetterQueueName := c.options.Queue.Name + ".deadLetter"

	opts := QueueSetupOptions{
		Exchange: QueueSetupExchangeOptions{
			name:       deadLetterExchange,
			kind:       mo.Some(deadLetterExchangeKind),
			durable:    mo.Some(true),
			autoDelete: mo.Some(false),
			internal:   mo.Some(false), // @TODO: should be `true` (breaking change)
			noWait:     mo.Some(false),
		},
		Queue: QueueSetupQueueOptions{
			name:       deadLetterQueueName,
			durable:    true,
			autoDelete: false,
			exclusive:  false,
			noWait:     false,
		},
		DeadLetter: QueueSetupDeadLetterOptions{
			name:       deadLetterExchange,
			routingKey: deadLetterQueueName,
		},
	}

	return c.setupQueue(channel, opts, nil, true, false)
}

func (c *Consumer) setupRetry(channel *amqp.Channel) error {
	opts := QueueSetupOptions{
		Exchange: QueueSetupExchangeOptions{
			name:       retryExchange,
			kind:       mo.Some(retryExchangeKind),
			durable:    mo.Some(true),
			autoDelete: mo.Some(false),
			internal:   mo.Some(false),
			noWait:     mo.Some(false),
		},
		Queue: QueueSetupQueueOptions{
			name:       c.options.Queue.Name + ".retry",
			durable:    c.options.Queue.Durable.OrElse(true),
			autoDelete: c.options.Queue.AutoDelete.OrElse(false),
			exclusive:  false,
			noWait:     false,
		},
		DeadLetter: QueueSetupDeadLetterOptions{
			name:       retryExchange,
			routingKey: c.options.Queue.Name,
		},
	}

	_, err := c.setupQueue(channel, opts, nil, true, true)

	return err
}

func (c *Consumer) setupDefer(channel *amqp.Channel, delay time.Duration) error {
	opts := QueueSetupOptions{
		Exchange: QueueSetupExchangeOptions{
			name: deferExchange,
		},
		Queue: QueueSetupQueueOptions{
			name:       c.options.Queue.Name + ".defer",
			durable:    c.options.Queue.Durable.OrElse(true),
			autoDelete: c.options.Queue.AutoDelete.OrElse(false),
			exclusive:  false,
			noWait:     false,
		},
		DeadLetter: QueueSetupDeadLetterOptions{
			name:       deferExchange,
			routingKey: c.options.Queue.Name,
		},
	}

	_, err := c.setupQueue(channel, opts, &delay, false, true)

	return err
}

func (c *Consumer) onChannelEvent(conn *amqp.Connection, channel *amqp.Channel) {
	onError := channel.NotifyClose(make(chan *amqp.Error))
	onCancel := channel.NotifyCancel(make(chan string))

	defer lo.Try0(func() { channel.Close() })

	for {
		select {
		case err := <-onError:
			if err != nil {
				logger("AMQP channel '%s': %s", c.name, err.Error())
			}

			err2 := c.setupConsumer(conn)
			if err2 != nil {
				logger("AMQP channel '%s': %s", c.name, err2.Error())
				go func() {
					// executed in a coroutine to avoid deadlock
					time.Sleep(1 * time.Second)
					onError <- nil
				}()
			}

			return

		case msg := <-onCancel:
			logger("AMQP channel '%s': %v", c.name, msg)

			err := c.setupConsumer(conn)
			if err != nil {
				logger("AMQP consumer '%s': %s", c.name, err.Error())
			}

			return

		case update := <-c.bindingUpdates.C:
			err := c.onBindingUpdate(channel, update.A)
			if err != nil {
				logger("AMQP consumer '%s': %s", c.name, err.Error())
				update.B(err)
			} else {
				update.B(nil)
			}
		}
	}
}

func (c *Consumer) onBindingUpdate(channel *amqp.Channel, update lo.Tuple2[bool, ConsumerOptionsBinding]) error {
	adding, binding := update.Unpack()

	queueToBind := c.options.Queue.Name

	if c.options.Defer.IsPresent() {
		queueToBind = c.options.Queue.Name + ".defer"
	}

	err := lo.TernaryF(
		adding,
		func() error {
			return channel.QueueBind(
				queueToBind,
				binding.RoutingKey,
				binding.ExchangeName,
				false,
				binding.Args.OrElse(nil),
			)
		}, func() error {
			return channel.QueueUnbind(
				queueToBind,
				binding.RoutingKey,
				binding.ExchangeName,
				binding.Args.OrElse(nil),
			)
		},
	)

	if err != nil {
		_ = channel.Close()
		return fmt.Errorf("failed to (un)bind queue '%s' to exchange '%s' using routing key '%s': %s", queueToBind, binding.ExchangeName, binding.RoutingKey, err.Error())
	}

	return nil
}

/**
 * Message stream
 */
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
			if c.options.RetryStrategy.IsPresent() {
				raw.Acknowledger = newRetryAcknowledger(
					c.retryProducer,
					c.options.Queue.Name+".retry",
					c.options.RetryStrategy.MustGet(),
					c.options.RetryConsistency.OrElse(EventuallyConsistentRetry),
					raw,
				)
			}

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

func (c *Consumer) AddBinding(exchangeName string, routingKey string, args mo.Option[amqp.Table]) error {
	binding := ConsumerOptionsBinding{
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Args:         args,
	}

	err := c.bindingUpdates.Send(lo.T2(true, binding))
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.options.Bindings = append(c.options.Bindings, binding)
	c.mu.Unlock()

	return nil
}

func (c *Consumer) RemoveBinding(exchangeName string, routingKey string, args mo.Option[amqp.Table]) error {
	binding := ConsumerOptionsBinding{
		ExchangeName: exchangeName,
		RoutingKey:   routingKey,
		Args:         args,
	}

	err := c.bindingUpdates.Send(lo.T2(false, binding))
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.options.Bindings = lo.Filter(c.options.Bindings, func(item ConsumerOptionsBinding, _ int) bool {
		return item.ExchangeName != exchangeName && item.RoutingKey != routingKey
	})
	c.mu.Unlock()

	return nil
}
