package pubsub

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/samber/lo"
	"github.com/samber/mo"
)

const (
// @TODO: Using a different exchange would be a breaking change.
// deadLetterExchange     = "internal.dlx"
// retryExchange          = "internal.retry"
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
	Metrics          ConsumerOptionsMetrics
	EnableDeadLetter mo.Option[bool]             // default false
	Defer            mo.Option[time.Duration]    // default no Defer
	ConsumeArgs      mo.Option[amqp.Table]       // default nil
	RetryStrategy    mo.Option[RetryStrategy]    // default no retry
	RetryConsistency mo.Option[RetryConsistency] // default eventually consistent
}

type QueueSetupExchangeOptions struct {
	name       mo.Option[string]
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
	args       mo.Option[amqp.Table]
}

type QueueSetupOptions struct {
	Exchange QueueSetupExchangeOptions
	Queue    QueueSetupQueueOptions
}

type Consumer struct {
	conn    *Connection
	name    string
	options ConsumerOptions

	delivery  chan *amqp.Delivery
	closeOnce sync.Once
	done      *rpc[struct{}, struct{}]

	mu             sync.RWMutex
	bindingUpdates *rpc[lo.Tuple2[bool, ConsumerOptionsBinding], error]

	retryProducer *Producer

	metrics []*metric
}

func NewConsumer(conn *Connection, name string, opt ConsumerOptions) *Consumer {
	doneCh := make(chan struct{})
	bindingUpdatesCh := make(chan<- lo.Tuple2[bool, ConsumerOptionsBinding], 10)

	c := Consumer{
		conn:    conn,
		name:    name,
		options: opt,

		delivery:  make(chan *amqp.Delivery),
		closeOnce: sync.Once{},
		done:      newRPC[struct{}, struct{}](doneCh),

		mu:             sync.RWMutex{},
		bindingUpdates: newRPC[lo.Tuple2[bool, ConsumerOptionsBinding], error](bindingUpdatesCh),

		retryProducer: nil,

		metrics: opt.Metrics.metrics(name),
	}

	if opt.RetryStrategy.IsPresent() {
		c.retryProducer = NewProducer(
			conn,
			name+".retry",
			ProducerOptions{
				Exchange: ProducerOptionsExchange{},
			},
		)
	}

	go c.lifecycle()

	return &c
}

func (svc *Consumer) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range svc.metrics {
		metric.Describe(ch)
	}
}

func (svc *Consumer) Collect(ch chan<- prometheus.Metric) {
	for _, metric := range svc.metrics {
		metric.Collect(ch)
	}
}

func (c *Consumer) lifecycle() {
	cancel, connectionListener := c.conn.ListenConnection()
	onConnect := make(chan struct{}, 42)
	onDisconnect := make(chan struct{}, 42)

	var conn *amqp.Connection
	var channel *amqp.Channel

	defer func() {
		safeCloseChan(onConnect)
		safeCloseChan(onDisconnect)
	}()

	for {
		select {
		case _conn := <-connectionListener:
			conn = _conn
			if conn != nil {
				onConnect <- struct{}{}
			} else {
				onDisconnect <- struct{}{}
			}

		case <-onConnect:
			channel = c.closeChannel(channel)

			if conn == nil || conn.IsClosed() {
				continue
			}

			_channel, onChannelClosed, err := c.setupConsumer(conn)
			if err != nil {
				logger(ScopeConsumer, c.name, "Could not start consumer", map[string]any{"error": err.Error()})
				time.Sleep(1 * time.Second) // retry in 1 second
				onConnect <- struct{}{}
			} else {
				channel = _channel
				go func() {
					// ok && err==nil -> channel closed
					// ok && err!=nil -> channel error (message timeout, connection error, etc...)
					err, ok := <-onChannelClosed
					if ok && err != nil {
						logger(ScopeChannel, c.name, "Channel closed: "+err.Reason, map[string]any{"error": err.Error()})
						onConnect <- struct{}{}
					}
				}()
			}

		case <-onDisconnect:
			channel = c.closeChannel(channel)

		case req := <-c.done.C:
			channel = c.closeChannel(channel) //nolint:ineffassign

			cancel()                          // first, remove from connection listeners
			safeCloseChan(c.bindingUpdates.C) // second, stop updating queue bindings
			drainChan(c.delivery)             // third, flush channel -- we don't requeue message since amqp will do it for us
			safeCloseChan(c.delivery)         // last, stop consuming messages

			// send response to rpc
			req.B(struct{}{})
			return
		}
	}
}

func (c *Consumer) closeChannel(channel *amqp.Channel) *amqp.Channel {
	if channel != nil && !channel.IsClosed() {
		channel.Close()
	}

	// Just to be sure we won't read twice some messages.
	// Also, it offers some garantee on message order on reconnect.
	drainChan(c.delivery)

	return nil
}

func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		_ = c.done.Send(struct{}{})
		safeCloseChan(c.done.C)

		if c.retryProducer != nil {
			_ = c.retryProducer.Close()
		}
	})

	return nil
}

func (c *Consumer) setupConsumer(conn *amqp.Connection) (*amqp.Channel, <-chan *amqp.Error, error) {
	// create a channel dedicated to this consumer
	channel, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	// create dead-letter queue if necessary
	queueArgs := c.options.Queue.Args.OrElse(nil)

	if c.options.EnableDeadLetter.OrElse(false) {
		deadLetterArgs, err2 := c.setupDeadLetter(channel)
		if err2 != nil {
			_ = channel.Close()
			return nil, nil, err2
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
		return nil, nil, err
	}

	err = channel.Qos(
		c.options.Message.PrefetchCount.OrElse(0),
		c.options.Message.PrefetchSize.OrElse(0),
		false,
	)
	if err != nil {
		_ = channel.Close()
		return nil, nil, err
	}

	queueToBind := c.options.Queue.Name

	// create defer queue if necessary
	if c.options.Defer.IsPresent() {
		err = c.setupDefer(channel, c.options.Defer.MustGet())
		if err != nil {
			_ = channel.Close()
			return nil, nil, err
		}

		queueToBind = c.options.Queue.Name + ".defer"
	}

	// binding exchange->queue
	c.mu.Lock()
	bindings := c.options.Bindings
	c.mu.Unlock()
	for _, b := range bindings {
		err = channel.QueueBind(
			queueToBind,
			b.RoutingKey,
			b.ExchangeName,
			false,
			b.Args.OrElse(nil),
		)
		if err != nil {
			_ = channel.Close()
			return nil, nil, err
		}
	}

	// create retry queue if necessary
	if c.options.RetryStrategy.IsPresent() {
		err = c.setupRetry(channel)
		if err != nil {
			_ = channel.Close()
			return nil, nil, err
		}
	}

	err = c.onMessage(channel)
	if err != nil {
		_ = channel.Close()
		return nil, nil, err
	}

	return channel, channel.NotifyClose(make(chan *amqp.Error)), nil
}

func (c *Consumer) setupQueue(channel *amqp.Channel, opts QueueSetupOptions, bindQueueToDeadLetter bool) error {
	err := channel.ExchangeDeclare(
		opts.Exchange.name.OrElse("amq.direct"),
		opts.Exchange.kind.OrElse(amqp.ExchangeDirect),
		opts.Exchange.durable.OrElse(true),
		opts.Exchange.autoDelete.OrElse(false),
		opts.Exchange.internal.OrElse(false),
		opts.Exchange.noWait.OrElse(false),
		nil,
	)
	if err != nil {
		return err
	}

	_, err = channel.QueueDeclare(
		opts.Queue.name,
		opts.Queue.durable,
		opts.Queue.autoDelete,
		opts.Queue.exclusive,
		opts.Queue.noWait,
		opts.Queue.args.OrElse(nil),
	)
	if err != nil {
		return err
	}

	// binding exchange->queue
	err = channel.QueueBind(
		opts.Queue.name,
		opts.Queue.name,
		opts.Exchange.name.OrElse("amq.direct"),
		false,
		nil,
	)
	if err != nil {
		return err
	}

	if bindQueueToDeadLetter {
		err = channel.QueueBind(
			c.options.Queue.Name,
			c.options.Queue.Name,
			opts.Exchange.name.OrElse("amq.direct"),
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Consumer) setupDeadLetter(channel *amqp.Channel) (map[string]any, error) {
	deadLetterQueueName := c.options.Queue.Name + ".deadLetter"

	args := amqp.Table{
		"x-dead-letter-exchange":    "amq.direct",
		"x-dead-letter-routing-key": deadLetterQueueName,
	}

	opts := QueueSetupOptions{
		Exchange: QueueSetupExchangeOptions{
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
	}

	return args, c.setupQueue(channel, opts, false)
}

func (c *Consumer) setupRetry(channel *amqp.Channel) error {
	opts := QueueSetupOptions{
		Exchange: QueueSetupExchangeOptions{
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
			args: mo.Some(amqp.Table{
				"x-dead-letter-exchange":    "amq.direct",
				"x-dead-letter-routing-key": c.options.Queue.Name,
			}),
		},
	}

	return c.setupQueue(channel, opts, true)
}

func (c *Consumer) setupDefer(channel *amqp.Channel, delay time.Duration) error {
	opts := QueueSetupOptions{
		Exchange: QueueSetupExchangeOptions{},
		Queue: QueueSetupQueueOptions{
			name:       c.options.Queue.Name + ".defer",
			durable:    c.options.Queue.Durable.OrElse(true),
			autoDelete: c.options.Queue.AutoDelete.OrElse(false),
			exclusive:  false,
			noWait:     false,
			args: mo.Some(amqp.Table{
				"x-dead-letter-exchange":    "amq.direct",
				"x-dead-letter-routing-key": c.options.Queue.Name,
				"x-message-ttl":             delay.Milliseconds(),
			}),
		},
	}

	return c.setupQueue(channel, opts, true)
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
