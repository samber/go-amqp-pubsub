package pubsub

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type AMQPConsumerBinding struct {
	ExchangeName string
	Key          string
	Args         amqp.Table
}

type AMQPConsumerOptions struct {
	QueueName              string
	QueueBindings          []AMQPConsumerBinding
	QueueDurable           *bool          // default true
	QueueAutoDelete        *bool          // default false
	QueueExclusiveConsumer *bool          // default false
	QueueTTL               *time.Duration // default none - see https://stackoverflow.com/a/21249132
	MessageAutoAck         *bool          // default false
	MessagePrefetch        int
	EnableDeadLetter       bool
	LazyConnection         bool // default false
}

type AMQPConsumer struct {
	Name    string
	options AMQPConsumerOptions

	conn *AMQPConnection

	channel             *amqp.Channel
	expectOpenedChannel bool
	connecting          *sync.Mutex

	// this chan is reused until we call consumer.Close()
	delivery chan amqp.Delivery
}

func NewAMQPConsumer(name string, conn *AMQPConnection, opt AMQPConsumerOptions) (*AMQPConsumer, error) {
	if conn == nil {
		panic("amqp connection cannot be nil")
	}

	if opt.QueueDurable == nil {
		tRue := true
		opt.QueueDurable = &tRue
	}

	if opt.QueueAutoDelete == nil {
		fAlse := false
		opt.QueueAutoDelete = &fAlse
	}

	if opt.QueueExclusiveConsumer == nil {
		fAlse := false
		opt.QueueExclusiveConsumer = &fAlse
	}

	if opt.MessageAutoAck == nil {
		fAlse := false
		opt.MessageAutoAck = &fAlse
	}

	c := &AMQPConsumer{
		Name:    name,
		options: opt,

		conn: conn,

		channel:             nil,
		expectOpenedChannel: false,
		connecting:          new(sync.Mutex),

		delivery: make(chan amqp.Delivery, opt.MessagePrefetch),
	}

	if !opt.LazyConnection {
		if err := c.lazyConnection(); err != nil {
			// we return consumer anyway, because amqp driver can reconnect itself
			return c, err
		}
	}

	return c, nil
}

func (c *AMQPConsumer) lazyConnection() error {
	c.connecting.Lock()
	defer c.connecting.Unlock()

	if !c.IsClosed() {
		return nil
	}

	if c.conn.IsClosed() {
		return fmt.Errorf("AMQP connection lost for consumer '%s'", c.Name)
	}

	// create a channel dedicated to this consumer
	channel, err := c.conn.conn.Channel()
	if err != nil {
		return err
	}

	// create dead-letter queue if necessary
	args, err := c.getDeadLetterConfig(channel)
	if err != nil {
		return err
	}

	if c.options.QueueTTL != nil {
		args["x-expires"] = int(c.options.QueueTTL.Milliseconds())
	}

	// create queue if not exist
	_, err = channel.QueueDeclare(
		c.options.QueueName,
		*c.options.QueueDurable,
		*c.options.QueueAutoDelete,
		*c.options.QueueExclusiveConsumer,
		false,
		args,
	)
	if err != nil {
		return err
	}

	// binding exchange->queue
	for _, b := range c.options.QueueBindings {
		err = channel.QueueBind(
			c.options.QueueName,
			b.Key,
			b.ExchangeName,
			false,
			b.Args,
		)
		if err != nil {
			return err
		}
	}

	err = channel.Qos(c.options.MessagePrefetch, 0, false)
	if err != nil {
		return err
	}

	delivery, err := channel.Consume(
		c.options.QueueName,
		c.Name,
		*c.options.MessageAutoAck,
		*c.options.QueueExclusiveConsumer,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// @TODO: we may have a leak of channels if it fails before this line
	c.channel = channel
	c.expectOpenedChannel = true

	go c.handleMessage(delivery)
	go c.handleClose()

	return nil
}

func (c *AMQPConsumer) getDeadLetterConfig(channel *amqp.Channel) (map[string]interface{}, error) {
	empty := map[string]interface{}{}

	if !c.options.EnableDeadLetter {
		return empty, nil
	}

	dlEx := "amq.direct"
	dlQ := c.options.QueueName + ".deadLetter"
	deadLetterArgs := map[string]interface{}{
		"x-dead-letter-exchange":    dlEx,
		"x-dead-letter-routing-key": dlQ,
	}

	_, err := channel.QueueDeclare(
		dlQ,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return empty, err
	}

	// binding dead-letter exchange->queue
	err = channel.QueueBind(
		dlQ,
		dlQ,
		dlEx,
		false,
		nil,
	)
	if err != nil {
		return empty, err
	}

	return deadLetterArgs, err
}

func (c *AMQPConsumer) handleClose() {
	for {
		if c.channel == nil {
			return
		}

		reason, ok := <-c.channel.NotifyClose(make(chan *amqp.Error))

		// exit this goroutine if closed by developer
		if !ok || !c.expectOpenedChannel {
			c.Close() // close again, ensure closed flag set when connection closed
			return
		}

		c.channel = nil
		logger.Printf("Connection to AMQP lost for consumer '%s'. Reason: %s. Is closed: %t.\n", c.Name, reason, c.IsClosed())

		// reconnect if not closed by developer
		for {
			if !c.expectOpenedChannel {
				return
			}

			// Restart message consuming
			err := c.lazyConnection()
			if err == nil {
				logger.Printf("Connection to AMQP restored for consumer '%s'\n", c.Name)
				return
			}

			// wait 2s for connection retry
			time.Sleep(*c.conn.options.Retry)
		}
	}
}

func (c *AMQPConsumer) handleMessage(delivery <-chan amqp.Delivery) {
	handle := func(msg amqp.Delivery) {
		if c.IsClosed() {
			_ = msg.Reject(true)
			return
		}

		c.delivery <- msg
	}

	// @TODO: check there is a back-pressure between delivery 1 and delivery 2
	// @TODO: we should probaby be blocking second iteration until first iteration is not ack/nack
	for msg := range delivery {
		// This lock is a bullshit for preventing a race condition+panic when Close() is called
		// @TODO: should be removed
		c.connecting.Lock()
		handle(msg)
		c.connecting.Unlock()
	}
}

func (c *AMQPConsumer) Close() error {
	c.connecting.Lock()
	defer c.connecting.Unlock()

	c.expectOpenedChannel = false
	if c.channel != nil {
		defer close(c.delivery) // need to defer, because it may crash
		ch := c.channel
		c.channel = nil

		_ = ch.Confirm(false)
		_ = ch.Cancel(c.Name, false)

		return ch.Close()
	}

	return nil
}

func (c *AMQPConsumer) IsClosed() bool {
	return !c.expectOpenedChannel || c.channel == nil || c.conn.IsClosed()
}

func (c *AMQPConsumer) Consume() <-chan amqp.Delivery {
	err := c.lazyConnection()
	if err != nil {
		// We won't block as the library run a retry in background
		logger.Printf("Cannot consume message from RabbitMQ: %s.\n", err.Error())
	}

	return c.delivery
}

// @TODO: Add exception recovery
func (c *AMQPConsumer) AddWorkers(runInGorouting bool, count int, callback func(index int, msg amqp.Delivery)) {
	if count < 1 {
		panic("cannot run less than 1 worker")
	}

	if !runInGorouting && count > 1 {
		panic("parallel workers must be executed into goroutines")
	}

	worker := func(index int) {
		delivery := c.Consume()

		for msg := range delivery {
			callback(index, msg)
		}
	}

	for i := 0; i < count; i++ {
		if runInGorouting {
			go worker(i)
		} else {
			worker(i)
		}
	}
}

func (c *AMQPConsumer) addBinding(binding AMQPConsumerBinding) error {
	// Lock for each binding instead of globally, to prevent too long lock.
	c.connecting.Lock()
	defer c.connecting.Unlock()

	for _, b := range c.options.QueueBindings {
		if b.ExchangeName == binding.ExchangeName && b.Key != binding.Key && !reflect.DeepEqual(b.Args, binding.Args) {
			return nil
		}
	}

	c.options.QueueBindings = append(c.options.QueueBindings, binding)

	if c.IsClosed() {
		return nil
	}

	return c.channel.QueueBind(
		c.options.QueueName,
		binding.Key,
		binding.ExchangeName,
		false,
		binding.Args,
	)
}

func (c *AMQPConsumer) AddBindings(bindings []AMQPConsumerBinding) error {
	for _, b := range bindings {
		err := c.addBinding(b)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *AMQPConsumer) removeBinding(binding AMQPConsumerBinding) error {
	// Lock for each binding instead of globally, to prevent too long lock.
	c.connecting.Lock()
	defer c.connecting.Unlock()

	for i, b := range c.options.QueueBindings {
		if b.ExchangeName == binding.ExchangeName && b.Key != binding.Key && !reflect.DeepEqual(b.Args, binding.Args) {
			c.options.QueueBindings = append(c.options.QueueBindings[:i], c.options.QueueBindings[i+1:]...)
		}
	}

	c.options.QueueBindings = append(c.options.QueueBindings, binding)

	if c.IsClosed() {
		return nil
	}

	// We unbind even if it was not bind already (binding could be set by different consumer)
	return c.channel.QueueUnbind(
		c.options.QueueName,
		binding.Key,
		binding.ExchangeName,
		binding.Args,
	)
}

func (c *AMQPConsumer) RemoveBindings(bindings []AMQPConsumerBinding) error {
	for _, b := range bindings {
		err := c.removeBinding(b)
		if err != nil {
			return err
		}
	}

	return nil
}
