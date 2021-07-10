package pubsub

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type AMQPProducerOptions struct {
	ExchangeName       string
	ExchangeKind       ExchangeKind
	ExchangeDurable    *bool // default true
	ExchangeAutoDelete *bool // default false
	LazyConnection     bool  // default false
}

type AMQPProducer struct {
	Name    string
	options AMQPProducerOptions

	conn *AMQPConnection

	channel             *amqp.Channel
	expectOpenedChannel bool
	connecting          *sync.Mutex
}

func NewAMQPProducer(name string, conn *AMQPConnection, opt AMQPProducerOptions) (*AMQPProducer, error) {
	if conn == nil {
		panic("amqp connection cannot be nil")
	}

	if opt.ExchangeDurable == nil {
		tRue := true
		opt.ExchangeDurable = &tRue
	}

	if opt.ExchangeAutoDelete == nil {
		fAlse := false
		opt.ExchangeAutoDelete = &fAlse
	}

	p := &AMQPProducer{
		Name:    name,
		options: opt,

		conn: conn,

		channel:             nil,
		expectOpenedChannel: false,
		connecting:          new(sync.Mutex),
	}

	if !opt.LazyConnection {
		if err := p.lazyConnection(); err != nil {
			// we return producer anyway, because amqp driver can reconnect itself
			return p, err
		}
	}

	return p, nil
}

func (p *AMQPProducer) lazyConnection() error {
	p.connecting.Lock()
	defer p.connecting.Unlock()

	if !p.IsClosed() {
		return nil
	}

	if p.conn.IsClosed() {
		return fmt.Errorf("AMQP connection lost for producer '%s'", p.Name)
	}

	// create a channel dedicated to this producer
	channel, err := p.conn.conn.Channel()
	if err != nil {
		return err
	}

	// create exchange if not exist
	err = channel.ExchangeDeclare(
		p.options.ExchangeName,
		string(p.options.ExchangeKind),
		*p.options.ExchangeDurable,
		*p.options.ExchangeAutoDelete,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// @TODO: we may have a leak of channels if it failes before this line
	p.channel = channel
	p.expectOpenedChannel = true

	go p.handleClose()

	return nil
}

func (p *AMQPProducer) handleClose() {
	for {
		if p.channel == nil {
			return
		}

		reason, ok := <-p.channel.NotifyClose(make(chan *amqp.Error))

		// exit this goroutine if closed by developer
		if !ok || !p.expectOpenedChannel {
			p.Close() // close again, ensure closed flag set when connection closed
			return
		}

		p.channel = nil
		logger.Printf("Connection to AMQP lost for producer '%s'. Reason: %s. Is closed: %t.\n", p.Name, reason, p.IsClosed())

		// reconnect if not closed by developer
		for {
			if !p.expectOpenedChannel {
				return
			}

			// We don't retry when the producer connect in lazy mode (when message is sent).
			if p.options.LazyConnection {
				return
			}

			err := p.lazyConnection()
			if err == nil {
				logger.Printf("Connection to AMQP restored for producer '%s'\n", p.Name)
				return
			}

			// wait 2s for connection retry
			time.Sleep(*p.conn.options.Retry)
		}
	}
}

func (p *AMQPProducer) Close() error {
	p.connecting.Lock()
	defer p.connecting.Unlock()

	p.expectOpenedChannel = false
	if p.channel != nil {
		c := p.channel
		p.channel = nil

		return c.Close()
	}

	return nil
}

func (p *AMQPProducer) IsClosed() bool {
	return !p.expectOpenedChannel || p.conn.IsClosed() || p.channel == nil
}

func (p *AMQPProducer) Publish(routingKey RoutingKey, msg amqp.Publishing) error {
	err := p.lazyConnection()
	if err != nil {
		return fmt.Errorf("Cannot publish message to RabbitMQ: %s.", err.Error())
	}

	if reflect.ValueOf(msg.AppId).IsZero() {
		msg.AppId = p.Name
	}
	if reflect.ValueOf(msg.Timestamp).IsZero() {
		msg.Timestamp = time.Now()
	}

	return p.channel.Publish(
		p.options.ExchangeName, // exchange
		string(routingKey),     // routing key
		false,                  // mandatory
		false,                  // immediate
		msg,                    // publishing
	)
}
