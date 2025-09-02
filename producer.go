package pubsub

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/samber/mo"
)

type ProducerOptionsExchange struct {
	Name mo.Option[string]       // default "amq.direct"
	Kind mo.Option[ExchangeKind] // default "direct"

	// optional arguments
	Durable    mo.Option[bool]       // default true
	AutoDelete mo.Option[bool]       // default false
	Internal   mo.Option[bool]       // default false
	NoWait     mo.Option[bool]       // default false
	Args       mo.Option[amqp.Table] // default nil
}

type ProducerOptions struct {
	Exchange ProducerOptionsExchange
}

type Producer struct {
	conn    *Connection
	name    string
	options ProducerOptions

	mu        sync.RWMutex
	channel   *amqp.Channel
	closeOnce sync.Once
	done      *rpc[struct{}, struct{}]
}

func NewProducer(conn *Connection, name string, opt ProducerOptions) *Producer {
	doneCh := make(chan struct{})

	p := &Producer{
		conn:    conn,
		name:    name,
		options: opt,

		mu:        sync.RWMutex{},
		channel:   nil,
		closeOnce: sync.Once{},
		done:      newRPC[struct{}, struct{}](doneCh),
	}

	go p.lifecycle()

	return p
}

func (p *Producer) lifecycle() {
	cancel, connectionListener := p.conn.ListenConnection()
	onConnect := make(chan struct{}, 42)
	onDisconnect := make(chan struct{}, 42)

	var conn *amqp.Connection

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
			p.closeChannel()

			if conn == nil || conn.IsClosed() {
				continue
			}

			_channel, onChannelClosed, err := p.setupProducer(conn)
			if err != nil {
				logger(ScopeProducer, p.name, "Could not start producer", map[string]any{"error": err.Error()})
				time.Sleep(1 * time.Second) // retry in 1 second
				onConnect <- struct{}{}
			} else {
				p.mu.Lock()
				p.channel = _channel
				p.mu.Unlock()

				go func() {
					// ok && err==nil -> channel closed
					// ok && err!=nil -> channel error (connection error, etc...)
					err, ok := <-onChannelClosed
					if ok && err != nil {
						logger(ScopeChannel, p.name, "Channel closed: "+err.Reason, map[string]any{"error": err.Error()})
						onConnect <- struct{}{}
					}
				}()
			}

		case <-onDisconnect:
			p.closeChannel()

		case req := <-p.done.C:
			cancel()
			req.B(struct{}{})
			return
		}
	}
}

func (p *Producer) closeChannel() {
	p.mu.Lock()
	if p.channel != nil && !p.channel.IsClosed() {
		_ = p.channel.Close()
		p.channel = nil
	}
	p.mu.Unlock()
}

func (p *Producer) Close() error {
	p.closeOnce.Do(func() {
		_ = p.done.Send(struct{}{})
		safeCloseChan(p.done.C)
	})

	return nil
}

func (p *Producer) setupProducer(conn *amqp.Connection) (*amqp.Channel, <-chan *amqp.Error, error) {
	// create a channel dedicated to this producer
	channel, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	// check if exchange is reserved and pre-declared
	if strings.HasPrefix(p.options.Exchange.Name.OrElse("amq.direct"), "amq.") {
		err = channel.ExchangeDeclarePassive(
			p.options.Exchange.Name.OrElse("amq.direct"),
			string(p.options.Exchange.Kind.OrElse(ExchangeKindDirect)),
			p.options.Exchange.Durable.OrElse(true),
			p.options.Exchange.AutoDelete.OrElse(false),
			p.options.Exchange.Internal.OrElse(false),
			p.options.Exchange.NoWait.OrElse(false),
			p.options.Exchange.Args.OrElse(nil),
		)
	} else {
		// create exchange if not exist
		err = channel.ExchangeDeclare(
			p.options.Exchange.Name.OrElse("amq.direct"),
			string(p.options.Exchange.Kind.OrElse(ExchangeKindDirect)),
			p.options.Exchange.Durable.OrElse(true),
			p.options.Exchange.AutoDelete.OrElse(false),
			p.options.Exchange.Internal.OrElse(false),
			p.options.Exchange.NoWait.OrElse(false),
			p.options.Exchange.Args.OrElse(nil),
		)
	}

	if err != nil {
		_ = channel.Close()
		return nil, nil, err
	}

	return channel, channel.NotifyClose(make(chan *amqp.Error)), nil
}

/**
 * API
 */

func (p *Producer) PublishWithContext(ctx context.Context, routingKey string, mandatory bool, immediate bool, msg amqp.Publishing) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.channel == nil {
		return fmt.Errorf("AMQP: channel '%s' not available", p.name)
	}

	return p.channel.PublishWithContext(
		ctx,
		p.options.Exchange.Name.OrElse("amq.direct"),
		routingKey,
		mandatory,
		immediate,
		msg,
	)
}

func (p *Producer) PublishWithDeferredConfirmWithContext(ctx context.Context, routingKey string, mandatory bool, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.channel == nil {
		return nil, fmt.Errorf("AMQP: channel '%s' not available", p.name)
	}

	return p.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		p.options.Exchange.Name.OrElse("amq.direct"),
		routingKey,
		mandatory,
		immediate,
		msg,
	)
}

func (p *Producer) Publish(routingKey string, mandatory bool, immediate bool, msg amqp.Publishing) error {
	return p.PublishWithContext(
		context.Background(),
		routingKey,
		mandatory,
		immediate,
		msg,
	)
}

func (p *Producer) PublishWithDeferredConfirm(routingKey string, mandatory bool, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	return p.PublishWithDeferredConfirmWithContext(
		context.Background(),
		routingKey,
		mandatory,
		immediate,
		msg,
	)
}
