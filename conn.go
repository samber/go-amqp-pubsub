package pubsub

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

type AMQPConnectionOptions struct {
	URI string

	Timeout   *time.Duration // optional - default 10s
	Heartbeat *time.Duration // optional - default 10s
	Retry     *time.Duration // optional - default 2s

	LazyConnection bool // default false
}

type AMQPConnection struct {
	Name    string
	options AMQPConnectionOptions

	conn   *amqp.Connection
	ticker *time.Ticker
}

func NewAMQPConnection(name string, opt AMQPConnectionOptions) (*AMQPConnection, error) {
	if opt.Timeout == nil {
		defaultTimeout := time.Duration(10 * time.Second)
		opt.Timeout = &defaultTimeout
	}

	if opt.Heartbeat == nil {
		defaultHeartbeat := time.Duration(10 * time.Second)
		opt.Heartbeat = &defaultHeartbeat
	}

	if opt.Retry == nil {
		defaultRetry := time.Duration(2 * time.Second)
		opt.Retry = &defaultRetry
	}

	conn := &AMQPConnection{
		Name:    name,
		options: opt,
	}

	conn.autoReconnect()

	if !opt.LazyConnection {
		if err := conn.connect(); err != nil {
			return nil, err
		}
	}

	return conn, nil
}

func (c *AMQPConnection) IsClosed() bool {
	return c.conn == nil || c.conn.IsClosed()
}

func (c *AMQPConnection) Close() {
	if c.ticker != nil {
		c.ticker.Stop()
	}

	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *AMQPConnection) connect() error {
	conn, err := amqp.DialConfig(c.options.URI, amqp.Config{
		Heartbeat: *c.options.Heartbeat,
		Dial:      amqp.DefaultDial(*c.options.Timeout),
	})
	if err != nil {
		return fmt.Errorf("AMQP %s: %s\n", c.Name, err.Error())
	}

	c.conn = conn

	return nil
}

// It does not reconnect.
func (c *AMQPConnection) autoReconnect() {
	c.ticker = time.NewTicker(*c.options.Retry)

	go func() {
		nbrPingsKO := 0

		for range c.ticker.C {
			// not thread safe
			if c.IsClosed() {
				logger.Printf("Connection to RabbitMQ(%s) was lost. Trying again in %s...\n", c.Name, c.options.Retry.String())
				nbrPingsKO++

				c.connect()
			} else if nbrPingsKO > 0 {
				logger.Printf("Connection to RabbitMQ(%s) is back. It's been down during %d seconds.\n", c.Name, 2*nbrPingsKO)
				nbrPingsKO = 0
			}
		}
	}()
}
