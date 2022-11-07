package pubsub

import (
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/samber/lo"
	"github.com/samber/mo"
)

type ConnectionOptions struct {
	URI    string
	Config amqp.Config

	// optional arguments
	ReconnectInterval mo.Option[time.Duration] // default 2s
	LazyConnection    mo.Option[bool]          // default false
}

type Connection struct {
	conn    *amqp.Connection
	name    string
	options ConnectionOptions

	// should be a generic sync.Map
	channelsMutex sync.Mutex
	channels      map[string]chan *amqp.Connection
	done          chan struct{}
}

func NewConnection(name string, opt ConnectionOptions) (*Connection, error) {
	c := &Connection{
		conn:    nil,
		name:    name,
		options: opt,

		channelsMutex: sync.Mutex{},
		channels:      map[string]chan *amqp.Connection{},
		done:          make(chan struct{}, 1),
	}

	err := c.lifecycle()

	return c, err
}

func (c *Connection) lifecycle() error {
	heartbeat := make(chan struct{}, 1)
	reconnect := make(chan struct{}, 1)

	if c.options.LazyConnection.OrElse(false) {
		reconnect <- struct{}{}
	} else {
		err := c.redial()
		if err != nil {
			return err
		}

		heartbeat <- struct{}{}
	}

	go func() {
		for {
			select {
			case <-reconnect:
				err := c.redial()
				if err != nil {
					logger("AMQP dial: %s", err.Error())
				}

				heartbeat <- struct{}{}

			case <-heartbeat:
				time.Sleep(c.options.ReconnectInterval.OrElse(2 * time.Second))

				ko := c.IsClosed()
				if ko {
					reconnect <- struct{}{}
				} else {
					heartbeat <- struct{}{}
				}

			case <-c.done:
				// disconnect
				if c.conn != nil {
					err := c.conn.Close()
					if err != nil {
						logger("AMQP: %s", err.Error())
					}

					c.conn = nil
				}

				c.notifyChannels(nil)
				close(c.done)

				// @TODO we should requeue messages

				return
			}
		}
	}()

	return nil
}

func (c *Connection) Close() error {
	// @TODO: should be blocking, until everything is properly closed.
	c.done <- struct{}{}
	return nil
}

// ListenConnection implements the Observable pattern.
func (c *Connection) ListenConnection() (func(), <-chan *amqp.Connection) {
	id := uuid.New().String()
	ch := make(chan *amqp.Connection, 42)

	cancel := func() {
		c.channelsMutex.Lock()
		defer c.channelsMutex.Unlock()

		delete(c.channels, id)
		close(ch)
	}

	c.channelsMutex.Lock()
	c.channels[id] = ch
	c.channelsMutex.Unlock()

	ch <- c.conn

	return cancel, ch
}

func (c *Connection) IsClosed() bool {
	c.channelsMutex.Lock()
	defer c.channelsMutex.Unlock()

	return c.conn == nil || c.conn.IsClosed()
}

func (c *Connection) redial() error {
	c.channelsMutex.Lock()
	conn := c.conn
	c.channelsMutex.Unlock()

	if conn != nil {
		lo.Try0(func() { conn.Close() }) // silent error
	}

	conn, err := amqp.DialConfig(c.options.URI, c.options.Config)

	c.notifyChannels(conn)

	if err != nil {
		c.conn = nil
	} else {
		c.conn = conn
	}

	return err
}

func (c *Connection) notifyChannels(conn *amqp.Connection) {
	c.channelsMutex.Lock()
	defer c.channelsMutex.Unlock()

	for _, v := range c.channels {
		v <- conn
	}
}
