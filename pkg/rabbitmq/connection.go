package rabbitmq

import (
	"context"
	"sync"
	"time"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

type connection struct {
	logger    log.Logger
	rdy       bool
	errorChan chan *amqp.Error
	conn      *amqp.Connection
	addr      string
	mutex     sync.Mutex
}

type Connection interface {
	CreateChannel() (*amqp.Channel, error)
}

func NewConnection(ctx context.Context, config cfg.Config, logger log.Logger, addr string) (Connection, error) {
	amqpConn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	errChan := make(chan *amqp.Error)

	amqpConn.NotifyClose(errChan)

	conn := &connection{
		logger:    logger,
		rdy:       true,
		errorChan: errChan,
		conn:      amqpConn,
		addr:      addr,
		mutex:     sync.Mutex{},
	}

	go conn.reconnect(ctx)

	return conn, err
}

func (c *connection) reconnect(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("closing connection due ctx done")
			if err := c.conn.Close(); err != nil {
				c.logger.Error(err.Error())
			}

			return
		case err := <-c.errorChan:
			c.mutex.Lock()
			c.rdy = false
			c.logger.Error("disconnected from connections: %w", err)

			for !c.rdy {
				time.Sleep(2 * time.Second)
				c.logger.Info("trying to reconnect to the server")

				amqpConn, err := amqp.Dial(c.addr)
				if err != nil {
					c.logger.Error("could not reconnect to rabbitmq server: %w", err)

					continue
				}

				c.errorChan = make(chan *amqp.Error)
				amqpConn.NotifyClose(c.errorChan)

				c.conn = amqpConn
				c.rdy = true
			}

			c.logger.Info("reconnected to rabbitmq server")
			c.mutex.Unlock()
			break

		}
	}
}

func (c *connection) CreateChannel() (*amqp.Channel, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.conn.Channel()
}
