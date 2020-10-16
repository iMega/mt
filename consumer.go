package mt

import (
	"errors"
	"fmt"

	"github.com/streadway/amqp"
)

type consumer struct {
	handler  func(*Request) error
	conn     *amqp.Connection
	queue    *amqp.Queue
	channel  *amqp.Channel
	options  *exchange
	dsn      string
	shutdown bool
}

func newConsumer(dsn string, options *exchange) *consumer {
	return &consumer{
		dsn:     dsn,
		options: options,
	}
}

func (c *consumer) connect() error {
	conn, err := dial(c.dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to amqp, %s", err)
	}
	c.conn = conn

	ch, err := channelDeclare(c.conn)
	if err != nil {
		return fmt.Errorf("failed to open channel, %s", err)
	}
	c.channel = ch

	if err := exchangeDeclare(c.channel, c.options); err != nil {
		return fmt.Errorf("failed to declare exchange, %s", err)
	}

	return nil
}

func (c *consumer) reConnect() (<-chan amqp.Delivery, error) {
	if err := c.connect(); err != nil {
		logmt.Errorf("failed to reconnect: %s", err)
	}

	return c.announce()
}

func (c *consumer) announce() (<-chan amqp.Delivery, error) {
	err := c.channel.Qos(c.options.Queue.PrefetchCount, 0, false)
	if err != nil {
		return nil, fmt.Errorf("failed to set Qos, %s", err)
	}

	queue, err := queueDeclare(c.channel, c.options)
	if err != nil {
		return nil, fmt.Errorf("Failed to declare queue, %s", err)
	}
	c.queue = queue

	delivery, err := getDelivery(c.channel, c.options.Queue.Name, c.options.Queue.Consumer)
	if err != nil {
		return nil, fmt.Errorf("failed to consume queue, %s", err)
	}

	return delivery, nil
}

func (c *consumer) handle(deliveries <-chan amqp.Delivery) {
	var err error
	for {
		tag := c.options.Queue.Consumer.Tag
		for d := range deliveries {
			err := c.handler(&Request{Body: d.Body, Header: Header(d.Headers)})
			if err == nil {
				logmt.Debugf("Message ACK, consumerTag=%s", tag)
				if err := d.Ack(false); err != nil {
					logmt.Errorf("Failed ACK message, %s", err)
				}
			} else {
				logmt.Debugf("Message NACK, consumerTag=%s", tag)
				if err := d.Nack(false, c.options.Queue.Consumer.Requeue); err != nil {
					logmt.Errorf("Failed ACK message, %s", err)
				}
			}
		}

		if c.shutdown {
			logmt.Debugf("consumer will close %s", tag)
			err := c.channel.Close()
			if err != nil {
				logmt.Errorf("consumer close channel %s,", tag)
			}
			c.channel = nil

			err = c.conn.Close()
			if err != nil {
				logmt.Errorf("consumer close connection %s,", tag)
			}
			c.conn = nil

			logmt.Debugf("consumer closed %s", tag)
			return
		}

		deliveries, err = c.reConnect()
		if err != nil {
			logmt.Errorf("Reconnecting Error: %s", err)
		}

		logmt.Debugf("consumer reconnected %s", c.options.Queue.Consumer.Tag)
	}
}

func getDelivery(
	channel *amqp.Channel,
	queueName string,
	consumer consume,
) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		queueName,
		consumer.Tag,
		consumer.NoAck,
		consumer.Exclusive,
		false,
		consumer.NoWait,
		consumer.Arguments,
	)
}

func channelDeclare(conn *amqp.Connection) (*amqp.Channel, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return channel, nil
}

func exchangeDeclare(channel *amqp.Channel, opts *exchange) error {
	if opts.Name == "" {
		return errors.New("exchange name is empty")
	}

	if opts.Type == "" {
		return errors.New("exchange type is empty")
	}

	if err := channel.ExchangeDeclare(
		opts.Name,
		opts.Type,
		opts.Durable,
		opts.AutoDelete,
		opts.Internal,
		opts.NoWait,
		opts.Arguments,
	); err != nil {
		return err
	}
	return nil
}

func queueDeclare(channel *amqp.Channel, opts *exchange) (*amqp.Queue, error) {
	q, err := channel.QueueDeclare(
		opts.Queue.Name,
		opts.Queue.Durable,
		opts.Queue.AutoDelete,
		false, // exclusive
		opts.Queue.NoWait,
		opts.Queue.Arguments,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue, %s", err)
	}

	err = channel.QueueBind(
		opts.Queue.Name,
		opts.Binding.Key,
		opts.Name,
		opts.Binding.NoWait,
		opts.Binding.Arguments,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue, %s", err)
	}

	return &q, nil
}
