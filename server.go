// Copyright © 2020 Dmitry Stoletov <info@imega.ru>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mt

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const version = "1.1.2"

var errHandleFuncServiceNameEmpty = errors.New("serviceName of handler func is empty")

// MassTransport is a interface.
type MassTransport interface {
	ConnectAndServe() error
	AddHandler(ServiceName, HandlerFunc)
	Call(ServiceName, Request, ReplyFunc) error
	Cast(ServiceName, Request) error
	Shutdown() error
	HealthCheck() bool
}

// Request from chan.
type Request struct {
	Header Header
	Body   []byte
}

type ServiceName string

type ReplyFunc func(Response) error

type HandlerFunc func(*Request, ReplyFunc) error

// A Header represents the key-value pairs in a message header.
type Header amqp.Table

// AddString adds the key, string value pair to the header.
func (h Header) AddString(key, value string) {
	h[key] = value
}

func (h Header) String(key string) string {
	if h == nil {
		return ""
	}

	v, ok := h[key].(string)
	if !ok {
		return ""
	}

	return v
}

// Response from chan.
type Response struct {
	Body []byte
}

// Handler ...
type Handler interface {
	Serve(*Request)
}

// NewMT get new instance.
func NewMT(opts ...Option) *MT {
	instance := &MT{
		log: newLogger(),
	}

	for _, opt := range opts {
		opt(instance)
	}

	return instance
}

// Option for MT.
type Option func(*MT)

// WithAMQP use dsn string.
func WithAMQP(dsn string) Option {
	return func(t *MT) {
		t.dsn = dsn
	}
}

// WithConfig set config.
func WithConfig(config Config) Option {
	return func(t *MT) {
		t.config = config
	}
}

// WithLogger set logger.
func WithLogger(log Logger) Option {
	return func(t *MT) {
		t.log = log
	}
}

type route struct {
	pattern  ServiceName
	handler  HandlerFunc
	consumer *consumer
}

type MT struct {
	dsn        string
	config     Config
	routes     []*route
	connection *amqp.Connection
	destructor sync.Once
	log        Logger
}

func (t *MT) ConnectAndServe() error {
	for _, route := range t.routes {
		if route.pattern == "" {
			return errHandleFuncServiceNameEmpty
		}

		conf := t.config.Services[route.pattern]
		route.consumer = newConsumer(t.log, t.dsn, &conf.Exchange)
		route.consumer.handler = route.handler
	}

	t.announce()

	return nil
}

func (t *MT) announce() {
	for _, route := range t.routes {
		if route.consumer.isDialing {
			continue
		}

		if err := route.consumer.connect(); err != nil {
			t.log.Errorf("failed to connect, %s", err)

			return
		}

		t.log.Debugf(
			"consumer connected to %s",
			route.consumer.options.Queue.Name,
		)

		delivery, err := route.consumer.announce()
		if err != nil {
			t.log.Errorf("failed to announce consumer")
		}

		t.log.Debugf(
			"consumer received a message from %s",
			route.consumer.options.Queue.Name,
		)

		go route.consumer.handle(delivery)
	}
}

func (t *MT) AddHandler(serviceName ServiceName, handler HandlerFunc) {
	r := &route{
		pattern: serviceName,
		handler: handler,
	}
	t.routes = append(t.routes, r)
}

func (t *MT) HealthCheck() bool {
	for _, route := range t.routes {
		if route.consumer.conn == nil || route.consumer.conn.IsClosed() {
			go t.announce()

			return false
		}
	}

	return true
}

const heartbeatInterval = 10

func dial(log Logger, dsn string) (*amqp.Connection, error) {
	maxRetries := 30

	for {
		conn, err := amqp.DialConfig(dsn, amqp.Config{
			Heartbeat: heartbeatInterval * time.Second,
			Locale:    "en_US",
			Properties: amqp.Table{
				"product": "MT client",
				"version": version,
			},
		})
		if err == nil {
			log.Debugf("connection established")

			return conn, nil
		}

		log.Errorf("failed to connect to amqp, %s", err)

		if maxRetries == 0 {
			return nil, fmt.Errorf("failed to connect to amqp, %w", err)
		}
		maxRetries--

		<-time.After(1 * time.Second)
	}
}

func (t *MT) isConnected() bool {
	return t.connection != nil
}

//nolint:funlen,cyclop
func (t *MT) Call(serviceName ServiceName, request Request, reply ReplyFunc) (err error) {
	if !t.isConnected() {
		conn, err := dial(t.log, t.dsn)
		if err != nil {
			return err
		}

		t.connection = conn
	}

	exchange := t.config.Services[serviceName].Exchange

	channel, err := t.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel, %w", err)
	}

	queueDeclareArgs := amqp.Table{}

	if exchange.ReplyQueue.Timeout > 0 {
		num := int64(time.Duration(exchange.ReplyQueue.Timeout) / time.Millisecond)
		queueDeclareArgs = amqp.Table{"x-expires": num}
	}

	queue, err := channel.QueueDeclare("", false, false, true, false, queueDeclareArgs)
	if err != nil {
		return fmt.Errorf("failed to declare queue, %w", err)
	}

	if exchange.ReplyQueue.BindToExchange {
		err = channel.QueueBind(
			queue.Name,
			queue.Name,
			exchange.Name,
			true,
			amqp.Table{},
		)
		if err != nil {
			return fmt.Errorf("%w, %s", errBindQueue, err)
		}
	}

	err = channel.Publish(
		exchange.Name,
		exchange.Binding.Key,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table(request.Header),
			Body:    request.Body,
			ReplyTo: queue.Name,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish a message, %w", err)
	}

	defer func() {
		_, err = channel.QueueDelete(queue.Name, false, false, false)
		if err != nil {
			return
		}

		err = channel.Close()
	}()

	t.log.Debugf("call to queue: %s", queue.Name)

	deliveries, err := getDelivery(channel, queue.Name, Consume{})
	if err != nil {
		t.log.Debugf("failed to consume to queue: %s, %s", queue.Name, err)

		return err
	}

	select {
	case d := <-deliveries:
		return reply(Response{Body: d.Body})
	case <-time.After(time.Duration(exchange.ReplyQueue.Timeout)):
		return err
	}
}

func (t *MT) Cast(serviceName ServiceName, request Request) (err error) {
	if !t.isConnected() {
		conn, err := dial(t.log, t.dsn)
		if err != nil {
			return err
		}

		t.connection = conn
	}

	channel, err := t.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel, %w", err)
	}

	defer func() {
		err = channel.Close()
	}()

	err = channel.Publish(
		t.config.Services[serviceName].Exchange.Name,
		t.config.Services[serviceName].Exchange.Binding.Key,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table(request.Header),
			Body:    request.Body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish a message, %w", err)
	}

	return nil
}

func (t *MT) Shutdown() error {
	t.destructor.Do(func() {
		t.channelCancel()

		maxRetries := 30
		for {
			allCloseConn := false
			for _, r := range t.routes {
				if r.consumer == nil {
					continue
				}
				if r.consumer.conn != nil {
					allCloseConn = true
				}
			}
			t.log.Debugf("Wait consumers %d seconds...", maxRetries)

			if !allCloseConn {
				break
			}

			if maxRetries == 0 {
				break
			}

			<-time.After(1 * time.Second)
			maxRetries--
		}

		if t.connection != nil {
			if err := t.connection.Close(); err != nil {
				t.log.Errorf("failed mt close connections %s", "")
			}
		}
	})

	return nil
}

func (t *MT) channelCancel() {
	for _, route := range t.routes {
		if route.consumer == nil {
			continue
		}

		route.consumer.shutdown = true
		tag := route.consumer.options.Queue.Consumer.Tag

		if route.consumer.channel == nil {
			continue
		}

		if err := route.consumer.channel.Cancel(tag, false); err != nil {
			t.log.Errorf("failed stops deliveries to the consume %s", tag, err)
		}
	}
}
