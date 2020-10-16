package mt

import (
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const version = "0.0.1"

var logmt = newLogger()

// MT is a interface
type MT interface {
	ConnectAndServe() error
	HandleFunc(serviceName string, handler func(*Request) error)
	Call(serviceName string, request Request, res func(response Response)) error
	Cast(serviceName string, request Request) error
	Shutdown() error
}

// Request from chan
type Request struct {
	Header Header
	Body   []byte
}

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

	return h[key].(string)
}

// Response from chan
type Response struct {
	Body []byte
}

// Handler ...
type Handler interface {
	Serve(*Request)
}

// NewMT get new instance
func NewMT(opts ...Option) MT {
	t := &mt{}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Option for MT
type Option func(*mt)

// WithAMQP use dsn string
func WithAMQP(dsn string) Option {
	return func(t *mt) {
		t.dsn = dsn
	}
}

// WithConfig set config
func WithConfig(config Config) Option {
	return func(t *mt) {
		t.config = config
	}
}

// WithLogger set logger
func WithLogger(log Logger) Option {
	return func(t *mt) {
		logmt = log
	}
}

type route struct {
	pattern  string
	handler  func(*Request) error
	consumer *consumer
}

type mt struct {
	dsn        string
	config     Config
	routes     []*route
	connection *amqp.Connection
	destructor sync.Once
}

func (t *mt) ConnectAndServe() error {
	for _, route := range t.routes {
		conf := t.config.Services[route.pattern]
		c := newConsumer(t.dsn, &conf.Exchange)
		route.consumer = c
		c.handler = route.handler

		err := c.connect()
		if err != nil {
			logmt.Errorf("failed connect consumer to amqp, %s", err)
			return err
		}

		logmt.Debugf("consumer connected to %s", c.options.Queue.Name)

		d, err := c.announce()
		if err != nil {
			logmt.Errorf("failed announce consumer")
		}

		logmt.Debugf("consumer received message from %s", c.options.Queue.Name)

		go c.handle(d)
	}

	return nil
}

func (t *mt) HandleFunc(serviceName string, handler func(*Request) error) {
	r := &route{
		pattern: serviceName,
		handler: handler,
	}
	t.routes = append(t.routes, r)
}

func dial(dsn string) (*amqp.Connection, error) {
	maxRetries := 30
	for {
		conn, err := amqp.DialConfig(dsn, amqp.Config{
			Heartbeat: 10 * time.Second,
			Locale:    "en_US",
			Properties: amqp.Table{
				"product": "MT client",
				"version": version,
			},
		})
		if err == nil {
			logmt.Debugf("connection established")
			return conn, nil
		}

		logmt.Errorf("failed to connect to amqp %s", err)

		if maxRetries == 0 {
			return nil, fmt.Errorf("failed connection to amqp, %s", err)
		}
		maxRetries--

		<-time.After(time.Duration(1 * time.Second))
	}
}

func (t *mt) isConnected() bool {
	return t.connection != nil
}

func (t *mt) Call(serviceName string, request Request, res func(response Response)) error {
	if !t.isConnected() {
		conn, err := dial(t.dsn)
		if err != nil {
			return err
		}
		t.connection = conn
	}

	ch, err := t.connection.Channel()
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare("", false, false, true, false, nil)
	if err != nil {
		return err
	}
	err = ch.Publish(
		t.config.Services[serviceName].Exchange.Name,
		t.config.Services[serviceName].Exchange.Binding.Key,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table(request.Header),
			Body:    request.Body,
			ReplyTo: q.Name,
		},
	)
	if err != nil {
		return err
	}

	defer func() error {
		ch.QueueDelete(q.Name, false, false, false)
		err := ch.Close()
		return err
	}()

	logmt.Debugf("call to queue: %s", q.Name)
	deliveries, err := getDelivery(ch, q.Name, consume{})
	if err != nil {
		return err
	}

	select {
	case d := <-deliveries:
		res(Response{Body: d.Body})
	case <-time.After(20 * time.Second):
		return err
	}

	return nil
}

func (t *mt) Cast(serviceName string, request Request) error {
	if !t.isConnected() {
		conn, err := dial(t.dsn)
		if err != nil {
			return err
		}
		t.connection = conn
	}

	ch, err := t.connection.Channel()
	if err != nil {
		return err
	}
	defer func() error {
		err := ch.Close()
		return err
	}()

	return ch.Publish(
		t.config.Services[serviceName].Exchange.Name,
		t.config.Services[serviceName].Exchange.Binding.Key,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table(request.Header),
			Body:    request.Body,
		},
	)
}

func (t *mt) Shutdown() error {
	t.destructor.Do(func() {

		for _, r := range t.routes {
			if r.consumer == nil {
				continue
			}

			r.consumer.shutdown = true
			tag := r.consumer.options.Queue.Consumer.Tag

			if r.consumer.channel == nil {
				continue
			}

			if err := r.consumer.channel.Cancel(tag, false); err != nil {
				logmt.Errorf("failed stops deliveries to the consume %s", tag, err)
			}
		}

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
			logmt.Debugf("Wait consumers %d seconds...", maxRetries)

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
			err := t.connection.Close()
			if err != nil {
				logmt.Errorf("failed mt close connections %s", "")
			}
		}

	})

	return nil
}
