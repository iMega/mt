package mt

import (
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const version = "1.0.0"

// MT is a interface.
type MT interface {
	ConnectAndServe() error
	HandleFunc(serviceName string, handler func(*Request) error)
	Call(serviceName string, request Request, res func(response Response)) error
	Cast(serviceName string, request Request) error
	Shutdown() error
	HealthCheck() bool
}

// Request from chan.
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
func NewMT(opts ...Option) MT {
	t := &mt{
		log: newLogger(),
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

// Option for MT.
type Option func(*mt)

// WithAMQP use dsn string.
func WithAMQP(dsn string) Option {
	return func(t *mt) {
		t.dsn = dsn
	}
}

// WithConfig set config.
func WithConfig(config Config) Option {
	return func(t *mt) {
		t.config = config
	}
}

// WithLogger set logger.
func WithLogger(log Logger) Option {
	return func(t *mt) {
		t.log = log
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
	log        Logger
}

func (t *mt) ConnectAndServe() error {
	for _, route := range t.routes {
		conf := t.config.Services[route.pattern]
		route.consumer = newConsumer(t.log, t.dsn, &conf.Exchange)
		route.consumer.handler = route.handler
	}

	t.announce()

	return nil
}

func (t *mt) announce() {
	for _, route := range t.routes {
		if route.consumer.isDialing {
			continue
		}

		if err := route.consumer.connect(); err != nil {
			t.log.Errorf("%s", err)

			return
		}

		t.log.Debugf(
			"consumer connected to %s",
			route.consumer.options.Queue.Name,
		)

		d, err := route.consumer.announce()
		if err != nil {
			t.log.Errorf("failed announce consumer")
		}

		t.log.Debugf(
			"consumer received message from %s",
			route.consumer.options.Queue.Name,
		)

		go route.consumer.handle(d)
	}
}

func (t *mt) HandleFunc(serviceName string, handler func(*Request) error) {
	r := &route{
		pattern: serviceName,
		handler: handler,
	}
	t.routes = append(t.routes, r)
}

func (t *mt) HealthCheck() bool {
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
			return nil, err
		}
		maxRetries--

		<-time.After(1 * time.Second)
	}
}

func (t *mt) isConnected() bool {
	return t.connection != nil
}

const callTimeout = 20

func (t *mt) Call(serviceName string, request Request, res func(response Response)) (err error) {
	if !t.isConnected() {
		conn, err := dial(t.log, t.dsn)
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

	defer func() {
		_, err = ch.QueueDelete(q.Name, false, false, false)
		if err != nil {
			return
		}

		err = ch.Close()
	}()

	t.log.Debugf("call to queue: %s", q.Name)

	deliveries, err := getDelivery(ch, q.Name, consume{})
	if err != nil {
		return err
	}

	select {
	case d := <-deliveries:
		res(Response{Body: d.Body})
	case <-time.After(callTimeout * time.Second):
		return err
	}

	return nil
}

func (t *mt) Cast(serviceName string, request Request) (err error) {
	if !t.isConnected() {
		conn, err := dial(t.log, t.dsn)
		if err != nil {
			return err
		}

		t.connection = conn
	}

	ch, err := t.connection.Channel()
	if err != nil {
		return err
	}

	defer func() {
		err = ch.Close()
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

func (t *mt) channelCancel() {
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
			t.log.Errorf("failed stops deliveries to the consume %s", tag, err)
		}
	}
}
