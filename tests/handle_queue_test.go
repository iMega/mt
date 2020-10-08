package acceptance

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/iMega/mt"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func Test_HandlingQueue(t *testing.T) {
	setup(t)

	var req *mt.Request
	forever := make(chan bool)
	t.Run("Create handler", func(t *testing.T) {
		confJS := []byte(`{
			"services":{
				"test_mailer":{
					"exchange":{
						"name": "test_mailer_ex",
						"type": "direct",
						"binding": {
							"key": "test_mailer_bind"
						},
						"queue": {
							"name": "test_mailer_queue",
							"durable": true
						}
					}
				}
			}
		}`)
		conf, err := mt.ParseConfig(confJS)
		assert.NoError(t, err)
		transit := mt.NewMT(mt.WithAMQP(getDSN()), mt.WithConfig(conf))

		transit.HandleFunc("test_mailer", func(request *mt.Request) error {
			defer func() { forever <- true }()
			req = request
			return nil
		})

		go func() {
			err := transit.ConnectAndServe()
			assert.NoError(t, err)
		}()
	})

	t.Run("Send message", func(t *testing.T) {

		maxRetries := 60
		for {
			ch, err := conn.Channel()
			assert.NoError(t, err)

			err = ch.ExchangeDeclarePassive("test_mailer_ex", "direct", false, false, false, false, nil)
			if err == nil {
				break
			}
			log.Printf("Failed exchange: %s", err)
			if maxRetries == 0 {
				break
			}
			log.Printf("iteration")
			maxRetries--
			<-time.After(time.Duration(1 * time.Second))
		}
		ch, err := conn.Channel()
		assert.NoError(t, err)

		ch.Publish("test_mailer_ex", "test_mailer_bind", false, false, amqp.Publishing{
			Body: []byte("qwerty"),
		})

	})

	<-forever

	t.Run("Assertion", func(t *testing.T) {
		assert.Equal(t, "qwerty", string(req.Body))
	})
}

func Test_CallMessage(t *testing.T) {
	setup(t)

	var actual []byte
	t.Run("Create handler message", func(t *testing.T) {
		ch, err := conn.Channel()
		assert.NoError(t, err)

		err = ch.ExchangeDeclare("test_mailer_ex", "direct", false, false, false, false, nil)
		assert.NoError(t, err)

		queue, err := ch.QueueDeclare(
			"test_mailer_queue", // name
			false,               // durable
			false,               // delete when unused
			false,               // exclusive
			false,               // no-wait
			nil,                 // arguments
		)
		assert.NoError(t, err)

		err = ch.QueueBind("test_mailer_queue", "test_mailer_bind", "test_mailer_ex", false, nil)
		assert.NoError(t, err)

		messages, err := ch.Consume(
			queue.Name, // queue
			"",         // consumer
			true,       // auto-ack
			false,      // exclusive
			false,      // no-local
			false,      // no-wait
			nil,        // args
		)
		assert.NoError(t, err)

		go func() {
			select {
			case msg := <-messages:
				err := ch.Publish("", msg.ReplyTo, false, false, amqp.Publishing{Body: msg.Body})
				assert.NoError(t, err)
			case <-time.After(30 * time.Second):
				log.Panic("nothing has been transferred11")
			}
		}()
	})

	t.Run("Call message", func(t *testing.T) {
		confJS := []byte(`{
			"services":{
				"test_mailer":{
					"exchange":{
						"name": "test_mailer_ex",
						"type": "direct",
						"binding": {
							"key": "test_mailer_bind"
						},
						"queue": {
							"name": "test_mailer_queue",
							"durable": true
						}
					}
				}
			}
		}`)
		conf, err := mt.ParseConfig(confJS)
		assert.NoError(t, err)

		transit := mt.NewMT(mt.WithAMQP(getDSN()), mt.WithConfig(conf))
		err = transit.Call("test_mailer", mt.Request{Body: []byte("qwerty")}, func(response mt.Response) {
			actual = response.Body
		})
		assert.NoError(t, err)
	})

	t.Run("Assertion", func(t *testing.T) {
		assert.Equal(t, "qwerty", string(actual))
	})
}

func Test_CastMessage(t *testing.T) {
	setup(t)

	forever := make(chan bool)
	var actual []byte
	t.Run("Create handler message", func(t *testing.T) {
		ch, err := conn.Channel()
		assert.NoError(t, err)

		err = ch.ExchangeDeclare("test_mailer_ex", "direct", false, false, false, false, nil)
		assert.NoError(t, err)

		queue, err := ch.QueueDeclare(
			"test_mailer_queue", // name
			false,               // durable
			false,               // delete when unused
			false,               // exclusive
			false,               // no-wait
			nil,                 // arguments
		)
		assert.NoError(t, err)

		err = ch.QueueBind("test_mailer_queue", "test_mailer_bind", "test_mailer_ex", false, nil)
		assert.NoError(t, err)

		messages, err := ch.Consume(
			queue.Name, // queue
			"",         // consumer
			true,       // auto-ack
			false,      // exclusive
			false,      // no-local
			false,      // no-wait
			nil,        // args
		)
		assert.NoError(t, err)

		go func() {
			select {
			case msg := <-messages:
				actual = msg.Body
				forever <- true
			case <-time.After(20 * time.Second):
				log.Panic("nothing has been transferred")
			}
		}()
	})

	t.Run("Cast message", func(t *testing.T) {
		confJS := []byte(`{
			"services":{
				"test_mailer":{
					"exchange":{
						"name": "test_mailer_ex",
						"type": "direct",
						"binding": {
							"key": "test_mailer_bind"
						},
						"queue": {
							"name": "test_mailer_queue",
							"durable": true
						}
					}
				}
			}
		}`)
		conf, err := mt.ParseConfig(confJS)
		assert.NoError(t, err)

		transit := mt.NewMT(mt.WithAMQP(getDSN()), mt.WithConfig(conf))
		err = transit.Cast("test_mailer", mt.Request{Body: []byte("qwerty")})
		assert.NoError(t, err)
	})

	<-forever

	t.Run("Assertion", func(t *testing.T) {
		assert.Equal(t, "qwerty", string(actual))
	})
}

func Test_ConcurrentConsumers(t *testing.T) {
	var (
		messages       int
		nackedMessages int
		consumers      []mt.MT
		forever        = make(chan bool)
	)

	const QtyMessageLimit = 1000

	setup(t)

	t.Run("create consumers", func(t *testing.T) {
		go func() {
			for {
				<-time.After(1 * time.Second)
				fmt.Println(messages)
				if messages >= QtyMessageLimit+nackedMessages {
					forever <- true
				}
			}
		}()

		confJS := []byte(`{
			"services":{
				"test_mailer":{
					"exchange":{
						"name": "test_mailer_ex",
						"type": "direct",
						"binding": {
							"key": "test_mailer_bind"
						},
						"queue": {
							"name": "test_mailer_queue",
							"durable": true,
							"prefetch_count": 20
						}
					}
				}
			}
		}`)
		conf, err := mt.ParseConfig(confJS)
		assert.NoError(t, err)

		for i := 0; i < 10; i++ {
			transit := mt.NewMT(mt.WithAMQP(getDSN()), mt.WithConfig(conf))
			consumers = append(consumers, transit)
		}

		for n, c := range consumers {
			c.HandleFunc("test_mailer", func(request *mt.Request) error {
				messages++
				r := rand.Intn(10)
				<-time.After(10 * time.Duration(r) * time.Millisecond)
				fmt.Println(n, "---", messages)

				if r == 0 {
					nackedMessages++
					return errors.New("NACKed message")
				}
				return nil
			})

			go func() {
				err := c.ConnectAndServe()
				assert.NoError(t, err)
			}()
		}

	})

	t.Run("Send message", func(t *testing.T) {

		maxRetries := 60
		for {
			ch, err := conn.Channel()
			assert.NoError(t, err)

			err = ch.ExchangeDeclarePassive("test_mailer_ex", "direct", false, false, false, false, nil)
			if err == nil {
				break
			}
			log.Printf("Failed exchange: %s", err)
			if maxRetries == 0 {
				break
			}
			log.Printf("iteration")
			maxRetries--
			<-time.After(time.Duration(1 * time.Second))
		}
		ch, err := conn.Channel()
		assert.NoError(t, err)

		for n := 0; n < QtyMessageLimit; n++ {
			ch.Publish("test_mailer_ex", "test_mailer_bind", false, false, amqp.Publishing{
				Body: []byte("1"),
			})
		}

	})

	<-forever

	t.Run("Assertion", func(t *testing.T) {
		assert.Equal(t, QtyMessageLimit+nackedMessages, messages)
	})
}
