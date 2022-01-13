package acceptance

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/imega/mt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"
)

var _ = Describe("Testing handle queue", func() {
	var (
		transit mt.MassTransport
		req     *mt.Request
		forever = make(chan bool)
		confJS  = []byte(`{
			"services":{
				"test_mailer":{
					"exchange":{
						"name": "` + exchangeName + `",
						"type": "direct",
						"durable": true,
						"binding": {
							"key":"` + keyName + `"
						},
						"queue": {
							"name":"` + queueName + `",
							"durable": true
						}
					}
				}
			}
		}`)
	)

	It("remove queue and exchange", func() {
		setup()
	})

	It("create handler", func() {
		conf, err := mt.ParseConfig(confJS)
		Expect(err).NotTo(HaveOccurred())

		transit = mt.NewMT(mt.WithAMQP(getDSN()), mt.WithConfig(conf))
		transit.AddHandler("test_mailer", func(request *mt.Request, reply mt.ReplyFunc) error {
			defer func() { forever <- true }()
			req = request

			return nil
		})

		go func() {
			err := transit.ConnectAndServe()
			Expect(err).NotTo(HaveOccurred())
		}()
	})

	It("send message", func() {
		maxRetries := 60
		for {
			ch, err := conn.Channel()
			Expect(err).NotTo(HaveOccurred())

			err = ch.ExchangeDeclarePassive(
				exchangeName,
				"direct",
				true,
				false,
				false,
				false,
				nil,
			)
			if err == nil {
				break
			}

			if maxRetries == 0 {
				break
			}

			maxRetries--
			<-time.After(time.Duration(1 * time.Second))
		}
		ch, err := conn.Channel()
		Expect(err).NotTo(HaveOccurred())

		<-time.After(time.Duration(300 * time.Millisecond))

		err = ch.Publish(
			"test_mailer_ex",
			"test_mailer_bind",
			false,
			false,
			amqp.Publishing{Body: []byte("qwerty")},
		)
		Expect(err).NotTo(HaveOccurred())
	})

	It("assertion", func() {
		<-forever
		Expect(string(req.Body)).To(Equal("qwerty"))

		err := transit.Shutdown()
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("Testing handle queue and reply", func() {
	var (
		transit mt.MassTransport
		actual  []byte
		confJS  = []byte(`{
			"services":{
				"test_mailer":{
					"exchange":{
						"name": "` + exchangeName + `",
						"type": "direct",
						"durable": true,
						"binding": {
							"key":"` + keyName + `"
						},
						"queue": {
							"name":"` + queueName + `",
							"durable": true
						}
					}
				}
			}
		}`)
	)

	It("remove queue and exchange", func() {
		setup()
	})

	It("create handler", func() {
		conf, err := mt.ParseConfig(confJS)
		Expect(err).NotTo(HaveOccurred())

		transit = mt.NewMT(mt.WithAMQP(getDSN()), mt.WithConfig(conf))
		transit.AddHandler("test_mailer", func(request *mt.Request, reply mt.ReplyFunc) error {
			body := append(request.Body, []byte("_world")...)

			return reply(mt.Response{Body: body})
		})

		go func() {
			err := transit.ConnectAndServe()
			Expect(err).NotTo(HaveOccurred())
		}()
	})

	It("send message", func() {
		<-time.After(time.Second)
		err := transit.Call(
			"test_mailer",
			mt.Request{Body: []byte("hello")},
			func(response mt.Response) error {
				actual = response.Body
				return nil
			},
		)
		Expect(err).NotTo(HaveOccurred())
	})

	It("assertion", func() {
		Expect(string(actual)).To(Equal("hello_world"))

		err := transit.Shutdown()
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("Testing call message", func() {
	var (
		transit mt.MassTransport
		actual  []byte
		confJS  = []byte(`{
			"services":{
				"test_mailer":{
					"exchange":{
						"name": "` + exchangeName + `",
						"type": "direct",
						"binding": {
							"key":"` + keyName + `"
						},
						"queue": {
							"name":"` + queueName + `",
							"durable": true
						}
					}
				}
			}
		}`)
	)

	It("remove queue and exchange", func() {
		setup()
	})

	It("create handler message", func() {

		ch, err := conn.Channel()
		Expect(err).NotTo(HaveOccurred())

		err = ch.ExchangeDeclare(exchangeName, "direct", true, false, false, false, nil)
		Expect(err).NotTo(HaveOccurred())

		queue, err := ch.QueueDeclare(
			queueName, // name
			true,      // durable
			false,     // delete when unused
			false,     // exclusive
			false,     // no-wait
			nil,       // arguments
		)
		Expect(err).NotTo(HaveOccurred())

		err = ch.QueueBind(queueName, keyName, exchangeName, false, nil)
		Expect(err).NotTo(HaveOccurred())

		messages, err := ch.Consume(
			queue.Name, // queue
			"",         // consumer
			true,       // auto-ack
			false,      // exclusive
			false,      // no-local
			false,      // no-wait
			nil,        // args
		)
		Expect(err).NotTo(HaveOccurred())

		go func() {
			select {
			case msg := <-messages:
				err := ch.Publish("", msg.ReplyTo, false, false, amqp.Publishing{Body: msg.Body})
				Expect(err).NotTo(HaveOccurred())
			case <-time.After(10 * time.Second):
				log.Panic("nothing has been transferred")
			}
		}()
	})

	It("call message", func() {
		conf, err := mt.ParseConfig(confJS)
		Expect(err).NotTo(HaveOccurred())

		transit = mt.NewMT(mt.WithAMQP(getDSN()), mt.WithConfig(conf))

		<-time.After(300 * time.Millisecond)

		err = transit.Call(
			"test_mailer",
			mt.Request{Body: []byte("qwerty")},
			func(response mt.Response) error {
				actual = response.Body
				return nil
			},
		)
		Expect(err).NotTo(HaveOccurred())
	})

	It("assertion", func() {
		Expect(string(actual)).To(Equal("qwerty"))

		err := transit.Shutdown()
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("Testing cast message", func() {
	var (
		transit mt.MassTransport
		actual  []byte
		forever = make(chan bool)
		confJS  = []byte(`{
			"services":{
				"test_mailer":{
					"exchange":{
						"name": "` + exchangeName + `",
						"type": "direct",
						"binding": {
							"key":"` + keyName + `"
						},
						"queue": {
							"name":"` + queueName + `",
							"durable": true
						}
					}
				}
			}
		}`)
	)

	It("remove queue and exchange", func() {
		setup()
	})

	It("create handler message", func() {
		ch, err := conn.Channel()
		Expect(err).NotTo(HaveOccurred())

		err = ch.ExchangeDeclare(exchangeName, "direct", false, false, false, false, nil)
		Expect(err).NotTo(HaveOccurred())

		queue, err := ch.QueueDeclare(
			queueName, // name
			true,      // durable
			false,     // delete when unused
			false,     // exclusive
			false,     // no-wait
			nil,       // arguments
		)
		Expect(err).NotTo(HaveOccurred())

		err = ch.QueueBind(queueName, keyName, exchangeName, false, nil)
		Expect(err).NotTo(HaveOccurred())

		messages, err := ch.Consume(
			queue.Name, // queue
			"",         // consumer
			true,       // auto-ack
			false,      // exclusive
			false,      // no-local
			false,      // no-wait
			nil,        // args
		)
		Expect(err).NotTo(HaveOccurred())

		go func() {
			select {
			case msg := <-messages:
				actual = msg.Body
				forever <- true
			case <-time.After(10 * time.Second):
				log.Panic("nothing has been transferred")
			}
		}()
	})

	It("cast message", func() {
		conf, err := mt.ParseConfig(confJS)
		Expect(err).NotTo(HaveOccurred())

		transit = mt.NewMT(mt.WithAMQP(getDSN()), mt.WithConfig(conf))
		err = transit.Cast("test_mailer", mt.Request{Body: []byte("qwerty")})
		Expect(err).NotTo(HaveOccurred())
	})

	It("assertion", func() {
		<-forever
		Expect(string(actual)).To(Equal("qwerty"))

		err := transit.Shutdown()
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = PDescribe("Testing concurrent consumers", func() {
	const QtyMessageLimit = 2000

	var (
		messages       int
		nackedMessages int
		consumers      []mt.MassTransport
		forever        = make(chan bool)
		confJS         = []byte(`{
			"services":{
				"test_mailer":{
					"exchange":{
						"name": "` + exchangeName + `",
						"type": "direct",
						"binding": {
							"key":"` + keyName + `"
						},
						"queue": {
							"name":"` + queueName + `",
							"durable": true,
							"prefetch_count": 20
						}
					}
				}
			}
		}`)
	)

	It("create consumers", func() {
		setup()

		go func() {
			for {
				<-time.After(1 * time.Second)
				fmt.Println("Quantity messages: ", messages, nackedMessages)
				if messages >= QtyMessageLimit+nackedMessages {
					forever <- true
				}
			}
		}()

		conf, err := mt.ParseConfig(confJS)
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 10; i++ {
			transit := mt.NewMT(mt.WithAMQP(getDSN()), mt.WithConfig(conf))
			consumers = append(consumers, transit)
		}

		for _, c := range consumers {
			c.AddHandler("test_mailer", func(request *mt.Request, reply mt.ReplyFunc) error {
				messages++

				r := rand.Intn(10)
				<-time.After(10 * time.Duration(r) * time.Millisecond)

				if r == 0 {
					nackedMessages++
					return errors.New("NACKed message")
				}

				return nil
			})

			go func(c mt.MassTransport) {
				err := c.ConnectAndServe()
				Expect(err).NotTo(HaveOccurred())
			}(c)
		}
	})

	It("send message", func() {
		maxRetries := 60
		for {
			ch, err := conn.Channel()
			Expect(err).NotTo(HaveOccurred())

			err = ch.ExchangeDeclarePassive(exchangeName, "direct", true, false, false, false, nil)
			if err == nil {
				break
			}

			if maxRetries == 0 {
				break
			}

			maxRetries--

			<-time.After(time.Duration(1 * time.Second))
		}
		ch, err := conn.Channel()
		Expect(err).NotTo(HaveOccurred())

		for n := 0; n < QtyMessageLimit; n++ {
			ch.Publish(exchangeName, keyName, false, false, amqp.Publishing{
				Body: []byte("1"),
			})
		}
	})

	It("assertion", func() {
		<-forever
		Expect(messages).To(Equal(QtyMessageLimit + nackedMessages))

		for _, c := range consumers {
			err := c.Shutdown()
			Expect(err).NotTo(HaveOccurred())
		}
	})
})
