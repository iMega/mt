package acceptance

import (
	"time"

	"github.com/iMega/mt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"
)

var _ = Describe("Testing graceful", func() {
	var (
		transit mt.MT
		confJS  = []byte(`{
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
				},
				"test_mailer_2":{
					"exchange":{
						"name": "test_mailer_ex_2",
						"type": "direct",
						"binding": {
							"key": "test_mailer_bind_2"
						},
						"queue": {
							"name": "test_mailer_queue_2",
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

		transit.HandleFunc("test_mailer", func(request *mt.Request) error {
			<-time.After(10 * time.Second)
			return nil
		})

		transit.HandleFunc("test_mailer_2", func(request *mt.Request) error {
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

			err = ch.ExchangeDeclarePassive("test_mailer_ex", "direct", true, false, false, false, nil)
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

		ch.Publish("test_mailer_ex", "test_mailer_bind", false, false, amqp.Publishing{
			Body: []byte("qwerty"),
		})

	})

	It("shutdown server", func() {
		err := transit.Shutdown()
		Expect(err).NotTo(HaveOccurred())
	})
})
