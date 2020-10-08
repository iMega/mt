package acceptance

import (
	"log"
	"time"

	"github.com/iMega/mt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"
)

var _ = Describe("Testing graceful", func() {
	Context("add block", func() {
		var confJS = []byte(`{
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

		conf, err := mt.ParseConfig(confJS)
		Expect(err).NotTo(HaveOccurred())
		transit := mt.NewMT(mt.WithAMQP(getDSN()), mt.WithConfig(conf))

		It("create handler", func() {

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
			Expect(err).NotTo(HaveOccurred())

			ch.Publish("test_mailer_ex", "test_mailer_bind", false, false, amqp.Publishing{
				Body: []byte("qwerty"),
			})

		})

		It("Shutdown server", func() {
			log.Println("Start shutdown")

			err := transit.Shutdown()
			Expect(err).NotTo(HaveOccurred())

			<-time.After(12 * time.Second)
		})
	})
})

// func Test_graceful(t *testing.T) {
// 	setup(t)

// 	confJS := []byte(`{
// 			"services":{
// 				"test_mailer":{
// 					"exchange":{
// 						"name": "test_mailer_ex",
// 						"type": "direct",
// 						"binding": {
// 							"key": "test_mailer_bind"
// 						},
// 						"queue": {
// 							"name": "test_mailer_queue",
// 							"durable": true
// 						}
// 					}
// 				},
// 				"test_mailer_2":{
// 					"exchange":{
// 						"name": "test_mailer_ex_2",
// 						"type": "direct",
// 						"binding": {
// 							"key": "test_mailer_bind_2"
// 						},
// 						"queue": {
// 							"name": "test_mailer_queue_2",
// 							"durable": true
// 						}
// 					}
// 				}
// 			}
// 		}`)
// 	conf, err := mt.ParseConfig(confJS)
// 	assert.NoError(t, err)
// 	transit := mt.NewMT(mt.WithAMQP(getDSN()), mt.WithConfig(conf))

// 	t.Run("Create handler", func(t *testing.T) {
// 		transit.HandleFunc("test_mailer", func(request *mt.Request) error {
// 			<-time.After(10 * time.Second)
// 			return nil
// 		})

// 		transit.HandleFunc("test_mailer_2", func(request *mt.Request) error {
// 			return nil
// 		})

// 		go func() {
// 			err := transit.ConnectAndServe()
// 			assert.NoError(t, err)
// 		}()
// 	})

// 	t.Run("Send message", func(t *testing.T) {

// 		maxRetries := 60
// 		for {
// 			ch, err := conn.Channel()
// 			assert.NoError(t, err)

// 			err = ch.ExchangeDeclarePassive("test_mailer_ex", "direct", false, false, false, false, nil)
// 			if err == nil {
// 				break
// 			}
// 			log.Printf("Failed exchange: %s", err)
// 			if maxRetries == 0 {
// 				break
// 			}
// 			log.Printf("iteration")
// 			maxRetries--
// 			<-time.After(time.Duration(1 * time.Second))
// 		}
// 		ch, err := conn.Channel()
// 		assert.NoError(t, err)

// 		ch.Publish("test_mailer_ex", "test_mailer_bind", false, false, amqp.Publishing{
// 			Body: []byte("qwerty"),
// 		})
// 	})

// 	t.Run("Shutdown server", func(t *testing.T) {
// 		log.Println("Start shutdown")
// 		err := transit.Shutdown()
// 		assert.NoError(t, err)
// 		<-time.After(12 * time.Second)
// 	})
// }
