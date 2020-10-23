package acceptance

import (
	"fmt"
	"os"
	"time"

	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"
)

var conn *amqp.Connection

func getConfigValue(key string) (string, error) {
	value := os.Getenv(key)

	if value == "" {
		return "", fmt.Errorf("value is empty")
	}

	return value, nil
}

type aMQP struct {
	Host string
	Port string
	User string
	Pass string
}

func (a *aMQP) String() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s/", a.User, a.Pass, a.Host, a.Port)
}

func getDSN() string {
	host, err := getConfigValue("AMQP_HOST")
	Expect(err).NotTo(HaveOccurred())

	port, err := getConfigValue("AMQP_PORT")
	Expect(err).NotTo(HaveOccurred())

	user, err := getConfigValue("AMQP_USER")
	Expect(err).NotTo(HaveOccurred())

	pass, err := getConfigValue("AMQP_PASS")
	Expect(err).NotTo(HaveOccurred())

	c := aMQP{
		Host: host,
		Port: port,
		User: user,
		Pass: pass,
	}

	return c.String()
}

func setup() {
	ch, err := conn.Channel()
	Expect(err).NotTo(HaveOccurred())

	_, err = ch.QueueDelete(queueName, false, false, false)
	Expect(err).NotTo(HaveOccurred())

	err = ch.ExchangeDelete(exchangeName, false, false)
	Expect(err).NotTo(HaveOccurred())

	<-time.After(time.Duration(300 * time.Millisecond))
}
