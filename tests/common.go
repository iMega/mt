package acceptance

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
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
	if err != nil {
		log.Fatalf("failed get value of config, %s", err)
	}

	port, err := getConfigValue("AMQP_PORT")
	if err != nil {
		log.Fatalf("failed get value of config, %s", err)
	}

	user, err := getConfigValue("AMQP_USER")
	if err != nil {
		log.Fatalf("failed get value of config, %s", err)
	}

	pass, err := getConfigValue("AMQP_PASS")
	if err != nil {
		log.Fatalf("failed get value of config, %s", err)
	}

	c := aMQP{
		Host: host,
		Port: port,
		User: user,
		Pass: pass,
	}

	return c.String()
}

func setup(t *testing.T) {
	ch, err := conn.Channel()
	assert.NoError(t, err)

	_, err = ch.QueueDelete("test_mailer_queue", false, false, false)
	assert.NoError(t, err)

	err = ch.ExchangeDelete("test_mailer_ex", false, false)
	assert.NoError(t, err)
}
