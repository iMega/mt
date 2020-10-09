package acceptance

import (
	"errors"
	"log"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/streadway/amqp"
)

var _ = BeforeSuite(func() {
	err := waitForSystemUnderTestReady()
	Expect(err).NotTo(HaveOccurred())
})

const (
	queueName    = "test_mailer_queue"
	keyName      = "test_mailer_bind"
	exchangeName = "test_mailer_ex"
)

func TestAcceptance(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Acceptance Suite")
}

func waitForSystemUnderTestReady() error {
	maxRetries := 60

	for {
		co, err := amqp.Dial(getDSN())
		if err == nil {
			conn = co
			break
		}
		log.Printf("ATTEMPTING TO CONNECT")
		if maxRetries == 0 {
			return errors.New("SUT is not ready for tests")
		}
		maxRetries--
		<-time.After(time.Duration(1 * time.Second))
	}

	return nil
}
