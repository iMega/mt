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

// func TestMain(m *testing.M) {
// 	log.Println("Starting tests")

// 	if err := waitForSystemUnderTestReady(); err != nil {
// 		log.Fatalf("Failed to connect to amqp")
// 	}

// 	os.Exit(m.Run())
// }

var _ = BeforeSuite(func() {
	log.Println("Starting tests")

	err := WaitForSystemUnderTestReady()
	Expect(err).NotTo(HaveOccurred())
})

func TestAcceptance(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Acceptance Suite")
}

func waitForSystemUnderTestReady() error {
	maxRetries := 60
	log.Println("Starting tests")

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
