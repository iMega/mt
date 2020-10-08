package acceptance

import (
	"testing"

	"github.com/iMega/mt"
	"github.com/sirupsen/logrus"
)

func Test_UseLogrus(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	logger := logrus.WithField("channel", "mt")

	trasit := mt.NewMT(mt.WithLogger(logger))
	trasit.HandleFunc("handle", func(request *mt.Request) error {
		return nil
	})
}
