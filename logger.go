package mt

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
)

const (
	errorLog int = iota
	infoLog
	debugLog
)

var label = []string{
	errorLog: "ERROR",
	infoLog:  "INFO",
	debugLog: "DEBUG",
}

type logger struct {
	loggers []*log.Logger
}

func newLogger() Logger {
	errorW := ioutil.Discard
	infoW := ioutil.Discard
	debugW := ioutil.Discard

	logLevel := os.Getenv("MT_LOG_LEVEL")
	switch logLevel {
	case "", label[errorLog], strings.ToLower(label[errorLog]):
		errorW = os.Stderr
	case label[infoLog], strings.ToLower(label[infoLog]):
		infoW = os.Stderr
		errorW = os.Stderr
	case label[debugLog], strings.ToLower(label[debugLog]):
		debugW = os.Stderr
		infoW = os.Stderr
		errorW = os.Stderr
	}

	return &logger{
		loggers: []*log.Logger{
			log.New(errorW, label[errorLog]+": ", log.LstdFlags),
			log.New(infoW, label[infoLog]+": ", log.LstdFlags),
			log.New(debugW, label[debugLog]+": ", log.LstdFlags),
		},
	}
}

func (l *logger) Infof(format string, args ...interface{}) {
	l.loggers[infoLog].Printf(format, args)
}

func (l *logger) Errorf(format string, args ...interface{}) {
	l.loggers[errorLog].Printf(format, args)
}

func (l *logger) Debugf(format string, args ...interface{}) {
	l.loggers[debugLog].Printf(format, args)
}
