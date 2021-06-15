// Copyright © 2020 Dmitry Stoletov <info@imega.ru>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mt

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
)

// Logger is an interface for logger to pass into mt server.
type Logger interface {
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Debugf(format string, args ...interface{})
}

const (
	errorLog int = iota
	infoLog
	debugLog
)

type logger struct {
	loggers []*log.Logger
}

func newLogger() Logger {
	label := []string{
		errorLog: "ERROR",
		infoLog:  "INFO",
		debugLog: "DEBUG",
	}
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
	l.loggers[infoLog].Printf(format, args...)
}

func (l *logger) Errorf(format string, args ...interface{}) {
	l.loggers[errorLog].Printf(format, args...)
}

func (l *logger) Debugf(format string, args ...interface{}) {
	l.loggers[debugLog].Printf(format, args...)
}
