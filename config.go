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
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// ParseConfig read config from json.
func ParseConfig(jsonConf []byte) (Config, error) {
	var conf Config

	err := json.Unmarshal(jsonConf, &conf)
	if err != nil {
		return conf, fmt.Errorf("failed to unmarshal, %w", err)
	}

	return conf, nil
}

// Config ...
type Config struct {
	Services map[ServiceName]Service `json:"services"`
}

type Service struct {
	Exchange Exchange `json:"exchange"`
}

type Exchange struct {
	Name       string     `json:"name,omitempty"`
	Type       string     `json:"type,omitempty"`
	Durable    bool       `json:"durable,omitempty"`
	AutoDelete bool       `json:"autodelete,omitempty"`
	Internal   bool       `json:"internal,omitempty"`
	NoWait     bool       `json:"nowait,omitempty"`
	Arguments  amqp.Table `json:"arguments,omitempty"`

	Binding Binding `json:"binding,omitempty"`
	Queue   Queue   `json:"queue,omitempty"`

	ReplyQueue ReplyQueue `json:"replyqueue,omitempty"`
}

func (e *Exchange) UnmarshalJSON(b []byte) error {
	type xExchange Exchange

	xEx := xExchange(DefaultExchange())

	if err := json.Unmarshal(b, &xEx); err != nil {
		return fmt.Errorf("failed to unmarshal, %w", err)
	}

	*e = Exchange(xEx)

	return nil
}

func DefaultExchange() Exchange {
	return Exchange{
		Name:       "",
		Type:       "",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Arguments:  amqp.Table{},
		Binding:    DefaultBinding(),
		Queue:      DefaultQueue(),
		ReplyQueue: ReplyQueue{
			Timeout: Duration(time.Minute),
		},
	}
}

type Binding struct {
	Key       string     `json:"key,omitempty"`
	NoWait    bool       `json:"nowait,omitempty"`
	Arguments amqp.Table `json:"arguments,omitempty"`
}

func DefaultBinding() Binding {
	return Binding{
		Key:       "",
		NoWait:    false,
		Arguments: amqp.Table{},
	}
}

type Queue struct {
	Name       string     `json:"name,omitempty"`
	Durable    bool       `json:"durable,omitempty"`
	AutoDelete bool       `json:"autodelete,omitempty"`
	NoWait     bool       `json:"nowait,omitempty"`
	Arguments  amqp.Table `json:"arguments,omitempty"`

	// @see http://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
	// Консумер не получит следующие n сообщений, пока не подтвердит предыдущие.
	PrefetchCount int `json:"prefetchCount,omitempty"`

	Consumer Consume `json:"consume,omitempty"`
}

func DefaultQueue() Queue {
	return Queue{
		Name:          "",
		Durable:       true,
		AutoDelete:    false,
		NoWait:        false,
		Arguments:     amqp.Table{},
		PrefetchCount: 0,
		Consumer:      DefaultConsumer(),
	}
}

type Consume struct {
	Tag string `json:"tag,omitempty"`
	// управление сообщением в случае невозможности обработки
	// true - сообщение вновь ставится в очередь
	// false - отбросить сообщение в отстойник, если он настроен. Иначе удалить сообщение
	Requeue   bool       `json:"requeue,omitempty"`
	NoAck     bool       `json:"noack,omitempty"`
	Exclusive bool       `json:"exclusive,omitempty"`
	NoWait    bool       `json:"nowait,omitempty"`
	Arguments amqp.Table `json:"arguments,omitempty"`
}

func DefaultConsumer() Consume {
	return Consume{
		Tag:       uniqueConsumerTag(),
		Requeue:   true,
		NoAck:     false,
		Exclusive: false,
		NoWait:    false,
		Arguments: amqp.Table{},
	}
}

func uniqueConsumerTag() string {
	return "ctag-" + uuid.New().String()
}

type ReplyQueue struct {
	BindToExchange bool     `json:"bindToExchange,omitempty"`
	Timeout        Duration `json:"timeout,omitempty"`
}

type Duration time.Duration

func (e *Duration) UnmarshalJSON(b []byte) error {
	val := ""

	if err := json.Unmarshal(b, &val); err != nil {
		return fmt.Errorf("failed to unmarshal, %w", err)
	}

	dur, err := time.ParseDuration(val)
	if err != nil {
		return fmt.Errorf("failed to parse, %w", err)
	}

	*e = Duration(dur)

	return nil
}
