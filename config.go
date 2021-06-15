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

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

// ParseConfig read config from json.
func ParseConfig(jsonConf []byte) (Config, error) {
	var conf Config

	err := json.Unmarshal(jsonConf, &conf)
	if err != nil {
		return conf, err
	}

	return conf, nil
}

// Config ...
type Config struct {
	Services map[string]service `json:"services"`
}

type service struct {
	Exchange exchange `json:"exchange"`
}

type exchange struct {
	Name       string     `json:"name,omitempty"`
	Type       string     `json:"type,omitempty"`
	Durable    bool       `json:"durable,omitempty"`
	AutoDelete bool       `json:"autodelete,omitempty"`
	Internal   bool       `json:"internal,omitempty"`
	NoWait     bool       `json:"nowait,omitempty"`
	Arguments  amqp.Table `json:"arguments,omitempty"`

	Binding binding `json:"binding,omitempty"`
	Queue   queue   `json:"queue,omitempty"`
}

func (e *exchange) UnmarshalJSON(b []byte) error {
	type xExchange exchange

	xEx := xExchange(defaultExchange())

	if err := json.Unmarshal(b, &xEx); err != nil {
		return err
	}

	*e = exchange(xEx)

	return nil
}

func defaultExchange() exchange {
	return exchange{
		Name:       "",
		Type:       "",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Arguments:  amqp.Table{},
		Binding:    defaultBinding(),
		Queue:      defaultQueue(),
	}
}

type binding struct {
	Key       string     `json:"key,omitempty"`
	NoWait    bool       `json:"nowait,omitempty"`
	Arguments amqp.Table `json:"arguments,omitempty"`
}

func defaultBinding() binding {
	return binding{
		Key:       "",
		NoWait:    false,
		Arguments: amqp.Table{},
	}
}

type queue struct {
	Name       string     `json:"name,omitempty"`
	Durable    bool       `json:"durable,omitempty"`
	AutoDelete bool       `json:"autodelete,omitempty"`
	NoWait     bool       `json:"nowait,omitempty"`
	Arguments  amqp.Table `json:"arguments,omitempty"`

	// @see http://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
	// Консумер не получит следующие n сообщений, пока не подтвердит предыдущие.
	PrefetchCount int `json:"prefetch_count,omitempty"`

	Consumer consume `json:"consume,omitempty"`
}

func defaultQueue() queue {
	return queue{
		Name:          "",
		Durable:       true,
		AutoDelete:    false,
		NoWait:        false,
		Arguments:     amqp.Table{},
		PrefetchCount: 0,
		Consumer:      defaultConsumer(),
	}
}

type consume struct {
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

func defaultConsumer() consume {
	return consume{
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
