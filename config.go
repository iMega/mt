package mt

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

type service struct {
	Exchange exchange `json:"exchange"`
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

// Config ...
type Config struct {
	Services map[string]service `json:"services"`
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

	//@see http://www.rabbitmq.com/blog/2012/04/25/rabbitmq-performance-measurements-part-2/
	//Консумер не получит следующие n сообщений, пока не подтвердит предыдущие.
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

// ParseConfig read config from json
func ParseConfig(jsonConf []byte) (Config, error) {
	var conf Config
	err := json.Unmarshal(jsonConf, &conf)
	if err != nil {
		return conf, err
	}
	return conf, nil
}

func uniqueConsumerTag() string {
	uid := uuid.New()

	return "ctag-" + uid.String()
}
