package bspgraph

import "github.com/iamleson98/go-search/bspgraph/message"

type Aggregator interface {
	Type() string
	Set(val interface{})
	Get() interface{}
	Aggregate(val interface{})
	Delta() interface{}
}

type Relayer interface {
	Relay(dst string, msg message.Message) error
}

type RelayerFunc func(string, message.Message) error

func (f RelayerFunc) Relay(dst string, msg message.Message) error {
	return f(dst, msg)
}
