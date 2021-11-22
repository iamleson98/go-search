package pipeline

import "context"

// Payload is implemented by values that can be sent through a pipeline
type Payload interface {
	Clone() Payload
	MarkAsProcessed()
}

type Processor interface {
	Process(context.Context, Payload) (Payload, error)
}

type ProcessorFunc func(context.Context, Payload) (Payload, error)

func (f ProcessorFunc) Process(ctx context.Context, p Payload) (Payload, error) {
	return f(ctx, p)
}

type StageParams interface {
	StageIndex() int
	Input() <-chan Payload
	Output() chan<- Payload
	Error() chan<- error
}

type StageRunner interface {
	Run(context.Context, StageParams)
}

type Source interface {
	Next(context.Context) bool
	Payload() Payload
	Error() error
}

type Sink interface {
	Consume(context.Context, Payload) error
}
