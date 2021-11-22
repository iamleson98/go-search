package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
)

var _ StageParams = (*workerParams)(nil)

type workerParams struct {
	stage int
	inCh  <-chan Payload
	outCh chan<- Payload
	errCh chan<- error
}

func (p *workerParams) StageIndex() int {
	return p.stage
}

func (p *workerParams) Input() <-chan Payload {
	return p.inCh
}

func (p *workerParams) Output() chan<- Payload {
	return p.outCh
}

func (p *workerParams) Error() chan<- error {
	return p.errCh
}

type Pipeline struct {
	stages []StageRunner
}

func New(stages ...StageRunner) *Pipeline {
	return &Pipeline{
		stages: stages,
	}
}

func (p *Pipeline) Process(ctx context.Context, source Source, sink Sink) error {
	var (
		wg                sync.WaitGroup
		pCtx, ctxCancelFn = context.WithCancel(ctx)
		stageCh           = make([]chan Payload, len(p.stages)+1)
		errCh             = make(chan error, len(p.stages)+2)
	)

	// Allocate channels for wiring together the source, the pipeline stages
	// and the output sink. The output of the i_th stage is used as an input
	// for the i+1_th stage. We need to allocate one extra channel than the
	// number of stages so we can also wire the source/sink.
	for i := 0; i < len(stageCh); i++ {
		stageCh[i] = make(chan Payload)
	}

	for i := 0; i < len(p.stages); i++ {
		wg.Add(1)
		go func(stageIndex int) {
			p.stages[stageIndex].Run(pCtx, &workerParams{
				stage: stageIndex,
				inCh:  stageCh[stageIndex],
				outCh: stageCh[stageIndex+1],
				errCh: errCh,
			})

			close(stageCh[stageIndex+1]) // ???
			wg.Done()
		}(i)
	}

	// start source and sink workers
	wg.Add(2)
	go func() {
		sourceWorker(pCtx, source, stageCh[0], errCh)

		// signal next stage that no more data is available.
		close(stageCh[0])
		wg.Done()
	}()

	go func() {
		sinkWorker(pCtx, sink, stageCh[len(stageCh)-1], errCh)
		wg.Done()
	}()

	// close the error channel once all workers exit
	go func() {
		wg.Wait()
		close(errCh)
		ctxCancelFn()
	}()

	// Collect any emitted errors and wrap then in a multi-error.
	var err error
	for pErr := range errCh {
		err = multierror.Append(err, pErr)
		ctxCancelFn()
	}
	return err
}

func sourceWorker(ctx context.Context, source Source, outCh chan<- Payload, errCh chan<- error) {
	for source.Next(ctx) {
		payload := source.Payload()
		select {
		case outCh <- payload:
		case <-ctx.Done():
			return
		}
	}

	if err := source.Error(); err != nil {
		wrappedErr := fmt.Errorf("pipeline source: %w", err)
		maybeEmitError(wrappedErr, errCh)
	}
}

func sinkWorker(ctx context.Context, sink Sink, inCh <-chan Payload, errCh chan<- error) {
	for {
		select {
		case payload, ok := <-inCh:
			if !ok {
				return
			}

			if err := sink.Consume(ctx, payload); err != nil {
				wrappedErr := fmt.Errorf("pipeline sink: %w", err)
				maybeEmitError(wrappedErr, errCh)
				return
			}
			payload.MarkAsProcessed()
		case <-ctx.Done():
			return
		}
	}
}

func maybeEmitError(err error, errCh chan<- error) {
	select {
	case errCh <- err:
	default:
	}
}
