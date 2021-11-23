package pipeline

import (
	"context"
	"fmt"
	"sync"
)

type fifo struct {
	proc Processor
}

func FIFO(proc Processor) StageRunner {
	return fifo{proc: proc}
}

func (r fifo) Run(ctx context.Context, params StageParams) {
	for {
		select {
		case <-ctx.Done():
			return
		case payloadIn, ok := <-params.Input():
			if !ok {
				return
			}

			payloadOut, err := r.proc.Process(ctx, payloadIn)
			if err != nil {
				wrappedErr := fmt.Errorf("pipeline stage %d: %w", params.StageIndex(), err)
				maybeEmitError(wrappedErr, params.Error())
				return
			}

			// If the processor did not output a payload for the next stage there is nothing we need to do.
			if payloadOut == nil {
				payloadIn.MarkAsProcessed()
				continue
			}

			select {
			case params.Output() <- payloadOut:
			case <-ctx.Done():
				return
			}
		}
	}
}

type fixedWorkerPool struct {
	fifos []StageRunner
}

func FixedWorkerPool(proc Processor, numWorkers int) StageRunner {
	if numWorkers <= 0 {
		panic("FixedWorkerPool: numWorkers")
	}

	fifos := make([]StageRunner, numWorkers)
	for i := 0; i < numWorkers; i++ {
		fifos[i] = FIFO(proc)
	}

	return &fixedWorkerPool{fifos: fifos}
}

func (p *fixedWorkerPool) Run(ctx context.Context, params StageParams) {
	var wg sync.WaitGroup

	for i := 0; i < len(p.fifos); i++ {
		wg.Add(1)
		go func(fifoIndex int) {
			p.fifos[fifoIndex].Run(ctx, params)
			wg.Done()
		}(i)
	}

	wg.Wait()
}

type dynamicWorkerPool struct {
	proc      Processor
	tokenPool chan struct{}
}

func DynamicWorkerPool(proc Processor, maxWorkers int) StageRunner {
	if maxWorkers <= 0 {
		panic("DynamicWorkerPool: maxWorkers must be > 0")
	}

	tokenPool := make(chan struct{}, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		tokenPool <- struct{}{}
	}

	return &dynamicWorkerPool{proc: proc, tokenPool: tokenPool}
}

func (p *dynamicWorkerPool) Run(ctx context.Context, params StageParams) {
stop:
	for {
		select {
		case <-ctx.Done():
			break stop
		case payloadIn, ok := <-params.Input():
			if !ok {
				break stop
			}

			var token struct{}
			select {
			case token = <-p.tokenPool:
			case <-ctx.Done():
				break stop
			}

			go func(payloadIn Payload, token struct{}) {
				defer func() {
					p.tokenPool <- token
				}()

				payloadOut, err := p.proc.Process(ctx, payloadIn)
				if err != nil {
					wrappedErr := fmt.Errorf("pipeline stage %d: %w", params.StageIndex(), err)
					maybeEmitError(wrappedErr, params.Error())
					return
				}

				if payloadOut == nil {
					payloadIn.MarkAsProcessed()
					return
				}

				select {
				case params.Output() <- payloadOut:
				case <-ctx.Done():
				}
			}(payloadIn, token)
		}
	}

	for i := 0; i < cap(p.tokenPool); i++ {
		<-p.tokenPool
	}
}

type broadcast struct {
	fifos []StageRunner
}

func Broadcast(procs ...Processor) StageRunner {
	if len(procs) == 0 {
		panic("BroadCast: at least one processor must be specified")
	}

	fifos := make([]StageRunner, len(procs))
	for i, p := range procs {
		fifos[i] = FIFO(p)
	}

	return &broadcast{fifos: fifos}
}

func (b *broadcast) Run(ctx context.Context, params StageParams) {
	var (
		wg   sync.WaitGroup
		inCh = make([]chan Payload, len(b.fifos))
	)

	for i := 0; i < len(b.fifos); i++ {
		wg.Add(1)
		inCh[i] = make(chan Payload)

		go func(fifoIndex int) {
			fifoParams := &workerParams{
				stage: params.StageIndex(),
				inCh:  inCh[fifoIndex],
				outCh: params.Output(),
				errCh: params.Error(),
			}
			b.fifos[fifoIndex].Run(ctx, fifoParams)
			wg.Done()
		}(i)
	}

done:
	for {
		select {
		case <-ctx.Done():
			break done
		case payload, ok := <-params.Input():
			if !ok {
				break done
			}
			for i := len(b.fifos) - 1; i >= 0; i-- {
				// As each FIFO might modify the payload, to
				// avoid data races we need to make a copy of
				// the payload for all FIFOs except the first.
				var fifoPayload = payload
				if i != 0 {
					fifoPayload = payload.Clone()
				}
				select {
				case <-ctx.Done():
					break done
				case inCh[i] <- fifoPayload:
					// payload sent to i_th FIFO
				}
			}
		}
	}

	for _, ch := range inCh {
		close(ch)
	}
	wg.Wait()
}
