package actor

import (
	"context"
	"fmt"
	"sync"
)

type (
	valueBag context.Context

	MsgRX struct {
		Msg  Msg
		Done bool

		setBaggage func(key, value interface{})
		getBaggage func(key interface{}) interface{}

		setSink func(bool)
	}
	Msg struct {
		bag valueBag

		Data  interface{}
		Error error
	}

	MsgObserver  func(ctx context.Context, rx MsgRX)
	MsgProcessor func(ctx context.Context, rx MsgRX) Msg
)

func (rx MsgRX) SetSink(sink bool) {
	rx.setSink(sink)
}

func (rx MsgRX) SetBaggage(key, value interface{}) {
	rx.setBaggage(key, value)
}

func (rx MsgRX) GetBaggage(key interface{}) interface{} {
	return rx.getBaggage(key)
}

func (o MsgObserver) asProc() MsgProcessor {
	return func(ctx context.Context, rx MsgRX) Msg {
		go func() {
			o(ctx, rx)
		}()
		return rx.Msg
	}
}

type (
	PoolHandleIn struct {
		Output chan<- Msg
		Wait   chan<- struct{}
	}
	PoolHandleOut struct {
		Output <-chan Msg
		Wait   <-chan struct{}
	}
)

func SpawnActorPool(ctx context.Context, n int, actor MsgProcessor, input <-chan Msg) PoolHandleOut {
	writePH, readPH := newPoolHandlePipe(n)
	spawnActorPoolInternal(ctx, n, actor, input, writePH)
	return readPH
}

func (ph PoolHandleIn) Close() {
	close(ph.Output)
	ph.Wait <- struct{}{}
	close(ph.Wait)
}

func (ph PoolHandleOut) Observe(ctx context.Context, n int, actor MsgObserver) PoolHandleOut {
	writePH, readPH := newPoolHandlePipe(n)
	spawnActorPoolInternal(ctx, n, actor.asProc(), ph.Output, writePH)
	return readPH
}

func newPoolHandlePipe(n int) (PoolHandleIn, PoolHandleOut) {
	closer := make(chan struct{}, 1)
	output := make(chan Msg, n)

	return PoolHandleIn{Output: output, Wait: closer}, PoolHandleOut{Output: output, Wait: closer}
}

func spawnActorPoolInternal(ctx context.Context, n int, actor MsgProcessor, input <-chan Msg, outputPH PoolHandleIn) {
	ctx, cancelCtx := context.WithCancel(ctx)

	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range input {
				runActorProcessor(ctx, MsgRX{Msg: msg, Done: false}, actor, outputPH.Output)
			}
		}()
	}

	go func() {
		wg.Wait()
		runActorProcessor(ctx, MsgRX{Msg: Msg{}, Done: true}, actor, nil)
		cancelCtx()
		outputPH.Close()
	}()
}

func runActorProcessor(ctx context.Context, rx MsgRX, actor MsgProcessor, output chan<- Msg) {
	bag := rx.Msg.bag
	if bag == nil {
		bag = context.Background()
	}
	rx.getBaggage = func(key interface{}) interface{} {
		return bag.Value(key)
	}
	rx.setBaggage = func(key, value interface{}) {
		bag = context.WithValue(bag, key, value)
	}

	isSink := false
	rx.setSink = func(b bool) {
		isSink = b
	}

	defer func() {
		r := recover()
		if r != nil {
			err := fmt.Errorf("panic: %v", r)
			if output != nil {
				output <- Msg{
					bag:   bag,
					Error: err,
				}
			}
		}
	}()

	result := actor(ctx, rx)
	if !isSink && output != nil {
		result.bag = bag
		output <- result
	}
}
