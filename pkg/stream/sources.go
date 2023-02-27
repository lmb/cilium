// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of Cilium

package stream

import (
	"context"
	"errors"
	"sync"

	"github.com/cilium/cilium/pkg/lock"
)

// Just creates an observable with a single item.
func Just[T any](item T) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T), complete func(error)) {
			go func() {
				if err := ctx.Err(); err != nil {
					complete(err)
				} else {
					next(item)
					complete(nil)
				}
			}()
		})
}

// Stuck creates an observable that never emits anything and
// just waits for the context to be cancelled.
// Mainly meant for testing.
func Stuck[T any]() Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T), complete func(error)) {
			go func() {
				<-ctx.Done()
				complete(ctx.Err())
			}()
		})
}

// Error creates an observable that fails immediately with given error.
func Error[T any](err error) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T), complete func(error)) {
			go complete(err)
		})
}

// Empty creates an empty observable that completes immediately.
func Empty[T any]() Observable[T] {
	return Error[T](nil)
}

// FromSlice converts a slice into an Observable.
func FromSlice[T any](items []T) Observable[T] {
	// Emit items in chunks to reduce overhead of mutex in ctx.Err().
	const chunkSize = 64
	return FuncObservable[T](
		func(ctx context.Context, next func(T), complete func(error)) {
			go func() {
				for chunk := 0; chunk < len(items); chunk += chunkSize {
					if err := ctx.Err(); err != nil {
						complete(err)
						return
					}
					for i := chunk; i < len(items) && i < chunk+chunkSize; i++ {
						next(items[i])
					}
				}
				complete(nil)
			}()
		})
}

// FromChannel creates an observable from a channel. The channel is consumed
// by the first observer.
func FromChannel[T any](in <-chan T) Observable[T] {
	return FuncObservable[T](
		func(ctx context.Context, next func(T), complete func(error)) {
			go func() {
				done := ctx.Done()
				for {
					select {
					case <-done:
						complete(ctx.Err())
						return
					case v, ok := <-in:
						if !ok {
							complete(nil)
							return
						}
						next(v)
					}
				}
			}()
		})
}

// Range creates an observable that emits integers in range from...to-1.
func Range(from, to int) Observable[int] {
	return FuncObservable[int](
		func(ctx context.Context, next func(int), complete func(error)) {
			go func() {
				for i := from; i < to; i++ {
					if ctx.Err() != nil {
						break
					}
					next(i)
				}
				complete(ctx.Err())
			}()
		})
}

type mcastSubscriber[T any] struct {
	next     func(T)
	complete func()
}

type MulticastOpt func(o *mcastOpts)

type mcastOpts struct {
	emitLatest bool
}

func (o mcastOpts) apply(opts []MulticastOpt) mcastOpts {
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Multicast options
var (
	// Emit the latest seen item when subscribing.
	EmitLatest = func(o *mcastOpts) { o.emitLatest = true }
)

// Multicast creates an observable that "multicasts" the emitted items to all observers.
func Multicast[T any](opts ...MulticastOpt) (mcast Observable[T], next func(T), complete func(error)) {
	var (
		mu          lock.Mutex
		subId       int
		subs        = make(map[int]mcastSubscriber[T])
		latestValue T
		completed   bool
		completeErr error
		haveLatest  bool
		opt         = mcastOpts{}.apply(opts)
	)

	next = func(item T) {
		mu.Lock()
		defer mu.Unlock()
		if completed {
			return
		}
		if opt.emitLatest {
			latestValue = item
			haveLatest = true
		}
		for _, sub := range subs {
			sub.next(item)
		}
	}

	complete = func(err error) {
		mu.Lock()
		defer mu.Unlock()
		completed = true
		completeErr = err
		for _, sub := range subs {
			sub.complete()
		}
		subs = nil
	}

	mcast = FuncObservable[T](
		func(ctx context.Context, subNext func(T), subComplete func(error)) {
			mu.Lock()
			if completed {
				mu.Unlock()
				go subComplete(completeErr)
				return
			}

			subCtx, cancel := context.WithCancel(ctx)
			thisId := subId
			subId++
			subs[thisId] = mcastSubscriber[T]{
				subNext,
				cancel,
			}

			// Continue subscribing asynchronously so caller is not blocked.
			go func() {
				if opt.emitLatest && haveLatest {
					subNext(latestValue)
				}
				mu.Unlock()

				// Wait for cancellation by observer, or completion from upstream.
				<-subCtx.Done()

				// Remove the observer and complete.
				var err error
				mu.Lock()
				delete(subs, thisId)
				if completed {
					err = completeErr
				} else {
					err = subCtx.Err()
				}
				mu.Unlock()
				subComplete(err)
			}()
		})

	return
}

// Unicast allows a single observer to subscribe to events.
//
// It has less overhead than [Multicast] for cases where fan-out is not required.
type Unicast[T any] struct {
	mu          sync.Mutex
	ctx         context.Context
	subNext     func(T)
	subComplete func(error)
}

func (u *Unicast[T]) Observe(ctx context.Context, next func(T), complete func(error)) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.ctx != nil {
		complete(errors.New("already being observed"))
		return
	}

	u.ctx = ctx
	u.subNext = next
	u.subComplete = complete
}

func (u *Unicast[T]) Next(item T) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.ctx == nil {
		return
	}

	select {
	case <-u.ctx.Done():
		u.subComplete(u.ctx.Err())
		u.ctx, u.subNext, u.subComplete = nil, nil, nil
		return
	default:
	}

	u.subNext(item)
}

func (u *Unicast[T]) Complete(err error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.ctx == nil {
		return
	}

	select {
	case <-u.ctx.Done():
		err = u.ctx.Err()
	default:
	}

	u.subComplete(err)
	u.ctx, u.subNext, u.subComplete = nil, nil, nil
}
