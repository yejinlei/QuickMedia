// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package memory is layer L0: bounded rings, wait-free primitives and the
// back-pressure policy that keeps one slow subscriber from stalling a path.
package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Ring errors.
var (
	ErrRingFull   = errors.New("ring full")
	ErrRingClosed = errors.New("ring closed")
)

// MaxRing is the default per-subscriber ring size in units.
const MaxRing = 512

// BoundedRing is a single-producer / single-consumer bounded FIFO.
//
// Every subscriber gets its own ring, which is what makes per-subscriber
// back-pressure possible without a shared lock on the broadcast path. The
// producer is the path's broadcaster goroutine; the consumer is the
// subscriber's read loop.
//
// On overflow the ring drops the *oldest* item, not the newest one. Keeping
// the newest item bounds the wall-clock delay a subscriber sees, which is the
// whole point of the drop-gop policy: a stalled client must not be allowed to
// grow everyone else's latency.
type BoundedRing struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []*RingItem
	capN   int
	closed bool

	slowTicks  atomic.Int64
	dropped    atomic.Int64
	maxFillPct atomic.Int64
}

// RingItem is a queued element plus the timestamps needed for latency metrics.
type RingItem struct {
	Unit      any
	Enqueued  time.Time
	Delivered time.Time
}

// Releaser is implemented by values a ring may have to throw away on its own.
//
// A ring drops its contents in three ways: overflow, and the two head- and
// tail-trim policies. If it did not know how to give a discarded value back,
// every dropped unit would pin its payload buffer until garbage collection,
// which is the exact leak back-pressure is supposed to prevent. The ring does
// not know what a payload is; it only knows that this method releases it.
type Releaser interface{ Release() }

func (r *BoundedRing) releaseValue(v any) {
	if v == nil {
		return
	}
	if rel, ok := v.(Releaser); ok {
		rel.Release()
	}
}

// NewBoundedRing creates a ring of the given capacity, clamped to at least 1
// and at most MaxRing.
func NewBoundedRing(capacity int) *BoundedRing {
	if capacity < 1 {
		capacity = 1
	}
	if capacity > MaxRing {
		capacity = MaxRing
	}
	r := &BoundedRing{items: make([]*RingItem, 0, capacity), capN: capacity}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Push enqueues a unit, dropping the oldest one when the ring is full. It never
// blocks, so the broadcaster cannot stall on a single slow subscriber.
func (r *BoundedRing) Push(unit any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.items) >= r.capN {
		if head := r.items[0]; head != nil {
			r.releaseValue(head.Unit)
			head.Unit = nil
		}
		r.items = r.items[1:]
		r.dropped.Add(1)
	}
	r.items = append(r.items, &RingItem{Unit: unit, Enqueued: time.Now()})
	if p := int64(r.fillPct()); p > r.maxFillPct.Load() {
		r.maxFillPct.Store(p)
	}
	r.cond.Signal()
}

// PushStrict enqueues a unit and returns ErrRingFull when the ring is full and
// the caller cannot afford to lose data. Used by capabilities that must not
// drop media, such as the recorder.
func (r *BoundedRing) PushStrict(unit any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRingClosed
	}
	if len(r.items) >= r.capN {
		return ErrRingFull
	}
	r.items = append(r.items, &RingItem{Unit: unit, Enqueued: time.Now()})
	if p := int64(r.fillPct()); p > r.maxFillPct.Load() {
		r.maxFillPct.Store(p)
	}
	r.cond.Signal()
	return nil
}

// Pop removes the oldest unit, blocking until one is available, the context is
// canceled, or the ring is closed.
//
// A canceled context returns immediately with a nil error, which is the rule
// every read loop in this tree relies on: the caller's ctx is the only way to
// interrupt a blocked read, and it must not look like EOF.
//
// The waiter is a channel rather than a timer because a timer belongs to this
// call alone. Signalling through a channel is what lets the watcher hand a
// cancel to a Pop that already finished waiting, or to none at all, without
// touching a stale timer. The watcher also observes done and exits when the
// call returns, so a busy read loop does not accumulate one goroutine per
// Pop.
func (r *BoundedRing) Pop(ctx context.Context) (*RingItem, error) {
	done := make(chan struct{})
	defer close(done)

	var wakeCh chan struct{}
	if ctx != nil && ctx.Done() != nil {
		// The watcher never takes the ring's mutex itself: this call already
		// holds it by the time it observes the wake signal, so a lock here
		// would deadlock on a cancel that lands mid-Pop. It only broadcasts to
		// readers parked on other Pop calls.
		wakeCh = make(chan struct{}, 1)
		go func() {
			<-ctx.Done()
			r.cond.Broadcast()
			select {
			case wakeCh <- struct{}{}:
			case <-done:
			}
		}()
	}

	// A wake signal already queued when we arrive is checked first. If it is
	// checked after cond.Wait returns, a broadcast that raced the wait would be
	// lost and this call would sit parked until the ring closes.
	if wakeCh != nil {
		select {
		case <-wakeCh:
			return nil, nil
		default:
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for len(r.items) == 0 && !r.closed {
		if wakeCh != nil {
			select {
			case <-wakeCh:
				return nil, nil
			default:
			}
		}
		r.cond.Wait()
	}
	if len(r.items) == 0 {
		return nil, ErrRingClosed
	}
	it := r.items[0]
	r.items = r.items[1:]
	it.Delivered = time.Now()
	return it, nil
}

// PopTimeout bounds the wait of a Pop call.
func (r *BoundedRing) PopTimeout(timeout time.Duration) (*RingItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return r.Pop(ctx)
}

// PopNonBlock returns the oldest unit without waiting.
func (r *BoundedRing) PopNonBlock() (*RingItem, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.items) == 0 {
		return nil, false
	}
	it := r.items[0]
	r.items = r.items[1:]
	it.Delivered = time.Now()
	return it, true
}

// FillPct reports how full the ring is right now, 0-100.
func (r *BoundedRing) FillPct() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fillPct()
}

func (r *BoundedRing) fillPct() int {
	if r.capN == 0 {
		return 0
	}
	return len(r.items) * 100 / r.capN
}

// Len reports the number of queued items.
func (r *BoundedRing) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

// Cap reports the configured capacity.
func (r *BoundedRing) Cap() int { return r.capN }

// Close wakes any blocked consumer and marks the ring unusable.
func (r *BoundedRing) Close() {
	r.mu.Lock()
	r.closed = true
	r.cond.Broadcast()
	r.mu.Unlock()
}

// DropUntil removes items from the head while pred matches. It returns how
// many items were removed. Used by the drop-gop policy: a stalled subscriber's
// queue is advanced to the next keyframe so the client resyncs on a GOP
// boundary instead of mid-GOP.
func (r *BoundedRing) DropUntil(pred func(any) bool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for len(r.items) > 0 && pred(r.items[0]) {
		if r.items[0].Unit != nil {
			r.releaseValue(r.items[0].Unit)
			r.items[0].Unit = nil
			n++
		}
		r.items = r.items[1:]
		r.dropped.Add(1)
	}
	return n
}

// DropToFill removes items from the head until the ring holds at most
// targetPct of its capacity. Used by the drop-toward-key policy, which bounds
// the wall-clock delay a stalled subscriber can accumulate.
func (r *BoundedRing) DropToFill(targetPct int) int {
	if targetPct < 0 {
		targetPct = 0
	}
	if targetPct > 100 {
		targetPct = 100
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for r.capN > 0 && len(r.items)*100/r.capN > targetPct {
		if r.items[0].Unit != nil {
			r.releaseValue(r.items[0].Unit)
			r.items[0].Unit = nil
			n++
		}
		r.items = r.items[1:]
		r.dropped.Add(1)
	}
	return n
}

// DropNewest removes n items from the tail. Used by the pause policy, which
// keeps the stalled subscriber's queued content intact and discards the newest
// arrivals instead, so the subscriber can drain what it already has without
// taking a discontinuity.
func (r *BoundedRing) DropNewest(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	dropped := 0
	for n > 0 && len(r.items) > 0 {
		if tail := r.items[len(r.items)-1]; tail.Unit != nil {
			r.releaseValue(tail.Unit)
			tail.Unit = nil
			dropped++
		}
		r.items = r.items[:len(r.items)-1]
		r.dropped.Add(1)
		n--
	}
	return dropped
}

// Closed reports whether Close has been called.
func (r *BoundedRing) Closed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

// MarkSlow records one tick in which the ring was at capacity. Four
// consecutive ticks is the eviction threshold used by the kernel.
func (r *BoundedRing) MarkSlow() { r.slowTicks.Add(1) }

// ResetSlow clears the slow counter after the ring drains.
func (r *BoundedRing) ResetSlow() { r.slowTicks.Store(0) }

// SlowTicks reports the current number of consecutive slow ticks.
func (r *BoundedRing) SlowTicks() int64 { return r.slowTicks.Load() }

// Dropped reports how many units overflowed the ring.
func (r *BoundedRing) Dropped() int64 { return r.dropped.Load() }

// MaxFillPct reports the peak fill percentage observed since creation.
func (r *BoundedRing) MaxFillPct() int { return int(r.maxFillPct.Load()) }

// ResetSlowAtFull is the eviction rule: a ring whose slow-tick counter has
// reached the threshold must be dropped. Exported as a predicate so the kernel
// and the benchmarks share one definition of "too slow".
func (r *BoundedRing) ShouldEvict(threshold int64) bool {
	return r.slowTicks.Load() >= threshold
}
