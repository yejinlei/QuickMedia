// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/yejinlei/quickmedia/memory"
)

// DefaultRingSize is the per-subscriber queue depth in units.
const DefaultRingSize = memory.MaxRing

// SubscriptionImpl is the kernel-granted subscription handle.
//
// Every subscriber owns exactly one, which is what makes per-subscriber
// back-pressure possible without a shared lock on the broadcast path. The
// broadcaster only ever calls AddUnit; the subscriber only ever calls
// ReadUnit. Nothing else crosses between them.
type SubscriptionImpl struct {
	path string

	ring *memory.BoundedRing
	ctx  context.Context
	kill context.CancelFunc

	canceled atomic.Bool
	reason   atomic.Value // CancelReason

	unitsRead atomic.Uint64
	bytesRead atomic.Uint64
	lastRead  atomic.Int64 // unixnano of the newest unit read, 0 = none
	closed    atomic.Bool
}

// NewSubscription creates a subscription with a ring of the given size. It is
// the only place a subscription is constructed; adapters receive it and never
// create one.
func NewSubscription(path string, ringSize int) *SubscriptionImpl {
	ctx, kill := context.WithCancel(context.Background())
	s := &SubscriptionImpl{
		path: path,
		ring: memory.NewBoundedRing(ringSize),
		ctx:  ctx,
		kill: kill,
	}
	s.reason.Store(CancelNone)
	return s
}

// Path implements Subscription.
func (s *SubscriptionImpl) Path() string { return s.path }

// ReadUnit implements StreamReader.
//
// It blocks until a unit is available, the caller's context is canceled, or the
// kernel closes the subscription. Closing the ring wakes every blocked reader,
// so no extra watcher goroutine is needed per subscription.
//
// A nil item with a nil error means the caller's context ended: the ring
// reports an interruption that is neither EOF nor a close, and treating it as
// a unit would dereference nothing. It is folded back into the cancel path so a
// read loop that passes its own ctx never sees a panic.
func (s *SubscriptionImpl) ReadUnit(ctx context.Context) (*Unit, error) {
	if s.Canceled() {
		return nil, s.cancelError()
	}

	it, err := s.ring.Pop(ctx)
	if err != nil {
		if s.Canceled() {
			return nil, s.cancelError()
		}
		return nil, ErrEOF
	}
	if it == nil {
		if s.Canceled() {
			return nil, s.cancelError()
		}
		return nil, ErrCanceled
	}
	u := it.Unit.(*Unit)
	s.unitsRead.Add(1)
	s.bytesRead.Add(uint64(len(u.Payload)))
	s.lastRead.Store(time.Now().UnixNano())
	return u, nil
}

// Tracks implements StreamReader. The subscription cannot see the path's track
// table, so callers use the path's description instead; the empty slice keeps
// the method available without leaking kernel internals.
func (s *SubscriptionImpl) Tracks() []*Track { return nil }

// Cancel implements StreamReader.
func (s *SubscriptionImpl) Cancel() {
	s.cancel(CancelSubscriber)
}

// Canceled implements Subscription.
func (s *SubscriptionImpl) Canceled() bool { return s.canceled.Load() }

// Reason implements Subscription.
func (s *SubscriptionImpl) Reason() CancelReason {
	r := s.reason.Load()
	if r == nil {
		return CancelNone
	}
	return r.(CancelReason)
}

// Stats implements Subscription.
func (s *SubscriptionImpl) Stats() SubscriptionStats {
	return SubscriptionStats{
		UnitsRead:      s.unitsRead.Load(),
		UnitsDropped:   uint64(s.ring.Dropped()),
		BytesRead:      s.bytesRead.Load(),
		SlowTicks:      uint64(s.ring.SlowTicks()),
		MaxRingFillPct: s.ring.MaxFillPct(),
	}
}

// AddUnit is the broadcaster's entry point. It never blocks, so one stalled
// subscriber can never stall a path.
func (s *SubscriptionImpl) AddUnit(u *Unit) {
	if s.Canceled() {
		if u != nil {
			u.Release()
		}
		return
	}
	s.ring.Push(u)
}

// FillPct reports how full the subscriber's queue is right now.
func (s *SubscriptionImpl) FillPct() int { return s.ring.FillPct() }

// RingFill reports the queue occupancy in units.
func (s *SubscriptionImpl) RingFill() int { return s.ring.Len() }

// ShouldEvict reports whether the subscriber has exceeded the slow-consumer
// threshold, which is what makes the kernel drop it.
func (s *SubscriptionImpl) ShouldEvict(threshold int64) bool {
	return s.ring.ShouldEvict(threshold)
}

// MarkSlow records one tick in which the subscriber's queue was at capacity.
func (s *SubscriptionImpl) MarkSlow() { s.ring.MarkSlow() }

// ResetSlow clears the slow counter after the queue drains.
func (s *SubscriptionImpl) ResetSlow() { s.ring.ResetSlow() }

// RingDropUntil advances the queue past everything that matches pred. It is
// the drop-gop policy: keep dropping until the head of the queue is decodable
// on its own, so a stalled client resyncs on a keyframe boundary.
func (s *SubscriptionImpl) RingDropUntil(pred func(any) bool) int {
	return s.ring.DropUntil(pred)
}

// RingDropToFill drains the queue toward a fill target.
func (s *SubscriptionImpl) RingDropToFill(targetPct int) int {
	return s.ring.DropToFill(targetPct)
}

// RingDropNewest discards the newest arrivals while leaving the queue intact.
func (s *SubscriptionImpl) RingDropNewest(n int) int {
	return s.ring.DropNewest(n)
}

// Close frees the subscription. It is idempotent and safe to call from any
// goroutine. Closing the ring wakes every reader blocked in ReadUnit, so the
// subscription is released in O(1) regardless of how many readers are stalled.
func (s *SubscriptionImpl) Close(reason CancelReason) {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	s.cancel(reason)
	s.ring.Close()
	s.drain()
}

// cancel marks the subscription canceled. The first reason wins so that a
// diagnostic reader sees the cause, not the consequence.
func (s *SubscriptionImpl) cancel(reason CancelReason) {
	if s.canceled.CompareAndSwap(false, true) {
		s.reason.Store(reason)
	}
	s.kill()
}

func (s *SubscriptionImpl) cancelError() error {
	switch s.Reason() {
	case CancelSubscriber:
		return ErrCanceled
	case CancelNone:
		return ErrCanceled
	default:
		return ErrEOF
	}
}

// drain returns every queued unit to the pool so that a closed subscription
// does not pin payload memory until garbage collection.
func (s *SubscriptionImpl) drain() {
	for {
		it, ok := s.ring.PopNonBlock()
		if !ok {
			return
		}
		if u, isUnit := it.Unit.(*Unit); isUnit && u != nil {
			u.Release()
		}
	}
}

// WaitIdle blocks until the subscription is closed. Test use only.
func (s *SubscriptionImpl) Wait() { <-s.ctx.Done() }

// LastRead reports the wall clock of the newest unit read, or the zero value
// when nothing has been read. Used by the heartbeat eviction rule.
func (s *SubscriptionImpl) LastRead() time.Time {
	n := s.lastRead.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}
