// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// releasedUnit is a stand-in for the kernel's Unit. It implements Releaser and
// counts how many times it was dropped, which is what the ring tests assert.
type releasedUnit struct {
	releases atomic.Int32
	payload  []byte
}

func (u *releasedUnit) Release() { u.releases.Add(1) }

func newUnit(b byte) *releasedUnit {
	return &releasedUnit{payload: []byte{b}}
}

// TestAcquireExactLength pins the contract the whole media plane depends on: a
// payload's length is authoritative, so a caller that asked for four bytes must
// not be handed a 256-byte buffer whose trailing bytes are leftovers from a
// previous owner. Those leftovers would be parsed as media.
func TestAcquireExactLength(t *testing.T) {
	for _, size := range []int{1, 4, 256, 257, 1024, 1025, 4096, 262145} {
		b := AcquireSlice(size)
		if len(b) != size {
			t.Fatalf("AcquireSlice(%d) has len %d", size, len(b))
		}
		if cap(b) < size {
			t.Fatalf("AcquireSlice(%d) has cap %d", size, cap(b))
		}
		ReleaseSlice(&b)
	}
}

// TestReleaseDetaches pins the fix for the aliasing hazard: ReleaseSlice
// returns the buffer to the pool and nils the field. Storing a *[]byte in the
// pool used to keep a pointer at the Unit's own field, which was nilled by the
// very call that stored it, so a later acquire handed back a nil or a reused
// buffer while the former owner still read it.
func TestReleaseDetaches(t *testing.T) {
	want := []byte{0x67, 0x42, 0xc0, 0x0a}
	field := AcquireSlice(len(want))
	copy(field, want)

	ReleaseSlice(&field)
	if field != nil {
		t.Fatal("ReleaseSlice did not clear the caller's field")
	}

	// The pool must be reusable, and must hand back a live buffer rather than
	// the (now nil) field it was released from.
	again := AcquireSlice(len(want))
	if again == nil {
		t.Fatal("pool returned a nil buffer after release")
	}
	if len(again) != len(want) {
		t.Fatalf("reused buffer length = %d, want %d", len(again), len(want))
	}
	ReleaseSlice(&again)
}

// TestUnpooledBuffersAreNotRecycled covers the rule that keeps adapter-owned
// memory safe: a payload that was never acquired is ignored on release.
func TestUnpooledBuffersAreNotRecycled(t *testing.T) {
	b := []byte{1, 2, 3}
	ReleaseSlice(&b)
	if b == nil {
		t.Fatal("a heap-allocated payload was stolen from its owner")
	}
	if len(b) != 3 {
		t.Fatalf("payload length = %d, want 3", len(b))
	}
}

// TestPoolBound caps the pool: once maxSlots buffers of one class are idle the
// next release is dropped, which is what keeps the pool footprint bounded under
// a spike.
func TestPoolBound(t *testing.T) {
	class := SizeClasses[0]

	// Every buffer is held first and released afterwards. Interleaving an
	// acquire with a release makes the pool oscillate between empty and one
	// entry, so it never reaches the cap and the bound goes untested.
	const wanted = maxSlots + 16
	held := make([][]byte, 0, wanted)
	for range wanted {
		b := AcquireSlice(class)
		if b == nil {
			t.Fatal("acquire returned nil")
		}
		held = append(held, b)
	}
	for i := range held {
		ReleaseSlice(&held[i])
	}

	for _, s := range PoolStatsOf() {
		if s.ClassSize != class {
			continue
		}
		if s.Idle != maxSlots {
			t.Fatalf("class %d idle %d, want the %d bound", class, s.Idle, maxSlots)
		}
		if s.Dropped < 1 {
			t.Fatal("an over-capacity buffer was pooled instead of dropped")
		}
		return
	}
	t.Fatalf("class %d not reported", class)
}

// TestRefsLastReference is the single rule reference counting stands on.
func TestRefsLastReference(t *testing.T) {
	r := Refs{}
	r.Retain()
	r.Retain()
	if r.Count() != 2 {
		t.Fatalf("count = %d, want 2", r.Count())
	}
	if r.Release() {
		t.Fatal("released the middle reference as if it were the last")
	}
	if !r.Release() {
		t.Fatal("the last reference did not report itself as last")
	}
}

// TestRingDropsOldestOnOverflow is the latency bound: a slow reader keeps the
// newest media, and the dropped unit must be released rather than pinned.
func TestRingDropsOldestOnOverflow(t *testing.T) {
	r := NewBoundedRing(4)

	for seq := range uint64(4) {
		r.Push(newUnit(byte(seq)))
	}
	if r.Dropped() != 0 {
		t.Fatalf("dropped %d before overflow", r.Dropped())
	}
	r.Push(newUnit(99))
	if r.Dropped() != 1 {
		t.Fatalf("dropped %d on overflow, want 1", r.Dropped())
	}
	if r.Len() != 4 {
		t.Fatalf("len = %d, want 4", r.Len())
	}
	if r.FillPct() != 100 {
		t.Fatalf("fill = %d%%, want 100%%", r.FillPct())
	}

	// The queue is [1,2,3,99]: 0 went, everything after it is in order and the
	// newest is last, which is what the broadcast path is supposed to hand a
	// stalled reader.
	want := []byte{1, 2, 3, 99}
	for i := range want {
		item, err := r.Pop(context.Background())
		if err != nil {
			t.Fatalf("pop %d: %v", i, err)
		}
		if got := item.Unit.(*releasedUnit).payload[0]; got != want[i] {
			t.Fatalf("slot %d = %d, want %d", i, got, want[i])
		}
	}
}

// TestRingCloseWakesReaders confirms a blocked reader cannot stall shutdown:
// Close is the only thing that wakes a reader waiting on an empty ring.
func TestRingCloseWakesReaders(t *testing.T) {
	r := NewBoundedRing(8)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		it, err := r.Pop(ctx)
		done <- err
		if it != nil {
			t.Error("a closed ring returned an item")
		}
	}()
	r.Close()

	select {
	case err := <-done:
		if err != ErrRingClosed {
			t.Fatalf("pop on closed ring = %v, want ErrRingClosed", err)
		}
	case <-ctx.Done():
		t.Fatal("a blocked reader was not woken by Close")
	}
}

// TestRingContextCancellation covers the other escape hatch: the reader's own
// context ends the wait without needing the ring closed.
func TestRingContextCancellation(t *testing.T) {
	r := NewBoundedRing(8)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	it, err := r.Pop(ctx)
	if it != nil {
		t.Fatalf("pop returned item %v", it)
	}
	if err != nil {
		t.Fatalf("pop with a canceled context = %v, want nil", err)
	}
}

// TestRingDropPolicies covers the three queue-reshaping rules the back-pressure
// policy selects between.
func TestRingDropPolicies(t *testing.T) {
	r := NewBoundedRing(16)
	for seq := range uint64(8) {
		r.Push(newUnit(byte(seq)))
	}

	if dropped := r.DropUntil(func(v any) bool { return false }); dropped != 0 {
		t.Fatalf("DropUntil removed %d, want 0", dropped)
	}
	if dropped := r.DropUntil(func(v any) bool { return true }); dropped != 8 {
		t.Fatalf("DropUntil removed %d, want 8", dropped)
	}
	if r.Len() != 0 {
		t.Fatalf("len after DropUntil = %d, want 0", r.Len())
	}

	for seq := range uint64(8) {
		r.Push(newUnit(byte(seq)))
	}
	if dropped := r.DropToFill(25); dropped != 4 {
		t.Fatalf("DropToFill removed %d, want 4", dropped)
	}
	if got := r.FillPct(); got > 25 {
		t.Fatalf("fill after DropToFill = %d%%, want <= 25%%", got)
	}

	for seq := range uint64(4) {
		r.Push(newUnit(byte(seq)))
	}
	if dropped := r.DropNewest(2); dropped != 2 {
		t.Fatalf("DropNewest removed %d, want 2", dropped)
	}
	if got := r.Len(); got != 6 {
		t.Fatalf("len after DropNewest = %d, want 6", got)
	}
}

// TestRingReleasesDiscardedUnits confirms a unit the ring threw away gives its
// payload back, and a unit the ring still holds does not.
func TestRingReleasesDiscardedUnits(t *testing.T) {
	r := NewBoundedRing(2)
	kept := make([]*releasedUnit, 4)
	for i := range 4 {
		kept[i] = newUnit(byte(i))
		r.Push(kept[i])
	}
	if r.Dropped() != 2 {
		t.Fatalf("dropped %d, want 2", r.Dropped())
	}
	if r.Len() != 2 {
		t.Fatalf("len = %d, want 2", r.Len())
	}
	for i := 0; i < 2; i++ {
		if got := kept[i].releases.Load(); got != 1 {
			t.Fatalf("dropped unit %d released %d times, want 1", i, got)
		}
	}
	for i := 2; i < 4; i++ {
		if got := kept[i].releases.Load(); got != 0 {
			t.Fatalf("queued unit %d released %d times, want 0", i, got)
		}
	}
	r.Close()
	// Close only wakes readers; the kernel's drain releases what is left, so a
	// closed ring must still hold its units untouched.
	for i := 2; i < 4; i++ {
		if got := kept[i].releases.Load(); got != 0 {
			t.Fatalf("unit %d released by Close, want 0", i)
		}
	}
}

// TestRingCapacityClamps covers the bounds that keep a bad configuration from
// producing a zero-capacity or unbounded queue.
func TestRingCapacityClamps(t *testing.T) {
	if c := NewBoundedRing(0).Cap(); c != 1 {
		t.Fatalf("cap(0) = %d, want 1", c)
	}
	if c := NewBoundedRing(-5).Cap(); c != 1 {
		t.Fatalf("cap(-5) = %d, want 1", c)
	}
	if c := NewBoundedRing(MaxRing + 1000).Cap(); c != MaxRing {
		t.Fatalf("cap over limit = %d, want %d", c, MaxRing)
	}
}

// TestRingSlowTicks is the eviction signal: four consecutive ticks at capacity
// marks a subscriber for eviction, and draining resets it.
func TestRingSlowTicks(t *testing.T) {
	r := NewBoundedRing(2)
	for range 4 {
		r.MarkSlow()
	}
	if !r.ShouldEvict(4) {
		t.Fatal("four slow ticks did not trip the eviction threshold")
	}
	if r.ShouldEvict(5) {
		t.Fatal("four slow ticks tripped a threshold of five")
	}
	r.ResetSlow()
	if r.SlowTicks() != 0 {
		t.Fatalf("slow ticks after reset = %d", r.SlowTicks())
	}
}

// TestRingConcurrentPushPop drives the ring the way the media plane does: a few
// producers feeding several consumers. It exists so the mutex and the cond
// signalling are exercised at churn, not just in sequence.
func TestRingConcurrentPushPop(t *testing.T) {
	const producers, perProducer = 2, 500
	r := NewBoundedRing(8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var produced atomic.Int64
	var pwg sync.WaitGroup
	for p := range producers {
		pwg.Add(1)
		go func(p int) {
			defer pwg.Done()
			for range perProducer {
				r.Push(newUnit(byte(p)))
				produced.Add(1)
			}
		}(p)
	}

	var consumed atomic.Int64
	var cwg sync.WaitGroup
	for range 4 {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				it, err := r.Pop(ctx)
				if err != nil {
					return
				}
				if it == nil {
					continue
				}
				if _, ok := it.Unit.(*releasedUnit); !ok {
					t.Error("ring held a non-unit")
					return
				}
				consumed.Add(1)
			}
		}()
	}

	pwg.Wait()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && r.Len() > 0 {
		time.Sleep(time.Millisecond)
	}
	r.Close()
	cancel()
	cwg.Wait()

	total := int64(producers * perProducer)
	if got := consumed.Load() + r.Dropped(); got != total {
		t.Fatalf("consumed %d + dropped %d = %d, want %d",
			consumed.Load(), r.Dropped(), got, total)
	}
	if consumed.Load() == 0 {
		t.Fatal("no unit was consumed")
	}
}
