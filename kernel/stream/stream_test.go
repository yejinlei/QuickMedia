// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package stream

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestNewUnitDoesNotAliasItsArgument is the invariant the whole media plane is
// built on: two subscribers of one path must be able to read their units at
// different times without one of them invalidating the other's payload.
//
// The old implementation replaced the argument's Payload with a pooled buffer,
// which left the caller holding a pointer at a field that the pool then
// handed to whoever asked next.
func TestNewUnitDoesNotAliasItsArgument(t *testing.T) {
	src := &Unit{
		TrackID: 2, Codec: "h264", Kind: KindVideo,
		Payload: []byte{0x67, 0x42, 0xc0, 0x0a}, PTS: time.Unix(10, 0),
		DTS: time.Unix(10, 0), Duration: time.Millisecond, Key: true, Sequence: 7,
		Flags: FlagFirstOfPath,
	}

	out := NewUnit(src)
	if out == src {
		t.Fatal("NewUnit returned its argument")
	}
	if sameBuffer(src.Payload, out.Payload) {
		t.Fatal("NewUnit aliased the argument's payload buffer")
	}
	if !out.Key || out.TrackID != 2 || out.Sequence != 7 || out.Codec != "h264" ||
		out.Kind != KindVideo || out.Flags != FlagFirstOfPath ||
		out.PTS != src.PTS || out.DTS != src.DTS || out.Duration != src.Duration {
		t.Fatalf("NewUnit did not copy metadata: %+v", out)
	}

	// Releasing the copy must not disturb the source at all: the source is not
	// pooled, and neither its payload nor its fields may move.
	out.Release()
	if len(src.Payload) != 4 || src.Payload[0] != 0x67 {
		t.Fatalf("releasing the copy clobbered the source payload: %x", src.Payload)
	}
}

// TestNewUnitRetains pins the reference count NewUnit promises. Every caller of
// NewUnit that hands the result to a ring relies on it already holding one
// reference, so the ring never releases a unit it does not own.
func TestNewUnitRetains(t *testing.T) {
	out := NewUnit(&Unit{Payload: []byte{1, 2, 3}})
	if out.RefCount() != 1 {
		t.Fatalf("NewUnit refcount = %d, want 1", out.RefCount())
	}
	out.Release()
	if out.RefCount() != 0 {
		t.Fatalf("after Release refcount = %d, want 0", out.RefCount())
	}
}

// TestNewUnitCopiesSmallPayloads confirms pooled copies are content-equal and
// independently releasable.
func TestNewUnitCopiesSmallPayloads(t *testing.T) {
	want := []byte{0x21, 0x6f, 0x84, 0x03, 0x80, 0xaa}
	src := &Unit{Payload: want}
	out := NewUnit(src)
	defer out.Release()

	if len(out.Payload) != len(want) {
		t.Fatalf("payload length = %d, want %d", len(out.Payload), len(want))
	}
	for i := range want {
		if out.Payload[i] != want[i] {
			t.Fatalf("payload[%d] = %x, want %x", i, out.Payload[i], want[i])
		}
	}
}

// TestNewUnitLargePayloadIsCopied covers the branch above the pool bound. It is
// the case that used to silently discard the payload: a frame bigger than 256
// KiB arrived at every adapter with an empty payload, which looked like a
// network drop and was not visible in any test that used small fixtures.
func TestNewUnitLargePayloadIsCopied(t *testing.T) {
	big := make([]byte, 256*1024+1)
	big[0], big[len(big)-1] = 0x67, 0xaa
	src := &Unit{Payload: big}

	out := NewUnit(src)
	if out.Payload == nil || len(out.Payload) != len(big) {
		t.Fatalf("payload was dropped: len = %d, want %d", len(out.Payload), len(big))
	}
	if out.Payload[0] != 0x67 || out.Payload[len(out.Payload)-1] != 0xaa {
		t.Fatalf("payload content = %x..%x, want 67..aa",
			out.Payload[0], out.Payload[len(out.Payload)-1])
	}
	if sameBuffer(big, out.Payload) {
		t.Fatal("a large payload shares its buffer with the source")
	}
	out.Release()
	if src.Payload == nil {
		t.Fatal("releasing the copy detached the source payload")
	}
	if src.Payload[0] != 0x67 {
		t.Fatalf("releasing the copy clobbered the source: %x", src.Payload[0])
	}
}

// TestNewUnitEmptyPayloadStaysEmpty documents the one case NewUnit must not
// touch: there is nothing to copy and the unit must still be usable.
func TestNewUnitEmptyPayloadStaysEmpty(t *testing.T) {
	out := NewUnit(&Unit{TrackID: 1, Codec: "aac"})
	if out.Payload != nil {
		t.Fatalf("payload = %v, want nil", out.Payload)
	}
	out.Release()
	if out.RefCount() != 0 {
		t.Fatalf("refcount = %d, want 0", out.RefCount())
	}
}

// sameBuffer reports whether two slices share one backing array. It is the only
// way to say it: slices compare by value, and == is reserved for nil, so a
// pointer compare is the check the aliasing assertions actually need.
func sameBuffer(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return a == nil && b == nil
	}
	return &a[0] == &b[0]
}

// TestRetainReleaseBalances is the unit-level analogue of the pool test: a unit
// may be handed off several times and must only be disposed of once.
func TestRetainReleaseBalances(t *testing.T) {
	u := NewUnit(&Unit{Payload: []byte{1}})
	u.Retain()
	if u.RefCount() != 2 {
		t.Fatalf("refcount = %d, want 2", u.RefCount())
	}
	u.Release()
	if u.RefCount() != 1 {
		t.Fatalf("refcount = %d, want 1", u.RefCount())
	}
	u.Release()
	// The payload is gone after the last reference: that is what keeps a unit
	// the ring dropped from pinning a pooled buffer.
	if u.Payload != nil {
		t.Fatal("the payload survived its last reference")
	}
	if u.RefCount() != 0 {
		t.Fatalf("refcount = %d, want 0", u.RefCount())
	}
}

// TestSubscriptionCancelsBlocks covers the reader half of the kernel contract:
// a read blocks on an empty queue, a Close ends it with ErrEOF, and a reader
// with a canceled context gets ErrCanceled rather than a panic.
func TestSubscriptionCancelsBlocks(t *testing.T) {
	s := NewSubscription("live", 4)
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := s.ReadUnit(ctx)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	s.Close(CancelPublisherGone)

	select {
	case err := <-done:
		if !errors.Is(err, ErrEOF) {
			t.Fatalf("read on a closed subscription = %v, want ErrEOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a blocked reader was not woken by Close")
	}

	// A canceled context is the other way out of the wait, and it must report
	// itself as a cancel rather than as the end of the stream.
	s2 := NewSubscription("live", 4)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s2.ReadUnit(ctx)
	if !errors.Is(err, ErrCanceled) {
		t.Fatalf("read with a canceled context = %v, want ErrCanceled", err)
	}
	s2.Close(CancelSubscriber)
}

// TestSubscriptionDrainsOnClose pins the memory rule: a subscription dropped
// with units still queued must give them to the pool and not to a reader.
func TestSubscriptionDrainsOnClose(t *testing.T) {
	s := NewSubscription("live", 4)
	var live []*Unit
	for i := range 5 {
		u := NewUnit(&Unit{Payload: []byte{byte(i)}})
		s.AddUnit(u)
		live = append(live, u)
	}
	s.Close(CancelPublisherGone)

	// Draining rather than keeping means a reader that races Close gets nothing.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if u, err := s.ReadUnit(ctx); err == nil {
		t.Fatalf("a closed subscription returned a unit %v", u)
	}
	for i, u := range live {
		if got := u.RefCount(); got != 0 {
			t.Fatalf("unit %d still held %d references after Close", i, got)
		}
	}
}

// TestSubscriptionCancelReason is the diagnostic contract: the kernel must
// report the cause of a cancel, not just that one happened.
func TestSubscriptionCancelReason(t *testing.T) {
	s := NewSubscription("live", 4)
	if s.Canceled() {
		t.Fatal("a fresh subscription reports itself canceled")
	}
	if got := s.Reason(); got != CancelNone {
		t.Fatalf("reason = %q, want none", got)
	}

	s.Cancel()
	if !s.Canceled() {
		t.Fatal("Cancel did not mark the subscription canceled")
	}
	if got := s.Reason(); got != CancelSubscriber {
		t.Fatalf("reason = %q, want subscriber", got)
	}
	if s.Path() != "live" {
		t.Fatalf("path = %q, want live", s.Path())
	}
}

// TestSubscriptionReleasesQueuedUnits pins the memory rule for a different
// reason than Close: a subscription dropped by the heartbeat must also give its
// queued frames back. Forgetting this is what made a stalled subscriber leak
// every frame it was fed.
func TestSubscriptionReleasesQueuedUnits(t *testing.T) {
	s := NewSubscription("live", 8)
	var live []*Unit
	for i := range 5 {
		u := NewUnit(&Unit{Payload: []byte{byte(i)}})
		s.AddUnit(u)
		live = append(live, u)
	}
	if got := s.RingFill(); got != 5 {
		t.Fatalf("ring fill = %d, want 5", got)
	}
	s.Close(CancelHeartbeat)
	for i, u := range live {
		if got := u.RefCount(); got != 0 {
			t.Fatalf("unit %d still held %d references after Close", i, got)
		}
	}
}

// TestSubscriptionStatsCounters is the only thing the control plane can see per
// subscriber, so each counter must move in the direction a real reader moves it.
func TestSubscriptionStatsCounters(t *testing.T) {
	s := NewSubscription("live", 8)
	s.AddUnit(NewUnit(&Unit{Payload: []byte{1, 2, 3}}))
	s.AddUnit(NewUnit(&Unit{Payload: []byte{4, 5, 6, 7}}))

	u, err := s.ReadUnit(context.Background())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	u.Release()

	st := s.Stats()
	if st.UnitsRead != 1 {
		t.Fatalf("units read = %d, want 1", st.UnitsRead)
	}
	if st.BytesRead != 3 {
		t.Fatalf("bytes read = %d, want 3", st.BytesRead)
	}
	// 1 of 2 queued units was consumed, and the fill is a percentage of the
	// 8-unit capacity, which is the number a slow-consumer decision reads.
	if got := s.RingFill(); got != 1 {
		t.Fatalf("ring fill = %d, want 1", got)
	}
	if got := s.FillPct(); got != 1*100/8 {
		t.Fatalf("fill pct = %d, want %d", got, 1*100/8)
	}
	if s.LastRead().IsZero() {
		t.Fatal("LastRead did not move")
	}
	s.Close(CancelSubscriber)
}

// TestSubscriptionCancelThenAddReleases covers the race a broadcaster hits: a
// unit that reaches a just-canceled subscription must be released here and not
// sit in a queue nobody will drain.
func TestSubscriptionCancelThenAddReleases(t *testing.T) {
	s := NewSubscription("live", 4)
	s.Cancel()
	u := NewUnit(&Unit{Payload: []byte{1, 2}})
	s.AddUnit(u)
	if got := u.RefCount(); got != 0 {
		t.Fatalf("a unit added to a canceled subscription kept %d references", got)
	}
}

// TestSubscriptionTracksIsDeliberatelyEmpty documents that a subscription cannot
// see a path's track table: adapters must use the session description instead.
func TestSubscriptionTracksIsDeliberatelyEmpty(t *testing.T) {
	s := NewSubscription("live", 4)
	if trks := s.Tracks(); trks != nil {
		t.Fatalf("Tracks = %v, want nil", trks)
	}
	s.Close(CancelSubscriber)
}
