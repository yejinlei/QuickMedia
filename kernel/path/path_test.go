// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package path

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yejinlei/quickmedia/kernel/stream"
)

// testCfg returns a pointer config, which is what NewPath takes.
func testCfg() *Config {
	cfg := DefaultConfig()
	return &cfg
}

// testTrack builds the one-track table every broadcast test uses. Keeping the
// fixture here rather than in each test makes the tests differ on the
// behaviour they check and not on the setup.
func testTrack() *stream.Track {
	return &stream.Track{ID: 1, Codec: "h264", Kind: stream.KindVideo}
}

// testWriter starts a publisher on a path and cleans it up with the test.
func testWriter(t *testing.T, p *Path, ringSize int) *streamWriter {
	t.Helper()
	w := p.NewWriter([]*stream.Track{testTrack()}, ringSize)
	if err := p.Publish(w); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Cleanup(func() { w.Close(nil) })
	return w
}

// testUnit builds a unit on track 1. It is not pooled: a test writer holds its
// units across a goroutine boundary, so the test keeps ownership plainly
// instead of racing the pool.
func testUnit(seq uint64) *stream.Unit {
	tm := time.Unix(0, 0).UTC().Add(time.Duration(seq) * time.Millisecond)
	return &stream.Unit{
		TrackID:  1,
		Codec:    "h264",
		Kind:     stream.KindVideo,
		Payload:  []byte{0x67, 0x42, 0xc0, 0x0a},
		PTS:      tm,
		DTS:      tm,
		Key:      seq%10 == 0,
		Sequence: seq,
	}
}

// waitFor polls f until it holds, bounding the wait so a regression that never
// converges reports a timeout instead of hanging the test binary.
func waitFor(t *testing.T, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestBroadcastForkPerSubscriber covers the invariant that makes a
// per-subscriber ring an isolation boundary at all.
//
// Two subscribers must never share a stream.Unit. The ring a unit enters owns
// it: it releases what it holds on overflow and on Close, so a second
// subscriber releasing a unit it did not own would drop the reference count to
// zero and hand the pooled payload back to the size-class pool while the first
// subscriber was still reading it. A shared unit also makes BackPressure read
// one subscriber's ring while deciding about another, which is why the stalled
// reader in the next test would never be evicted.
func TestBroadcastForkPerSubscriber(t *testing.T) {
	p := NewPath("fork", testCfg())
	defer p.Close(stream.CancelNone)

	w := testWriter(t, p, 64)

	subA, err := p.Subscribe(64)
	if err != nil {
		t.Fatalf("subscribe A: %v", err)
	}
	defer subA.Close(stream.CancelSubscriber)
	subB, err := p.Subscribe(64)
	if err != nil {
		t.Fatalf("subscribe B: %v", err)
	}
	defer subB.Close(stream.CancelSubscriber)

	const n = 20
	for seq := range uint64(n) {
		u := testUnit(seq)
		if err := w.WriteUnit(u); err != nil {
			t.Fatalf("write %d: %v", seq, err)
		}
		// The broadcaster only ever holds the copies it forks; this test's
		// own reference goes back to the pool here.
		u.Release()
	}

	waitFor(t, 5*time.Second, func() bool {
		return subA.RingFill() == n && subB.RingFill() == n
	})

	uA, err := subA.ReadUnit(context.Background())
	if err != nil {
		t.Fatalf("read A: %v", err)
	}
	uB, err := subB.ReadUnit(context.Background())
	if err != nil {
		t.Fatalf("read B: %v", err)
	}

	// Each copy is a separate allocation holding exactly one reference: the
	// broadcaster releases its own copy once the ring has taken it.
	if uA == uB {
		t.Fatal("two subscribers share one Unit; the second Release would free the first subscriber's buffer")
	}
	if uA.RefCount() < 1 || uB.RefCount() < 1 {
		t.Fatalf("ref counts A=%d B=%d, want 1 1", uA.RefCount(), uB.RefCount())
	}
	if uA.Sequence != uB.Sequence {
		t.Fatalf("sequences A=%d B=%d, want equal", uA.Sequence, uB.Sequence)
	}
	if !bytes.Equal(uA.Payload, uB.Payload) {
		t.Fatalf("payloads diverge: %x vs %x", uA.Payload, uB.Payload)
	}

	// The hazard itself: releasing one subscriber's copy must not empty the
	// other subscriber's payload.
	uA.Release()
	if len(uB.Payload) == 0 {
		t.Fatal("releasing subscriber A's unit emptied subscriber B's payload")
	}
	if uB.RefCount() < 1 {
		t.Fatalf("subscriber B's ref count after A's release = %d, want >= 1", uB.RefCount())
	}
	if !bytes.Equal(uB.Payload, testUnit(0).Payload) {
		t.Fatalf("subscriber B's payload corrupted: %x", uB.Payload)
	}
	uB.Release()
}

// TestBroadcastEvictsStalledSubscriberOnly is the regression for passing the
// shared unit into BackPressure.Apply: the policy must read the stalled
// subscriber's own ring, drop that subscriber, and leave a reader that keeps
// up untouched.
func TestBroadcastEvictsStalledSubscriberOnly(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BackPressure.EvictThreshold = 1
	p := NewPath("slow", &cfg)
	defer p.Close(stream.CancelNone)

	w := testWriter(t, p, 64)

	healthy, err := p.Subscribe(512)
	if err != nil {
		t.Fatalf("subscribe healthy: %v", err)
	}
	defer healthy.Close(stream.CancelSubscriber)
	stalled, err := p.Subscribe(4)
	if err != nil {
		t.Fatalf("subscribe stalled: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			u, err := healthy.ReadUnit(ctx)
			if err != nil {
				return
			}
			u.Release()
		}
	}()

	for seq := range uint64(50) {
		u := testUnit(seq)
		if err := w.WriteUnit(u); err != nil {
			t.Fatalf("write %d: %v", seq, err)
		}
		u.Release()
	}

	waitFor(t, 5*time.Second, func() bool { return stalled.Canceled() })
	if got := stalled.Reason(); got != stream.CancelRingFull {
		t.Fatalf("stalled subscriber reason = %v, want %v", got, stream.CancelRingFull)
	}
	if p.Subscribers() != 1 {
		t.Fatalf("subscribers after eviction = %d, want 1", p.Subscribers())
	}
	cancel()
	if got := healthy.Reason(); got == stream.CancelRingFull {
		t.Fatal("a subscriber that kept up was evicted as if it were stalled")
	}
}

// TestPublishConflictIsFatal pins the single-publisher rule at the path level,
// which is where a 409 becomes a closed writer instead of a leaked connection.
func TestPublishConflictIsFatal(t *testing.T) {
	p := NewPath("conf", testCfg())
	defer p.Close(stream.CancelNone)

	testWriter(t, p, 16)

	// With no readers a republish replaces the publisher inside the retain
	// window, which is the reconnect rule. A reader makes it a real conflict.
	one, err := p.Subscribe(16)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer one.Close(stream.CancelSubscriber)

	second := p.NewWriter([]*stream.Track{testTrack()}, 16)
	if err := p.Publish(second); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting publish = %v, want ErrConflict", err)
	}
	if !second.Closed() {
		t.Fatal("a rejected writer was left open")
	}
}

// TestWriterRejectsUnknownTrack covers the validation the track table exists
// for: a unit whose identifier the publisher never declared must be refused and
// released, not queued into a ring that has no way to describe it.
func TestWriterRejectsUnknownTrack(t *testing.T) {
	p := NewPath("badid", testCfg())
	defer p.Close(stream.CancelNone)

	w := testWriter(t, p, 16)

	u := stream.NewUnit(&stream.Unit{
		TrackID: 99,
		Codec:   "h264",
		Kind:    stream.KindVideo,
		Payload: []byte{0x01},
		PTS:     time.Unix(0, 0).UTC(),
		DTS:     time.Unix(0, 0).UTC(),
	})
	if err := w.WriteUnit(u); err == nil {
		t.Fatal("a unit for an undeclared track was accepted")
	}
	if u.RefCount() != 0 {
		t.Fatalf("rejected unit kept %d references", u.RefCount())
	}
}

// TestSubscribeRequiresPublisher is the rule the manager retries on: a path
// with no publisher cannot hand out a ring that will never be fed.
func TestSubscribeRequiresPublisher(t *testing.T) {
	p := NewPath("idle", testCfg())
	defer p.Close(stream.CancelNone)

	if _, err := p.Subscribe(16); !errors.Is(err, ErrNotPublished) {
		t.Fatalf("subscribe on an idle path = %v, want ErrNotPublished", err)
	}
}
