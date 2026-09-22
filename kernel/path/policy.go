// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package path

import (
	"time"

	"github.com/yejinlei/quickmedia/kernel/stream"
	"github.com/yejinlei/quickmedia/memory"
)

// Policy selects how the kernel reacts when one subscriber falls behind.
//
// All three policies are non-blocking for the broadcaster. The difference is
// which content is sacrificed, which is a property of the content rather than
// of the client: a stalled client that would rebuffer regardless is better
// served a discontinuity than it is a slowly shrinking queue.
type Policy string

const (
	// PolicyDropGOP drops toward the next keyframe. It is the default because
	// it is the only policy that is correct for every consumer: rebuffering
	// resumes on a decodable picture instead of a mid-GOP reference.
	PolicyDropGOP Policy = "drop-gop"

	// PolicyDropTowardKey drops until the queue has drained to a small fill
	// target, then falls back to drop-gop. It is for clients that must hold a
	// wall-clock budget, which is what a WebRTC peer connection is.
	PolicyDropTowardKey Policy = "drop-toward-key"

	// PolicyPause stops pushing to a stalled subscriber and discards new
	// arrivals instead of the queued ones, so the subscriber can drain what it
	// already has. It is the least lossy option and the only one that ever
	// increases a subscriber's observed delay, which is why it is not default.
	PolicyPause Policy = "pause"
)

// Defaults that match the architecture baseline (L0 in §8 M0): a per-path
// broadcast queue of writerInRing units, a per-subscriber queue of 512 units,
// and eviction after four consecutive ticks at capacity.
const (
	writerInRing     = 256
	subRingSize      = stream.DefaultRingSize
	slowEvictTicks   = 4
	maxSubscriptions = 1000
)

// BackPressure is the per-path back-pressure state shared by every subscriber.
type BackPressure struct {
	Policy Policy
	// DropToPct is the fill target used by drop-toward-key.
	DropToPct int
	// EvictThreshold is the slow-tick count after which a subscriber is dropped.
	EvictThreshold int64
	// MaxSubscriptions caps how many readers one path may feed.
	MaxSubscriptions int
	// WriterInRing bounds the publisher-to-broadcaster channel.
	WriterInRing int
	// SubRingSize is the per-subscriber queue depth.
	SubRingSize int
}

// NewDefaultBackPressure returns the policy set that ships by default.
func NewDefaultBackPressure() BackPressure {
	return BackPressure{
		Policy:           PolicyDropGOP,
		DropToPct:        50,
		EvictThreshold:   slowEvictTicks,
		MaxSubscriptions: maxSubscriptions,
		WriterInRing:     writerInRing,
		SubRingSize:      subRingSize,
	}
}

// clampPolicy rejects unknown policy names instead of failing open. A misspelled
// config key must not silently switch a live path to a different media behavior.
func (b *BackPressure) clampPolicy() {
	switch b.Policy {
	case PolicyDropGOP, PolicyDropTowardKey, PolicyPause:
	default:
		b.Policy = PolicyDropGOP
	}
	if b.EvictThreshold < 1 {
		b.EvictThreshold = slowEvictTicks
	}
	if b.MaxSubscriptions < 1 {
		b.MaxSubscriptions = maxSubscriptions
	}
	if b.WriterInRing < 1 {
		b.WriterInRing = writerInRing
	}
	if b.SubRingSize < 1 {
		b.SubRingSize = subRingSize
	}
}

// Apply re-shapes a stalled subscriber's queue according to the policy. It
// returns true when the subscriber should be evicted now.
//
// The order of the checks is deliberate: eviction is decided from the slow
// ticks, not from the current fill, so that a subscriber which recovered this
// tick but has been chronically slow is still dropped. Resetting the slow
// counter only happens on the else branch.
func (b *BackPressure) Apply(sub *stream.SubscriptionImpl, u *stream.Unit) bool {
	if sub.ShouldEvict(b.EvictThreshold) {
		return true
	}

	filled := sub.FillPct()
	if filled < 50 {
		sub.ResetSlow()
		return false
	}
	sub.MarkSlow()

	switch b.Policy {
	case PolicyDropGOP:
		sub.RingDropUntil(func(v any) bool {
			un, ok := v.(*stream.Unit)
			return ok && !un.Key
		})

	case PolicyDropTowardKey:
		if dropped := sub.RingDropToFill(b.DropToPct); dropped == 0 {
			sub.RingDropUntil(func(v any) bool {
				un, ok := v.(*stream.Unit)
				return ok && !un.Key
			})
		}

	case PolicyPause:
		if filled >= 95 {
			sub.RingDropNewest(1)
		}
	}
	return sub.ShouldEvict(b.EvictThreshold)
}

// Heartbeat is the slow-consumer rule that is independent of fill: a
// subscriber whose queue is not full but which has stopped reading is holding
// a ring slot forever and must be dropped rather than kept.
func (b *BackPressure) Heartbeat(sub *stream.SubscriptionImpl, timeout time.Duration) bool {
	last := sub.LastRead()
	if last.IsZero() {
		return false
	}
	return time.Since(last) > timeout
}

// IsFull reports whether a ring is at capacity, which is the input to the
// slow-tick counter. Kept here rather than inlined so that the eviction rule
// and the benchmark suite read the same predicate.
func (b *BackPressure) IsFull(ring *memory.BoundedRing) bool {
	return ring.Cap() > 0 && ring.Len() >= ring.Cap()
}
