// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package clock provides the absolute time base used by the media plane.
//
// Every Unit PTS/DTS in QuickMedia is an absolute time anchored to this
// package's epoch. Sources that do not deliver timestamps get them derived
// here by running the track forward in order, and the derived units are
// flagged so downstream consumers can detect the interpolation boundary.
package clock

import (
	"sync"
	"time"
)

// epochRef is the NTP epoch: 1900-01-01T00:00:00Z.
var epochRef = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

// epochAnchor is the wall clock instant chosen at process start. All absolute
// times are offsets from it, so a restart resets the base, which is correct:
// RTP timestamps restart too.
var epochAnchor = time.Now().UTC()

var anchorMu sync.Mutex

// NTPEpoch converts an NTP timestamp (seconds + fractional seconds as uint32)
// into an absolute time. The fractional part is the standard 2^32 units.
func NTPEpoch(sec uint32, frac uint32) time.Time {
	ns := int64(frac) * 1000000000 / 1 << 32
	dur := time.Duration(int64(sec))*time.Second + time.Duration(ns)
	return epochRef.Add(dur)
}

// FromNTP is the short form of NTPEpoch.
func FromNTP(sec, frac uint32) time.Time { return NTPEpoch(sec, frac) }

// Anchor returns the wall-clock instant the epoch is pinned to.
func Anchor() time.Time { return epochAnchor }

// SetAnchor pins the epoch to a wall-clock instant. Exported so benchmarks and
// tests can reproduce a deterministic timeline.
func SetAnchor(t time.Time) {
	anchorMu.Lock()
	epochAnchor = t.UTC()
	anchorMu.Unlock()
}

// Timebase is the absolute-time state of one track. It runs the track forward
// when a source does not provide timestamps.
type Timebase struct {
	mu       sync.Mutex
	anchor   time.Time // first absolute time observed or derived
	now      time.Time // last assigned DTS
	has      bool
	inferred int64 // units that had to be derived
	disc     int64 // units with an incomplete timestamp
}

// Init anchors the timebase to a specific absolute time.
func (t *Timebase) Init(at time.Time) {
	t.mu.Lock()
	t.anchor = at
	t.now = at
	t.has = true
	t.mu.Unlock()
}

// InitNow anchors the timebase to the current wall clock.
func (t *Timebase) InitNow() { t.Init(time.Now().UTC()) }

// Assign returns the absolute times to give a unit.
//
// When both source times are set they are used directly. Otherwise the track
// is run forward by delta and infer is set, so the caller can mark the unit as
// a discontinuity.
func (t *Timebase) Assign(srcPTS, srcDTS time.Time, delta time.Duration) (PTS, DTS time.Time, infer bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.has {
		t.now = time.Now().UTC()
		t.anchor = t.now
		t.has = true
	}

	if !srcDTS.IsZero() {
		t.now = srcDTS
		if srcPTS.IsZero() {
			t.disc++
			t.now = t.now.Add(delta)
			t.inferred++
			return t.now, t.now, true
		}
		return srcPTS, srcDTS, false
	}

	if !srcPTS.IsZero() {
		// DTS derived from PTS. The kernel does not track B-frames, so PTS and
		// DTS coincide for audio and for video without decode delay.
		t.now = srcPTS
		t.inferred++
		return srcPTS, srcPTS, true
	}

	// fully derived: run the track forward by its frame duration
	t.now = t.now.Add(delta)
	t.inferred++
	return t.now, t.now, true
}

// Now returns the last assigned DTS.
func (t *Timebase) Now() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.now
}

// AnchorOf returns the timebase anchor.
func (t *Timebase) AnchorOf() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.anchor
}

// Inferred returns how many units had their time derived by the kernel.
func (t *Timebase) Inferred() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.inferred
}

// Discontinuities returns how many units arrived with an incomplete timestamp.
func (t *Timebase) Discontinuities() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.disc
}
