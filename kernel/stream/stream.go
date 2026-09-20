// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package stream defines the frozen media-surface contract of QuickMedia.
//
// The contract below is normative for all layers:
//
//  1. Unit is the only currency of the media plane. It is immutable, reference
//     counted and must never be mutated in place by a consumer.
//  2. Payload carries Elementary-Stream level data only: no container framing,
//     no transport headers.
//  3. PTS/DTS are absolute times anchored to clock.NTPEpoch; zero means "unknown".
//  4. The kernel knows no protocol: nothing in this package may refer to a wire
//     protocol, a container format, or a codec vendor. That rule is enforced by
//     the grep gate in tools/lint.
//  5. Layer L3 (protocol adapters) reaches the kernel exclusively through
//     StreamReader / StreamWriter; it never touches Stream internals.
//  6. Subscribers hold a Subscription handle granted by the kernel. They never
//     own a path.
package stream

import (
	"context"
	"errors"
	"time"

	"github.com/yejinlei/quickmedia/memory"
)

// CodecID identifies an elementary codec. It is opaque to the kernel: no
// value in this package names a codec. Concrete IDs live in the L2 codec
// registry (container package), which is the only layer allowed to reason about
// codec identity.
type CodecID string

// CodecKind classifies a track.
type CodecKind int

const (
	KindVideo CodecKind = iota
	KindAudio
	KindData
)

func (k CodecKind) String() string {
	switch k {
	case KindVideo:
		return "video"
	case KindAudio:
		return "audio"
	case KindData:
		return "data"
	default:
		return "unknown"
	}
}

// UnitFlags carries codec-independent flags that consumers may need.
type UnitFlags uint32

const (
	// FlagDiscontinuity marks a unit produced when the source did not provide a
	// usable timestamp and the kernel had to interpolate PTS by order + duration.
	FlagDiscontinuity UnitFlags = 1 << iota
	FlagConfig
	FlagFirstOfPath
)

// String implements fmt.Stringer.
func (f UnitFlags) String() string {
	switch {
	case f == 0:
		return "none"
	case f&FlagConfig != 0:
		return "config"
	case f&FlagDiscontinuity != 0:
		return "discontinuity"
	case f&FlagFirstOfPath != 0:
		return "first-of-path"
	default:
		return "mixed"
	}
}

// Track describes one media track. Params carries codec-specific configuration
// as key/value pairs (for instance sps / pps as base16 strings) instead of
// growing this struct per codec.
type Track struct {
	ID        TrackID
	Codec     CodecID
	Kind      CodecKind
	Params    map[string]string
	Bandwidth uint64
	Timescale uint64
}

// TrackID indexes a track inside a Stream. It must match the ordering used by
// the kernel so that protocol adapters can resolve a media index into a track.
type TrackID uint32

// Unit is the only currency of the media plane.
//
// It is immutable and reference counted. Consumers must not write to Payload
// and must Release() the unit when done with it. Payload memory comes from the
// size-class pool when it is <= memory.PoolMaxSize, otherwise from the heap.
type Unit struct {
	TrackID  TrackID
	Codec    CodecID
	Kind     CodecKind
	Payload  []byte
	PTS      time.Time // absolute; zero = unknown
	DTS      time.Time // absolute; zero = unknown
	Duration time.Duration
	Key      bool
	Sequence uint64 // monotonically increasing per track
	Flags    UnitFlags

	// pooled records whether Payload came from the size-class pool, so that
	// Release returns it only in the case where QuickMedia owns it.
	pooled bool
	refs   memory.Refs
}

// Retain increments the reference count.
func (u *Unit) Retain() {
	if u == nil {
		return
	}
	u.refs.Retain()
}

// Release decrements the reference count and returns the payload buffer to the
// pool when the last reference goes away.
//
// Only buffers that entered through NewUnit are returned; adapters that attach
// a payload directly keep ownership of it and QuickMedia will not recycle it.
func (u *Unit) Release() {
	if u == nil {
		return
	}
	if u.refs.Release() {
		if u.pooled && u.Payload != nil {
			memory.ReleaseSlice(&u.Payload)
			u.Payload = nil
		}
	}
}

// RefCount returns the current reference count. Test/diagnostic use only.
func (u *Unit) RefCount() int32 { return u.refs.Count() }

// NewUnit returns a unit whose payload is safe to hand out to consumers.
//
// It never mutates its argument. The caller still owns the unit it passed in
// and may keep reading it: if NewUnit replaced the argument's Payload with a
// pooled buffer, that buffer would be handed out again by the next
// AcquireSlice while the caller was still using it. The argument's Payload is
// only read; the result gets its own. That is also what makes BroadcastUnit
// able to fork a unit into several subscriber rings without the argument and
// the copies sharing one buffer.
//
// The payload is always copied. The copy is taken from the size-class pool when
// it fits one and from the heap otherwise, but it is never skipped: a unit with
// an empty payload is media that simply vanished, and a nil payload is not
// distinguishable from no payload. The large-payload branch keeps pooled=false
// so Release does not hand a buffer back that QuickMedia never took from a pool
// in the first place.
func NewUnit(u *Unit) *Unit {
	n := &Unit{
		TrackID:  u.TrackID,
		Codec:    u.Codec,
		Kind:     u.Kind,
		PTS:      u.PTS,
		DTS:      u.DTS,
		Duration: u.Duration,
		Key:      u.Key,
		Sequence: u.Sequence,
		Flags:    u.Flags,
	}
	if u.Payload != nil {
		n.Payload = memory.AcquireSlice(len(u.Payload))
		copy(n.Payload, u.Payload)
		n.pooled = len(n.Payload) <= memory.PoolMaxSize
	}
	n.Retain()
	return n
}

// StreamReader is the kernel-to-adaptor read interface. A nil error and a nil
// unit is not produced; callers must poll with a timeout via context.
type StreamReader interface {
	ReadUnit(ctx context.Context) (*Unit, error)
	Tracks() []*Track
	Cancel()
}

// StreamWriter is the adaptor-to-kernel write interface. Only publishers use it.
type StreamWriter interface {
	WriteUnit(*Unit) error
	Err() error
	Closed() bool
}

// Subscription is the handle the kernel grants to subscribers. It is the only
// lifetime handle an adaptor or capability may keep on a path.
type Subscription interface {
	StreamReader
	// Path returns the name of the subscribed path.
	Path() string
	// Canceled reports whether the subscription was dropped by the kernel
	// (slow consumer) or by the subscriber.
	Canceled() bool
	// Reason returns the kernel-side cancel reason, if any.
	Reason() CancelReason
	// Stats returns counters accumulated since subscription.
	Stats() SubscriptionStats
}

// CancelReason describes why the kernel dropped a subscription.
type CancelReason string

const (
	CancelPublisherGone CancelReason = "publisher-gone"
	CancelRingFull      CancelReason = "ring-full"
	CancelHeartbeat     CancelReason = "heartbeat-timeout"
	CancelPathClosed    CancelReason = "path-closed"
	CancelSubscriber    CancelReason = "subscriber"
	CancelNone          CancelReason = "none"
)

// SubscriptionStats carries per-subscriber counters.
type SubscriptionStats struct {
	UnitsRead      uint64
	UnitsDropped   uint64
	BytesRead      uint64
	SlowTicks      uint64
	MaxRingFillPct int
}

// ErrEOF is returned when the publisher is gone and the retain window expired.
var ErrEOF = errors.New("stream eof")

// ErrCanceled is returned when the subscription was canceled.
var ErrCanceled = errors.New("subscription canceled")

// ErrNoCommonProfile is returned by negotiation when source and sink share no
// supported profile. Adaptors must surface it, never fail silently.
var ErrNoCommonProfile = errors.New("no common profile")
