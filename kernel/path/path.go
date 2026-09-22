// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package path is layer L5: the session and stream core.
//
// A Path is one unit of multiplexed media content: one publisher, many
// subscribers, a bounded producer-to-broadcaster ring, and one subscription
// per subscriber. This is the only package in the tree that owns path
// lifecycle; every other package reaches it through Manager or through the
// subscription it was granted.
//
// Locking order, which must never be reversed: Path.mu → Path.subMu →
// streamWriter.mu.
package path

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Sentinel errors.
var (
	ErrConflict     = errors.New("path: publisher conflict (409)")
	ErrInvalidPath  = errors.New("path: invalid path")
	ErrNoSuchPath   = errors.New("path: no such path")
	ErrNotPublished = errors.New("path: not published")
	ErrTooMany      = errors.New("path: at subscription cap")
)

// State is one node of the path state machine:
//
//	idle → publishing → published → idle
//
// The retain window keeps the path alive for cfg.Retain after the publisher
// leaves so that a reconnecting publisher does not race with a dropped
// subscriber.
type State int

const (
	StateIdle State = iota
	StatePublishing
	StatePublished
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StatePublishing:
		return "publishing"
	case StatePublished:
		return "published"
	default:
		return "unknown"
	}
}

// PathStats is a snapshot of one path for the control plane.
type PathStats struct {
	Name         string
	State        State
	Subscribers  int
	UnitsRead    uint64
	BytesRead    uint64
	UnitsDropped uint64
	Tracks       []*stream.Track
	HasPublisher bool
	RetainUntil  time.Time
}

// Path is one unit of multiplexed content: at most one publisher, many
// subscribers, and a bounded broadcast loop from publisher to each subscriber.
//
// A path never owns a protocol. It holds a track table and Units, which is
// what makes "relay this path into any adapter" a property of the registry
// rather than of this type.
type Path struct {
	name string
	cfg  *Config

	mu        sync.Mutex
	state     State
	w         *streamWriter
	retainAt  time.Time
	idleSince time.Time
	closed    bool

	subMu sync.Mutex
	subs  map[*stream.SubscriptionImpl]struct{}
}

func NewPath(name string, cfg *Config) *Path {
	return &Path{name: name, cfg: cfg, state: StateIdle, subs: make(map[*stream.SubscriptionImpl]struct{})}
}

func (p *Path) Name() string { return p.name }

func (p *Path) State() State {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

func (p *Path) IsClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func (p *Path) HasPublisher() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.w != nil
}

func (p *Path) Tracks() []*stream.Track {
	p.mu.Lock()
	w := p.w
	p.mu.Unlock()
	if w == nil {
		return nil
	}
	return w.Tracks()
}

func (p *Path) RetainUntil() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.retainAt
}

// Publish installs a publisher. It is the only transition into StatePublishing,
// which is what makes the single-publisher rule enforceable in exactly one
// place instead of being re-implemented per adapter.
//
// Two replacement cases are allowed, both explicit so that an accidental
// republish is never silently treated as a reconnect:
//   - no readers and the previous publisher left within the retain window;
//   - no readers and the retain window has expired (a fresh start).
//
// Anything else is a hard 409, with the new writer marked closed so the
// adapter does not leak a connection.
func (p *Path) Publish(w *streamWriter) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		w.Close(ErrNoSuchPath)
		return ErrNoSuchPath
	}
	if p.w == nil {
		p.w = w
		p.state = StatePublishing
		p.idleSince = time.Time{}
		p.mu.Unlock()
		return nil
	}

	if p.subsLockedCount() > 0 {
		p.mu.Unlock()
		w.Close(ErrConflict)
		return fmt.Errorf("%w: %s", ErrConflict, p.name)
	}

	if p.retainAt.IsZero() || time.Now().Before(p.retainAt) {
		old := p.w
		p.w = w
		p.state = StatePublishing
		p.idleSince = time.Time{}
		p.mu.Unlock()
		old.Close(ErrConflict)
		return nil
	}

	p.mu.Unlock()
	w.Close(ErrConflict)
	return fmt.Errorf("%w: %s", ErrConflict, p.name)
}

// PublisherLost is the publisher-disconnect signal. It is called from the
// manager when an adapter's source ends, or when Close runs. It is idempotent.
func (p *Path) PublisherLost() {
	p.mu.Lock()
	if p.w == nil || p.closed {
		p.mu.Unlock()
		return
	}
	p.w = nil
	p.state = StateIdle
	p.retainAt = time.Now().Add(p.cfg.Retain)
	p.idleSince = time.Now()
	p.mu.Unlock()
}

func (p *Path) subsLockedCount() int {
	p.subMu.Lock()
	defer p.subMu.Unlock()
	return len(p.subs)
}

func (p *Path) Subscribers() int { return p.subsLockedCount() }

func (p *Path) AddSubscriber(sub *stream.SubscriptionImpl) error {
	p.subMu.Lock()
	if p.closed {
		p.subMu.Unlock()
		sub.Close(stream.CancelPathClosed)
		return ErrNoSuchPath
	}
	if len(p.subs) >= p.cfg.MaxSubscriptions {
		p.subMu.Unlock()
		sub.Close(stream.CancelSubscriber)
		return ErrTooMany
	}
	p.subs[sub] = struct{}{}
	p.subMu.Unlock()

	p.mu.Lock()
	if p.state == StatePublishing {
		p.state = StatePublished
	}
	p.retainAt = time.Time{}
	p.idleSince = time.Time{}
	p.mu.Unlock()
	return nil
}

func (p *Path) RemoveSubscriber(sub *stream.SubscriptionImpl) {
	p.subMu.Lock()
	delete(p.subs, sub)
	n := len(p.subs)
	p.subMu.Unlock()

	p.mu.Lock()
	if p.state == StatePublished && n == 0 {
		p.state = StatePublishing
		if p.w != nil {
			p.retainAt = time.Now().Add(p.cfg.Retain)
			p.idleSince = time.Now()
		}
	}
	p.mu.Unlock()
}

// BroadcastUnit fans a unit out to every subscriber. It never blocks on any
// subscriber, which is the core non-blocking guarantee of the media plane:
// the publisher's latency is independent of the slowest reader.
//
// Every subscriber gets its own Unit, because a ring owns the Units it holds:
// it releases them on overflow and on close, so a unit shared between two
// subscribers would have the second Release return the pooled buffer while the
// first subscriber is still reading it. The copy is cheap — the payload is one
// memcpy into a size-class buffer — and it is what makes a per-subscriber ring
// a real isolation boundary rather than a shared one.
//
// The policy check runs outside subMu so that a policy-driven eviction does not
// re-enter the map while the lock is held. Eviction is decided here rather than
// in the manager sweep so that it happens within one unit of the stall being
// detected, not up to one sweep interval later, and it reads the stalled
// subscriber's own ring rather than a ring the policy never filled.
func (p *Path) BroadcastUnit(u *stream.Unit) {
	p.subMu.Lock()
	subs := make([]*stream.SubscriptionImpl, 0, len(p.subs))
	for sub := range p.subs {
		subs = append(subs, sub)
	}
	p.subMu.Unlock()

	var dropped []*stream.SubscriptionImpl
	for _, sub := range subs {
		if sub.Canceled() {
			dropped = append(dropped, sub)
			continue
		}
		copyU := stream.NewUnit(u)
		if p.cfg.BackPressure.Apply(sub, copyU) {
			copyU.Release()
			dropped = append(dropped, sub)
		} else {
			// The ring takes the reference NewUnit held. Releasing here would
			// leave a zero-reference unit in the ring: the reader would get a
			// unit whose payload had already been handed back to the pool.
			// AddUnit releases it itself if the subscription is canceled in the
			// window between the check above and the push.
			sub.AddUnit(copyU)
		}
	}

	for _, sub := range dropped {
		// Removal must precede Close: a caller that polls the subscriber set
		// must never see one that is already canceled, because eviction is
		// decided by the map rather than by a flag on the subscriber. Closing
		// first leaves a window in which the count still includes a dead
		// reader, which is what makes a load test read one subscriber too many.
		p.RemoveSubscriber(sub)
		sub.Close(stream.CancelRingFull)
	}
}

// ScanSubs invokes fn once per subscriber. fn returning true removes that
// subscriber. Used by the eviction sweep to reap a subscriber that has stopped
// reading, which the fill-based policy inside BroadcastUnit does not catch.
//
// Removal happens inside fn's call rather than afterwards, so a removed
// subscriber cannot be broadcast to while the sweep is still walking:
// BroadcastUnit snapshots the map under the same lock.
func (p *Path) ScanSubs(fn func(*stream.SubscriptionImpl) bool) {
	p.subMu.Lock()
	defer p.subMu.Unlock()
	for sub := range p.subs {
		if fn(sub) {
			delete(p.subs, sub)
		}
	}
}

// Releasable reports whether the manager may reclaim this path: closed, or
// empty (no publisher, no subscribers) and past the retain window.
func (p *Path) Releasable() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return true
	}
	if p.w != nil || p.subsLockedCount() > 0 {
		return false
	}
	// A path that has never held a publisher is waiting for one, not expired.
	// Reaping it would be a race with the advertisement window: an RTMP
	// publisher that needs two seconds of stream time before it commits its
	// track table would find the path gone, closed by the sweep, the moment it
	// finished advertising. retainAt and idleSince are only set by
	// PublisherLost, so their absence means this path never went idle.
	if p.retainAt.IsZero() && p.idleSince.IsZero() {
		return false
	}
	if !p.retainAt.IsZero() && time.Now().Before(p.retainAt) {
		return false
	}
	if !p.idleSince.IsZero() && time.Since(p.idleSince) < p.cfg.Retain {
		return false
	}
	return true
}

// Close removes the path and drops every subscriber. It is idempotent and safe
// to call concurrently with publishing and subscribing.
func (p *Path) Close(reason stream.CancelReason) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	w := p.w
	p.w = nil
	p.state = StateIdle
	p.mu.Unlock()

	p.subMu.Lock()
	subs := make([]*stream.SubscriptionImpl, 0, len(p.subs))
	for sub := range p.subs {
		subs = append(subs, sub)
	}
	p.subs = make(map[*stream.SubscriptionImpl]struct{})
	p.subMu.Unlock()

	for _, sub := range subs {
		sub.Close(reason)
	}
	if w != nil {
		w.Close(nil)
	}
}

// Stats returns a snapshot of the path for the control plane.
func (p *Path) Stats() PathStats {
	p.mu.Lock()
	st := PathStats{
		Name:         p.name,
		State:        p.state,
		HasPublisher: p.w != nil,
		RetainUntil:  p.retainAt,
	}
	if p.w != nil {
		st.UnitsRead = p.w.UnitsWritten()
		st.BytesRead = p.w.BytesWritten()
		st.UnitsDropped = uint64(p.w.Dropped())
		st.Tracks = p.w.Tracks()
	}
	p.mu.Unlock()
	st.Subscribers = p.Subscribers()
	return st
}

// Subscribe claims a subscription on this path, refusing when there is no
// publisher. The manager wraps this in a bounded retry to support the case
// where a client connects before its source exists.
func (p *Path) Subscribe(ringSize int) (*stream.SubscriptionImpl, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrNoSuchPath
	}
	if p.w == nil {
		p.mu.Unlock()
		return nil, ErrNotPublished
	}
	p.mu.Unlock()

	sub := stream.NewSubscription(p.name, ringSize)
	if err := p.AddSubscriber(sub); err != nil {
		return nil, err
	}
	return sub, nil
}

// SubscribeBlocked is Subscribe plus a bounded wait for a publisher to appear.
// timeout <= 0 means try once and fail, which is how the caller opts out of
// blocking in a request handler.
func (p *Path) SubscribeBlocked(ctx context.Context, ringSize int, timeout time.Duration) (stream.Subscription, error) {
	deadline := time.Now().Add(timeout)
	for {
		sub, err := p.Subscribe(ringSize)
		if err == nil {
			return sub, nil
		}
		if !errors.Is(err, ErrNotPublished) {
			return nil, err
		}
		if timeout <= 0 {
			return nil, err
		}
		until := time.Until(deadline)
		if until <= 0 {
			return nil, ErrNotPublished
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(until):
			return nil, ErrNotPublished
		}
	}
}

// NewWriter builds the publisher-side writer for a path from the track table
// an adapter negotiated. Track identifiers are assigned by the caller, which is
// the one place the kernel is responsible for ID assignment: see
// pubSession.Begin.
func (p *Path) NewWriter(tracks []*stream.Track, ringSize int) *streamWriter {
	return newStreamWriter(p, p.name, tracks, ringSize)
}
