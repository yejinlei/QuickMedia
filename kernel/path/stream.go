// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package path

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yejinlei/quickmedia/kernel/stream"
)

// streamWriter is the L5 side of the publisher contract: the only object that
// can move a path from idle toward published, and the only thing a publisher
// writes into.
//
// Its input is a bounded channel, not an unbounded queue. A publisher that
// outpaces the broadcast worker is told so explicitly (stream.ErrCanceled)
// rather than being allowed to grow memory without limit, which is the same
// back-pressure rule applied at the other end of the path.
type streamWriter struct {
	mu     sync.Mutex
	name   string
	err    error
	closed bool
	in     chan *stream.Unit
	done   chan struct{}

	owner   *Path
	ownerMu sync.RWMutex

	tracksMu sync.RWMutex
	trs      map[stream.TrackID]*stream.Track
	trsOrder []*stream.Track
	seq      map[stream.TrackID]uint64

	written atomic.Uint64
	bytes   atomic.Uint64
	dropped atomic.Uint64
}

func (w *streamWriter) WriteUnit(u *stream.Unit) error {
	if u == nil {
		return errors.New("path: nil unit")
	}
	if w.IsClosed() {
		u.Release()
		return stream.ErrEOF
	}

	if err := w.prepare(u); err != nil {
		u.Release()
		return err
	}

	select {
	case w.in <- u:
		w.written.Add(1)
		w.bytes.Add(uint64(len(u.Payload)))
		return nil
	default:
		u.Release()
		w.dropped.Add(1)
		return stream.ErrCanceled
	}
}

// prepare fills in the fields an adapter cannot know and validates the unit
// against the track table declared for the path.
func (w *streamWriter) prepare(u *stream.Unit) error {
	w.tracksMu.Lock()
	defer w.tracksMu.Unlock()

	t, ok := w.trs[u.TrackID]
	if !ok {
		return errors.New("path: unit for unknown track")
	}
	if u.Codec == "" {
		u.Codec = t.Codec
	}
	if u.Kind == 0 {
		u.Kind = t.Kind
	}
	u.Sequence = w.seq[u.TrackID]
	w.seq[u.TrackID]++

	// Adapters that only deliver PTS leave DTS unset; the kernel must not
	// propagate an unknown DTS downstream, because every container multiplexer
	// needs one to segment. Derive it and mark the unit so a consumer can
	// detect the interpolation boundary.
	if u.DTS.IsZero() {
		if u.PTS.IsZero() {
			u.DTS = time.Now().UTC()
		} else {
			u.DTS = u.PTS
		}
		u.Flags |= stream.FlagDiscontinuity
	}
	return nil
}

// Tracks returns the declared track table in declaration order, which is the
// order of ascending track identifiers. The map alone would be the only lookup
// needed, but a map has no order: iterating it would let two identical streams
// present their tracks differently each time, and every multiplexer and every
// track id round trip assumes the kernel's ordering.
func (w *streamWriter) Tracks() []*stream.Track {
	w.tracksMu.RLock()
	defer w.tracksMu.RUnlock()
	out := make([]*stream.Track, 0, len(w.trsOrder))
	for _, t := range w.trsOrder {
		out = append(out, t)
	}
	return out
}

func (w *streamWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *streamWriter) IsClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// Closed implements stream.StreamWriter.
func (w *streamWriter) Closed() bool { return w.IsClosed() }

// Tracks is the declared track table. stream.StreamWriter does not require
// Tracks, so this is a separate method on the concrete writer rather than part
// of the frozen interface: adapters get it through PublishSession, not through
// the writer.

func (w *streamWriter) Close(err error) {
	w.mu.Lock()
	wasClosed := w.closed
	w.closed = true
	if !wasClosed && w.err == nil {
		w.err = err
	}
	if !wasClosed {
		close(w.in)
		close(w.done)
	}
	w.mu.Unlock()

	w.releaseOwner()
}

// releaseOwner is the publisher-loss signal. It is always called, from either
// the explicit Close path or the worker's exit path, and is idempotent.
func (w *streamWriter) releaseOwner() {
	w.ownerMu.Lock()
	o := w.owner
	w.owner = nil
	w.ownerMu.Unlock()
	if o != nil {
		o.PublisherLost()
	}
}

// worker pumps the bounded input into the path's broadcast worker. It runs in
// its own goroutine so that a stalled broadcast cannot stall the publisher's
// network read loop, and so that the two lock domains never nest.
func (w *streamWriter) worker() {
	defer w.releaseOwner()
	for u := range w.in {
		w.ownerMu.RLock()
		o := w.owner
		w.ownerMu.RUnlock()
		if o == nil {
			u.Release()
			continue
		}
		u.Retain()
		o.BroadcastUnit(u)
		u.Release()
	}
}

// UnitsWritten reports how many units the publisher pushed.
func (w *streamWriter) UnitsWritten() uint64 { return w.written.Load() }

// BytesWritten reports how many payload bytes the publisher pushed.
func (w *streamWriter) BytesWritten() uint64 { return w.bytes.Load() }

// QueueLen reports how many units are waiting to be broadcast.
func (w *streamWriter) QueueLen() int { return len(w.in) }

// Dropped reports how many units were refused because the input was full.
func (w *streamWriter) Dropped() uint64 { return w.dropped.Load() }

func newStreamWriter(p *Path, name string, tracks []*stream.Track, ringSize int) *streamWriter {
	w := &streamWriter{
		name:  name,
		in:    make(chan *stream.Unit, ringSize),
		done:  make(chan struct{}),
		owner: p,
		trs:   make(map[stream.TrackID]*stream.Track, len(tracks)),
		seq:   make(map[stream.TrackID]uint64, len(tracks)),
	}
	w.trsOrder = append(w.trsOrder, tracks...)
	for _, t := range tracks {
		w.trs[t.ID] = t
	}
	go w.worker()
	return w
}
