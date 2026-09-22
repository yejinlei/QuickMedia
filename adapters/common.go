// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package adapters is layer L3: protocol adapters.
//
// Each protocol in this tree is one registry.Adapter implementation plus a thin
// listener loop. The listener loop is what makes the layer testable without a
// wire protocol: it is a function of (net.Conn, registry.Adapter, path.Manager)
// and nothing else.
//
// Dependency direction is strict. This tree may import kernel/, container/,
// transport/ and the upstream protocol libraries. Nothing below L3 may import
// this tree; the grep gate in tools/lint enforces that the kernel names no
// protocol.
package adapters

import (
	"sync"

	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Session is the shared half of the kernel's Source/Sink contracts.
//
// Both interfaces end in the same four methods (Err, Done, Close, RemoteAddr),
// which is the kernel's "observe the session and tear it down" surface. Keeping
// the plumbing here means a new protocol adapter writes only the media side of
// the conversation.
//
// Everything here is idempotent by construction: the kernel closes a session
// after its Done channel fires, and the adapter's own read loop also calls End
// when the client goes away. Both paths converge on sync.Once.
type Session struct {
	remote  string
	mu      sync.Mutex
	sub     stream.Subscription
	err     error
	done    chan struct{}
	once    sync.Once
	onClose func()
}

// NewSession builds a live session. onClose releases the client connection once
// the session ends, from whichever goroutine ends it.
func NewSession(remote string, onClose func()) *Session {
	return &Session{remote: remote, done: make(chan struct{}), onClose: onClose}
}

// Err implements registry.Source and registry.Sink.
func (d *Session) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

// Done implements registry.Source and registry.Sink.
func (d *Session) Done() <-chan struct{} { return d.done }

// DoneClosed reports whether the session has ended, from either side.
func (d *Session) DoneClosed() bool {
	select {
	case <-d.done:
		return true
	default:
		return false
	}
}

// Close implements registry.Source and registry.Sink.
func (d *Session) Close() error { d.End(nil); return nil }

// RemoteAddr implements registry.Source and registry.Sink.
func (d *Session) RemoteAddr() string { return d.remote }

// Sub implements registry.Sink. It returns nil for a publish session, which is
// how one type serves both directions.
func (d *Session) Sub() stream.Subscription {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sub
}

// setErr records the terminal error, keeping the first one. A session can end
// from several goroutines at once — the network read loop, the subscription
// loop, and the kernel's Close — and the reader needs the cause, not whichever
// won the race.
func (d *Session) setErr(err error) {
	d.mu.Lock()
	if d.err == nil {
		d.err = err
	}
	d.mu.Unlock()
}

// setSub records the subscription a play session claimed.
func (d *Session) SetSub(sub stream.Subscription) {
	d.mu.Lock()
	d.sub = sub
	d.mu.Unlock()
}

// end closes the session exactly once and calls the adapter's cleanup.
// End terminates the session, keeping the first terminal error and firing the
// adapter's cleanup exactly once.
func (d *Session) End(err error) {
	if err != nil {
		d.setErr(err)
	}
	d.once.Do(func() {
		close(d.done)
		if d.onClose != nil {
			d.onClose()
		}
	})
}
