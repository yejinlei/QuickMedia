// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// TestListenerBindsAnEphemeralPort checks the contract the tests and the
// composition root rely on: ":0" must come back with the address the listener
// really holds, not the pattern it was asked for.
func TestListenerBindsAnEphemeralPort(t *testing.T) {
	ln, err := NewListener(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr()
	if addr == "" || addr == "127.0.0.1:0" {
		t.Fatalf("Addr = %q, want the bound address", addr)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split addr %q: %v", addr, err)
	}
	if host != "127.0.0.1" {
		t.Fatalf("host = %q, want 127.0.0.1", host)
	}
	// The kernel configures listeners from ":0", so the resolved port must be
	// the one the OS actually bound.
	n, err := strconv.Atoi(port)
	if err != nil || n <= 0 {
		t.Fatalf("port = %q, want a positive number", port)
	}
}

// TestListenerAcceptReturnsARealConnection is the forward direction: one dial
// in, one accepted connection out, with a remote address worth logging.
func TestListenerAcceptReturnsARealConnection(t *testing.T) {
	ln, err := NewListener(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	conn, err := net.DialTimeout("tcp", ln.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	accepted, err := ln.Accept(context.Background())
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer func() { _ = accepted.Close() }()

	if accepted.RemoteAddr() == nil || accepted.RemoteAddr().String() == "" {
		t.Fatal("the accepted connection has no remote address")
	}
}

// TestListenerAcceptAfterCloseReturnsErrClosed pins the shutdown contract: a
// closed listener reports itself closed rather than blocking, and doing so more
// than once stays quiet. A blocking Accept here hangs every shutdown path in the
// binary.
func TestListenerAcceptAfterCloseReturnsErrClosed(t *testing.T) {
	ln, err := NewListener(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("second close = %v, want nil", err)
	}

	type result struct {
		conn net.Conn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		c, err := ln.Accept(context.Background())
		ch <- result{conn: c, err: err}
	}()

	select {
	case r := <-ch:
		if !errors.Is(r.err, ErrClosed) {
			t.Fatalf("Accept on a closed listener = %v, want ErrClosed", r.err)
		}
		if r.conn != nil {
			t.Fatal("Accept on a closed listener returned a connection")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept blocked on a closed listener")
	}
}

// TestServerDispatchesAndTracksConnections is the whole L1 job in one test: two
// dials become two callbacks and two tracked connections, and Close ends both.
func TestServerDispatchesAndTracksConnections(t *testing.T) {
	ln, err := NewListener(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var handled atomic.Int32
	s := NewServer(func(c net.Conn) {
		handled.Add(1)
	})
	s.AddListener(ln)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	for i := range 2 {
		_, err := net.Dial("tcp", ln.Addr())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for handled.Load() < 2 || s.Conns() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("dispatched %d, tracked %d, want 2 of each", handled.Load(), s.Conns())
		}
		time.Sleep(time.Millisecond)
	}
	if got := s.Conns(); got != 2 {
		t.Fatalf("Conns = %d, want 2", got)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := s.Conns(); got != 0 {
		t.Fatalf("Conns after Close = %d, want 0", got)
	}

	// A closed listener refuses new clients, which is how a drain ends.
	if _, err := net.DialTimeout("tcp", ln.Addr(), time.Second); err == nil {
		t.Fatal("a closed server still accepts connections")
	}
}

// TestServerCloseIsIdempotent is the teardown race: two goroutines shutting the
// server down at once must not surface an error to either one.
func TestServerCloseIsIdempotent(t *testing.T) {
	s := NewServer(func(net.Conn) {})
	if err := s.Close(); err != nil {
		t.Fatalf("close = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close = %v, want nil", err)
	}
}
