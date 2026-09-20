// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package transport is layer L1: the network transport layer.
//
// It provides the connection multiplexer and listener that all protocol
// adapters use to accept and manage connections. The kernel owns the lifecycle
// of the transport; adapters own the protocol state on top of it.
//
// Design constraints:
//   - Single-port multiplexing (one TCP port serving multiple protocols) is
//     deferred to M3. This MVP binds separate listeners per protocol.
//   - Connection migration (moving a connection to a new endpoint) is deferred.
//   - QUIC/H3/WebTransport are not implemented; the interfaces exist for them.
//
// Dependency direction: adapters/ imports transport/, never the reverse.
// transport/ imports no kernel packages.
package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
)

// ErrClosed is returned by a listener or connection after Close has been called.
var ErrClosed = errors.New("transport: closed")

// Listener is a network listener that can accept connections.
type Listener struct {
	addr   string
	l      net.Listener
	tls    *tls.Config
	mu     sync.Mutex
	closed bool
}

// NewListener creates a TCP listener on addr. If cfg is non-nil, TLS is
// enabled with the provided configuration. The listener is started immediately.
func NewListener(ctx context.Context, addr string, cfg *tls.Config) (*Listener, error) {
	var l net.Listener
	var err error
	if cfg != nil {
		l, err = tls.Listen("tcp", addr, cfg)
	} else {
		l, err = net.Listen("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("transport: listen %s: %w", addr, err)
	}
	return &Listener{
		addr: l.Addr().String(),
		l:    l,
		tls:  cfg,
	}, nil
}

// Addr returns the listener's address.
func (ln *Listener) Addr() string {
	return ln.addr
}

// Accept blocks until a connection arrives or the listener is closed.
func (ln *Listener) Accept(ctx context.Context) (net.Conn, error) {
	conn, err := ln.l.Accept()
	if err != nil {
		ln.mu.Lock()
		closed := ln.closed
		ln.mu.Unlock()
		if closed {
			return nil, ErrClosed
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return nil, err
		}
	}
	return conn, nil
}

// Close stops the listener.
func (ln *Listener) Close() error {
	ln.mu.Lock()
	defer ln.mu.Unlock()
	if ln.closed {
		return nil
	}
	ln.closed = true
	return ln.l.Close()
}

// Server owns the listeners and dispatches incoming connections to the
// appropriate protocol handler.
type Server struct {
	mu        sync.RWMutex
	listeners []*Listener
	conns     map[net.Conn]struct{}
	onConn    func(net.Conn)
	closed    bool
}

// NewServer creates a server that dispatches connections to onConn.
func NewServer(onConn func(net.Conn)) *Server {
	return &Server{
		conns:  make(map[net.Conn]struct{}),
		onConn: onConn,
	}
}

// AddListener registers a listener with the server. Connections from this
// listener are dispatched to the server's onConn callback.
func (s *Server) AddListener(ln *Listener) {
	s.mu.Lock()
	s.listeners = append(s.listeners, ln)
	s.mu.Unlock()
}

// Start begins accepting connections on all registered listeners.
func (s *Server) Start(ctx context.Context) error {
	s.mu.RLock()
	listeners := make([]*Listener, len(s.listeners))
	copy(listeners, s.listeners)
	s.mu.RUnlock()

	for _, ln := range listeners {
		go s.acceptLoop(ctx, ln)
	}
	return nil
}

// acceptLoop accepts connections on a listener and dispatches them.
func (s *Server) acceptLoop(ctx context.Context, ln *Listener) {
	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil || err == ErrClosed {
				return
			}
			continue
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		go s.onConn(conn)
	}
}

// Close closes all listeners and connections.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	listeners := s.listeners
	conns := make([]net.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.conns = make(map[net.Conn]struct{})
	s.mu.Unlock()

	var errs []error
	for _, ln := range listeners {
		if err := ln.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, c := range conns {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Conns returns the number of active connections.
func (s *Server) Conns() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.conns)
}
