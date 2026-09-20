// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package path — Manager, the only entry point the rest of QuickMedia uses.
//
// Manager owns the path table, the publisher/play sessions handed to adapters,
// and the periodic sweep that reaps slow subscribers and empty paths. The
// kernel keeps no field for any protocol: adapters are looked up in the
// registry, so adding one never touches this file.
package path

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Sentinel errors for the manager layer.
var (
	ErrPathBad     = errors.New("path: bad name")
	ErrPathMissing = errors.New("path: missing")
)

// pathNameRe is the character set a path may use. It mirrors what the protocols
// in scope can carry in a URL segment without escaping, which is what makes a
// path portable between adapters.
var pathNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._\-/]{0,254}$`)

// sweepInterval bounds how fast the manager reacts to a stalled subscriber.
const sweepInterval = 100 * time.Millisecond

// Manager is the process-wide path table.
type Manager struct {
	cfg *Config

	mu    sync.Mutex
	paths map[string]*Path

	opened bool
	ticker *time.Ticker
	done   chan struct{}
	wg     sync.WaitGroup
}

// NewManager builds a manager with a normalized configuration.
func NewManager(cfg Config) *Manager {
	cfg.normalize()
	return &Manager{
		cfg:   &cfg,
		paths: make(map[string]*Path),
		done:  make(chan struct{}),
	}
}

// Open starts the sweep loop. It is idempotent. The WaitGroup is armed before
// the lock is released, so a concurrent Close cannot observe opened=true with
// no goroutine to wait for.
func (m *Manager) Open() {
	m.mu.Lock()
	if m.opened {
		m.mu.Unlock()
		return
	}
	m.opened = true
	m.ticker = time.NewTicker(sweepInterval)
	m.wg.Add(1)
	m.mu.Unlock()

	go m.sweepLoop()
}

// Close stops the sweep and drops every path.
func (m *Manager) Close() {
	m.mu.Lock()
	if !m.opened {
		m.mu.Unlock()
		return
	}
	m.opened = false
	if m.ticker != nil {
		m.ticker.Stop()
		// The ticker is left intact: sweepLoop selects on its channel, and
		// niling it here races the select that is about to wake on done.
	}
	close(m.done)
	m.mu.Unlock()

	m.wg.Wait()

	m.mu.Lock()
	names := make([]string, 0, len(m.paths))
	for name := range m.paths {
		names = append(names, name)
	}
	m.paths = make(map[string]*Path)
	m.mu.Unlock()

	for _, name := range names {
		NewPath(name, m.cfg).Close(stream.CancelPathClosed)
	}
}

// validPath validates a path name.
func validPath(name string) (string, error) {
	if !pathNameRe.MatchString(name) {
		return "", errors.Join(ErrPathBad, errors.New(name))
	}
	return name, nil
}

// createPath gets or creates a path, refusing names that already exist.
func (m *Manager) createPath(name string) (*Path, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.paths[name]; exists {
		return nil, ErrConflict
	}
	if len(m.paths) >= m.cfg.MaxPaths {
		return nil, ErrTooMany
	}
	p := NewPath(name, m.cfg)
	m.paths[name] = p
	return p, nil
}

func (m *Manager) get(name string) (*Path, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.paths[name]
	return p, ok
}

func (m *Manager) dropPath(name string) {
	m.mu.Lock()
	delete(m.paths, name)
	m.mu.Unlock()
}

// Publish runs one publish session. It allocates the path, hands the adapter a
// PublishSession, and observes the resulting Source so the path notices when
// its publisher goes away.
func (m *Manager) Publish(ctx context.Context, ad registry.Adapter, req registry.SinkRequest) (registry.Source, error) {
	name, err := validPath(req.Path)
	if err != nil {
		return nil, err
	}
	p, err := m.createPath(name)
	if err != nil {
		return nil, err
	}

	s := newPubSession(m, p, req)
	src, err := ad.Publish(ctx, s)
	if err != nil {
		p.Close(stream.CancelPublisherGone)
		m.dropPath(name)
		return nil, err
	}
	m.observeSource(p, src)
	return src, nil
}

// Play runs one play session.
func (m *Manager) Play(ctx context.Context, ad registry.Adapter, req registry.SrcRequest) (registry.Sink, error) {
	name, err := validPath(req.Path)
	if err != nil {
		return nil, err
	}

	p, ok := m.get(name)
	if !ok {
		if req.SubscribeTimeout <= 0 {
			return nil, ErrNoSuchPath
		}
		p, err = m.waitPath(ctx, name, req.SubscribeTimeout)
		if err != nil {
			return nil, err
		}
	}

	s := newPlaySession(m, p, req)
	sink, err := ad.Play(ctx, s)
	if err != nil {
		if sub := s.sub; sub != nil {
			sub.Close(stream.CancelSubscriber)
			p.RemoveSubscriber(sub)
		}
		return nil, err
	}

	go func() {
		<-sink.Done()
		if sub := sink.Sub(); sub != nil {
			if impl, ok := sub.(*stream.SubscriptionImpl); ok {
				impl.Close(stream.CancelSubscriber)
				p.RemoveSubscriber(impl)
			} else {
				sub.Cancel()
			}
		}
	}()
	return sink, nil
}

// observeSource watches a publishing session and translates its end into a
// path state change.
func (m *Manager) observeSource(p *Path, src registry.Source) {
	go func() {
		defer src.Close()
		<-src.Done()
		if src.Err() != nil {
			p.Close(stream.CancelPublisherGone)
			return
		}
		p.PublisherLost()
	}()
}

// waitPath blocks until the named path exists, the timeout elapses, or ctx ends.
func (m *Manager) waitPath(ctx context.Context, name string, timeout time.Duration) (*Path, error) {
	deadline := time.Now().Add(timeout)
	for {
		if p, ok := m.get(name); ok {
			return p, nil
		}
		until := time.Until(deadline)
		if until <= 0 {
			return nil, ErrNoSuchPath
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(until):
			return nil, ErrNoSuchPath
		}
	}
}

// Subscribe claims a subscription on the named path, blocking for a publisher.
// Used by relay and by the control plane's read-only peek.
func (m *Manager) Subscribe(ctx context.Context, name string, timeout time.Duration) (stream.Subscription, error) {
	p, ok := m.get(name)
	if !ok {
		if timeout <= 0 {
			return nil, ErrNoSuchPath
		}
		var err error
		if p, err = m.waitPath(ctx, name, timeout); err != nil {
			return nil, err
		}
	}
	return p.SubscribeBlocked(ctx, m.cfg.RingSize, timeout)
}

// Paths returns a snapshot of every path, for the control plane.
func (m *Manager) Paths() []PathStats {
	m.mu.Lock()
	paths := make([]*Path, 0, len(m.paths))
	for _, p := range m.paths {
		paths = append(paths, p)
	}
	m.mu.Unlock()

	out := make([]PathStats, 0, len(paths))
	for _, p := range paths {
		out = append(out, p.Stats())
	}
	return out
}

// RemovePath tears down a path if it exists.
func (m *Manager) RemovePath(name string) bool {
	p, ok := m.get(name)
	if !ok {
		return false
	}
	p.Close(stream.CancelPathClosed)
	m.dropPath(name)
	return true
}

// PathCount reports how many paths are live.
func (m *Manager) PathCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.paths)
}

func (m *Manager) sweepLoop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.done:
			return
		case <-m.ticker.C:
		}
		m.sweepOnce()
	}
}

func (m *Manager) sweepOnce() {
	m.mu.Lock()
	paths := make([]*Path, 0, len(m.paths))
	for _, p := range m.paths {
		paths = append(paths, p)
	}
	m.mu.Unlock()

	for _, p := range paths {
		p.ScanSubs(func(sub *stream.SubscriptionImpl) bool {
			if sub.Canceled() {
				return true
			}
			if m.cfg.BackPressure.Heartbeat(sub, m.cfg.HeartbeatTimeout) {
				sub.Close(stream.CancelHeartbeat)
				return true
			}
			return false
		})
		if p.Releasable() {
			p.Close(stream.CancelPathClosed)
			m.dropPath(p.Name())
		}
	}
}

// --- sessions -------------------------------------------------------------

// pubSession implements registry.PublishSession for one publish request.
type pubSession struct {
	m     *Manager
	p     *Path
	req   registry.SinkRequest
	name  string
	relay bool

	// relaySub is the subscription to the source path when this session is a
	// relay. Its units are appended to this path's broadcast fan-out.
	relaySub *stream.SubscriptionImpl
}

func newPubSession(m *Manager, p *Path, req registry.SinkRequest) *pubSession {
	return &pubSession{
		m:     m,
		p:     p,
		req:   req,
		name:  p.Name(),
		relay: req.Relay,
	}
}

func (s *pubSession) Request() registry.SinkRequest { return s.req }

func (s *pubSession) Relay() bool { return s.relay }

func (s *pubSession) PathName() string { return s.name }

// Begin allocates the track identifiers, claims the publish slot, and returns
// the writer.
//
// The kernel assigns TrackID from the declaration order and returns the
// assigned tracks, which is what makes §2.2's "TrackID must match kernel
// ordering" enforceable: an adapter cannot publish an ID the kernel has not
// sanctioned, so every container multiplexer sees a consistent table.
func (s *pubSession) Begin(tracks []*stream.Track) (stream.StreamWriter, error) {
	if len(tracks) == 0 {
		return nil, errors.New("path: empty track table")
	}

	assigned := make([]*stream.Track, 0, len(tracks))
	for i, t := range tracks {
		t.ID = stream.TrackID(i + 1)
		assigned = append(assigned, t)
	}

	w := s.p.NewWriter(assigned, s.m.cfg.WriterInRing)
	if err := s.p.Publish(w); err != nil {
		return nil, err
	}

	if s.relay {
		sub, err := s.m.Subscribe(context.Background(), s.req.RelayPath, s.req.WaitPublisherTimeout)
		if err != nil {
			w.Close(err)
			return nil, err
		}
		impl, ok := sub.(*stream.SubscriptionImpl)
		if !ok {
			sub.Cancel()
			w.Close(errors.New("path: bad relay subscription"))
			return nil, err
		}
		s.relaySub = impl
		go s.pump(w)
	}
	return w, nil
}

// pump forwards a relay's source subscription into this path's writer. It ends
// when the source ends, at which point the relay path returns to idle and keeps
// its content for the retain window like any other path.
//
// The unit ReadUnit returns is handed to WriteUnit rather than released: both
// sides of this pipe use the same ownership rule, and a Release here would
// recycle a pooled buffer the worker is still broadcasting.
func (s *pubSession) pump(w *streamWriter) {
	defer w.Close(nil)
	for {
		u, err := s.relaySub.ReadUnit(context.Background())
		if err != nil {
			return
		}
		if w.WriteUnit(u) != nil {
			return
		}
	}
}

// playSession implements registry.PlaySession for one play request.
type playSession struct {
	m   *Manager
	p   *Path
	req registry.SrcRequest

	sub *stream.SubscriptionImpl
}

func newPlaySession(m *Manager, p *Path, req registry.SrcRequest) *playSession {
	return &playSession{m: m, p: p, req: req}
}

func (s *playSession) Request() registry.SrcRequest { return s.req }

func (s *playSession) Tracks() []*stream.Track { return s.p.Tracks() }

func (s *playSession) CodecParams() map[stream.CodecID]map[string]string {
	out := make(map[stream.CodecID]map[string]string)
	for _, t := range s.p.Tracks() {
		if t.Params == nil {
			continue
		}
		out[t.Codec] = t.Params
	}
	return out
}

func (s *playSession) Subscribe(ctx context.Context) (stream.Subscription, error) {
	sub, err := s.p.SubscribeBlocked(ctx, s.m.cfg.RingSize, s.req.SubscribeTimeout)
	if err != nil {
		return nil, err
	}
	s.sub = sub.(*stream.SubscriptionImpl)
	return sub, nil
}
