// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package hls is the HLS protocol adapter, built on
// github.com/bluenviron/gohlslib for both the segmenter and the playlist HTTP
// surface.
//
// gohlslib is a muxer and not a server: it produces one playlist per stream and
// routes requests by filename. What is QuickMedia's here is the mapping from a
// kernel path onto a muxer instance, the track-table translation, and the fact
// that one muxer outlives one HTTP request. The muxer is keyed by path name
// because a playlist is a resource rather than a request: two players of one
// stream share the segments, and a new player must not start a muxer that
// re-segments from the head of the stream.
//
// HLS is the one protocol whose client this layer cannot observe. A player
// polling a playlist can leave at any moment without saying so, so there is no
// connection to watch and the session that feeds the muxer is keyed off the
// publisher, not off the player. The feed is a goroutine that starts when the
// first subscriber for a path arrives and runs until the publisher is gone,
// which is the only moment at which the segments stop advancing and the muxer
// becomes stale.
//
// Timestamps are where this adapter cannot be thin. gohlslib wants a monotonic
// PTS and derives its own DTS with an extractor that must start at a random
// access, so the kernel's absolute instants are re-anchored here and the DTS is
// never passed. Passing the kernel's DTS would put two independent clocks into
// one playlist, and the segment boundaries would be wrong.
package hls

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	gohlslib "github.com/bluenviron/gohlslib/v2"
	"github.com/bluenviron/gohlslib/v2/pkg/codecs"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"

	"github.com/yejinlei/quickmedia/adapters"
	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/kernel/path"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Version is this module's version.
const Version = "0.1.0"

const (
	codecH264 stream.CodecID = "h264"
	codecAAC  stream.CodecID = "aac"

	// videoClockRate is the PTS clock gohlslib expects for H.264.
	videoClockRate = 90000
	// defaultSegmentCount is the segments kept per stream. It is above the three
	// that gohlslib requires for a non-LL variant, because a player needs more
	// than the minimum to have anything to catch up on.
	defaultSegmentCount = 4
	// defaultSegmentDuration bounds a segment so a late joiner does not wait a
	// full keyframe interval plus a segment to start.
	defaultSegmentDuration = time.Second
)

// Server owns one gohlslib Muxer per path.
//
// It holds its own context rather than the request's because an HLS session
// outlives the request that started it. The HTTP handler returns as soon as the
// player is attached; if it passed r.Context() to the kernel, that context would
// be canceled the moment the handler returned and the subscription would be
// reaped before a single segment was written.
type Server struct {
	mgr      *path.Manager
	dir      string
	segCount int
	segDur   time.Duration

	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	mux   map[string]*muxEntry
	feeds map[string]*feed
}

// muxEntry pairs a path's muxer with the tracks it was built with, so a second
// subscriber can reuse it rather than starting a muxer that re-segments from
// the head of the stream.
type muxEntry struct {
	muxer *gohlslib.Muxer
	vt    *gohlslib.Track
	at    *gohlslib.Track
}

// feed tracks the one goroutine that writes a path's segments.
type feed struct {
	cancel context.CancelFunc
	done   <-chan struct{}
}

// Options configures the muxer defaults.
type Options struct {
	// Dir is the segment directory. Empty means RAM, the right default for a
	// live-only server.
	Dir string
	// SegmentCount is the segments kept. Defaults to 4.
	SegmentCount int
	// SegmentDuration bounds each segment. Defaults to 1s.
	SegmentDuration time.Duration
}

// NewServer builds the handler and the per-path muxer table.
func NewServer(m *path.Manager, opts Options) *Server {
	s := &Server{
		mgr: m, mux: make(map[string]*muxEntry), feeds: make(map[string]*feed),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	if opts.Dir != "" {
		s.dir = opts.Dir
	}
	if opts.SegmentCount > 0 {
		s.segCount = opts.SegmentCount
	} else {
		s.segCount = defaultSegmentCount
	}
	if opts.SegmentDuration > 0 {
		s.segDur = opts.SegmentDuration
	} else {
		s.segDur = defaultSegmentDuration
	}
	return s
}

// Handler returns the http.Handler. It is an http.Handler rather than a
// net.Listener so the caller can mount it under any mux and behind any TLS
// terminator, which is the same constraint the kernel places on L1.
func (s *Server) Handler() http.Handler { return s }

// Close stops the feeds and closes every muxer.
//
// The feeds must finish before their muxers are closed: the muxer is not safe
// to close under a live writer, and a segment that rotates at the same instant
// as Close races it on the same writer's buffer. Canceling the feed context is
// a drain, never a write, so it cannot call into a muxer that is mid-close.
func (s *Server) Close() {
	s.cancel()

	var feeds []<-chan struct{}
	var muxes []*muxEntry
	s.mu.Lock()
	for name, f := range s.feeds {
		feeds = append(feeds, f.done)
		f.cancel()
		delete(s.feeds, name)
	}
	for name, e := range s.mux {
		muxes = append(muxes, e)
		delete(s.mux, name)
	}
	s.mu.Unlock()

	until := time.Now().Add(5 * time.Second)
	for _, done := range feeds {
		select {
		case <-done:
		case <-time.After(time.Until(until)):
		}
	}

	for _, e := range muxes {
		e.muxer.Close()
	}
}

// ServeHTTP serves the playlists and segments of one path.
//
// The path is the first path segment, so /live/index.m3u8 is path "live"
// playing the master playlist and /live/out.m3u8 is path "live" playing a
// media playlist. Splitting there keeps gohlslib's own filename routing intact,
// which is the only routing that knows which file is a playlist and which is a
// segment.
//
// A request to a path with no feed creates one, because there is no other way
// for a player to pull a stream: the muxer is built from a subscription, and
// the subscription is claimed here. A path that does not exist fails fast
// rather than blocking, which is what a 404 on a bad URL should look like.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name, err := pathOf(r.URL.Path)
	if err != nil {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	e, err := s.ensureFeedFromPath(name)
	if err != nil {
		http.Error(w, fmt.Sprintf("stream unavailable: %v", err), http.StatusNotFound)
		return
	}
	// The muxer routes by filename alone, so the path segment has to be removed
	// before it reaches the handler. Without this a request to /live/index.m3u8
	// resolves to "index.m3u8" inside the muxer's path table, which also keys
	// off the basename, and the request silently hits a different muxer.
	r2 := *r
	r2.URL.Path = strings.TrimPrefix(r.URL.Path, "/"+name)
	if r2.URL.Path == "" {
		r2.URL.Path = "/"
	}
	e.muxer.Handle(w, &r2)
}

// --- registry.Adapter ------------------------------------------------------

// ModuleInfo implements registry.Adapter.
func (a *Adapter) ModuleInfo() registry.ModuleInfo {
	return registry.ModuleInfo{
		Name: "hls", Version: Version, Type: registry.TAdapter,
		MinKernel: "0.1.0", Priority: 100, Dir: "adapters/hls",
	}
}

// Schemes implements registry.Adapter.
func (a *Adapter) Schemes() []string { return []string{"http", "https"} }

// SupportsScheme implements registry.Adapter.
func (a *Adapter) SupportsScheme(scheme string) bool {
	return scheme == "http" || scheme == "https"
}

// CanPublish implements registry.Adapter. HLS is a pull protocol with no
// upstream direction.
func (a *Adapter) CanPublish() bool { return false }

// CanPlay implements registry.Adapter.
func (a *Adapter) CanPlay() bool { return true }

func init() { registry.Register(&Adapter{}) }

// Adapter is the HLS registry adapter, one instance per session.
type Adapter struct {
	remote string
	srv    *Server
}

// Play implements registry.Adapter.
//
// The subscription the kernel hands back becomes the feed's source, so the
// first subscriber for a path starts the feed and later ones find it already
// running. Splitting the two cases inside ensureFeed is what keeps the feed
// running exactly once per path rather than once per player.
func (a *Adapter) Play(ctx context.Context, s registry.PlaySession) (registry.Sink, error) {
	srv := a.srv
	if srv == nil {
		return nil, errors.New("hls: no server")
	}
	sub, err := s.Subscribe(ctx)
	if err != nil {
		return nil, err
	}
	// The track table is captured here, not inside ensureFeed, because the
	// session the kernel hands over is only valid for the duration of this call
	// and ensureFeed returns before the request that started it has finished.
	trks := s.Tracks()
	if _, err := srv.ensureFeed(s.Request().Path, trks, sub); err != nil {
		sub.Cancel()
		return nil, err
	}

	// The sink observes the feed rather than owning it: the feed is the only
	// thing that can decide this path's segments have stopped advancing, and
	// several players of one path must share that one decision.
	sink := adapters.NewSession(a.remote, nil)
	sink.SetSub(sub)
	go func() {
		<-srv.feedDone(s.Request().Path)
		sink.End(nil)
	}()
	return sink, nil
}

// Publish implements registry.Adapter.
func (a *Adapter) Publish(context.Context, registry.PublishSession) (registry.Source, error) {
	return nil, errors.New("hls: publishing is not supported")
}

// --- muxer and feed management --------------------------------------------

// ensureFeed returns the path's muxer, building one from trks and sub if one
// does not exist yet. It is the single place a muxer comes from, which is what
// keeps ServeHTTP and the registry path from racing to build two.
//
// trks arrives as an argument rather than being read off sub because the
// subscription's own Tracks is deliberately empty: the kernel does not expose a
// path's track table through a subscription. A muxer built from it has no
// tracks, which is what every HLS request failed with.
//
// A feed that already exists is returned unchanged rather than rebuilt. This is
// the case that two concurrent requests exercise, and tearing the muxer down on
// it would cancel a subscription someone else is still reading, which empties
// the segment table for every player already attached.
func (s *Server) ensureFeed(name string, trks []*stream.Track, sub stream.Subscription) (*muxEntry, error) {
	if e, _, err := s.takeover(name, sub); err != nil {
		return nil, err
	} else if e != nil {
		return e, nil
	}
	return s.build(name, trks, sub)
}

// takeover claims a subscription for a path that has no feed yet.
//
// It returns the feed that already exists when one is running. The caller must
// keep that feed's muxer, which is the case that matters: a player that loses a
// race to another player must be served the same stream, not its own new one.
// Returning a nil muxer here used to make every losing request read from the
// winner's subscription, which no one drained, and the path's ring filled
// until the heartbeat sweep tore the feed down under the still-waiting
// requests.
//
// It returns a nil subscription when one already exists, so the caller does not
// open a ring it never reads from.
func (s *Server) takeover(name string, sub stream.Subscription) (*muxEntry, stream.Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.mux[name]; ok {
		if sub != nil {
			sub.Cancel()
		}
		return e, nil, nil
	}
	return nil, sub, nil
}

// build creates a muxer for a path and starts its feed.
func (s *Server) build(name string, trks []*stream.Track, sub stream.Subscription) (*muxEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.mux[name]; ok {
		if sub != nil {
			sub.Cancel()
		}
		return e, nil
	}
	if sub == nil {
		return nil, errors.New("hls: no subscription for path")
	}

	vt, at, err := toHLS(trks)
	if err != nil {
		sub.Cancel()
		return nil, err
	}
	e := &muxEntry{muxer: s.newMuxer(vt, at), vt: vt, at: at}
	if err := e.muxer.Start(); err != nil {
		sub.Cancel()
		return nil, fmt.Errorf("hls: muxer start: %w", err)
	}
	s.mux[name] = e

	fctx, fcancel := context.WithCancel(s.ctx)
	ch := make(chan struct{})
	s.feeds[name] = &feed{cancel: fcancel, done: ch}
	go func() {
		defer close(ch)
		if err := drive(fctx, e, sub); err != nil {
			s.teardown(name)
		}
	}()
	return e, nil
}

func (s *Server) feedDone(name string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.feeds[name]; ok {
		return f.done
	}
	ch := make(chan struct{})
	close(ch)
	return ch
}

// teardown drops a path's muxer and feed, releasing the muxer's segment storage.
func (s *Server) teardown(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.feeds[name]; ok {
		f.cancel()
		delete(s.feeds, name)
	}
	if e, ok := s.mux[name]; ok {
		e.muxer.Close()
		delete(s.mux, name)
	}
}

// newMuxer builds one muxer for a path.
//
// The variant is set explicitly because gohlslib defaults to Low-Latency HLS,
// which rejects a SegmentCount below seven at Start. QuickMedia serves classic
// HLS: it does not emit partial segments, and a muxer that refuses to start
// over a configuration this adapter never asked for would make every request a
// 404 with no way to diagnose it from the server side.
func (s *Server) newMuxer(vt, at *gohlslib.Track) *gohlslib.Muxer {
	tracks := make([]*gohlslib.Track, 0, 2)
	if vt != nil {
		tracks = append(tracks, vt)
	}
	if at != nil {
		tracks = append(tracks, at)
	}
	return &gohlslib.Muxer{
		Tracks:             tracks,
		Variant:            gohlslib.MuxerVariantMPEGTS,
		SegmentCount:       s.segCount,
		SegmentMinDuration: s.segDur,
		Directory:          s.dir,
	}
}

// pathOf returns the first path segment.
func pathOf(p string) (string, error) {
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	if i := strings.IndexByte(p, '/'); i >= 0 {
		p = p[:i]
	}
	if p == "" {
		return "", errors.New("hls: empty path")
	}
	return p, nil
}

// ensureFeedFromPath claims a subscription directly and starts the feed.
//
// The track table is resolved through the path here because there is no
// PublishSession or PlaySession on the request path: an HTTP GET is all the
// adapter gets. The path's own table is the same source the registry path uses,
// so both entry points agree on what a muxer is built from.
func (s *Server) ensureFeedFromPath(name string) (*muxEntry, error) {
	sub, err := s.mgr.Subscribe(s.ctx, name, 0)
	if err != nil {
		return nil, err
	}
	return s.build(name, s.tracksForPath(name), sub)
}

// tracksForPath returns a path's track table, or nil when the path does not
// exist or has no publisher yet.
func (s *Server) tracksForPath(name string) []*stream.Track {
	for _, st := range s.mgr.Paths() {
		if st.Name == name {
			return st.Tracks
		}
	}
	return nil
}

// drive reads units from the subscription and feeds the muxer until the
// publisher is gone or the feed context ends. It runs exactly once per path.
//
// It reports the read error rather than swallowing it: the caller tears the
// muxer down on any non-nil return, and rebuilding it on a transient failure
// makes every player see a feed that never advances.
func drive(ctx context.Context, e *muxEntry, sub stream.Subscription) error {
	if sub == nil {
		return nil
	}
	var anchor time.Time
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		u, err := sub.ReadUnit(ctx)
		if err != nil {
			return err
		}
		if anchor.IsZero() {
			if !u.PTS.IsZero() {
				anchor = u.PTS
			} else {
				anchor = time.Now().UTC()
			}
		}
		err = writeHLS(e, anchor, u)
		// Release runs before the write, never as a defer: a defer in a loop
		// body runs at return, which would pin every payload buffer for the
		// life of the feed.
		u.Release()
		if err != nil {
			return err
		}
	}
}

// writeHLS feeds one unit to the muxer.
//
// The kernel carries AVCC-length-prefixed access units, the storage form, and
// HLS wants one slice per NAL body. The split is mechanical and the container
// layer already does it, so it is reused rather than re-implemented.
//
// Every slice handed to the muxer is a copy. The NAL slices produced by
// ParseAU are views into the unit's payload, and that payload goes back to the
// pool on the very next line. gohlslib holds the payloads it was given and
// reads them from its own goroutine when it rotates a segment, so handing it a
// view would let it read a buffer that had been reissued to another subscriber.
func writeHLS(e *muxEntry, anchor time.Time, u *stream.Unit) error {
	pts := max(u.PTS.Sub(anchor).Nanoseconds(), int64(0))
	ntp := time.Now().UTC()
	switch u.Codec {
	case codecAAC:
		if e.at == nil {
			return errors.New("hls: no audio track")
		}
		return e.muxer.WriteMPEG4Audio(e.at, ntp, pts, [][]byte{copyBytes(u.Payload)})
	case codecH264:
		if e.vt == nil {
			return errors.New("hls: no video track")
		}
		var au [][]byte
		for _, n := range container.ParseAU(u.Payload) {
			au = append(au, copyBytes(n.Data))
		}
		if len(au) == 0 {
			return nil
		}
		return e.muxer.WriteH264(e.vt, ntp, pts, au)
	default:
		return fmt.Errorf("hls: unsupported codec %s", u.Codec)
	}
}

// copyBytes returns a copy of b, or b itself when there is nothing to copy.
func copyBytes(b []byte) []byte {
	if len(b) == 0 {
		return b
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// --- track mapping ---------------------------------------------------------

// toHLS maps a subscription's track table to gohlslib tracks.
func toHLS(trks []*stream.Track) (*gohlslib.Track, *gohlslib.Track, error) {
	var vt, at *gohlslib.Track
	for _, t := range trks {
		p := t.Params
		switch t.Codec {
		case codecH264:
			sps, err := hex.DecodeString(p["sps"])
			if err != nil || len(sps) == 0 {
				return nil, nil, fmt.Errorf("hls: h264 track missing sps: %w", err)
			}
			pps, err := hex.DecodeString(p["pps"])
			if err != nil || len(pps) == 0 {
				return nil, nil, fmt.Errorf("hls: h264 track missing pps: %w", err)
			}
			vt = &gohlslib.Track{
				Codec:     &codecs.H264{SPS: sps, PPS: pps},
				ClockRate: videoClockRate,
			}
		case codecAAC:
			rate, _ := strconv.Atoi(p["sampleRate"])
			ch, _ := strconv.Atoi(p["numberOfChannels"])
			if rate == 0 {
				return nil, nil, errors.New("hls: aac track missing sampleRate")
			}
			at = &gohlslib.Track{
				Codec: &codecs.MPEG4Audio{Config: mpeg4audio.AudioSpecificConfig{
					Type:          mpeg4audio.ObjectTypeAACLC,
					SampleRate:    rate,
					ChannelConfig: uint8(ch),
				}},
				ClockRate: rate,
			}
		default:
			// Refusing rather than dropping the track is deliberate: a stream
			// missing one codec plays back as video with no audio, which looks
			// to the viewer like a working feed with a broken source.
			return nil, nil, fmt.Errorf("hls: unsupported codec %s", t.Codec)
		}
	}
	if vt == nil && at == nil {
		return nil, nil, errors.New("hls: path has no tracks")
	}
	return vt, at, nil
}
