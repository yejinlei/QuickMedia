// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package hls

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/container/h264"
	"github.com/yejinlei/quickmedia/kernel/path"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"

	gohlslib "github.com/bluenviron/gohlslib/v2"
)

// testTracks is the description every test publishes. It is deliberately
// carried by the module's own fixture generators rather than by hand-written
// bytes, so an adapter test fails when the fixture changes instead of quietly
// publishing a configuration the codecs do not understand.
func testTracks() []*stream.Track {
	return []*stream.Track{
		{
			Codec: codecH264, Kind: stream.KindVideo, Timescale: container.Timescale,
			Params: map[string]string{
				"sps": hex.EncodeToString(h264.SyntheticSPS()),
				"pps": hex.EncodeToString(h264.SyntheticPPS()),
			},
		},
		{
			Codec: codecAAC, Kind: stream.KindAudio, Timescale: 44100,
			Params: map[string]string{"sampleRate": "44100", "numberOfChannels": "2"},
		},
	}
}

// frameInterval is the PTS cadence the fixtures publish at: a 25 fps
// keyframe rate.
const frameInterval = 40 * time.Millisecond

// pushGap is the wall-clock space between two written frames. It is far smaller
// than frameInterval, so the test does not spend real time writing media whose
// timestamps are already spaced. Media time is what a muxer segments on.
const pushGap = 3 * time.Millisecond

// segmentCount is how many access units every test pushes into the path.
//
// The number is not arbitrary. gohlslib closes a segment only when a random
// access unit arrives whose DTS is at least segmentMinDuration past the
// segment's start, so a muxer started with the adapter's default 1s segment has
// no content until roughly the twenty-fifth frame. A fixture that writes two
// frames makes every playlist request block inside the muxer's condition
// variable, which reads as an upstream hang when the defect is that the test
// never fed enough media. Writing past the boundary rather than to it keeps the
// assertion independent of the rounding.
const segmentCount = 45

// startPublisher publishes the path and returns the writer the kernel handed
// the adapter. Nothing is pushed: the caller decides when the stream starts.
func startPublisher(t *testing.T, m *path.Manager, name string) stream.StreamWriter {
	t.Helper()
	ad := &fakeAdapter{begin: testTracks()}
	src, err := m.Publish(context.Background(), ad, registry.SinkRequest{Path: name})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Cleanup(func() { src.Close() })
	if ad.w == nil {
		t.Fatal("publish did not hand back a writer")
	}
	return ad.w
}

// primeFeed claims the path's subscription and builds its muxer, which is the
// step a playlist request performs.
//
// It must run before the first frame is pushed. The muxer is created here, not
// by the publisher, and a unit broadcast into a path with no subscriber is
// dropped: a frame written before this call can never reach the muxer. A test
// that pushes everything first builds a muxer that has nothing to read, and
// every request blocks in the muxer's condition variable until it times out.
func primeFeed(t *testing.T, srv *Server, m *path.Manager, name string) stream.StreamWriter {
	t.Helper()
	w := startPublisher(t, m, name)
	if _, err := srv.ensureFeedFromPath(name); err != nil {
		t.Fatalf("prime feed %q: %v", name, err)
	}
	return w
}

// pushFrames writes n video access units and n audio access units starting at
// the unit with index from, alternating so the muxer sees an interleaved stream
// the way a real encoder produces one.
//
// Every video unit is a keyframe, which is what makes the segment boundary fall
// inside the frame count rather than after it: gohlslib rotates only on a
// random access unit, so a non-IDR at the boundary would not have closed the
// segment at all.
//
// It is synchronous: each unit goes into a bounded ring the feed goroutine
// drains, so returning here means the frames are queued rather than merely
// sent. PTS advances by frameInterval per unit; the wall-clock sleep only keeps
// the feed goroutine from being starved by a full ring.
func pushFrames(w stream.StreamWriter, from, n int) {
	vau := h264.SyntheticAU(true)
	aau := []byte{0x21, 0x6f, 0x84, 0x03, 0x80, 0xaa}
	anchor := time.Unix(0, 0).UTC()
	for i := range n {
		pts := anchor.Add(time.Duration(from+i) * frameInterval)
		if err := w.WriteUnit(stream.NewUnit(&stream.Unit{
			TrackID: 1, Codec: codecH264, Kind: stream.KindVideo,
			Payload: vau, PTS: pts, DTS: pts, Key: true,
		})); err != nil {
			panic(fmt.Sprintf("pushFrames video: %v", err))
		}
		if err := w.WriteUnit(stream.NewUnit(&stream.Unit{
			TrackID: 2, Codec: codecAAC, Kind: stream.KindAudio,
			Payload: aau, PTS: pts, DTS: pts,
		})); err != nil {
			panic(fmt.Sprintf("pushFrames audio: %v", err))
		}
		time.Sleep(pushGap)
	}
}

// waitForSegment blocks until the muxer behind the path has produced a
// segment, and returns the master playlist once one exists.
//
// It waits inside the muxer rather than polling a path stat: gohlslib's
// multivariant handler blocks until the stream has content, so a single request
// is a readiness probe and a polling loop would only add jitter. A real player
// makes the same call, which is why the test issues it.
//
// The deadline is enforced per attempt. An attempt that still blocks when it
// expires is reported rather than left running: a blocked muxer handler would
// otherwise outlive the test that started it.
func waitForSegment(t *testing.T, srv *Server, name string, timeout time.Duration) (int, string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		e, err := srv.ensureFeedFromPath(name)
		if err != nil {
			t.Fatalf("no muxer for %q: %v", name, err)
		}
		h := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			e.muxer.Handle(h, httptest.NewRequest(http.MethodGet, "/index.m3u8", nil))
			close(done)
		}()
		select {
		case <-done:
			if h.Code == http.StatusOK {
				return h.Code, h.Body.String()
			}
		case <-time.After(time.Until(deadline)):
		}
	}
	t.Fatalf("the muxer produced no segment for %q within %v", name, timeout)
	return 0, ""
}

// waitForSegmentCtx is waitForSegment for callers that cannot fail the test
// themselves. It reports a nil error once the muxer has a segment rather than
// blocking forever, which is what makes it safe to call from a test that is
// only checking readiness.
func waitForSegmentCtx(srv *Server, name string, timeout time.Duration) (int, string, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		e, err := srv.ensureFeedFromPath(name)
		if err != nil {
			return 0, "", fmt.Errorf("no muxer for %q: %w", name, err)
		}
		h := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			e.muxer.Handle(h, httptest.NewRequest(http.MethodGet, "/index.m3u8", nil))
			close(done)
		}()
		select {
		case <-done:
			if h.Code == http.StatusOK {
				return h.Code, h.Body.String(), nil
			}
		case <-time.After(time.Until(deadline)):
		}
	}
	return 0, "", fmt.Errorf("the muxer produced no segment for %q within %v", name, timeout)
}

// get serves one playlist request and returns the status and body.
func get(srv *Server, u string) (int, string) {
	h := httptest.NewRecorder()
	srv.ServeHTTP(h, httptest.NewRequest(http.MethodGet, u, nil))
	return h.Code, h.Body.String()
}

// getBytes is get for a request whose body is media rather than text.
func getBytes(srv *Server, u string) (int, []byte) {
	h := httptest.NewRecorder()
	srv.ServeHTTP(h, httptest.NewRequest(http.MethodGet, u, nil))
	return h.Code, h.Body.Bytes()
}

// mediaSegments lists the segment URIs a media playlist names, skipping the
// tags and the URI of a variant a test does not care about.
func mediaSegments(media string) []string {
	var out []string
	for line := range strings.SplitSeq(media, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out
}

// playlistUri returns the first line that is not a comment, an attribute, or a
// blank. It is all the parsing a playlist needs for the requests the tests make.
func playlistUri(body string) string {
	for line := range strings.SplitSeq(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		return line
	}
	return ""
}

// TestHLSPlaylistEndToEnd is the one that matters: a real publish into a real
// path, a real HTTP request out the muxer, and a real playlist back.
//
// Each of four bugs this adapter has had fails it differently:
//
//   - "hls: path has no tracks", the feed built its muxer from a subscription's
//     empty track table instead of from the session's description.
//   - a 404, the feed torn down immediately because a non-EOF read was treated
//     as the end of the stream.
//   - a 500 on a valid feed, an SPS the muxer's bitstream parser cannot
//     decode, which is what a hand-written fixture produces.
//   - a valid playlist with no segments, which no assertion here catches but
//     which a player shows as a stream that never starts.
func TestHLSPlaylistEndToEnd(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	srv := NewServer(m, Options{})
	defer srv.Close()
	w := primeFeed(t, srv, m, "live")

	// Push only after the muxer exists: a unit broadcast into a path with no
	// subscriber is dropped, so the first frames decide whether the muxer has
	// anything to segment at all.
	pushFrames(w, 0, segmentCount)

	// The muxer is built above, so the wait is for content rather than for a
	// muxer to exist. This is the assertion a real player makes.
	code, body := waitForSegment(t, srv, "live", 5*time.Second)
	if code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", code, body)
	}
	if body == "" {
		t.Fatal("the playlist is empty")
	}
	if got := m.PathCount(); got != 1 {
		t.Fatalf("path count = %d, want 1", got)
	}
	t.Logf("master playlist (%d bytes):\n%s", len(body), body)

	// The master playlist points at a media playlist, which is what a player
	// follows. Fetch it through the same handler and confirm the segments it
	// lists actually exist, so a playlist that names missing segments fails
	// here rather than at the first real client.
	uri := playlistUri(body)
	if uri == "" {
		t.Fatalf("no media playlist in %q", body)
	}
	code, media := get(srv, "/live/"+uri)
	if code != http.StatusOK {
		t.Fatalf("media playlist %s: status = %d, body = %q", uri, code, media)
	}
	segs := mediaSegments(media)
	if len(segs) == 0 {
		t.Fatalf("media playlist names no segments:\n%s", media)
	}
	// A playlist that names a missing segment is what a player renders as a
	// stream that never starts. Fetch the first one and check the MPEG-TS sync
	// byte: that is the one byte that distinguishes real multiplexed media from
	// an empty or error page.
	sc, seg := getBytes(srv, "/live/"+segs[0])
	if sc != http.StatusOK {
		t.Fatalf("segment %s: status = %d", segs[0], sc)
	}
	if len(seg) == 0 {
		t.Fatal("the segment is empty")
	}
	if seg[0] != 0x47 {
		t.Fatalf("segment does not start with the MPEG-TS sync byte: %x", seg[:min(4, len(seg))])
	}

	// The path keeps producing: a player that joined mid-stream must see the
	// playlist advance rather than a file that was never extended. The second
	// push continues the PTS of the first rather than restarting it, which is
	// what keeps the second segment distinct from the first.
	segBefore := len(segs)
	pushFrames(w, segmentCount, segmentCount)
	code, later := get(srv, "/live/"+uri)
	if code != http.StatusOK {
		t.Fatalf("media playlist again: status = %d", code)
	}
	after := mediaSegments(later)
	// A playlist that still names the same first segment is what a stalled feed
	// looks like from a client. With SegmentCount at four the head must move.
	if got := after[0]; got == segs[0] {
		t.Fatalf("the media playlist did not advance: still starting at %s", got)
	}
	if got := len(after); got < segBefore {
		t.Fatalf("the media playlist shrank from %d to %d segments", segBefore, got)
	}
	t.Logf("master (%d bytes), media (%d bytes), segment %s (%d bytes), head %s -> %s",
		len(body), len(media), segs[0], len(seg), segs[0], after[0])
}

// TestHLSMediaPlaylist covers the media playlist a player follows after the
// master one. Both files route through the same muxer, so a request to one
// must not disturb the other.
func TestHLSMediaPlaylist(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	srv := NewServer(m, Options{})
	defer srv.Close()
	w := primeFeed(t, srv, m, "live")
	pushFrames(w, 0, segmentCount)

	code, master := waitForSegment(t, srv, "live", 5*time.Second)
	if code != http.StatusOK {
		t.Fatalf("master: status = %d, body = %q", code, master)
	}
	uri := playlistUri(master)
	code, media := get(srv, "/live/"+uri)
	if code != http.StatusOK {
		t.Fatalf("%s: status = %d, body = %q", uri, code, media)
	}
	if len(mediaSegments(media)) == 0 {
		t.Fatalf("the media playlist names no segments:\n%s", media)
	}
	// The master playlist must be served again unchanged: one muxer per path
	// means the two files come from one muxer, and a racing feed table would
	// replace it under the second request.
	code, again := get(srv, "/live/index.m3u8")
	if code != http.StatusOK {
		t.Fatalf("master again: status = %d", code)
	}
	if !strings.Contains(again, uri) {
		t.Fatalf("the master playlist no longer names %q", uri)
	}
}

// TestHLSSegmentServing covers the request a player issues after following the
// media playlist: the actual .ts file.
//
// A playlist that resolves but whose segments return nothing is the failure that
// shows up in production as a stream that never starts, so this is asserted
// separately from the playlist itself. The MPEG-TS sync byte is the check: it is
// the one byte that separates real multiplexed media from an empty body or an
// error page.
func TestHLSSegmentServing(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	srv := NewServer(m, Options{})
	defer srv.Close()
	w := primeFeed(t, srv, m, "live")
	pushFrames(w, 0, segmentCount)

	_, master := waitForSegment(t, srv, "live", 5*time.Second)
	uri := playlistUri(master)
	code, media := get(srv, "/live/"+uri)
	if code != http.StatusOK {
		t.Fatalf("media playlist: status = %d, body = %q", code, media)
	}
	segs := mediaSegments(media)
	if len(segs) == 0 {
		t.Fatalf("no segments in %q", media)
	}

	for _, s := range segs {
		code, body := getBytes(srv, "/live/"+s)
		if code != http.StatusOK {
			t.Fatalf("segment %s: status = %d", s, code)
		}
		if len(body) == 0 {
			t.Fatalf("segment %s: empty body", s)
		}
		if body[0] != 0x47 {
			t.Fatalf("segment %s does not start with the MPEG-TS sync byte: %x", s, body[:min(4, len(body))])
		}
		// An MPEG-TS file is a multiple of the 188 byte packet, so a body whose
		// length is not one is a page that was served through the wrong handler.
		if got := len(body) % 188; got != 0 {
			t.Fatalf("segment %s length %d is not a multiple of 188 (remainder %d)", s, len(body), got)
		}
	}
}

// TestHLSUnknownPathReturns404 pins the failure mode for a bad URL: it must
// fail fast rather than block on a publisher that will never arrive.
func TestHLSUnknownPathReturns404(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	srv := NewServer(m, Options{})
	defer srv.Close()

	code, _ := get(srv, "/missing/index.m3u8")
	if code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", code)
	}
	code, _ = get(srv, "/")
	if code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", code)
	}
}

// TestHLSPathParsing covers the URL convention the muxer mounts under.
func TestHLSPathParsing(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"/live/index.m3u8", "live", true},
		{"/live/out0.ts", "live", true},
		{"/live", "live", true},
		{"/live/", "live", true},
		{"/a/b/c", "a", true},
		{"/", "", false},
		{"//", "", false},
	}
	for _, c := range cases {
		got, err := pathOf(c.in)
		if (err == nil) != c.ok {
			t.Fatalf("pathOf(%q) err=%v, want ok=%v", c.in, err, c.ok)
		}
		if err == nil && got != c.want {
			t.Fatalf("pathOf(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestHLSAdapterContract pins the split: HLS has no upstream direction, so a
// publish must be refused rather than silently accepted.
func TestHLSAdapterContract(t *testing.T) {
	ad := &Adapter{}
	if ad.CanPublish() {
		t.Fatal("HLS must not claim to publish")
	}
	if !ad.CanPlay() {
		t.Fatal("HLS must claim to play")
	}
	if _, err := ad.Publish(context.Background(), nil); err == nil {
		t.Fatal("publish must be refused")
	}
	if got := ad.ModuleInfo().Type; got != registry.TAdapter {
		t.Fatalf("type = %v, want adapter", got)
	}
	for _, scheme := range ad.Schemes() {
		if !ad.SupportsScheme(scheme) {
			t.Fatalf("scheme %s declared but not supported", scheme)
		}
	}
	// A server-less adapter must fail rather than claim a session.
	if _, err := ad.Play(context.Background(), nil); err == nil {
		t.Fatal("play with no server must fail")
	}
}

// TestHLSFeedDoesNotReportFailureAsSuccess guards the drive loop's error
// handling.
//
// A subscription whose ReadUnit returns an error must not look like the end of
// a stream. Returning nil there makes every subsequent request rebuild the
// muxer, so it never advances and every player sees a frozen feed.
func TestHLSFeedDoesNotReportFailureAsSuccess(t *testing.T) {
	if err := drive(context.Background(), &muxEntry{}, &fakeSub{err: io.EOF}); err == nil {
		t.Fatal("an EOF read must not be reported as success")
	}
	if err := drive(context.Background(), &muxEntry{}, &fakeSub{err: io.ErrUnexpectedEOF}); err == nil {
		t.Fatal("a hard error must not be reported as success")
	}
}

// TestHLSWriteHLSPayloads exercises writeHLS against a real muxer.
//
// The muxer resolves tracks by pointer, so the track it was built with and the
// track the unit is written under must be the same value. That is the failure
// this adapter can make silently: a unit whose payload reaches a nil codec
// panics inside the muxer, and the feed reports it as the end of the stream.
func TestHLSWriteHLSPayloads(t *testing.T) {
	vt, at, err := toHLS(testTracks())
	if err != nil {
		t.Fatalf("toHLS: %v", err)
	}
	m := &gohlslib.Muxer{
		Tracks:             []*gohlslib.Track{vt, at},
		Variant:            gohlslib.MuxerVariantMPEGTS,
		SegmentCount:       defaultSegmentCount,
		SegmentMinDuration: defaultSegmentDuration,
	}
	if err := m.Start(); err != nil {
		t.Fatalf("muxer start: %v", err)
	}
	defer m.Close()

	e := &muxEntry{muxer: m, vt: vt, at: at}
	anchor := time.Unix(0, 0).UTC()

	vu := stream.NewUnit(&stream.Unit{
		TrackID: 1, Codec: codecH264, Kind: stream.KindVideo,
		Payload: h264.SyntheticAU(true), PTS: anchor,
	})
	if err := writeHLS(e, anchor, vu); err != nil {
		t.Fatalf("video unit: %v", err)
	}
	vu.Release()

	au := stream.NewUnit(&stream.Unit{
		TrackID: 2, Codec: codecAAC, Kind: stream.KindAudio,
		Payload: []byte{0x21, 0x6f, 0x84, 0x03, 0x80, 0xaa}, PTS: anchor,
	})
	if err := writeHLS(e, anchor, au); err != nil {
		t.Fatalf("audio unit: %v", err)
	}
	au.Release()
}

// TestHLSWriteHLSRejectsUnknownCodec covers the default branch.
func TestHLSWriteHLSRejectsUnknownCodec(t *testing.T) {
	e := &muxEntry{}
	u := stream.NewUnit(&stream.Unit{
		TrackID: 1, Codec: "hevc", Kind: stream.KindVideo,
		Payload: h264.SyntheticAU(true), PTS: time.Unix(0, 0).UTC(),
	})
	defer u.Release()
	if err := writeHLS(e, time.Unix(0, 0).UTC(), u); err == nil {
		t.Fatal("an unsupported codec must be refused")
	}
}

// TestHLSWriteHLSConfigOnly writes an access unit that carries parameter sets
// and no picture. The muxer skips it, and so must this adapter.
func TestHLSWriteHLSConfigOnly(t *testing.T) {
	vt, at, err := toHLS(testTracks())
	if err != nil {
		t.Fatalf("toHLS: %v", err)
	}
	m := &gohlslib.Muxer{
		Tracks:             []*gohlslib.Track{vt, at},
		Variant:            gohlslib.MuxerVariantMPEGTS,
		SegmentCount:       defaultSegmentCount,
		SegmentMinDuration: defaultSegmentDuration,
	}
	if err := m.Start(); err != nil {
		t.Fatalf("muxer start: %v", err)
	}
	defer m.Close()

	e := &muxEntry{muxer: m, vt: vt, at: at}
	u := stream.NewUnit(&stream.Unit{
		TrackID: 1, Codec: codecH264, Kind: stream.KindVideo,
		Payload: container.EncodeAU([][]byte{h264.SyntheticSPS(), h264.SyntheticPPS()}),
		PTS:     time.Unix(0, 0).UTC(),
	})
	if err := writeHLS(e, time.Unix(0, 0).UTC(), u); err != nil {
		t.Fatalf("a config-only unit must be accepted: %v", err)
	}
	u.Release()
}

// TestHLSConcurrentRequests sends a burst of requests for one path. The muxer
// is keyed by path and every request takes the same lock, so a lost wakeup in
// the feed table would show up here as a panic or a hang.
//
// The frames are pushed before the burst, and the test waits for the first
// segment to land. gohlslib's multivariant handler blocks on a condition
// variable until the stream has content, so a burst aimed at a path with no
// media never returns: every request parks inside upstream code that QuickMedia
// does not control, and the test reports a hang instead of the result it asked
// for. Waiting makes the burst exercise the contention the test names — sixteen
// readers of one muxer, one feed — and nothing else.
func TestHLSConcurrentRequests(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	srv := NewServer(m, Options{})
	defer srv.Close()
	w := startPublisher(t, m, "live")
	if _, err := srv.ensureFeedFromPath("live"); err != nil {
		t.Fatalf("prime feed: %v", err)
	}
	pushFrames(w, 0, segmentCount)
	if _, _, err := waitForSegmentCtx(srv, "live", 5*time.Second); err != nil {
		t.Fatalf("no segment for %q: %v", "live", err)
	}

	var wg sync.WaitGroup
	fails := make(chan string, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if code, body := get(srv, "/live/index.m3u8"); code != http.StatusOK {
				fails <- body
			}
		}()
	}
	wg.Wait()
	close(fails)
	for b := range fails {
		t.Fatalf("request failed: %q", b)
	}
}

// --- fakes -----------------------------------------------------------------

// fakeAdapter publishes a fixed track table, which is all the tests need.
type fakeAdapter struct {
	begin []*stream.Track
	w     stream.StreamWriter
}

func (a *fakeAdapter) ModuleInfo() registry.ModuleInfo {
	return registry.ModuleInfo{Name: "test", Version: "0.0.0", Type: registry.TAdapter}
}

func (a *fakeAdapter) Schemes() []string            { return []string{"test"} }
func (a *fakeAdapter) SupportsScheme(s string) bool { return s == "test" }
func (a *fakeAdapter) CanPublish() bool             { return true }
func (a *fakeAdapter) CanPlay() bool                { return false }
func (a *fakeAdapter) Play(context.Context, registry.PlaySession) (registry.Sink, error) {
	return nil, io.EOF
}

func (a *fakeAdapter) Publish(ctx context.Context, s registry.PublishSession) (registry.Source, error) {
	w, err := s.Begin(a.begin)
	if err != nil {
		return nil, err
	}
	a.w = w
	return newFakeSource(w), nil
}

// fakeSource publishes until the manager tears it down. It never writes on its
// own: the tests push frames through the writer they were handed, which is what
// makes the segment boundary deterministic.
type fakeSource struct {
	done chan struct{}
	w    stream.StreamWriter
	once bool
}

func newFakeSource(w stream.StreamWriter) *fakeSource {
	return &fakeSource{done: make(chan struct{}), w: w}
}

func (s *fakeSource) Err() error            { return nil }
func (s *fakeSource) Done() <-chan struct{} { return s.done }
func (s *fakeSource) RemoteAddr() string    { return "test" }
func (s *fakeSource) Close() error {
	if !s.once {
		s.once = true
		close(s.done)
	}
	return nil
}

// fakeSub is a subscription that returns one error.
type fakeSub struct{ err error }

func (f *fakeSub) ReadUnit(context.Context) (*stream.Unit, error) { return nil, f.err }
func (f *fakeSub) Tracks() []*stream.Track                        { return nil }
func (f *fakeSub) Cancel()                                        {}
func (f *fakeSub) Canceled() bool                                 { return false }
func (f *fakeSub) Path() string                                   { return "" }
func (f *fakeSub) Reason() stream.CancelReason                    { return "" }
func (f *fakeSub) Stats() stream.SubscriptionStats                { return stream.SubscriptionStats{} }

var _ stream.Subscription = (*fakeSub)(nil)
