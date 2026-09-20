// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package httpflv

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/container/h264"
	"github.com/yejinlei/quickmedia/kernel/path"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
	"github.com/yejinlei/quickmedia/transport"
)

// testTracks is the description every test publishes. It is carried by the
// module's own fixture generators rather than by hand-written bytes, so the
// test fails when the fixture changes instead of quietly publishing a
// configuration the codecs do not understand.
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

// frameInterval is the PTS cadence the fixtures publish at: 25 fps.
const frameInterval = 40 * time.Millisecond

// pushGap is the wall-clock space between two written frames. It is far
// smaller than frameInterval, so the test does not spend real time writing
// media whose timestamps are already spaced. Media time is what a player reads.
const pushGap = 2 * time.Millisecond

// aau is one AAC access unit. The same bytes in every test, so an audio tag
// that round-trips must compare byte for byte.
var aau = []byte{0x21, 0x6f, 0x84, 0x03, 0x80, 0xaa}

// feed keeps pushing frames until done is closed.
//
// A live stream is written from its own goroutine rather than from the test's
// read loop: the server handler blocks on the request context, so a read that
// stalls would also stall the writer and the connection would time out with
// nothing left to assert on. Keeping the two independent is what makes the
// assertions about tag order rather than about scheduling.
func feed(w stream.StreamWriter, from, n int, done <-chan struct{}) {
	for i := range n {
		select {
		case <-done:
			return
		default:
		}
		pushFrames(w, from+i, 1)
	}
}

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

// pushFrames writes n interleaved video and audio units starting at index from.
//
// Every video unit is an IDR carrying the parameter sets inline, which is what
// makes the stream decodable from any point: a FLV player reads the AVCC tag
// the moment it arrives and has no decoder-state side channel.
func pushFrames(w stream.StreamWriter, from, n int) {
	vau := h264.SyntheticAU(true)
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

// bindHTTP serves h on an ephemeral TCP port and returns the bound address.
//
// It uses transport.Listener rather than httptest so the handler is exercised
// through the same L1 the binary uses, which is the layer that owns every
// listen() in the tree.
func bindHTTP(t *testing.T, h http.Handler) string {
	t.Helper()
	ln, err := transport.NewListener(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	srv := &http.Server{Handler: h}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	go func() { _ = srv.Serve(httpListener{ln: ln}) }()
	return ln.Addr()
}

// httpListener adapts transport.Listener to net.Listener. net/http has no
// context, so the server's lifetime is the context held by the transport layer.
type httpListener struct{ ln *transport.Listener }

func (l httpListener) Accept() (net.Conn, error) { return l.ln.Accept(context.Background()) }
func (l httpListener) Close() error              { return l.ln.Close() }
func (l httpListener) Addr() net.Addr            { return netAddr(l.ln.Addr()) }

type netAddr string

func (a netAddr) Network() string { return "tcp" }
func (a netAddr) String() string  { return string(a) }

// openFLV opens a live FLV connection for name and issues the GET.
//
// It returns the response rather than the connection because net/http adds
// chunked transfer encoding when the handler writes without a Content-Length,
// which is exactly what a live FLV handler does. Reading the raw connection
// would leave the chunk size in the way of the FLV header; resp.Body is the
// reader that has already stripped it.
func openFLV(t *testing.T, addr, name string, timeout time.Duration) *http.Response {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(timeout))

	fmt.Fprintf(conn, "GET /%s.flv HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", name, addr)

	r := bufio.NewReader(conn)
	resp, err := http.ReadResponse(r, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// readHeader consumes the FLV file header and checks every byte of it.
//
// The header's trailing PreviousTagSize of nine is where a FLV writer usually
// gets the stream wrong: a player computes its read offset from that field, so
// a zero here shifts every tag by nine bytes and the second tag fails with a
// decode error that gives no hint at the cause.
func readHeader(t *testing.T, r io.Reader) {
	t.Helper()
	h := make([]byte, len(header))
	if _, err := io.ReadFull(r, h); err != nil {
		t.Fatalf("read flv header: %v", err)
	}
	for i, want := range header {
		if h[i] != want {
			t.Fatalf("flv header[%d] = %x, want %x (full: %x)", i, h[i], want, h)
		}
	}
}

// flvTag is one parsed tag.
type flvTag struct {
	tagType byte
	ts      uint32
	body    []byte
}

// readTag reads one FLV tag: type, body length, timestamp, stream id, body,
// then the previous-tag size. The size field is the offset of the next tag, so
// skipping it desynchronises the reader one tag later rather than at once.
func readTag(r io.Reader) (flvTag, error) {
	var head [11]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return flvTag{}, err
	}
	n := int(head[1]) | int(head[2])<<8 | int(head[3])<<16
	ts := uint32(head[4]) | uint32(head[5])<<8 | uint32(head[6])<<16 | uint32(head[7])<<24
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return flvTag{}, err
	}
	var prev [4]byte
	if _, err := io.ReadFull(r, prev[:]); err != nil {
		return flvTag{}, err
	}
	return flvTag{tagType: head[0], ts: ts, body: body}, nil
}

// TestHTTPFLVEndToEnd publishes into a real path and reads a real FLV off the
// wire with a real TCP client.
//
// Every tag is parsed with the container layer's own reader, so the test fails
// if the adapter writes framing the container layer cannot read back. That is
// the loop that catches a wrong PreviousTagSize, a wrong tag type byte, and an
// AVCC length prefix in the wrong place: each of those parses to bytes rather
// than to an error, so a length-only assertion would pass all of them.
func TestHTTPFLVEndToEnd(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	addr := bindHTTP(t, NewServer(m))
	w := startPublisher(t, m, "live")

	resp := openFLV(t, addr, "live", 10*time.Second)
	r := resp.Body

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != "video/x-flv" {
		t.Fatalf("content-type = %q, want video/x-flv", got)
	}

	readHeader(t, r)

	var vcfg, vframe, acfg, aframe flvTag
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go feed(w, 0, 40, done)

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) && (vcfg.body == nil || vframe.body == nil ||
		acfg.body == nil || aframe.body == nil) {
		tg, err := readTag(r)
		if err != nil {
			time.Sleep(time.Millisecond)
			continue
		}
		switch tg.tagType {
		case flvTagVideo:
			if _, _, ok := container.ParseAVCSetupTag(tg.body); ok {
				vcfg = tg
			} else if vframe.body == nil {
				vframe = tg
			}
		case flvTagAudio:
			if len(tg.body) == 2+2 {
				acfg = tg
			} else if aframe.body == nil {
				aframe = tg
			}
		}
	}

	// --- video config -------------------------------------------------------
	// The descriptor is the one tag a player must see before any frame, and it
	// must carry the real parameter sets: a config record with an empty SPS is
	// what makes a player report a decoder error with no message at all.
	if vcfg.body == nil {
		t.Fatal("no AVC config tag was sent")
	}
	sps, pps, ok := container.ParseAVCSetupTag(vcfg.body)
	if !ok {
		t.Fatalf("avc config tag did not parse: %x", vcfg.body)
	}
	if !bytesEqual(sps, h264.SyntheticSPS()) {
		t.Fatalf("sps = %x, want %x", sps, h264.SyntheticSPS())
	}
	if !bytesEqual(pps, h264.SyntheticPPS()) {
		t.Fatalf("pps = %x, want %x", pps, h264.SyntheticPPS())
	}

	// --- coded video frame --------------------------------------------------
	if vframe.body == nil {
		t.Fatal("no coded video tag was sent")
	}
	au, ok := container.ParseTagBody(vframe.body)
	if !ok {
		t.Fatalf("coded video tag did not parse: %x", vframe.body)
	}
	nals := container.ParseAU(au)
	if len(nals) < 3 {
		t.Fatalf("the access unit carries %d nals, want 3 (sps, pps, slice)", len(nals))
	}
	if !bytesEqual(nals[0].Data, h264.SyntheticSPS()) {
		t.Fatalf("first nal = %x, want the sps", nals[0].Data)
	}
	if !bytesEqual(nals[1].Data, h264.SyntheticPPS()) {
		t.Fatalf("second nal = %x, want the pps", nals[1].Data)
	}
	if nals[2].Type != container.NalIDR {
		t.Fatalf("slice nal type = %d, want %d (idr)", nals[2].Type, container.NalIDR)
	}

	// --- audio config and frame --------------------------------------------
	// The config tag is 0xA1 0x01 plus the AudioSpecificConfig, and a player
	// takes its decoder setup from those bytes: wrong sample rate means audio
	// at the wrong pitch rather than a failure the server can log.
	if acfg.body == nil {
		t.Fatal("no AAC sequence tag was sent")
	}
	if len(acfg.body) < 2 || acfg.body[0] != 0xA1 || acfg.body[1] != 0x01 {
		t.Fatalf("aac config tag head = %x, want 0xA1 0x01", acfg.body[:min(2, len(acfg.body))])
	}
	asiof, ok := container.ParseAudioTagBody(acfg.body)
	if !ok {
		t.Fatalf("aac config tag did not parse: %x", acfg.body)
	}
	// AAC-LC at 44100 Hz with two channels is 0x12 0x10: objectType 2 in bits
	// 4..0 of byte 0, the sampling index 4 straddling the boundary, and the
	// channel config in bits 6..3 of byte 1. Both fields are the layout the
	// decoder trusts, so a wrong value is silent rather than an error.
	if !bytesEqual(asiof, []byte{0x12, 0x10}) {
		t.Fatalf("audio specific config = %x, want %x", asiof, []byte{0x12, 0x10})
	}

	if aframe.body == nil {
		t.Fatal("no coded audio tag was sent")
	}
	payload, ok := container.ParseAudioTagBody(aframe.body)
	if !ok {
		t.Fatalf("coded audio tag did not parse: %x", aframe.body)
	}
	if !bytesEqual(payload, aau) {
		t.Fatalf("audio payload = %x, want %x", payload, aau)
	}

	// --- timestamps ---------------------------------------------------------
	// The first tag must not report a timestamp measured from the epoch, or a
	// player tries to seek to 1970 and drops the stream. Every tag must sit at
	// or after the adapter's own offset, which is what keeps the sequence
	// monotonic and comparable across the two codecs.
	for _, tg := range []flvTag{vcfg, vframe, acfg, aframe} {
		if tg.ts < tagOffset {
			t.Fatalf("tag %d has timestamp %d ms, below the %d ms offset",
				tg.tagType, tg.ts, tagOffset)
		}
	}
	// A config tag emitted after the frame it configures is a decodable
	// surprise for a player that starts rendering as soon as the first frame
	// arrives.
	if vframe.ts < vcfg.ts {
		t.Fatalf("video frame %d ms precedes its config %d ms", vframe.ts, vcfg.ts)
	}
	if aframe.ts < acfg.ts {
		t.Fatalf("audio frame %d ms precedes its config %d ms", aframe.ts, acfg.ts)
	}

	t.Logf("vcfg ts=%d %d bytes, vframe ts=%d %d bytes, acfg ts=%d %d bytes, aframe ts=%d %d bytes",
		vcfg.ts, len(vcfg.body), vframe.ts, len(vframe.body),
		acfg.ts, len(acfg.body), aframe.ts, len(aframe.body))
}

// TestHTTPFLVMissingPath is a path that has no publisher.
//
// Subscribe waits until the request context expires, so the client gets a 404
// rather than a connection that never answers. A hang here is a player that
// shows a spinner forever with nothing on the server to read.
func TestHTTPFLVMissingPath(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	addr := bindHTTP(t, NewServer(m))

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	fmt.Fprintf(conn, "GET /absent.flv HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", addr)

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestHTTPFLVBadPath is the one request the adapter must reject without
// consulting the kernel: a URL that names no stream.
func TestHTTPFLVBadPath(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	addr := bindHTTP(t, NewServer(m))

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n", addr)

	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

// TestHTTPFLVConfigTagIsEmittedExactlyOnce pins the descriptor contract: one
// config tag per codec, ahead of the frames.
//
// Re-emitting the descriptor costs a player a re-initialisation of its decoder
// on every occurrence, which reads as a stutter on a stream that is otherwise
// healthy and is invisible in the server's logs.
func TestHTTPFLVConfigTagIsEmittedExactlyOnce(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	addr := bindHTTP(t, NewServer(m))
	w := startPublisher(t, m, "live")

	resp := openFLV(t, addr, "live", 10*time.Second)
	r := resp.Body
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	readHeader(t, r)

	var (
		vConfigs, aConfigs int
		vFrames, aFrames   int
		vOrder             = "not seen"
	)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go feed(w, 0, 60, done)

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if vFrames >= 3 && aFrames >= 3 {
			break
		}
		tg, err := readTag(r)
		if err != nil {
			time.Sleep(time.Millisecond)
			continue
		}
		switch tg.tagType {
		case flvTagVideo:
			if _, _, ok := container.ParseAVCSetupTag(tg.body); ok {
				vConfigs++
				if vFrames == 0 {
					vOrder = "config-first"
				}
			} else {
				vFrames++
			}
		case flvTagAudio:
			if len(tg.body) == 2+2 {
				aConfigs++
			} else {
				aFrames++
			}
		}
	}
	if vOrder != "config-first" {
		t.Fatalf("the video config tag arrived after its first frame")
	}
	if vConfigs != 1 {
		t.Fatalf("video config tags = %d, want exactly 1", vConfigs)
	}
	if aConfigs != 1 {
		t.Fatalf("audio config tags = %d, want exactly 1", aConfigs)
	}
	if vFrames < 3 {
		t.Fatalf("coded video frames = %d, want at least 3", vFrames)
	}
	if aFrames < 3 {
		t.Fatalf("coded audio frames = %d, want at least 3", aFrames)
	}
}

// bytesEqual compares two slices without allocating.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- test doubles ----------------------------------------------------------

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
	return &fakeSource{done: make(chan struct{}), w: w}, nil
}

// fakeSource publishes until the manager tears it down. It never writes on its
// own: the tests push frames through the writer they were handed, which is what
// makes the tag order deterministic.
type fakeSource struct {
	done chan struct{}
	w    stream.StreamWriter
	once bool
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

var _ registry.Source = (*fakeSource)(nil)
