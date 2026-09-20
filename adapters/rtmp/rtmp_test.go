// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package rtmp

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"sync"
	"testing"
	"time"

	gortmplib "github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
	rtmpmsg "github.com/bluenviron/gortmplib/pkg/message"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/container/h264"
	"github.com/yejinlei/quickmedia/kernel/path"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
	"github.com/yejinlei/quickmedia/transport"
)

// frameInterval is the PTS cadence the fixtures publish at: a 25 fps keyframe
// rate, so media time advances without the test spending wall clock on it.
const frameInterval = 40 * time.Millisecond

// aau is one AAC access unit. The same bytes in every test, so a payload that
// crosses the wire must compare byte for byte.
var aau = []byte{0x21, 0x6f, 0x84, 0x03, 0x80, 0xaa}

// testTracks is the description a play test publishes through the kernel.
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
// It is synchronous: each unit goes into a bounded ring a feed goroutine drains,
// so returning here means the frames are queued. Every video unit is a keyframe,
// which is what makes a segment or segmenter close on one of them.
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
		time.Sleep(2 * time.Millisecond)
	}
}

// startRTMP listens on an ephemeral port and runs one ServeConn per accepted
// connection, which is the shape the composition root uses.
func startRTMP(t *testing.T, m *path.Manager) string {
	t.Helper()
	ln, err := transport.NewListener(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var wg sync.WaitGroup
	go func() {
		for {
			conn, err := ln.Accept(context.Background())
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := ServeConn(context.Background(), conn, m); err != nil {
					t.Logf("session ended: %v", err)
					// ServeConn returned an error, so no session owns the
					// connection: nothing else closes it.
					_ = conn.Close()
					return
				}
			}()
		}
	}()
	t.Cleanup(func() { ln.Close(); wg.Wait() })
	return ln.Addr()
}

// rtmpTracks is the codec table a client advertises. The parameters are carried
// by the module's own fixture generators, so the adapter has real bytes to
// convert rather than a placeholder a decoder would reject.
func rtmpTracks() []*gortmplib.Track {
	return []*gortmplib.Track{
		{Codec: &codecs.H264{SPS: h264.SyntheticSPS(), PPS: h264.SyntheticPPS()}},
		{Codec: &codecs.MPEG4Audio{Config: &mpeg4audio.AudioSpecificConfig{
			Type:          mpeg4audio.ObjectTypeAACLC,
			SampleRate:    44100,
			ChannelConfig: 2,
		}}},
	}
}

type rtConn struct {
	c   *gortmplib.Client
	drn chan rtmResult
}

type rtmResult struct {
	msg rtmpmsg.Message
	err error
}

// connectRTMP initializes a gortmplib client for addr and app, and drains the
// receive direction in a single goroutine that the test reads results from.
//
// One goroutine owns the connection: gortmplib reads the chunk state machine
// through the raw conn with no internal lock, so two concurrent readers
// interleave mid-frame and wedge each other. In production the server does
// exactly the same thing, but in the other direction: the adapter holds one read
// loop per session. A real publisher gets its ping replies on that same stream,
// which is what this channel is a stand-in for.
func connectRTMP(t *testing.T, addr, app string, publish bool) *rtConn {
	t.Helper()
	u, err := url.Parse("rtmp://" + addr + "/" + app)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	c := &gortmplib.Client{URL: u, Publish: publish}
	if err := c.Initialize(context.Background()); err != nil {
		t.Fatalf("rtmp initialize: %v", err)
	}
	t.Cleanup(c.Close)

	r := &rtConn{c: c, drn: make(chan rtmResult, 32)}
	done := make(chan struct{})
	go func() {
		defer close(r.drn)
		_ = c.NetConn().SetReadDeadline(time.Now().Add(60 * time.Second))
		for {
			select {
			case <-done:
				return
			default:
			}
			msg, err := c.Read()
			select {
			case r.drn <- rtmResult{msg: msg, err: err}:
			case <-done:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { close(done) })
	return r
}

// readWithTimeout waits up to d for one message on the connection.
func (r *rtConn) readWithTimeout(t *testing.T, d time.Duration) (rtmpmsg.Message, error) {
	t.Helper()
	select {
	case v, ok := <-r.drn:
		if !ok {
			return nil, io.EOF
		}
		return v.msg, v.err
	case <-time.After(d):
		return nil, io.EOF
	}
}

// TestRTMPPublish is the ingest direction: a real gortmplib publisher on a real
// TCP connection, and the kernel path it lands in.
//
// The assertion is on the kernel state rather than on the wire, because the
// wire is symmetric: both directions carry the same chunk framing, so bytes
// moving is not evidence the codecs were decoded. What must be checked is that
// the negotiated track table reached the kernel intact, the parameter sets byte
// for byte, and that the frames the client sent arrived in the kernel's ring. A
// wrong AVC repack would let the path exist and the publisher connect, which a
// protocol-level check would pass while a decoder saw garbage.
func TestRTMPPublish(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	addr := startRTMP(t, m)
	// RTMP needs an app and a stream key, and the adapter uses the URL path as
	// the kernel's path name, so the name a path validator would accept.
	c := connectRTMP(t, addr, "live/test", true)

	w := &gortmplib.Writer{Conn: c.c, Tracks: rtmpTracks()}
	if err := w.Initialize(); err != nil {
		t.Fatalf("rtmp writer: %v", err)
	}

	// The kernel carries an access unit AVCC-length-prefixed; the library takes
	// the NAL bodies, so the round trip is split then reassemble and must come
	// back byte identical.
	bodies := make([][]byte, 0, 3)
	for _, n := range container.ParseAU(h264.SyntheticAU(true)) {
		bodies = append(bodies, n.Data)
	}
	if len(bodies) == 0 {
		t.Fatal("the fixture produced no access unit")
	}
	wantAU := h264.SyntheticAU(true)

	// Pushing must outlast the server's handshake, not race it. gortmplib's
	// Reader.Initialize scans the incoming stream until it has seen two seconds
	// of stream time before it commits the track table, and only then does the
	// adapter call Begin. A push that stops before that deadline leaves the
	// server finishing its handshake into a silent client, whose read loop sees
	// EOF and ends the session before the path is ever created. 40 ms cadence
	// means 55 frames is 2.2 s of stream time.
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		for i := range 55 {
			select {
			case <-done:
				return
			default:
			}
			dts := time.Duration(i) * frameInterval
			if err := w.WriteH264(w.Tracks[0], dts, dts, bodies); err != nil {
				t.Logf("publish stopped: %v", err)
				return
			}
			if err := w.WriteMPEG4Audio(w.Tracks[1], dts, aau); err != nil {
				t.Logf("publish stopped: %v", err)
				return
			}
			time.Sleep(3 * time.Millisecond)
		}
	}()

	// The path is created by the kernel from the negotiated track table.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.PathCount() > 0 && len(m.Paths()[0].Tracks) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := m.PathCount(); got != 1 {
		t.Fatalf("path count = %d, want 1", got)
	}

	paths := m.Paths()
	if len(paths) != 1 || paths[0].Name != "live/test" {
		t.Fatalf("paths = %+v, want one path named live/test", paths)
	}
	trks := paths[0].Tracks
	if len(trks) != 2 {
		t.Fatalf("tracks = %d, want 2", len(trks))
	}

	// The kernel assigns track identifiers in declaration order, which is what
	// keeps a republished stream on the same track ids.
	if trks[0].ID != 1 || trks[1].ID != 2 {
		t.Fatalf("track ids = %d,%d, want 1,2", trks[0].ID, trks[1].ID)
	}
	if trks[0].Codec != codecH264 || trks[0].Kind != stream.KindVideo {
		t.Fatalf("video track = %s/%s, want h264/video", trks[0].Codec, trks[0].Kind)
	}
	if trks[1].Codec != codecAAC || trks[1].Kind != stream.KindAudio {
		t.Fatalf("audio track = %s/%s, want aac/audio", trks[1].Codec, trks[1].Kind)
	}

	// The parameter sets must survive the wire byte for byte. An adapter that
	// republishes this path derives its own codec descriptor from these values,
	// so truncating one here would make a downstream player render no video with
	// no error anywhere in the log.
	sps, pps := trks[0].Params["sps"], trks[0].Params["pps"]
	if sps == "" || pps == "" {
		t.Fatalf("h264 track params = %v, want sps and pps", trks[0].Params)
	}
	if sps != hex.EncodeToString(h264.SyntheticSPS()) {
		t.Fatalf("sps = %s, want %s", sps, hex.EncodeToString(h264.SyntheticSPS()))
	}
	if pps != hex.EncodeToString(h264.SyntheticPPS()) {
		t.Fatalf("pps = %s, want %s", pps, hex.EncodeToString(h264.SyntheticPPS()))
	}
	if trks[1].Params["sampleRate"] != "44100" || trks[1].Params["numberOfChannels"] != "2" {
		t.Fatalf("aac track params = %v", trks[1].Params)
	}

	// A subscriber must read what the publisher sent. This is the assertion that
	// separates "the path was created" from "media crossed the kernel": the ring
	// is filled only by frames the adapter decoded from the connection.
	sub, err := m.Subscribe(context.Background(), "live/test", 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	t.Cleanup(func() { sub.Cancel() })

	var videoGot, audioGot bool
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for !videoGot || !audioGot {
		u, err := sub.ReadUnit(sctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		switch u.Codec {
		case codecH264:
			// The published access unit must come back as the same AVCC stream:
			// the adapter splits NAL bodies for RTMP and repacks them, which is
			// the one transform this codec undergoes.
			if bytesEqual(u.Payload, wantAU) {
				videoGot = true
			}
			if !u.Key {
				t.Fatalf("an IDR access unit arrived not marked as a keyframe")
			}
		case codecAAC:
			if bytesEqual(u.Payload, aau) {
				audioGot = true
			}
		default:
			t.Fatalf("unexpected codec %s", u.Codec)
		}
		u.Release()
	}
	t.Logf("published via rtmp: %d tracks, %d bytes sent", len(trks), c.c.BytesSent())
}

// TestRTMPPlay is the egress direction: a real gortmplib player pulling from a
// path published by an in-test adapter.
//
// The sequence start is what a player must receive before its first frame: the
// library builds its track table from it, and a player that reads a coded frame
// before it has the codec description closes the stream silently. Asserting on
// both is the minimum a decoder needs to start.
func TestRTMPPlay(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	addr := startRTMP(t, m)
	w := startPublisher(t, m, "live/test")
	c := connectRTMP(t, addr, "live/test", false)

	// Pushing must start after the client is attached: a unit broadcast into a
	// path with no subscriber is dropped, so the frames the player is about to
	// read have to arrive while it is already subscribed.
	go func() {
		time.Sleep(80 * time.Millisecond)
		pushFrames(w, 0, 40)
	}()

	deadline := time.Now().Add(10 * time.Second)
	gotVideoSeq, gotAudioSeq, videoFrames, audioFrames := false, false, 0, 0
	for time.Now().Before(deadline) && !(gotVideoSeq && videoFrames > 0 && gotAudioSeq && audioFrames > 0) {
		msg, err := c.readWithTimeout(t, 200*time.Millisecond)
		if err != nil {
			continue
		}
		switch t_ := msg.(type) {
		case *rtmpmsg.VideoExSequenceStart:
			if t_.FourCC == rtmpmsg.FourCCAVC && t_.AVCHeader != nil &&
				len(t_.AVCHeader.SequenceParameterSets) > 0 &&
				len(t_.AVCHeader.SequenceParameterSets[0].NALUnit) > 0 &&
				len(t_.AVCHeader.PictureParameterSets) > 0 &&
				len(t_.AVCHeader.PictureParameterSets[0].NALUnit) > 0 {
				gotVideoSeq = true
			}
		case *rtmpmsg.VideoExCodedFrames:
			if t_.FourCC == rtmpmsg.FourCCAVC && len(t_.Payload) > 0 {
				videoFrames++
			}
		case *rtmpmsg.Video:
			if t_.Codec == rtmpmsg.CodecH264 && len(t_.Payload) > 0 {
				gotVideoSeq = true
				videoFrames++
			}
		case *rtmpmsg.AudioExSequenceStart:
			gotAudioSeq = true
		case *rtmpmsg.AudioExCodedFrames:
			if t_.FourCC == rtmpmsg.FourCCMP4A && len(t_.Payload) > 0 {
				audioFrames++
			}
		case *rtmpmsg.Audio:
			if t_.Codec == rtmpmsg.CodecMPEG4Audio && len(t_.Payload) > 0 {
				gotAudioSeq = true
				audioFrames++
			}
		}
	}
	if !gotVideoSeq {
		t.Fatal("the h264 codec descriptor never reached the player")
	}
	if !gotAudioSeq {
		t.Fatal("the aac codec descriptor never reached the player")
	}
	if videoFrames == 0 {
		t.Fatal("no coded h264 frame reached the player")
	}
	if audioFrames == 0 {
		t.Fatal("no coded aac frame reached the player")
	}
	t.Logf("played via rtmp: video %d frames, audio %d frames", videoFrames, audioFrames)
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

// fakeSource publishes until the manager tears it down.
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
