// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package rtsp

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"testing"
	"time"

	gortsplib "github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	gsp "github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	"github.com/pion/rtp"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/container/h264"
	"github.com/yejinlei/quickmedia/kernel/path"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// frameInterval is the PTS cadence the fixtures publish at: a 25 fps keyframe
// rate, which lets media time advance without the test spending wall clock on
// it. RTSP is clock-based rather than sequence-based, so the cadence is what a
// player's media clock steps by.
const frameInterval = 40 * time.Millisecond

// aau is one AAC access unit. The same bytes in every test, so a payload that
// crosses the wire must compare byte for byte.
var aau = []byte{0x21, 0x6f, 0x84, 0x03, 0x80, 0xaa}

// pathName is the stream a test announces, plays and expects to find in the
// kernel's path table.
const pathName = "live/stream"

// frame is one access unit as it arrives in a client callback: the unpacked
// payload plus the RTP headers it carried.
type frame struct {
	payload []byte
	key     bool
	seq     uint16
	ts      uint32
}

// testTracks is the description a play test publishes through the kernel. The
// parameter sets come from the codec module's own fixtures, so the SDP this
// adapter generates is one a decoder would accept rather than a placeholder.
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

// startRTSP binds the RTSP control listener on an ephemeral port and returns
// the address a client should talk to.
//
// Ephemeral ports are deliberate: a fixed port makes the test depend on the
// host, and the server is exercised exactly as the composition root exercises
// it, which is Start on whatever the operator configured. UDP is left unbound
// because the clients force TCP; the library requires RTP and RTCP to be an
// even pair on consecutive ports, which ":0" cannot provide, and the server
// would only waste a bind on ports nothing would use.
func startRTSP(t *testing.T, m *path.Manager) string {
	t.Helper()
	srv := NewServer(m, Options{RTSPAddress: "127.0.0.1:0"})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("rtsp start: %v", err)
	}
	t.Cleanup(srv.Close)
	return srv.gortsrv.NetListener().Addr().String()
}

// startPublisher publishes the path through the kernel and returns the writer
// the adapter opened. Nothing is pushed: the caller decides when the stream
// starts, which is what separates "the path exists" from "media flows".
func startPublisher(t *testing.T, m *path.Manager, name string) stream.StreamWriter {
	t.Helper()
	ad := &fakeAdapter{begin: testTracks()}
	src, err := m.Publish(context.Background(), ad, registry.SinkRequest{Path: name})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })
	if ad.w == nil {
		t.Fatal("publish did not hand back a writer")
	}
	return ad.w
}

// pushFrames writes n interleaved video and audio units starting at index from.
//
// Every video unit is a keyframe, which is what makes a receiver that joins
// partway resync on a decodable boundary. Units are interleaved the way an
// encoder emits them, so the kernel's per-track sequence counters both advance.
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

// rtspSDP is the description a test publisher announces.
func rtspSDP() *description.Session {
	return &description.Session{
		Title: "QuickMedia test",
		Medias: []*description.Media{
			{
				Type: description.MediaTypeVideo,
				Formats: []gsp.Format{&gsp.H264{
					PayloadTyp: videoPayloadTyp, PacketizationMode: 1,
					SPS: h264.SyntheticSPS(), PPS: h264.SyntheticPPS(),
				}},
			},
			{
				Type: description.MediaTypeAudio,
				Formats: []gsp.Format{&gsp.MPEG4Audio{
					PayloadTyp: audioPayloadTyp, ProfileLevelID: 1, SizeLength: 13,
					Config: &mpeg4audio.AudioSpecificConfig{
						Type:          mpeg4audio.ObjectTypeAACLC,
						SampleRate:    44100,
						ChannelConfig: 2,
					},
				}},
			},
		},
	}
}

// rtspURL builds the RTSP URL for one path.
func rtspURL(addr, name string) *base.URL {
	u, err := base.ParseURL("rtsp://" + addr + "/" + name)
	if err != nil {
		panic(err)
	}
	return u
}

// sdpOf describes one path through a throwaway client. A client that is already
// publishing is in RECORD, where DESCRIBE is not allowed, so this is the only
// way to observe the SDP a player would receive from a live path.
func sdpOf(t *testing.T, addr, name string) (*description.Session, error) {
	t.Helper()
	tcp := gortsplib.ProtocolTCP
	c := &gortsplib.Client{
		Scheme: "rtsp", Host: addr, Protocol: &tcp, UserAgent: "quickmedia-test",
	}
	if err := c.Start(); err != nil {
		return nil, err
	}
	defer c.Close()
	desc, _, err := c.Describe(rtspURL(addr, name))
	return desc, err
}

// unpackFrames returns one unpacker per described media, in SDP order. The
// library's Format has no unpack method, so the client side of a test must
// decode with the codec layer that the server used to encode: that is the
// assertion that the RTP on the wire is decodable, which a packet count alone
// cannot make.
func unpackFrames(t *testing.T, medias []*description.Media) []registry.RTPUnpacker {
	t.Helper()
	out := make([]registry.RTPUnpacker, len(medias))
	for i, medi := range medias {
		var id stream.CodecID
		switch medi.Type {
		case description.MediaTypeVideo:
			if _, ok := medi.Formats[0].(*gsp.H264); !ok {
				t.Fatalf("media %d: video is not H264", i)
			}
			id = codecH264
		case description.MediaTypeAudio:
			if _, ok := medi.Formats[0].(*gsp.MPEG4Audio); !ok {
				t.Fatalf("media %d: audio is not MPEG4Audio", i)
			}
			id = codecAAC
		default:
			t.Fatalf("media %d: unsupported type %s", i, medi.Type)
		}
		pc, err := registry.SelectCodec(id)
		if err != nil {
			t.Fatalf("media %d: no codec %s: %v", i, id, err)
		}
		out[i], err = pc.NewRTPUnpacker()
		if err != nil {
			t.Fatalf("media %d: unpacker: %v", i, err)
		}
	}
	return out
}

// mediaIndex returns the position of one media in a client's description.
func mediaIndex(medias []*description.Media, target *description.Media) int {
	for i, m := range medias {
		if m == target {
			return i
		}
	}
	return -1
}

// captureRTP wires one client into the unpackers and returns the channel the
// completed access units appear on. The payload is copied because the unpacker
// reuses pooled memory on its next call.
func captureRTP(t *testing.T, tc *gortsplib.Client, medias []*description.Media) (chan frame, func()) {
	t.Helper()
	unpack := unpackFrames(t, medias)
	done := make(chan struct{})
	ch := make(chan frame, 256)

	tc.OnPacketRTPAny(func(medi *description.Media, forma gsp.Format, pkt *rtp.Packet) {
		idx := mediaIndex(medias, medi)
		if idx < 0 {
			return
		}
		u, err := unpack[idx].Unpack(pkt.Payload, pkt.SequenceNumber, pkt.Timestamp, pkt.Marker)
		if err != nil || u == nil {
			return
		}
		select {
		case ch <- frame{
			payload: append([]byte(nil), u.Payload...),
			key:     u.Key, seq: pkt.SequenceNumber, ts: pkt.Timestamp,
		}:
		case <-done:
		}
	})
	return ch, func() { close(done) }
}

// packMedia turns one kernel unit into the RTP payloads an adapter writes for it.
func packMedia(t *testing.T, u *stream.Unit, seq uint16, ts uint32) [][]byte {
	t.Helper()
	pc, err := registry.SelectCodec(u.Codec)
	if err != nil {
		t.Fatalf("no codec %s: %v", u.Codec, err)
	}
	packer, err := pc.NewRTPPacker()
	if err != nil {
		t.Fatalf("packer: %v", err)
	}
	payloads, _, _ := packer.Pack(u, seq, ts)
	if len(payloads) == 0 {
		t.Fatalf("codec %s produced no packets", u.Codec)
	}
	return payloads
}

// sendUnit writes one unit as RTP packets for one media. Both header bytes come
// from the client's own description: the library finds a packet's writer by the
// version and payload type, so writing with the zero header the packer emits
// would find no format and dereference a nil rather than report an error.
func sendUnit(t *testing.T, tc *gortsplib.Client, medi *description.Media, u *stream.Unit,
	seq uint16, ts uint32) {
	t.Helper()
	if len(medi.Formats) == 0 {
		t.Fatal("the described media carries no format")
	}
	payloadTyp := medi.Formats[0].PayloadType()
	payloads := packMedia(t, u, seq, ts)
	for j, pl := range payloads {
		if err := tc.WritePacketRTP(medi, &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    payloadTyp,
				SequenceNumber: seq + uint16(j), Timestamp: ts,
				Marker: j == len(payloads)-1,
			},
			Payload: pl,
		}); err != nil {
			t.Fatalf("write packet: %v", err)
		}
	}
}

// waitTracks polls until the path is visible with its track table, which is the
// point at which the server is ready to accept media.
func waitTracks(t *testing.T, m *path.Manager) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m.PathCount() > 0 && len(m.Paths()[0].Tracks) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the announced path never became visible")
}

// TestRTSPPublish is the ingest direction: a real RTSP publisher over a real
// TCP connection, and the kernel path its media lands in.
//
// The assertion is on the kernel state, not on the control exchange. ANNOUNCE
// succeeding proves only that the SDP parsed; the track table and the access
// units are what have to survive. A packetization error would still let a
// publisher connect and keep sending, which is how a server can look healthy
// in the logs while every downstream player gets a corrupt stream.
func TestRTSPPublish(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	addr := startRTSP(t, m)
	sdp := rtspSDP()

	tcp := gortsplib.ProtocolTCP
	tc := &gortsplib.Client{
		Scheme:    "rtsp",
		Host:      addr,
		Protocol:  &tcp,
		UserAgent: "quickmedia-test",
	}
	if err := tc.StartRecording("rtsp://"+addr+"/"+pathName, sdp); err != nil {
		t.Fatalf("start recording: %v", err)
	}
	t.Cleanup(tc.Close)

	// The library keys a client's setup table by the media pointers of the
	// session that was announced, and DESCRIBE cannot run in the state RECORD
	// that StartRecording leaves behind. Media is therefore written for sdp,
	// which is the session the client actually negotiated, and the SDP the
	// server serves is checked separately in its own client below.
	waitTracks(t, m)

	wantAU := h264.SyntheticAU(true)
	anchor := time.Now().UTC()
	const n = 20
	for i := range n {
		ts := uint32(i) * uint32(container.Timescale) / uint32(time.Second/frameInterval)
		video := stream.NewUnit(&stream.Unit{
			TrackID: 1, Codec: codecH264, Kind: stream.KindVideo,
			Payload: wantAU, PTS: anchor.Add(time.Duration(i) * frameInterval),
			DTS: anchor.Add(time.Duration(i) * frameInterval), Key: true,
		})
		sendUnit(t, tc, sdp.Medias[0], video, uint16(i*2), ts)

		audio := stream.NewUnit(&stream.Unit{
			TrackID: 2, Codec: codecAAC, Kind: stream.KindAudio,
			Payload: aau, PTS: anchor.Add(time.Duration(i) * frameInterval),
			DTS: anchor.Add(time.Duration(i) * frameInterval),
		})
		sendUnit(t, tc, sdp.Medias[1], audio, uint16(i*2+1), ts)
	}

	// A subscriber must read what the publisher sent. This is the assertion that
	// separates "the path was created" from "media crossed the kernel": the ring
	// is filled only by frames the adapter unpacked from the connection.
	sub, err := m.Subscribe(context.Background(), pathName, 0)
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

	// A published stream must also describe itself through a separate client:
	// this is the exchange a player makes, and it must carry the parameter sets
	// the publisher sent. It cannot run on tc, which is already in RECORD, where
	// the library forbids DESCRIBE.
	desc, err := sdpOf(t, addr, pathName)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if len(desc.Medias) != 2 {
		t.Fatalf("described %d medias, want 2", len(desc.Medias))
	}
	descH264, ok := desc.Medias[0].Formats[0].(*gsp.H264)
	if !ok {
		t.Fatalf("described video format = %T, want H264", desc.Medias[0].Formats[0])
	}
	if !bytesEqual(descH264.SPS, h264.SyntheticSPS()) || !bytesEqual(descH264.PPS, h264.SyntheticPPS()) {
		t.Fatal("the described parameter sets do not match what was announced")
	}
	descAAC, ok := desc.Medias[1].Formats[0].(*gsp.MPEG4Audio)
	if !ok {
		t.Fatalf("described audio format = %T, want MPEG4Audio", desc.Medias[1].Formats[0])
	}
	if descAAC.Config == nil || descAAC.Config.SampleRate != 44100 || descAAC.Config.ChannelConfig != 2 {
		t.Fatalf("described audio config = %+v", descAAC.Config)
	}

	if got := m.PathCount(); got != 1 {
		t.Fatalf("path count = %d, want 1", got)
	}
	paths := m.Paths()
	if len(paths) != 1 || paths[0].Name != pathName {
		t.Fatalf("paths = %+v, want one path named %s", paths, pathName)
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
	t.Logf("published via rtsp: %d tracks, subscriber read video %v audio %v",
		len(trks), videoGot, audioGot)
}

// TestRTSPPlay is the egress direction: a real RTSP player pulling from a path
// published by an in-test adapter.
//
// The description a player receives must carry the parameter sets the publisher
// announced, and the frames must arrive as RTP that a receiver can unpack back
// into the access units the kernel holds. Asserting on both is the minimum a
// decoder needs to start.
func TestRTSPPlay(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	addr := startRTSP(t, m)
	w := startPublisher(t, m, pathName)

	tcp := gortsplib.ProtocolTCP
	tc := &gortsplib.Client{
		Scheme:    "rtsp",
		Host:      addr,
		Protocol:  &tcp,
		UserAgent: "quickmedia-test",
	}
	if err := tc.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(tc.Close)

	// DESCRIBE on the client that will also play, because SetupAll takes the
	// session the request was answered with: a description read by a different
	// session would be a nil key in the client's setup table, and the library
	// would dereference it rather than report an error. This call also sets the
	// baseURL that SetupAll with a nil URL inherits.
	desc, _, err := tc.Describe(rtspURL(addr, pathName))
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if len(desc.Medias) != 2 {
		t.Fatalf("described %d medias, want 2", len(desc.Medias))
	}

	h264f, ok := desc.Medias[0].Formats[0].(*gsp.H264)
	if !ok {
		t.Fatalf("video format = %T, want H264", desc.Medias[0].Formats[0])
	}
	if !bytesEqual(h264f.SPS, h264.SyntheticSPS()) || !bytesEqual(h264f.PPS, h264.SyntheticPPS()) {
		t.Fatal("the described parameter sets do not match what was published")
	}
	aacf, ok := desc.Medias[1].Formats[0].(*gsp.MPEG4Audio)
	if !ok {
		t.Fatalf("audio format = %T, want MPEG4Audio", desc.Medias[1].Formats[0])
	}
	if aacf.Config == nil || aacf.Config.SampleRate != 44100 || aacf.Config.ChannelConfig != 2 {
		t.Fatalf("described audio config = %+v", aacf.Config)
	}

	// SETUP needs a base URL: the media lines this server sends carry no
	// control attribute, so each media's URL falls back to the base the caller
	// supplies, and a nil one is an error rather than a fallback to the
	// described URL.
	if err := tc.SetupAll(rtspURL(addr, pathName), desc.Medias); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := tc.Play(nil); err != nil {
		t.Fatalf("play: %v", err)
	}

	frames, stopCapture := captureRTP(t, tc, desc.Medias)
	t.Cleanup(stopCapture)

	// Pushing must start after the player is attached: a unit broadcast into a
	// path with no subscriber is dropped, so the frames the player is about to
	// read have to arrive while it is already subscribed.
	go func() {
		time.Sleep(100 * time.Millisecond)
		pushFrames(w, 0, 40)
	}()

	wantAU := h264.SyntheticAU(true)
	deadline := time.Now().Add(10 * time.Second)
	var videoUnits, audioUnits, videoKey int
	var lastTS, maxTS uint32
	for time.Now().Before(deadline) && (videoUnits < 3 || audioUnits < 3) {
		select {
		case f := <-frames:
			switch {
			case bytesEqual(f.payload, wantAU):
				videoUnits++
				if f.key {
					videoKey++
				}
				if f.ts > lastTS {
					lastTS = f.ts
				}
			case bytesEqual(f.payload, aau):
				audioUnits++
				if f.ts > maxTS {
					maxTS = f.ts
				}
			}
		case <-time.After(300 * time.Millisecond):
		}
	}

	if videoUnits < 3 {
		t.Fatalf("unpacking %d video access units, want at least 3", videoUnits)
	}
	if audioUnits < 3 {
		t.Fatalf("unpacking %d audio access units, want at least 3", audioUnits)
	}
	if videoKey != videoUnits {
		t.Fatalf("only %d of %d video access units were marked as keyframes", videoKey, videoUnits)
	}
	if maxTS == 0 {
		t.Fatal("the audio media clock never advanced")
	}
	t.Logf("played via rtsp: video %d units, audio %d units, video clock %d, audio clock %d",
		videoUnits, audioUnits, lastTS, maxTS)
}

// TestRTSPPublishTwiceIsRejected covers the single-publisher rule at the RTSP
// boundary: a second ANNOUNCE for a path that already has a publisher must fail
// closed rather than silently replacing the first, which is where a control
// plane would otherwise lose a live stream to a probe.
func TestRTSPPublishTwiceIsRejected(t *testing.T) {
	m := path.NewManager(path.DefaultConfig())
	m.Open()
	defer m.Close()

	addr := startRTSP(t, m)
	sdp := rtspSDP()

	tcp := gortsplib.ProtocolTCP
	first := &gortsplib.Client{
		Scheme: "rtsp", Host: addr, Protocol: &tcp, UserAgent: "quickmedia-test",
	}
	if err := first.StartRecording("rtsp://"+addr+"/"+pathName, sdp); err != nil {
		t.Fatalf("first announce: %v", err)
	}
	t.Cleanup(first.Close)
	waitTracks(t, m)

	// A second publisher must not be able to take over the path. The kernel
	// refuses the conflicting publish, and the refusal must reach the client as
	// a status code rather than a silent replacement.
	second := &gortsplib.Client{
		Scheme: "rtsp", Host: addr, Protocol: &tcp, UserAgent: "quickmedia-test",
	}
	err := second.StartRecording("rtsp://"+addr+"/"+pathName, sdp)
	if err == nil {
		second.Close()
		t.Fatal("a second ANNOUNCE for a live path was accepted")
	}
	if got := m.PathCount(); got != 1 {
		t.Fatalf("path count after a second announce = %d, want 1", got)
	}
	t.Logf("second announce refused: %v", err)
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
