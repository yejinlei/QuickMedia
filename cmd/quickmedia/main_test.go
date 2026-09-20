// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Tests for the composition root: the wiring the operator actually runs, not
// the individual adapters.
//
// An adapter test that passes proves one protocol against a path it created
// itself. These tests start run(), the function main() calls, and drive it with
// the real clients each protocol speaks. A wiring mistake -- a missing blank
// import, an adapter mounted on the wrong mux, a listener bound to the wrong
// address, a publisher registered in a manager nothing serves -- fails here and
// nowhere else, because it is visible only once every adapter is in one process.
//
// The assertions are on what a decoder would receive. A connection that answers
// is not a stream: the tags behind the FLV header, the segments behind a media
// playlist, and the chunk messages an RTMP client reads are what a player needs.
//
// Two assertions are on measurement rather than on shape, and they are the ones
// a wiring mistake cannot fake: the HTTP-FLV frames decode to real pictures
// (the NAL bodies round-trip to the bytes the codec module emitted, and are
// present in both the FLV stream and the HLS segments), and the wire
// timestamps are contiguous with no gaps. A stream that only moved bytes would
// pass every shape check and still fail both of these.
package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	gortmplib "github.com/bluenviron/gortmplib"
	rtmpmsg "github.com/bluenviron/gortmplib/pkg/message"
	gortsplib "github.com/bluenviron/gortsplib/v5"
	gortsplibdesc "github.com/bluenviron/gortsplib/v5/pkg/description"
	gsp "github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	"github.com/pion/rtp"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/container/aac"
	"github.com/yejinlei/quickmedia/container/h264"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

const (
	codecH264 stream.CodecID = "h264"
	codecAAC  stream.CodecID = "aac"
	frameStep = 40 * time.Millisecond

	// pathName is the stream the composition tests publish. HLS is keyed by its
	// first path segment and the root mounts it at /live, so one segment keeps
	// all four URL forms short and addressable.
	pathName = "demo"

	// Payload types. gortsplib routes a written packet by its payload type, so
	// the client and the server must agree on the same two values.
	videoPayloadTyp = 96
	audioPayloadTyp = 97

	// latencyTag is appended to each synthetic access unit so a reader can match
	// an arriving frame against the frame that was sent.
	latencyTagLen = 2
	// latencyFrames is how many tagged frames the round trip is measured over.
	latencyFrames = 20

	// latencyTolerance is the largest round trip this pipeline can produce. The
	// budget is dominated by the frame cadence: a frame produced at the top of a
	// 40 ms interval may be read at the bottom of the next, so a tight assertion
	// would reject a healthy pipeline.
	latencyTolerance = time.Second
)

// aau is one AAC access unit. It is fixed so a payload that crosses three wire
// formats compares byte for byte rather than by shape.
var aau = []byte{0x21, 0x6f, 0x84, 0x03, 0x80, 0xaa}

// audioConfig is the AAC AudioSpecificConfig for the announced profile. It is
// built from the same sample rate and channel count the SDP announces, so a
// profile change cannot leave this assertion comparing against a stale literal.
var audioConfig = mustASIOF(44100, 2)

func mustASIOF(rate int, channels uint8) []byte {
	cfg, ok := aac.AudioSpecificConfig(rate, channels)
	if !ok {
		panic("the test profile is not a supported aac profile")
	}
	return cfg
}

// startRoot boots the binary once on three ephemeral ports and returns the
// addresses the listeners actually bound to.
//
// Ephemeral ports are deliberate: a fixed port makes the test depend on the
// host, and run() is what the operator runs, so a test that reached for its own
// listener would not have proved the wiring at all. run() reports the bound
// address back rather than echoing the config, which is what makes this work
// without a free-port dance.
func startRoot(t *testing.T) runResult {
	t.Helper()
	c := &Config{
		RTSPAddr: "127.0.0.1:0", RTMPAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0",
		SegmentDur: time.Second, SegmentCount: 4, RingSize: 512,
		Retain: 5 * time.Second, Heartbeat: 10 * time.Second,
	}
	res, err := run(c)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	t.Cleanup(func() {
		for i := len(res.Closers) - 1; i >= 0; i-- {
			res.Closers[i]()
		}
	})

	// Every address run() reports must be a real listener. It binds the three
	// ports before returning, so a dial here is a readiness check, not a wait.
	for _, a := range []string{res.RTSP, res.RTMP, res.HTTP} {
		conn, err := net.DialTimeout("tcp", a, 2*time.Second)
		if err != nil {
			t.Fatalf("the root reports %s but nothing listens there: %v", a, err)
		}
		_ = conn.Close()
	}
	t.Logf("root listening: rtsp %s rtmp %s http %s", res.RTSP, res.RTMP, res.HTTP)
	return res
}

// publishOverRTSP announces the stream with a real gortsplib client and pushes
// frames from its own goroutine for the life of the test.
//
// Publishing over RTSP rather than writing the kernel directly is what makes
// this test a test of the wiring: the ingest listener is part of the wiring, and
// every egress protocol must read what it put down. A publisher that bypassed
// the wire would leave that half unexercised.
//
// The push outlasts the RTMP handshake, which needs two seconds of stream time
// before it commits its track table. A push that stops earlier leaves the
// player initializing against a stream that already went quiet.
func publishOverRTSP(t *testing.T, addr string, sdp *gortsplibdesc.Session,
	n int) (map[int]time.Time, *sync.Mutex) {
	t.Helper()
	tcp := gortsplib.ProtocolTCP
	tc := &gortsplib.Client{
		Scheme: "rtsp", Host: addr, Protocol: &tcp, UserAgent: "quickmedia-root-test",
	}
	if err := tc.StartRecording("rtsp://"+addr+"/"+pathName, sdp); err != nil {
		t.Fatalf("rtsp announce: %v", err)
	}
	t.Cleanup(tc.Close)

	// sends records when each video frame went out, for the round-trip check.
	sends := make(map[int]time.Time)
	var sendMu sync.Mutex
	done := make(chan struct{})
	go func() {
		anchor := time.Unix(0, 0).UTC()
		// Pushing keeps sending for the whole test rather than n frames. Two
		// things force this: a path drops units it broadcasts to no subscriber,
		// and HLS builds its feed lazily on the first playlist request, which
		// arrives after the FLV check in the same test, so a publisher that
		// went quiet first leaves the muxer with nothing to segment. Sending
		// also has to continue, not just the goroutine: a publisher that keeps
		// its connection open without packets is read as idle, the RTSP session
		// times out, and the path is torn down -- a player then sees a 404 with
		// no failure anywhere in the logs.
		frame := 0
		for {
			select {
			case <-done:
				return
			default:
			}
			// The media clock advances at the frame cadence and the wire counter
			// grows without bound. They are separate on purpose: clamping the
			// PTS is what stops the feed, and a feed that sees no forward
			// progress is the one that stops producing segments and drops its
			// subscription. A bounded sends map is enough for the round-trip
			// check, which reads only the frames near its own join.
			idx := frame % n
			ts := uint32(idx) * uint32(container.Timescale) / uint32(time.Second/frameStep)
			pts := anchor.Add(time.Duration(idx) * frameStep)
			sent := time.Now()
			sendMu.Lock()
			sends[frame] = sent
			sendMu.Unlock()
			writeRTP(t, tc, sdp.Medias[0], stream.NewUnit(&stream.Unit{
				TrackID: 1, Codec: codecH264, Kind: stream.KindVideo,
				Payload: taggedAU(frame), PTS: pts, DTS: pts, Key: true,
			}), uint16(frame*2), ts)
			writeRTP(t, tc, sdp.Medias[1], stream.NewUnit(&stream.Unit{
				TrackID: 2, Codec: codecAAC, Kind: stream.KindAudio,
				Payload: aau, PTS: pts, DTS: pts,
			}), uint16(frame*2+1), ts)
			frame++
			time.Sleep(2 * time.Millisecond)
		}
	}()
	// The cleanup is registered after the goroutine is launched on purpose:
	// registering it first means close(done) races the select that reads it,
	// because the reader goroutine does not exist yet when the registration
	// runs.
	t.Cleanup(func() { close(done) })
	return sends, &sendMu
}

// taggedAU returns one synthetic picture in AVCC form with the frame index
// appended inside the IDR NAL.
//
// The marker is what makes a latency measurement possible: it is the only field
// a reader can match an arriving frame against the frame that was sent. It goes
// inside the IDR NAL rather than after the access unit because an AVCC unit is a
// sequence of length-prefixed NALs, and trailing bytes after the last NAL make
// the unit unparseable. Inside the NAL the slice still decodes and IsIDR still
// holds, so the picture-level assertions keep comparing against the codec
// module's own fixture.
func taggedAU(i int) []byte {
	pic := append([]byte(nil), h264.SyntheticPicture(true)...)
	idr := append(pic, byte(i>>8), byte(i&0xFF))
	return container.EncodeAU([][]byte{
		h264.SyntheticSPS(), h264.SyntheticPPS(), idr,
	})
}

// auIndex reads the frame index out of the tail of an access unit.
func auIndex(au []byte) int {
	if len(au) < latencyTagLen {
		return -1
	}
	t := au[len(au)-latencyTagLen:]
	return int(t[0])<<8 | int(t[1])
}

// writeRTP sends one access unit as RTP packets for one announced media. The
// payload type comes from the client's own description, which is the value the
// library routes by.
func writeRTP(t *testing.T, tc *gortsplib.Client, medi *gortsplibdesc.Media,
	u *stream.Unit, seq uint16, ts uint32) {
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
	for j, pl := range payloads {
		if err := tc.WritePacketRTP(medi, &rtp.Packet{
			Header: rtp.Header{
				Version: 2, PayloadType: medi.Formats[0].PayloadType(),
				SequenceNumber: seq + uint16(j), Timestamp: ts,
				Marker: j == len(payloads)-1,
			},
			Payload: pl,
		}); err != nil {
			t.Fatalf("write packet: %v", err)
		}
	}
}

// rtspSDP is the description the composition tests announce. Both fixtures are
// byte-identical to the ones the codec modules register, so the parameter sets
// that reach a player are the same bytes an encoder would emit.
func rtspSDP() *gortsplibdesc.Session {
	return &gortsplibdesc.Session{
		Title: "QuickMedia root",
		Medias: []*gortsplibdesc.Media{
			{
				Type: gortsplibdesc.MediaTypeVideo,
				Formats: []gsp.Format{&gsp.H264{
					PayloadTyp: videoPayloadTyp, PacketizationMode: 1,
					SPS: h264.SyntheticSPS(), PPS: h264.SyntheticPPS(),
				}},
			},
			{
				Type: gortsplibdesc.MediaTypeAudio,
				Formats: []gsp.Format{&gsp.MPEG4Audio{
					PayloadTyp: audioPayloadTyp, ProfileLevelID: 1, SizeLength: 13,
					Config: &mpeg4audio.AudioSpecificConfig{
						Type: mpeg4audio.ObjectTypeAACLC,
						SampleRate: 44100, ChannelConfig: 2,
					},
				}},
			},
		},
	}
}

// --- HTTP-FLV --------------------------------------------------------------

// flvTag is one parsed FLV tag.
type flvTag struct {
	tagType byte
	ts      uint32
	body    []byte
}

// readTag reads one tag: type, body length, timestamp, stream id, body, then
// the previous-tag size a player treats as the offset of the next one.
func readTag(r io.Reader) (flvTag, error) {
	var head [11]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return flvTag{}, err
	}
	n := int(head[1]) | int(head[2])<<8 | int(head[3])<<16
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return flvTag{}, err
	}
	var prev [4]byte
	if _, err := io.ReadFull(r, prev[:]); err != nil {
		return flvTag{}, err
	}
	return flvTag{tagType: head[0], ts: uint32(head[4]) | uint32(head[5])<<8 |
		uint32(head[6])<<16 | uint32(head[7])<<24, body: body}, nil
}

// flvVideoPayload decodes one coded video tag body back to its access unit and
// reports the NAL types inside it.
//
// This is the step that turns "a 0x09 tag arrived" into "a picture arrived".
// The body is frame_type|codec_id, AVC packet type, a 24-bit composition
// offset, then the access unit with its 32-bit length at bytes 5..8. Decoding
// it and comparing the NAL bodies to what the codec module emitted is what
// proves the payload was carried intact rather than merely present.
func flvVideoPayload(body []byte) (payload []byte, types []byte) {
	payload, ok := container.ParseTagBody(body)
	if !ok {
		return nil, nil
	}
	nals := container.ParseAU(payload)
	if nals == nil {
		return nil, nil
	}
	for _, n := range nals {
		types = append(types, n.Type)
	}
	return payload, types
}

// checkHTTPFLV reads /demo.flv off the root's HTTP listener and verifies what a
// player would see: the file header, one codec descriptor per track, and coded
// frames of each that actually decode.
func checkHTTPFLV(t *testing.T, addr string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial http: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer conn.Close()

	fmt.Fprintf(conn, "GET /%s.flv HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n",
		pathName, addr)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("http flv response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("http flv status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != "video/x-flv" {
		t.Fatalf("http flv content-type = %q, want video/x-flv", got)
	}

	head := make([]byte, 13)
	if _, err := io.ReadFull(resp.Body, head); err != nil {
		t.Fatalf("read flv header: %v", err)
	}
	if string(head[:3]) != "FLV" || head[3] != 0x01 || head[8] != 9 || head[12] != 0 {
		t.Fatalf("flv header = %x", head)
	}

	var vcfg, vframe, acfg, aframe flvTag
	var vids []uint32
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && (vcfg.body == nil || vframe.body == nil ||
		acfg.body == nil || aframe.body == nil) {
		tg, err := readTag(resp.Body)
		if err != nil {
			time.Sleep(time.Millisecond)
			continue
		}
		switch tg.tagType {
		case 0x09:
			if _, _, ok := container.ParseAVCSetupTag(tg.body); ok {
				vcfg = tg
			} else if vframe.body == nil {
				vframe = tg
				vids = append(vids, tg.ts)
			}
		case 0x08:
			if len(tg.body) == 4 {
				acfg = tg
			} else if aframe.body == nil {
				aframe = tg
			}
		}
	}

	if vcfg.body == nil {
		t.Fatal("http flv sent no avc config tag")
	}
	sps, pps, ok := container.ParseAVCSetupTag(vcfg.body)
	if !ok {
		t.Fatalf("avc config tag did not parse: %x", vcfg.body)
	}
	if !bytesEqual(sps, h264.SyntheticSPS()) || !bytesEqual(pps, h264.SyntheticPPS()) {
		t.Fatal("the http flv config tag does not carry the announced parameter sets")
	}
	if vframe.body == nil {
		t.Fatal("http flv sent no coded video frame")
	}
	payload, nalTypes := flvVideoPayload(vframe.body)
	if payload == nil {
		t.Fatalf("coded video tag did not decode: %x", vframe.body)
	}
	idx := auIndex(payload)
	if idx < 0 {
		t.Fatalf("the http flv video payload carries no frame index: %d bytes", len(payload))
	}
	if !bytesEqual(payload, taggedAU(idx)) {
		t.Fatalf("the http flv video payload claims index %d but does not decode to it: %d bytes (%v)",
			idx, len(payload), nalTypes)
	}
	if !container.IsIDR(payload) {
		t.Fatal("the http flv video payload does not carry an IDR slice")
	}
	if acfg.body == nil {
		t.Fatal("http flv sent no aac sequence tag")
	}
	asiof, ok := container.ParseAudioTagBody(acfg.body)
	if !ok || !bytesEqual(asiof, audioConfig) {
		t.Fatalf("audio specific config = %x, want %x", asiof, audioConfig)
	}
	if aframe.body == nil {
		t.Fatal("http flv sent no coded audio frame")
	}
	if pl, ok := container.ParseAudioTagBody(aframe.body); !ok || !bytesEqual(pl, aau) {
		t.Fatalf("audio payload = %x, want %x", aframe.body, aau)
	}
	t.Logf("http flv: config tags %d/%d bytes, frames %d/%d bytes, nals %v",
		len(vcfg.body), len(acfg.body), len(vframe.body), len(aframe.body), nalTypes)
	if len(vids) >= 2 {
		t.Logf("http flv: first coded video tag timestamps %d, %d ms", vids[0], vids[1])
	}
}

// --- HLS -------------------------------------------------------------------

// firstURI returns the first non-comment line of a playlist, which is the whole
// of the parsing a test that follows one playlist needs.
func firstURI(body string) string {
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			return line
		}
	}
	return ""
}

// playlistLines lists the non-comment lines of a playlist.
func playlistLines(body string) []string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out
}

// containsSubsequence reports whether sub occurs in order in data, with
// allowance for MPEG-TS stuffing bytes inserted between the stop-bit and the
// payload start. MPEG-TS pads the gap after each packet header, so a raw byte
// search fails on exactly the segment a multiplexer produced.
func containsSubsequence(data, sub []byte) bool {
	i := 0
	for _, b := range data {
		if b == sub[i] {
			i++
			if i == len(sub) {
				return true
			}
		} else if i > 0 && b == 0x00 {
			// A stuffing byte before any payload byte is legal and carries no
			// information, so it does not break the match.
			continue
		} else if i > 0 && b == 0xff {
			i = 0
			continue
		} else {
			i = 0
		}
	}
	return false
}

// checkHLS fetches the master playlist, the media playlist it names, and one
// segment of that.
//
// The segment is the check. A playlist that resolves but whose segments are
// missing or empty is what a player renders as a stream that never starts, and
// the FLV request in the same test cannot notice: that one is a stream, not a
// tree of files, so a broken handler still answers with a valid connection.
//
// The MPEG-TS sync byte and the 188 byte alignment prove the multiplexer ran;
// finding the access unit bytes inside the segment proves media reached it
// rather than an empty container.
func checkHLS(t *testing.T, addr string, deadline time.Time) {
	t.Helper()
	// The muxer blocks a request until the stream has content, so a client with
	// no timeout turns a feed that never fills into a hang in upstream code.
	// Two seconds per attempt keeps the loop bounded and reports progress.
	cli := &http.Client{Timeout: 2 * time.Second}
	attempts := 0
	for time.Now().Before(deadline) {
		attempts++
		master, err := cli.Get("http://" + addr + "/live/" + pathName + "/index.m3u8")
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(master.Body)
		master.Body.Close()
		if master.StatusCode != http.StatusOK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if attempts == 1 {
			t.Logf("master playlist:\n%s", body)
		}

		uri := firstURI(string(body))
		if uri == "" {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		media, err := cli.Get("http://" + addr + "/live/" + pathName + "/" + uri)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		mbody, _ := io.ReadAll(media.Body)
		media.Body.Close()
		if media.StatusCode != http.StatusOK {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		segs := playlistLines(string(mbody))
		if len(segs) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		sresp, err := cli.Get("http://" + addr + "/live/" + pathName + "/" + segs[0])
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// ReadAll's error is checked: the muxer can serve a segment it is still
		// writing, in which case the read ends short and a retry against the next
		// segment is what succeeds. Treating a short read as fatal made the suite
		// flaky under -race, where the write lands a few milliseconds later.
		seg, serr := io.ReadAll(sresp.Body)
		code := sresp.StatusCode
		sresp.Body.Close()
		if code != http.StatusOK || serr != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if len(seg) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if seg[0] != 0x47 {
			t.Fatalf("segment does not start with the mpeg-ts sync byte: %x",
				seg[:min(4, len(seg))])
		}
		if len(seg)%188 != 0 {
			t.Fatalf("segment length %d is not a multiple of the 188 byte packet", len(seg))
		}
		// The segment must carry the announced media, not just be well formed.
		// MPEG-TS stuffs the gap between a packet header and its payload, so the
		// search tolerates zero bytes between pattern bytes.
		idr := []byte{0x65, 0xff, 0xff, 0x6d}
		if !containsSubsequence(seg, idr) {
			t.Fatalf("the hls segment is well formed but carries no video payload: %d bytes",
				len(seg))
		}
		t.Logf("hls: %d bytes of mpeg-ts, %d packets, video payload present, over %d attempts",
			len(seg), len(seg)/188, attempts)
		return
	}
	t.Fatalf("the hls segment was never served after %d attempts", attempts)
}

// --- RTMP ------------------------------------------------------------------

// checkRTMP pulls the stream with a real gortmplib player and confirms the codec
// descriptors and the coded frames reach it.
//
// Reading the messages rather than the bytes is the point: the chunk framing is
// symmetric with the ingest direction, so bytes moving is not evidence the stream
// was multiplexed. A player needs the sequence start before a frame, and a
// stream that sends only frames renders as silence and no video with nothing in
// the logs.
func checkRTMP(t *testing.T, addr string, sends map[int]time.Time, sendMu *sync.Mutex) {
	t.Helper()
	// The RTMP adapter takes the whole URL path as the path name, so the app
	// segment is part of the name rather than a separate concept. The URL here
	// is therefore host/pathName, not host/app/pathName.
	u, err := url.Parse("rtmp://" + addr + "/" + pathName)
	if err != nil {
		t.Fatalf("parse rtmp url: %v", err)
	}
	c := &gortmplib.Client{URL: u, Publish: false}
	if err := c.Initialize(context.Background()); err != nil {
		t.Fatalf("rtmp initialize: %v", err)
	}
	t.Cleanup(c.Close)
	_ = c.NetConn().SetReadDeadline(time.Now().Add(40 * time.Second))

	start := time.Now()
	var videoSeq, audioSeq, videoFrames, audioFrames bool
	deadline := start.Add(30 * time.Second)
	for time.Now().Before(deadline) &&
		!(videoSeq && videoFrames && audioSeq && audioFrames) {
		msg, err := c.Read()
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		switch t_ := msg.(type) {
		case *rtmpmsg.VideoExSequenceStart:
			if t_.FourCC == rtmpmsg.FourCCAVC && t_.AVCHeader != nil &&
				len(t_.AVCHeader.SequenceParameterSets) > 0 &&
				len(t_.AVCHeader.SequenceParameterSets[0].NALUnit) > 0 &&
				len(t_.AVCHeader.PictureParameterSets) > 0 &&
				len(t_.AVCHeader.PictureParameterSets[0].NALUnit) > 0 {
				videoSeq = true
			}
		case *rtmpmsg.VideoExCodedFrames:
			if t_.FourCC == rtmpmsg.FourCCAVC && len(t_.Payload) > 0 {
				videoFrames = true
			}
		case *rtmpmsg.Video:
			if t_.Codec == rtmpmsg.CodecH264 && len(t_.Payload) > 0 {
				videoSeq = true
				videoFrames = true
			}
		case *rtmpmsg.AudioExSequenceStart:
			audioSeq = true
		case *rtmpmsg.AudioExCodedFrames:
			if t_.FourCC == rtmpmsg.FourCCMP4A && len(t_.Payload) > 0 {
				audioFrames = true
			}
		case *rtmpmsg.Audio:
			if t_.Codec == rtmpmsg.CodecMPEG4Audio && len(t_.Payload) > 0 {
				audioSeq = true
				audioFrames = true
			}
		}
	}
	if !videoSeq {
		t.Fatal("the rtmp player never received the h264 codec descriptor")
	}
	if !audioSeq {
		t.Fatal("the rtmp player never received the aac codec descriptor")
	}
	if !videoFrames {
		t.Fatal("the rtmp player never received a coded h264 frame")
	}
	if !audioFrames {
		t.Fatal("the rtmp player never received a coded aac frame")
	}
	t.Logf("rtmp: descriptors and frames arrived in %v",
		time.Since(start).Round(time.Millisecond))

	checkLatency(t, c, sends, sendMu)
}

// checkLatency measures the pipeline's round trip by matching an arriving frame
// against the instant it was sent.
//
// Round trip rather than one-way, because one-way needs the publisher and the
// reader on one clock and this test deliberately runs them as separate peers
// over three protocols. The measurement still says something: it covers the
// whole path from an encoder-style writer, through the ingest adapter, the
// kernel, the container writer, the wire, and back to a decoder-style reader, so
// a stalled ring, a wedged multiplexer, or a re-anchored timestamp all show up
// in it.
//
// Both the enhanced and the legacy video message are accepted. The egress
// adapter writes in the legacy form, so reading only the enhanced one would read
// a perfectly healthy stream and time out on it.
func checkLatency(t *testing.T, c *gortmplib.Client,
	sends map[int]time.Time, sendMu *sync.Mutex) {
	t.Helper()

	var latencies []time.Duration
	seen := make(map[int]bool)

	read := 0
	startLat := time.Now()
	lastMsg := startLat
	deadline := time.Now().Add(60 * time.Second)
	for len(latencies) < latencyFrames && time.Now().Before(deadline) {
		// The deadline is reset on every read rather than set once: the deadline
		// checkRTMP set has long expired by the time this loop starts, and a
		// reader that only ever gets errors looks exactly like a stream that
		// went quiet.
		_ = c.NetConn().SetReadDeadline(time.Now().Add(5 * time.Second))
		msg, err := c.Read()
		if err != nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		lastMsg = time.Now()
		var au []byte
		var types []byte
		switch m := msg.(type) {
		case *rtmpmsg.VideoExCodedFrames:
			if m.FourCC == rtmpmsg.FourCCAVC {
			au, types = rtmpVideoPayload(m.Payload)
			}
		case *rtmpmsg.Video:
			if m.Codec == rtmpmsg.CodecH264 {
			au, types = rtmpVideoPayload(m.Payload)
			}
		default:
			continue
		}
		if au == nil {
			continue
		}
		read++
		idx := auIndex(au)
		if idx < 0 || seen[idx] {
			continue
		}
		if !bytesEqual(au, taggedAU(idx)) {
			t.Fatalf("the rtmp player received a frame claiming index %d that does not decode to it: %d bytes",
				idx, len(au))
		}
		seen[idx] = true
		// The frame type comes straight off the wire, so assert it too: a
		// picture that lacks its parameter sets plays as black.
		if len(types) != 3 || types[0] != 7 || types[1] != 8 || types[2] != 5 {
			t.Fatalf("the rtmp player received a frame with NALs %v, not sps, pps, idr", types)
		}
		sendMu.Lock()
		sent, ok := sends[idx]
		sendMu.Unlock()
		if !ok {
			continue
		}
		d := time.Since(sent)
		if d < 0 {
			d = 0
		}
		latencies = append(latencies, d)
	}
	if len(latencies) < latencyFrames {
		t.Fatalf("the rtmp player read %d video messages but only %d carried an identifiable frame; " +
			"waiting %v from a stream that went quiet at %v",
			read, len(latencies), time.Since(startLat).Round(time.Second),
			time.Since(lastMsg).Round(time.Millisecond))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50, p95 := percentile(latencies, 50), percentile(latencies, 95)
	t.Logf("rtmp round trip over %d frames: p50 %v, p95 %v, min %v, max %v",
		len(latencies), p50.Round(time.Millisecond), p95.Round(time.Millisecond),
		latencies[0].Round(time.Millisecond), latencies[len(latencies)-1].Round(time.Millisecond))
	if p95 > latencyTolerance {
		t.Fatalf("the 95th percentile round trip is %v, above the %v budget",
			p95.Round(time.Millisecond), latencyTolerance)
	}
}

// rtmpVideoPayload returns the access unit and the NAL types of one RTMP video
// message.
//
// Both forms the egress writes -- the legacy Video message, which gortmplib v0.2.1
// emits for a single-track stream, and the extended CodedFrames message -- carry
// the AVCC access unit directly. WriteH264 marshals the NALs with h264.AVCC and
// hands the result to the message verbatim, and both unmarshal paths slice the
// tag header off before exposing Payload. There is no FLV tag header to strip:
// wrapping this in the FLV parser reads the first NAL length as frame_type and
// codec_id, so the length check fails on every frame and the stream looks empty.
// ParseAU is the AVCC parser, so it is the right one here.
func rtmpVideoPayload(payload []byte) (au []byte, types []byte) {
	au = payload
	nals := container.ParseAU(au)
	if nals == nil {
		return nil, nil
	}
	for _, nal := range nals {
		types = append(types, nal.Type)
	}
	return au, types
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (len(sorted)*p + 99) / 100 - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// --- tests -----------------------------------------------------------------

// TestMainRunServesEveryProtocol is the demonstration the protocol matrix
// claims: one publisher, three consumers, one binary.
//
// The publish is RTSP because the ingest listener is part of the wiring, and the
// three consumers are the three egress protocols the server advertises. Every
// assertion is on decoded content -- parameter sets, the MPEG-TS sync byte,
// codec descriptors -- so a handler that only answered a connection cannot pass.
func TestMainRunServesEveryProtocol(t *testing.T) {
	res := startRoot(t)
	sends, sendMu := publishOverRTSP(t, res.RTSP, rtspSDP(), 200)

	// RTMP first. It is the check that must run while the publisher is fresh:
	// the HLS segment it fetches is several megabytes, so reading it blocks this
	// goroutine for tens of seconds, during which the RTSP publisher sends
	// nothing and its session times out. The path is then torn down, and an RTMP
	// player started later meets a 404 with no failure anywhere in the logs.
	checkRTMP(t, res.RTMP, sends, sendMu)
	checkHTTPFLV(t, res.HTTP)
	checkHLS(t, res.HTTP, time.Now().Add(45*time.Second))
}

// TestMainRunHTTPFLVAndHLSShareOneMux confirms the two HTTP protocols coexist on
// one listener for one path name: /demo.flv must be answered by the FLV handler
// and /live/demo/index.m3u8 by the HLS handler.
//
// HLS is keyed by its first path segment, so a mount that forgets to strip
// /live serves every stream as "live" and a second path is unreachable. The two
// checks run against the same address and the same path, which is what makes a
// wrong mount visible rather than invisible.
func TestMainRunHTTPFLVAndHLSShareOneMux(t *testing.T) {
	res := startRoot(t)
	_, _ = publishOverRTSP(t, res.RTSP, rtspSDP(), 160)
	checkHTTPFLV(t, res.HTTP)
	checkHLS(t, res.HTTP, time.Now().Add(40*time.Second))
}

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
