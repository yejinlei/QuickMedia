// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package container

import (
	"bytes"
	"testing"
	"time"

	"github.com/yejinlei/quickmedia/kernel/stream"
)

// TestFLVVideoTagRoundTrip pins the AVC coded-frame layout.
//
// The layout is frame_type | packet_type | composition_offset(3) |
// AVCC length(4) | access unit. Every byte of the header is load-bearing:
// the length prefix is what ffmpeg's h264_mp4toannexb expects, and dropping
// it turns one access unit into one giant malformed NAL that renders nothing.
func TestFLVVideoTagRoundTrip(t *testing.T) {
	sps := []byte{0x67, 0x64, 0x00, 0x1e, 0xac, 0x23, 0xd1, 0x00}
	pps := []byte{0x68, 0xee, 0x3c, 0x80}
	nals := [][]byte{sps, pps, {0x65, 0x14, 0x01, 0xab, 0x3c, 0x80, 0x20, 0x01}}
	au := EncodeAU(nals)

	w := NewFLVWriterAt(time.Unix(1700000000, 0))
	u := &stream.Unit{
		Kind: stream.KindVideo, Payload: au,
		PTS: time.Unix(1700000030, 0).Add(200 * time.Millisecond),
		DTS: time.Unix(1700000030, 0),
	}
	u.Retain()
	defer u.Release()

	frames, err := w.Pack(u)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1", len(frames))
	}
	body := frames[0].Data

	if body[0] != 0x17 || body[1] != flvPacketCoded {
		t.Fatalf("header = %02x %02x, want 17 %02x", body[0], body[1], flvPacketCoded)
	}

	payload, ok := ParseTagBody(body)
	if !ok {
		t.Fatal("ParseTagBody rejected a valid coded frame")
	}
	if !bytes.Equal(payload, au) {
		t.Fatalf("payload mismatch:\n got % x\nwant % x", payload, au)
	}
	if !IsVideoKey(u) || !frames[0].Key {
		t.Fatal("IDR access unit must be reported as a key frame")
	}
}

// TestFLVInterFrameHead pins the first byte of a non-key frame.
//
// frame_type is a 4-bit field, so key and inter are alternatives. Writing them
// as two bits that OR together produces 0x37, which is frame_type 3: the spec
// does not define it, ParseTagBody rejects it, and the writer's own key check
// passes anyway because the codec id is still 7. Neither failure is visible
// until a real player refuses the stream, so the byte is asserted directly.
func TestFLVInterFrameHead(t *testing.T) {
	// A non-IDR slice: NAL type 1, not 5. IsVideoKey falls back to IsIDR, so
	// an IDR NAL anywhere in the unit would turn this into a key frame.
	au := EncodeAU([][]byte{{0x01, 0x14, 0x01, 0xab, 0x3c, 0x80, 0x20, 0x01}})

	w := NewFLVWriterAt(time.Unix(1700000000, 0))
	u := &stream.Unit{
		Kind: stream.KindVideo, Payload: au,
		PTS: time.Unix(1700000030, 0),
		DTS: time.Unix(1700000030, 0),
	}
	u.Retain()
	defer u.Release()

	frames, err := w.Pack(u)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1", len(frames))
	}
	body := frames[0].Data
	if body[0] != 0x27 || body[1] != flvPacketCoded {
		t.Fatalf("header = %02x %02x, want 27 %02x", body[0], body[1], flvPacketCoded)
	}
	if ParseTagType(body) != flvTagVideo {
		t.Fatalf("ParseTagType = 0x%02x, want video (0x%02x)", ParseTagType(body), flvTagVideo)
	}
	if frames[0].Key {
		t.Fatal("a non-IDR access unit must not be reported as a key frame")
	}
}

// TestFLVAVCSetupTagRoundTrip checks that the decoder configuration record
// survives a write and read, and that its bytes match what ffmpeg emits.
func TestFLVAVCSetupTagRoundTrip(t *testing.T) {
	sps := []byte{0x67, 0x64, 0x00, 0x1e, 0xac, 0x23, 0xd1, 0x00}
	pps := []byte{0x68, 0xee, 0x3c, 0x80}

	f := AVCSetupTag(sps, pps)
	if !f.Config {
		t.Fatal("setup tag must carry Config")
	}
	body := f.Data

	// ffmpeg writes 0x17 0x00 then the 3-byte composition offset and the
	// SPS length before the SPS, which is why the SPS starts at index 9 and
	// the PPS length is at 17.
	wantHead := []byte{0x17, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08}
	if !bytes.Equal(body[:9], wantHead) {
		t.Fatalf("head mismatch:\n got % x\nwant % x", body[:9], wantHead)
	}
	wantPpsLen := []byte{0x00, 0x00, 0x00, 0x04}
	if !bytes.Equal(body[17:21], wantPpsLen) {
		t.Fatalf("pps length mismatch:\n got % x\nwant % x", body[17:21], wantPpsLen)
	}
	if !bytes.Equal(body[9:9+len(sps)], sps) || !bytes.Equal(body[21:], pps) {
		t.Fatalf("config record = % x, want SPS then PPS", body)
	}

	gotSps, gotPps, ok := ParseAVCSetupTag(body)
	if !ok {
		t.Fatal("ParseAVCSetupTag rejected its own output")
	}
	if !bytes.Equal(gotSps, sps) || !bytes.Equal(gotPps, pps) {
		t.Fatalf("round trip: sps % x pps % x, want % x % x", gotSps, gotPps, sps, pps)
	}

	// A coded frame must not parse as a config record.
	if _, _, ok := ParseAVCSetupTag([]byte{0x17, flvPacketCoded, 0, 0, 0}); ok {
		t.Fatal("coded frame misread as a decoder configuration record")
	}
}

// TestFLVAACSequenceTag checks the AAC sequence start, which is the audio
// equivalent of the SPS+PPS record. Without it the audio track is refused
// outright rather than playing badly.
func TestFLVAACSequenceTag(t *testing.T) {
	asiof := []byte{0x11, 0x90}

	f := AACSequenceTag(asiof)
	if !f.Config {
		t.Fatal("sequence tag must carry Config")
	}
	want := []byte{0xA1, 0x01, 0x11, 0x90}
	if !bytes.Equal(f.Data, want) {
		t.Fatalf("sequence tag = % x, want % x", f.Data, want)
	}

	payload, ok := ParseAudioTagBody(f.Data)
	if !ok || !bytes.Equal(payload, asiof) {
		t.Fatalf("sequence payload = % x (ok=%v), want % x", payload, ok, asiof)
	}
}

// TestFLVAudioTagHeader pins the AAC tag header bytes.
//
// Byte 0 encodes sound_format 10 in its high nibble. Any other value makes the
// decoder interpret the AAC access unit as a different codec entirely, which is
// a hard failure: ffmpeg wrote 0xA1 for this stream, and so does Adobe's spec.
func TestFLVAudioTagHeader(t *testing.T) {
	w := NewFLVWriterAt(time.Time{})
	u := &stream.Unit{Kind: stream.KindAudio, Payload: []byte{0x21, 0x6f, 0x84}}
	u.Retain()
	defer u.Release()

	frames, err := w.Pack(u)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1", len(frames))
	}
	body := frames[0].Data
	if body[0] != 0xA1 || body[1] != 0x01 {
		t.Fatalf("audio header = %02x %02x, want a1 01", body[0], body[1])
	}
	if ParseTagType(body) != flvTagAudio {
		t.Fatalf("ParseTagType = 0x%02x, want audio", ParseTagType(body))
	}
	payload, ok := ParseAudioTagBody(body)
	if !ok || !bytes.Equal(payload, u.Payload) {
		t.Fatalf("payload = % x (ok=%v)", payload, ok)
	}
}

// TestFLVTagFramingRoundTrip checks the 11-byte tag header and the
// PreviousTagSize field. A one-byte error here desynchronises every tag after
// it, so the whole stream is unreadable.
func TestFLVTagFramingRoundTrip(t *testing.T) {
	body := []byte{0x17, flvPacketCoded, 0, 0, 0, 0, 0, 0, 0x02, 0x65, 0x12}
	tag := EncodeTag(flvTagVideo, 30000, uint32(len(body)), body)

	// Header + body + PreviousTagSize.
	if want := 11 + len(body) + 4; len(tag) != want {
		t.Fatalf("tag length = %d, want %d", len(tag), want)
	}
	if tag[0] != flvTagVideo {
		t.Fatalf("type byte = 0x%02x, want 0x%02x", tag[0], flvTagVideo)
	}
	if got := uint32(tag[1]) | uint32(tag[2])<<8 | uint32(tag[3])<<16; got != uint32(len(body)) {
		t.Fatalf("size field = %d, want %d", got, len(body))
	}
	if got := uint32(tag[4]) | uint32(tag[5])<<8 | uint32(tag[6])<<16 | uint32(tag[7])<<24; got != 30000 {
		t.Fatalf("timestamp field = %d, want 30000", got)
	}

	// PreviousTagSize is the last four bytes: header + body, little-endian.
	// All four bytes matter because skipping the high byte silently desyncs the
	// stream the moment two tags are written.
	off := 11 + len(body)
	if got := uint32(tag[off]) | uint32(tag[off+1])<<8 | uint32(tag[off+2])<<16 | uint32(tag[off+3])<<24; got != uint32(len(body))+11 {
		t.Fatalf("previous tag size = %d, want %d", got, len(body)+11)
	}
	if len(tag) != off+4 {
		t.Fatalf("tag length = %d, want %d", len(tag), off+4)
	}

	// A stream of two tags, prefixed with a 13-byte header, must re-split
	// cleanly. The header is 13 bytes, not 12: signature(3) + version(1) +
	// flags(1) + header dataSize(4) + previousTagSize0(4).
	var doc []byte
	doc = append(doc, 'F', 'L', 'V', 0x01, 0x00, 0, 0, 0, 0, 0, 0, 0, 0)
	doc = append(doc, EncodeTag(flvTagVideo, 30000, uint32(len(body)), body)...)
	doc = append(doc, EncodeTag(flvTagAudio, 30000, 4, []byte{0xA1, 0x01, 0x11, 0x90})...)

	bodies, types, err := ParseFLVTags(doc)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(bodies) != 2 || len(types) != 2 {
		t.Fatalf("got %d bodies / %d types, want 2/2", len(bodies), len(types))
	}
	if !bytes.Equal(bodies[0], body) {
		t.Fatalf("video body = % x, want % x", bodies[0], body)
	}
	if types[0] != flvTagVideo || types[1] != flvTagAudio {
		t.Fatalf("types = 0x%02x 0x%02x, want 0x%02x 0x%02x",
			types[0], types[1], flvTagVideo, flvTagAudio)
	}

	// A truncated trailing tag must stop the scan rather than panic.
	trunc := doc[:len(doc)-2]
	if _, _, err := ParseFLVTags(trunc); err != nil {
		t.Fatalf("truncated tail must not error: %v", err)
	}
}

// TestFLVSplitOversizeUnit checks that an access unit larger than one tag is
// split without corrupting the bytes, which is the case a high-bitrate key
// frame falls into.
func TestFLVSplitOversizeUnit(t *testing.T) {
	big := bytes.Repeat([]byte{0x80}, flvMaxTagSize+500)
	w := NewFLVWriterAt(time.Time{})
	u := &stream.Unit{Kind: stream.KindVideo, Payload: big, PTS: time.Unix(1, 0), DTS: time.Unix(1, 0)}
	u.Retain()
	defer u.Release()

	frames, err := w.Pack(u)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if len(frames) < 2 {
		t.Fatalf("got %d frames, want at least 2", len(frames))
	}
	var roundtrip []byte
	for _, f := range frames {
		p, ok := ParseTagBody(f.Data)
		if !ok {
			t.Fatalf("split frame %d did not parse: % x", len(roundtrip), f.Data[:8])
		}
		roundtrip = append(roundtrip, p...)
	}
	if !bytes.Equal(roundtrip, big) {
		t.Fatalf("split output = %d bytes, want %d", len(roundtrip), len(big))
	}
}
