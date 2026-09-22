// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package aac

import (
	"bytes"
	"testing"
	"time"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// packer and unpacker are the kernel's interfaces, aliased so the helpers below
// read the same way an adapter's does.
type (
	aacPacker   = registry.RTPPacker
	aacUnpacker = registry.RTPUnpacker
)

// selectModule resolves this codec through the registry, which is how every
// consumer of it gets one.
func selectModule() (registry.CodecPacker, error) {
	return registry.SelectCodec(CodecID)
}

// TestRTPRoundTrip covers RFC 3640: a two-byte header holding the config length
// in the high byte and the access unit length in the low byte.
//
// The length byte is the whole story — there is no splitting for this codec, so
// the only way an audio frame can go missing is for the length to lie. The
// assertion is on the recovered payload, not the header, because the header is
// transport framing and the media is what a player needs.
func TestRTPRoundTrip(t *testing.T) {
	au := []byte{0x21, 0x6f, 0x84, 0x03, 0x80, 0xaa, 0xbb}

	p, err := newRTPPacker()
	if err != nil {
		t.Fatal(err)
	}
	up, err := newRTPUnpacker()
	if err != nil {
		t.Fatal(err)
	}

	payloads, seq, ts := p.Pack(audioUnit(au), 42, 88200)
	if len(payloads) != 1 {
		t.Fatalf("payloads = %d, want 1", len(payloads))
	}
	pl := payloads[0]
	if pl[0] != 0x00 {
		t.Fatalf("config length byte = 0x%02x, want 0x00", pl[0])
	}
	if pl[1] != byte(len(au)) {
		t.Fatalf("au length byte = %d, want %d", pl[1], len(au))
	}
	if !bytes.Equal(pl[2:], au) {
		t.Fatalf("payload = % x, want % x", pl[2:], au)
	}
	if seq != 43 {
		t.Fatalf("seq = %d, want 43", seq)
	}
	if ts != 88200 {
		t.Fatalf("ts = %d, want 88200", ts)
	}

	got, err := up.Unpack(pl, 42, 88200, true)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}
	if got == nil {
		t.Fatal("unpack returned a nil unit")
	}
	if !bytes.Equal(got.Payload, au) {
		t.Fatalf("payload = % x, want % x", got.Payload, au)
	}
	// Audio is key on every frame: AAC is not intra/inter-frame dependent the
	// way a video codec is, so a player must be able to enter on any one.
	if !got.Key {
		t.Fatal("an audio frame must be reported as key")
	}
	// PTS is derived from the RTP timestamp at the codec's real clock rate.
	want := container.FromTransportHz(88200, aacTimescale)
	if !got.PTS.Equal(want) {
		t.Fatalf("pts = %v, want %v", got.PTS, want)
	}
	if !bytes.Equal(got.Payload, au) {
		t.Fatal("payload mutated after unpack")
	}
}

// TestRTPRejectsOversizedAudio checks that an access unit above the payload
// budget is refused instead of silently truncated.
//
// A truncated audio frame is not an error a player reports: it is a silent gap.
// That makes this the failure mode worth pinning, because it looks like normal
// playback to anyone watching.
func TestRTPRejectsOversizedAudio(t *testing.T) {
	p, err := newRTPPacker()
	if err != nil {
		t.Fatal(err)
	}
	if got := p.MaxPayload(); got != maxPayload {
		t.Fatalf("max payload = %d, want %d", got, maxPayload)
	}

	au := make([]byte, maxPayload-1)
	payloads, seq, ts := p.Pack(audioUnit(au), 5, 100)
	if len(payloads) != 0 {
		t.Fatalf("an oversized access unit produced %d payloads, want 0", len(payloads))
	}
	if seq != 5 || ts != 100 {
		t.Fatalf("seq/ts = %d/%d, want 5/100 (unchanged)", seq, ts)
	}

	// Exactly at the limit is legal: the 2-byte header plus the budget fills the
	// payload, and the difference is one byte of the boundary.
	au = make([]byte, maxPayload-2)
	payloads, _, _ = p.Pack(audioUnit(au), 5, 100)
	if len(payloads) != 1 {
		t.Fatalf("at the limit payloads = %d, want 1", len(payloads))
	}
}

// TestRTPUnpackerRejectsBadFrames pins the parser's refusals.
func TestRTPUnpackerRejectsBadFrames(t *testing.T) {
	up, err := newRTPUnpacker()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := up.Unpack(nil, 0, 0, true); err == nil {
		t.Fatal("an empty payload must be refused")
	}
	if _, err := up.Unpack([]byte{0x00}, 0, 0, true); err == nil {
		t.Fatal("a payload without a length byte must be refused")
	}
	if _, err := up.Unpack([]byte{0x01, 0x05, 1, 2, 3}, 0, 0, true); err == nil {
		t.Fatal("a payload advertising a config must be refused")
	}
	// The header says five bytes follow, only three do. Without this the unpacker
	// would hand back a frame whose length the header does not support.
	if _, err := up.Unpack([]byte{0x00, 0x05, 1, 2, 3}, 0, 0, true); err == nil {
		t.Fatal("a length mismatch must be refused")
	}
}

// TestADTSRoundTrip packs a unit through the ADTS container packer and reads it
// back through the unpacker, asserting the raw AAC payload survives unchanged.
//
// ADTS is the container a raw audio stream uses, so this is the only format
// where packing and unpacking are both exercised for this codec.
func TestADTSRoundTrip(t *testing.T) {
	au := []byte{0x21, 0x6f, 0x84, 0x03, 0x80, 0xaa, 0xbb, 0xcc}

	m, err := selectModule()
	if err != nil {
		t.Fatal(err)
	}
	m.SetContainerParams(map[string]string{"sampleRate": "44100", "numberOfChannels": "2"})
	p, err := m.NewContainerPacker(registry.FormatADTS)
	if err != nil {
		t.Fatal(err)
	}
	un, err := m.NewContainerUnpacker(registry.FormatADTS)
	if err != nil {
		t.Fatal(err)
	}

	frames, err := p.Pack(audioUnit(au))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(frames))
	}
	data := frames[0].Data
	if len(data) != adtsFrameLen+len(au) {
		t.Fatalf("frame length = %d, want %d", len(data), adtsFrameLen+len(au))
	}

	units, err := un.Feed(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("units = %d, want 1", len(units))
	}
	if !bytes.Equal(units[0].Payload, au) {
		t.Fatalf("payload = % x, want % x", units[0].Payload, au)
	}
}

// TestADTSMultipleFrames packs several units into one stream and checks the
// unpacker walks all of them in order, which is what keeps a raw stream
// frame-aligned over time.
func TestADTSMultipleFrames(t *testing.T) {
	aur := [][]byte{
		{0x21, 0x6f, 0x84, 0x03, 0x80},
		{0x21, 0x6f, 0x84, 0x03, 0x80, 0xaa},
		{0x21, 0x6f, 0x84, 0x03, 0x80, 0xaa, 0xbb},
	}

	m, err := selectModule()
	if err != nil {
		t.Fatal(err)
	}
	m.SetContainerParams(map[string]string{"sampleRate": "48000", "numberOfChannels": "1"})
	p, err := m.NewContainerPacker(registry.FormatADTS)
	if err != nil {
		t.Fatal(err)
	}
	un, err := m.NewContainerUnpacker(registry.FormatADTS)
	if err != nil {
		t.Fatal(err)
	}

	var raw []byte
	for _, au := range aur {
		frames, err := p.Pack(audioUnit(au))
		if err != nil {
			t.Fatal(err)
		}
		if len(frames) != 1 {
			t.Fatalf("frames = %d, want 1", len(frames))
		}
		raw = append(raw, frames[0].Data...)
	}

	units, err := un.Feed(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != len(aur) {
		t.Fatalf("units = %d, want %d", len(units), len(aur))
	}
	for i, u := range units {
		if !bytes.Equal(u.Payload, aur[i]) {
			t.Fatalf("frame %d payload = % x, want % x", i, u.Payload, aur[i])
		}
	}
}

// audioUnit builds one audio unit with an explicit time, so tests can assert on
// PTS and not only on the payload.
func audioUnit(au []byte) *stream.Unit {
	return stream.NewUnit(&stream.Unit{
		TrackID: 2, Codec: CodecID, Kind: stream.KindAudio,
		Payload: au, PTS: time.UnixMilli(1200), DTS: time.UnixMilli(1200),
	})
}

func newRTPPacker() (aacPacker, error) {
	m, err := selectModule()
	if err != nil {
		return nil, err
	}
	return m.NewRTPPacker()
}

func newRTPUnpacker() (aacUnpacker, error) {
	m, err := selectModule()
	if err != nil {
		return nil, err
	}
	return m.NewRTPUnpacker()
}
