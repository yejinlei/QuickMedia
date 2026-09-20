// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package h264

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// TestFLVConfigTagRoundTrip checks that the SPS and PPS survive the trip into
// and out of an AVC decoder configuration record.
//
// FLV has no in-band syntax for parameter sets, so this tag is the only
// channel they have. A player that receives it truncated renders nothing at
// all, which is why the assertion is on the recovered bytes and not on the tag
// size.
func TestFLVConfigTagRoundTrip(t *testing.T) {
	sps := syntheticSPS()
	pps := syntheticPPS()

	m, err := registry.SelectCodec(CodecID)
	if err != nil {
		t.Fatal(err)
	}
	m.SetContainerParams(map[string]string{
		"sps": hex.EncodeToString(sps),
		"pps": hex.EncodeToString(pps),
	})
	p, err := m.NewContainerPacker(registry.FormatFLV)
	if err != nil {
		t.Fatal(err)
	}

	frames := p.ConfigFrames()
	if len(frames) != 1 {
		t.Fatalf("got %d config frames, want 1", len(frames))
	}
	f := frames[0]
	if !f.Config || !f.Key {
		t.Fatal("the AVC config record must be marked Config and Key")
	}

	// Byte 0 is frame_type 4 | codec_id 7, byte 1 is packet_type 0. A player
	// reads both before looking at the payload, so both are asserted.
	if f.Data[0] != 0x17 || f.Data[1] != 0x00 {
		t.Fatalf("tag header = %02x %02x, want 41 00", f.Data[0], f.Data[1])
	}

	gotSPS, gotPPS, ok := container.ParseAVCSetupTag(f.Data)
	if !ok {
		t.Fatal("ParseAVCSetupTag rejected the tag this packer wrote")
	}
	if !bytes.Equal(gotSPS, sps) {
		t.Fatalf("sps = % x, want % x", gotSPS, sps)
	}
	if !bytes.Equal(gotPPS, pps) {
		t.Fatalf("pps = % x, want % x", gotPPS, pps)
	}

	// No parameter sets, no tag: an empty descriptor is indistinguishable from a
	// broken stream.
	m.SetContainerParams(map[string]string{})
	p2, err := m.NewContainerPacker(registry.FormatFLV)
	if err != nil {
		t.Fatal(err)
	}
	if got := p2.ConfigFrames(); len(got) != 0 {
		t.Fatalf("empty params produced %d config frames, want 0", len(got))
	}
}

// TestFLVCodedFrameRoundTrip packs a coded frame and reads the access unit back
// out, which is the path every FLV sink takes after the config record.
func TestFLVCodedFrameRoundTrip(t *testing.T) {
	au := SyntheticAU(true)

	m, err := registry.SelectCodec(CodecID)
	if err != nil {
		t.Fatal(err)
	}
	m.SetContainerParams(map[string]string{
		"sps": hex.EncodeToString(syntheticSPS()),
		"pps": hex.EncodeToString(syntheticPPS()),
	})
	p, err := m.NewContainerPacker(registry.FormatFLV)
	if err != nil {
		t.Fatal(err)
	}

	frames, err := p.Pack(videoUnit(au, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1", len(frames))
	}
	if !frames[0].Key {
		t.Fatal("an IDR access unit must be reported as a key frame")
	}

	got, ok := container.ParseTagBody(frames[0].Data)
	if !ok {
		t.Fatal("ParseTagBody rejected the coded frame this packer wrote")
	}
	if !bytes.Equal(got, au) {
		t.Fatalf("payload = % x, want % x", got, au)
	}
	if container.ParseTagType(frames[0].Data) != 9 {
		t.Fatalf("tag type = %d, want video (9)", container.ParseTagType(frames[0].Data))
	}
}

// TestFMP4PassThrough covers fragmented MP4, whose sample is the kernel's AVCC
// payload as-is.
//
// The pass-through is deliberate: the representation was chosen so that an fMP4
// sample and a kernel unit are the same bytes. Pinning it here catches an
// accidental conversion later, which would cost a copy per frame on the hot
// path.
func TestFMP4PassThrough(t *testing.T) {
	au := SyntheticAU(false)

	m, err := registry.SelectCodec(CodecID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.NewContainerPacker(registry.FormatFMP4)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.ConfigFrames(); len(got) != 0 {
		t.Fatalf("fMP4 emitted %d config frames, want 0", len(got))
	}

	frames, err := p.Pack(videoUnit(au, 3))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1", len(frames))
	}
	if !bytes.Equal(frames[0].Data, au) {
		t.Fatalf("sample = % x, want % x", frames[0].Data, au)
	}
}

// TestAnnexBRoundTrip packs a unit into Annex-B and reads it back, asserting the
// start codes are inserted on the way in and stripped on the way out.
//
// Annex-B is the form RTSP feeds, so a round trip here exercises the boundary
// the adapters sit on either side of. The NAL set is asserted rather than just
// the length, because a mis-placed start code changes the first NAL's type and
// turns an IDR into something a decoder refuses.
//
// The Config flag is asserted separately. A config-only unit is the one a
// late-joining decoder needs, so dropping it on the flag means the stream plays
// for a few frames and then stops, with no error anywhere in the logs.
func TestAnnexBRoundTrip(t *testing.T) {
	au := SyntheticAU(true)

	m, err := registry.SelectCodec(CodecID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.NewContainerPacker(registry.FormatAnnexB)
	if err != nil {
		t.Fatal(err)
	}
	un, err := m.NewContainerUnpacker(registry.FormatAnnexB)
	if err != nil {
		t.Fatal(err)
	}

	frames, err := p.Pack(videoUnit(au, 4))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1", len(frames))
	}
	// A start code prefixes every NAL, so it opens the stream rather than
	// closing it. Asserting the opening catches a packer that forgets the code
	// on the first NAL, which is the one a decoder reads first.
	if !bytes.HasPrefix(frames[0].Data, []byte{0, 0, 0, 1}) {
		t.Fatal("an Annex-B stream must open with a start code")
	}
	// Three NALs means three start codes. A missing one in the middle merges two
	// NALs into one and silently corrupts every frame after it.
	if got := bytes.Count(frames[0].Data, []byte{0, 0, 0, 1}); got != 3 {
		t.Fatalf("start codes = %d, want 3", got)
	}
	if !frames[0].Key {
		t.Fatal("an IDR access unit must be reported as a key frame")
	}
	if frames[0].Config {
		t.Fatal("a coded access unit must not be reported as a config frame")
	}

	units, err := un.Feed(frames[0].Data)
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("units = %d, want 1", len(units))
	}
	if !bytes.Equal(units[0].Payload, au) {
		t.Fatalf("payload = % x, want % x", units[0].Payload, au)
	}
	nals := container.ParseAU(units[0].Payload)
	if len(nals) != 3 || nals[0].Type != container.NalSPS || nals[1].Type != container.NalPPS {
		t.Fatalf("nal types = % d, want SPS, PPS, picture", typesOf(nals))
	}
}

// TestAnnexBConfigFrame marks an access unit that is only parameter sets.
//
// An encoder that retransmits SPS and PPS ahead of every IDR writes such a unit
// as a separate access unit. If the packer does not flag it, the kernel drops it
// as a config frame and a late joiner arrives into a stream with no parameters.
func TestAnnexBConfigFrame(t *testing.T) {
	au := container.EncodeAU([][]byte{syntheticSPS(), syntheticPPS()})

	m, err := registry.SelectCodec(CodecID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.NewContainerPacker(registry.FormatAnnexB)
	if err != nil {
		t.Fatal(err)
	}

	frames, err := p.Pack(videoUnit(au, 4))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("got %d frames, want 1", len(frames))
	}
	if !frames[0].Config {
		t.Fatal("an access unit of parameter sets must be marked Config")
	}
	if frames[0].Key {
		t.Fatal("a config-only access unit must not be marked Key")
	}

	un, err := m.NewContainerUnpacker(registry.FormatAnnexB)
	if err != nil {
		t.Fatal(err)
	}
	units, err := un.Feed(frames[0].Data)
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 || !bytes.Equal(units[0].Payload, au) {
		t.Fatalf("units = %d, want 1 byte-identical", len(units))
	}
}

// TestTSRoundTrip multiplexes access units into transport packets and demuxes
// them back out.
//
// The TS unpacker completes a PES when the next payload-unit-start arrives, so
// one unit alone leaves the last PES buffered. Two units are packed here, which
// is the real shape of a live stream and flushes the first unit out.
func TestTSRoundTrip(t *testing.T) {
	au1 := SyntheticAU(true)
	au2 := SyntheticAU(false)

	m, err := registry.SelectCodec(CodecID)
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.NewContainerPacker(registry.FormatTS)
	if err != nil {
		t.Fatal(err)
	}
	un, err := m.NewContainerUnpacker(registry.FormatTS)
	if err != nil {
		t.Fatal(err)
	}

	var raw [][]byte
	for _, au := range [][]byte{au1, au2} {
		frames, err := p.Pack(videoUnit(au, 5))
		if err != nil {
			t.Fatal(err)
		}
		if len(frames) < 1 {
			t.Fatal("no transport packets emitted")
		}
		for _, f := range frames {
			if len(f.Data) != 188 {
				t.Fatalf("transport packet length = %d, want 188", len(f.Data))
			}
			if f.Data[0] != 0x47 {
				t.Fatalf("sync byte = 0x%02x, want 0x47", f.Data[0])
			}
			raw = append(raw, f.Data)
		}
	}

	units, err := un.Feed(bytes.Join(raw, nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 {
		t.Fatalf("units = %d, want 1", len(units))
	}
	if !bytes.Equal(units[0].Payload, au1) {
		t.Fatalf("payload = % x, want % x", units[0].Payload, au1)
	}
	if !container.IsVideoKey(units[0]) {
		t.Fatal("a demuxed IDR must be reported as a key frame")
	}

	// The second unit lands on the next PES boundary, which is what completed the
	// first one above.
	units, err = un.Feed(bytes.Repeat([]byte{0x47}, 200))
	if err != nil {
		t.Fatal(err)
	}
	if len(units) != 1 || !bytes.Equal(units[0].Payload, au2) {
		t.Fatalf("units = %d, want 1 carrying the second access unit", len(units))
	}
}

// videoUnit builds one video unit carrying the given access unit.
func videoUnit(au []byte, trackID stream.TrackID) *stream.Unit {
	return stream.NewUnit(&stream.Unit{
		TrackID: trackID, Codec: CodecID, Kind: stream.KindVideo, Payload: au,
	})
}

// typesOf renders NAL types for a failure message.
func typesOf(nals []container.Nalu) []uint8 {
	out := make([]uint8, 0, len(nals))
	for _, n := range nals {
		out = append(out, uint8(n.Type))
	}
	return out
}
