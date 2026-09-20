// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package h264

import (
	"bytes"
	"testing"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// packer and unpacker are the kernel's interfaces, aliased so the test helpers
// below read the same way an adapter's does.
type (
	packer   = registry.RTPPacker
	unpacker = registry.RTPUnpacker
)

// selectModule resolves this codec through the registry, which is how every
// consumer of it gets one.
func selectModule() (registry.CodecPacker, error) {
	return registry.SelectCodec(CodecID)
}

// TestRTPSTAPARoundTrip covers the single-packet aggregation path.
//
// A multi-NAL access unit that fits in one payload is packed into one STAP-A
// packet. The assertion is on the reassembled access unit, not on the packet
// bytes: STAP-A is transport framing, so what has to survive the round trip is
// the media itself and the type of each NAL inside it. SPS and PPS are the
// two NALs a decoder cannot recover after a mid-stream loss, which is why the
// test checks their presence and not merely their count.
func TestRTPSTAPARoundTrip(t *testing.T) {
	au := SyntheticAU(true)

	p, err := newRTPPacker()
	if err != nil {
		t.Fatal(err)
	}
	up, err := newRTPUnpacker()
	if err != nil {
		t.Fatal(err)
	}

	payloads, seq, ts := p.Pack(unitOf(au, 1), 100, 90000)
	if len(payloads) != 1 {
		t.Fatalf("payloads = %d, want 1 (stap-a aggregation)", len(payloads))
	}
	if payloads[0][0]&0x1F != container.NalSTAPA {
		t.Fatalf("first payload type = %d, want STAP-A (%d)", payloads[0][0]&0x1F, container.NalSTAPA)
	}
	if seq != 101 {
		t.Fatalf("seq = %d, want 101", seq)
	}
	if ts != 90000 {
		t.Fatalf("ts = %d, want 90000", ts)
	}

	got, err := up.Unpack(payloads[0], 100, 90000, true)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}
	if got == nil {
		t.Fatal("unpack returned a nil unit")
	}
	if !bytes.Equal(got.Payload, au) {
		t.Fatalf("payload = % x, want % x", got.Payload, au)
	}
	// The access unit is byte-identical, so the NAL set is too. Verify the types
	// separately anyway: a repacked unit with the same total length and the same
	// NAL count can still have the parameter sets reordered, and a player that
	// sees the picture before the SPS renders nothing.
	nals := container.ParseAU(got.Payload)
	if len(nals) != 3 {
		t.Fatalf("nals = %d, want 3", len(nals))
	}
	if nals[0].Type != container.NalSPS || nals[1].Type != container.NalPPS || nals[2].Type != container.NalIDR {
		t.Fatalf("nal types = %d,%d,%d, want 7,8,5", nals[0].Type, nals[1].Type, nals[2].Type)
	}
}

// TestRTPSingleNALRoundTrip covers the unaggregated path, where the picture is
// small enough to travel alone. It has to be tested separately because the
// packer takes a different branch, and a packet written by that branch carries
// no length prefix for the unpacker to check.
func TestRTPSingleNALRoundTrip(t *testing.T) {
	au := container.EncodeAU([][]byte{SyntheticPicture(false)})

	p, _ := newRTPPacker()
	up, _ := newRTPUnpacker()

	payloads, seq, _ := p.Pack(unitOf(au, 1), 7, 300)
	if len(payloads) != 1 {
		t.Fatalf("payloads = %d, want 1", len(payloads))
	}
	if payloads[0][0]&0x1F != container.NalNonIDR {
		t.Fatalf("payload type = %d, want non-IDR slice (1)", payloads[0][0]&0x1F)
	}
	if seq != 8 {
		t.Fatalf("seq = %d, want 8", seq)
	}

	// The marker bit is what tells the unpacker a bare NAL is complete. With it
	// absent the unpacker has no other way to know the access unit ended, so it
	// must return nothing; asserting both directions pins the contract.
	if u, err := up.Unpack(payloads[0], 7, 300, false); err != nil || u != nil {
		t.Fatalf("unpack without marker: unit=%v err=%v, want nil,nil", u, err)
	}
	got, err := up.Unpack(payloads[0], 7, 300, true)
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}
	if !bytes.Equal(got.Payload, au) {
		t.Fatalf("payload = % x, want % x", got.Payload, au)
	}
	if container.IsIDR(got.Payload) {
		t.Fatal("a non-IDR picture must not be reported as key")
	}
}

// TestRTPFragmentationRoundTrip covers FU-A: one NAL larger than the payload
// budget splits into several packets and must reassemble into the exact bytes it
// started as.
//
// This is the path most likely to corrupt a stream silently. A single dropped
// fragment or a mis-set start/end bit yields a unit the unpacker hands back as
// if it were valid, and the player then has to show the damage. The assertion
// on the reassembled body is therefore byte-exact, and the NRI bits are
// checked too, because losing them changes how every subsequent NAL in the
// stream is interpreted.
func TestRTPFragmentationRoundTrip(t *testing.T) {
	original := pad(SyntheticPicture(true), 2400)
	au := container.EncodeAU([][]byte{original})

	p, _ := newRTPPacker()
	if got := p.MaxPayload(); got != maxPayload {
		t.Fatalf("max payload = %d, want %d", got, maxPayload)
	}
	up, _ := newRTPUnpacker()

	payloads, seq, ts := p.Pack(unitOf(au, 1), 50, 45000)
	if len(payloads) < 2 {
		t.Fatalf("payloads = %d, want >= 2 (fragmented)", len(payloads))
	}
	// Every fragment must carry the FU-A indicator, and the payload budget must
	// be respected. Exceeding it produces an oversized UDP datagram that is
	// silently dropped on the wire, which is exactly the failure this test is
	// here to catch.
	for i, pl := range payloads {
		if pl[0]&0x1F != container.NalFUA {
			t.Fatalf("payload %d type = %d, want FU-A (28)", i, pl[0]&0x1F)
		}
		if len(pl) > maxPayload {
			t.Fatalf("payload %d length = %d, exceeds max %d", i, len(pl), maxPayload)
		}
	}
	if payloads[len(payloads)-1][1]&0x40 == 0 {
		t.Fatal("last fragment has no end bit set")
	}
	if payloads[0][1]&0x80 == 0 {
		t.Fatal("first fragment has no start bit set")
	}
	if seq != 50+uint16(len(payloads)) {
		t.Fatalf("seq = %d, want %d", seq, 50+uint16(len(payloads)))
	}
	if ts != 45000 {
		t.Fatalf("ts = %d, want 45000", ts)
	}

	var unit *stream.Unit
	for i, pl := range payloads {
		var err error
		unit, err = up.Unpack(pl, uint16(50+i), 45000, i == len(payloads)-1)
		if err != nil {
			t.Fatalf("unpack fragment %d: %v", i, err)
		}
		if i < len(payloads)-1 && unit != nil {
			t.Fatalf("fragment %d completed the unit early", i)
		}
	}
	if unit == nil {
		t.Fatal("fragmentation produced no unit")
	}
	if !bytes.Equal(unit.Payload, au) {
		t.Fatalf("reassembled length = %d, want %d", len(unit.Payload), len(au))
	}
	nals := container.ParseAU(unit.Payload)
	if len(nals) != 1 {
		t.Fatalf("nals = %d, want 1", len(nals))
	}
	if !bytes.Equal(nals[0].Data, original) {
		t.Fatal("reassembled NAL does not match the original")
	}
	if nals[0].Type != container.NalIDR {
		t.Fatalf("reassembled type = %d, want IDR (5)", nals[0].Type)
	}
	if !container.IsVideoKey(unit) {
		t.Fatal("a reassembled IDR must be reported as key")
	}
}

// TestRTPRejectsGarbage pins the unpacker's refusal behaviour, because a
// decoder-fed garbage path looks exactly like a corrupted stream from the
// player's side and is much harder to diagnose later.
func TestRTPRejectsGarbage(t *testing.T) {
	up, _ := newRTPUnpacker()
	if _, err := up.Unpack(nil, 0, 0, true); err == nil {
		t.Fatal("an empty payload must be refused")
	}
	if _, err := up.Unpack([]byte{container.NalFUA}, 0, 0, true); err == nil {
		t.Fatal("a fragment without a header byte must be refused")
	}
	if _, err := up.Unpack([]byte{container.NalSTAPA, 0x00, 0x10, 0x65, 0x01}, 0, 0, true); err == nil {
		t.Fatal("a truncated STAP-A must be refused")
	}
}

// newRTPPacker builds a packer through the module factory, the way the
// protocol adapters do.
func newRTPPacker() (packer, error) {
	m, err := selectModule()
	if err != nil {
		return nil, err
	}
	return m.NewRTPPacker()
}

// newRTPUnpacker builds an unpacker through the module factory.
func newRTPUnpacker() (unpacker, error) {
	m, err := selectModule()
	if err != nil {
		return nil, err
	}
	return m.NewRTPUnpacker()
}

// unitOf builds one video unit carrying the given access unit.
func unitOf(au []byte, trackID stream.TrackID) *stream.Unit {
	return stream.NewUnit(&stream.Unit{
		TrackID: trackID, Codec: CodecID, Kind: stream.KindVideo, Payload: au,
	})
}

// pad returns a copy of n grown to at least length n. The padding bytes are
// inert, so the result is a NAL with the right header and the right size.
func pad(n []byte, size int) []byte {
	if len(n) >= size {
		return append([]byte{}, n...)
	}
	out := make([]byte, size)
	copy(out, n)
	return out
}
