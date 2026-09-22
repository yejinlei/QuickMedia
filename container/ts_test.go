// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package container

import (
	"bytes"
	"testing"
	"time"

	"github.com/yejinlei/quickmedia/kernel/stream"
)

// t0 anchors every container test to a fixed instant. A fixed time keeps the
// transport timestamps deterministic, which is what makes a golden-byte
// comparison possible.
var t0 = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

// videoUnit builds a video unit for the muxer tests. Durations are constant
// because nothing about the transport framing depends on them.
func videoUnit(payload []byte, key bool, dts, pts time.Time) *stream.Unit {
	return &stream.Unit{
		Kind: stream.KindVideo, Payload: payload, Key: key,
		DTS: dts, PTS: pts, Duration: 33 * time.Millisecond,
	}
}

// groupPES splits a transport stream into PES packets by payload-unit-start.
// A PES can span several transport packets, so the reassembly boundary is the
// next PUSI rather than the end of a packet.
func groupPES(t *testing.T, buf []byte, pid uint16) [][]byte {
	t.Helper()
	var groups [][]byte
	var cur []byte
	for _, p := range ParseTS(buf) {
		h := ParseTSHeader(p)
		if h.PID != pid {
			continue
		}
		payload := TSPayload(p)
		if len(payload) == 0 {
			continue
		}
		if h.PUSI {
			if len(cur) > 0 {
				groups = append(groups, cur)
			}
			cur = append([]byte(nil), payload...)
		} else {
			cur = append(cur, payload...)
		}
	}
	if len(cur) > 0 {
		groups = append(groups, cur)
	}
	return groups
}

// TestTSWriterProducesPackets pins the transport packet header contract:
// sync byte, PID, payload-unit-start indicator, and adaptation-field length.
// Every byte here is a value a receiver uses to make a decision, so a wrong
// bit is a silent loss of video rather than an obvious failure.
func TestTSWriterProducesPackets(t *testing.T) {
	w := NewTSWriter(TSConfig{StreamPID: 0x0100, StreamType: TSStreamTypeH264})
	if got, want := w.StreamPID(), uint16(0x0100); got != want {
		t.Fatalf("StreamPID = %d, want %d", got, want)
	}
	if got := w.PATPMT(); len(got) == 0 {
		t.Fatal("PATPMT produced no packets")
	}

	pkts := w.Pack(videoUnit(testIDR(), true, t0, t0.Add(time.Millisecond)))
	if len(pkts) == 0 {
		t.Fatal("Pack produced no packets")
	}

	var pusis int
	for i, p := range pkts {
		if len(p) != tsPacketSize {
			t.Fatalf("packet %d length %d, want %d", i, len(p), tsPacketSize)
		}
		if p[0] != 0x47 {
			t.Fatalf("packet %d sync byte 0x%02x", i, p[0])
		}
		h := ParseTSHeader(p)
		if h.PID != 0x0100 {
			t.Fatalf("packet %d PID 0x%04x, want 0x0100", i, h.PID)
		}
		if h.PUSI {
			pusis++
		}
		// The adaptation field must not overrun the packet: its length counts
		// everything from byte 5 to the end of the packet.
		if h.AUF {
			if int(p[4])+5 > tsPacketSize {
				t.Fatalf("packet %d adaptation length %d overruns the packet", i, p[4])
			}
		}
	}
	if pusis != 1 {
		t.Fatalf("PUSI set on %d packets, want exactly 1", pusis)
	}
}

// TestTSSyncTables checks the program association table and program map table
// are both present, on distinct PIDs, and that the PAT points at the PMT.
func TestTSSyncTables(t *testing.T) {
	w := NewTSWriter(TSConfig{StreamPID: 0x0100, StreamType: TSStreamTypeH264})
	sync := w.PATPMT()

	var patBody, pmtBody []byte
	for _, p := range sync {
		h := ParseTSHeader(p)
		switch h.PID {
		case 0x0001:
			patBody = TSPayload(p)
		case 0x0101:
			pmtBody = TSPayload(p)
		}
	}
	if len(patBody) == 0 {
		t.Fatal("no PAT payload")
	}
	if len(pmtBody) == 0 {
		t.Fatal("no PMT payload")
	}
	// PAT section (after TSPayload strips the packet header):
	// [0]=marker 0x00, [1]=table_id, [2]=0xB0, [3]=sec_len,
	// [4:6]=program_number, [6]=reserved|network_flag|network_PID_hi,
	// [7:9]=PMT_PID.
	if len(patBody) < 9 {
		t.Fatalf("PAT section too short: %d", len(patBody))
	}
	wantPMT := uint16(0x0101)
	pid, ok := ReadU16(patBody[7:])
	if !ok {
		t.Fatal("PAT section too short to hold the PMT PID")
	}
	if pid != wantPMT {
		t.Fatalf("PAT points at PID %d, want %d", pid, wantPMT)
	}
	// The PAT and PMT must not share a PID, or a receiver cannot tell a program
	// association table from a program map table.
	if w.patPID == w.pmtPID {
		t.Fatalf("PAT and PMT collide on PID %d", w.patPID)
	}
}

// TestTSRoundTripDemux is the end-to-end test: pack units into a byte stream,
// then split, reassemble, and parse it back out. The comparison is exact,
// because the container must preserve payload bytes verbatim.
func TestTSRoundTripDemux(t *testing.T) {
	w := NewTSWriter(TSConfig{StreamPID: 0x0100, StreamType: TSStreamTypeH264})

	units := []*stream.Unit{
		videoUnit(testIDR(), true, t0, t0),
		videoUnit(testIDR(), false, t0.Add(33*time.Millisecond), t0.Add(33*time.Millisecond)),
		videoUnit(testSPSPPS(), true, t0.Add(time.Second), t0.Add(time.Second)),
	}

	var buf []byte
	for _, u := range units {
		for _, p := range w.Pack(u) {
			buf = append(buf, p...)
		}
	}
	// Re-emit the sync tables partway through, as the muxer does after a
	// keyframe. A correct demuxer must skip them.
	for _, p := range w.PATPMT() {
		buf = append(buf, p...)
	}

	groups := groupPES(t, buf, 0x0100)
	if len(groups) != len(units) {
		t.Fatalf("demuxed %d PES packets, want %d", len(groups), len(units))
	}
	for i, g := range groups {
		body, ok := ParsePES(g)
		if !ok {
			t.Fatalf("unit %d: unparseable PES of length %d", i, len(g))
		}
		if !bytes.Equal(body, units[i].Payload) {
			t.Errorf("unit %d payload mismatch\ngot  %x\nwant %x", i, body, units[i].Payload)
		}
	}
}

// TestTSPESLengthAndPCR makes two structural checks that only show up when a
// receiver uses the PES length field instead of inferring the boundary from
// the next start code, and when it uses PCR instead of the stream timestamps.
func TestTSPESLengthAndPCR(t *testing.T) {
	w := NewTSWriter(TSConfig{StreamPID: 0x0100, StreamType: TSStreamTypeH264})
	dts, pts := t0.Add(time.Second), t0.Add(time.Second+time.Millisecond)
	pkts := w.Pack(videoUnit(testIDR(), true, dts, pts))

	groups := groupPES(t, nil, 0x0100) // nothing yet
	_ = groups
	var buf []byte
	for _, p := range pkts {
		buf = append(buf, p...)
	}
	g := groupPES(t, buf, 0x0100)
	if len(g) != 1 {
		t.Fatalf("got %d PES packets, want 1", len(g))
	}
	body := g[0]

	// PES header: start code (4), length (2), flag (1), reserved byte (1),
	// data-unit-length (1) = 9 bytes before any optional data.
	if len(body) < 9 {
		t.Fatalf("PES shorter than its fixed header: %d", len(body))
	}
	flags := body[6]
	dataLen := int(body[8])
	// PES length counts from the flag byte through the end of the PES:
	// flags(1) + reserved(1) + data-unit-length(1) + optional header + payload.
	// body[9:] already includes both the optional header and the payload,
	// so the expected length is 3 + len(body[9:]).
	pesLen := int(body[4])<<8 | int(body[5])
	want := 3 + len(body[9:])
	if pesLen != want {
		t.Fatalf("PES length %d, want %d (body len %d, dataLen %d)", pesLen, want, len(body), dataLen)
	}
	// flags: reserved(3) + marker(1) + optional(1) + DTS(1) + PTS(1) + PCR(1)
	if flags&0x80 == 0 {
		t.Error("optional header flag not set")
	}
	if flags&0x04 == 0 {
		t.Error("keyframe PES missing PCR flag")
	}
	if flags&0x08 == 0 {
		t.Error("PES missing PTS flag")
	}
	if flags&0x10 == 0 {
		t.Error("PES missing DTS flag")
	}
	// The optional header is 3 bytes (reserved+flags+datagen) + 2 buffer size
	// + 6 PCR + 5 PTS + 5 DTS = 21.
	if dataLen != 21 {
		t.Fatalf("data-unit-length %d, want 21", dataLen)
	}
}

// TestParseTSAlignmentRecovery proves that a stream truncated mid-packet can
// still be read, because the muxer's own output starts at a packet boundary.
func TestParseTSAlignmentRecovery(t *testing.T) {
	w := NewTSWriter(TSConfig{StreamPID: 0x0100, StreamType: TSStreamTypeH264})

	// Build a payload that avoids the 0x47 sync byte so alignment recovery can
	// be tested without false positives from payload data.
	big := make([]byte, 400)
	for i := range big {
		big[i] = byte((i*7 + 3) & 0xFF) // simple pattern that doesn't hit 0x47 often
	}

	pkts := w.Pack(videoUnit(big, true, t0, t0))
	if len(pkts) < 2 {
		t.Fatalf("need at least two packets, got %d", len(pkts))
	}

	// Build a buffer that starts mid-packet: skip the first 37 bytes of the
	// first packet, then include the full second packet.
	var buf []byte
	buf = append(buf, pkts[0][37:]...)
	buf = append(buf, pkts[1]...)

	// ParseTS should recover the second packet, which is the only complete
	// 188-byte packet aligned at a 0x47 sync byte.
	got := ParseTS(buf)
	if len(got) != 1 {
		t.Fatalf("got %d packets, want 1", len(got))
	}

	// The recovered packet should be pkts[1], not the misaligned fragment.
	if !bytes.Equal(got[0], pkts[1]) {
		t.Fatalf("recovered packet mismatch\ngot  %x\nwant %x", got[0], pkts[1])
	}
}

// TestParseTSAlignmentRecoveryFromZero covers the degenerate streams.
func TestParseTSAlignmentRecoveryFromZero(t *testing.T) {
	if got := ParseTS(nil); len(got) != 0 {
		t.Fatalf("nil stream produced %d packets", len(got))
	}
	if got := ParseTS([]byte{0x47, 0x00}); len(got) != 0 {
		t.Fatalf("partial packet produced %d packets", len(got))
	}
	if got := ParseTS([]byte{0xFF, 0xFF, 0xFF}); len(got) != 0 {
		t.Fatalf("garbage produced %d packets", len(got))
	}
}

// TestCRC32MPEG verifies the PAT section's CRC as a receiver would: over every
// byte except the CRC itself, compared against the stored value. The tamper
// case proves the check actually fails rather than passing on a constant.
func TestCRC32MPEG(t *testing.T) {
	w := NewTSWriter(TSConfig{StreamPID: 0x0100, StreamType: TSStreamTypeH264})
	pkt := w.PATPMT()[0]
	section := TSPayload(pkt)
	if len(section) < 9 {
		t.Fatalf("PAT section too short: %d", len(section))
	}

	stored, ok := ReadU32(section[len(section)-4:])
	if !ok {
		t.Fatal("cannot read stored CRC")
	}
	if got := crc32mpeg(section[:len(section)-4]); got != stored {
		t.Fatalf("PAT section CRC %08x, stored %08x", got, stored)
	}

	tampered := append([]byte(nil), section...)
	tampered[5] ^= 0x01
	if crc32mpeg(tampered[:len(tampered)-4]) == stored {
		t.Fatal("tampered section still verifies")
	}
}
