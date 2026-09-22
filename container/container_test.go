// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package container

import (
	"bytes"
	"testing"
	"time"
)

// hour32 is one hour. Named separately so the multiples in the timestamp
// tests stay constant expressions rather than runtime calls.
const hour32 = time.Hour

// testSPSPPS builds a minimal SPS + PPS access unit. The SPS is the
// 160x120, level 40 profile 66 (Baseline) example from the spec, which is
// enough for a real decoder to parse. Using a real profile rather than an
// arbitrary byte run is what makes the container round-trip test meaningful:
// the container must not alter the NAL content.
func testSPSPPS() []byte {
	sps := []byte{
		0x67, 0x42, 0xC0, 0x1E, 0xD9, 0x00, 0xA0, 0x4D,
		0xC0, 0x41, 0xF0, 0x10, 0x10, 0x10, 0x10, 0x10,
	}
	pps := []byte{0x68, 0xCE, 0x38, 0x80}
	return EncodeAU([][]byte{sps, pps})
}

// testIDR is a tiny access unit carrying an IDR NAL.
func testIDR() []byte {
	return EncodeAU([][]byte{{0x65, 0x10, 0x03, 0x00, 0x80, 0x02, 0x92, 0x00, 0x00, 0x00, 0x03, 0x00, 0x80, 0x00, 0x00, 0x17, 0x80, 0x00, 0x00, 0x1e, 0x00, 0xa0, 0xa0, 0xa0, 0xa0, 0xa0, 0xa0, 0xa0, 0xa0, 0xa0}})
}

func TestEncodeDecodeAU(t *testing.T) {
	spsPPS := testSPSPPS()
	nals := ParseAU(spsPPS)
	if len(nals) != 2 {
		t.Fatalf("want 2 NALs, got %d", len(nals))
	}
	if nals[0].Type != NalSPS || nals[1].Type != NalPPS {
		t.Fatalf("types = %d,%d", nals[0].Type, nals[1].Type)
	}
	if !bytes.Equal(EncodeAU([][]byte{nals[0].Data, nals[1].Data}), spsPPS) {
		t.Fatal("round trip mismatch")
	}
}

func TestParseAUMalformed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"empty", nil},
		{"short", []byte{0x00, 0x00, 0x01}},
		{"zero-len", []byte{0x00, 0x00, 0x00, 0x00, 0x67}},
		{"overrun", []byte{0x00, 0x00, 0x00, 0x05, 0x67, 0x42}},
	} {
		if got := ParseAU(tc.payload); got != nil {
			t.Errorf("%s: want nil, got %v", tc.name, got)
		}
	}
}

func TestIsIDRAndConfig(t *testing.T) {
	if !IsIDR(testIDR()) {
		t.Error("IDR not detected")
	}
	if IsIDR(testSPSPPS()) {
		t.Error("SPS/PPS reported as IDR")
	}
	if !IsConfig(testSPSPPS()) {
		t.Error("SPS/PPS not detected as config")
	}
	if IsConfig(testIDR()) {
		t.Error("IDR reported as config")
	}
	if IsConfig(nil) {
		t.Error("nil reported as config")
	}
}

// TestIsConfigOnly covers the distinction that decides whether the kernel
// drops a unit as a config frame.
//
// An encoder that retransmits SPS and PPS ahead of its IDR ships one access
// unit containing all three NALs. IsConfig reports that unit as configuration,
// which is correct for a writer choosing where to emit a descriptor but wrong
// for a kernel deciding whether to deliver it: treating that unit as config
// drops the IDR, and every keyframe group loses its first picture. That is the
// regression IsConfigOnly exists to hold back.
func TestIsConfigOnly(t *testing.T) {
	if !IsConfigOnly(testSPSPPS()) {
		t.Error("SPS/PPS only not detected as config-only")
	}
	if IsConfigOnly(testIDR()) {
		t.Error("a picture reported as config-only")
	}
	if IsConfigOnly(nil) {
		t.Error("nil reported as config-only")
	}

	// SPS + PPS + IDR in one unit: the config test says yes, the config-only
	// test must say no.
	mixed := EncodeAU([][]byte{{0x67, 0x42}, {0x68, 0xce}, {0x65, 0x10}})
	if !IsConfig(mixed) {
		t.Fatal("a unit carrying parameter sets must be config")
	}
	if IsConfigOnly(mixed) {
		t.Fatal("a unit carrying a picture must not be config-only")
	}
}

func TestTransportRoundTrip(t *testing.T) {
	epoch := time.Unix(0, 0).UTC()
	for _, off := range []time.Duration{
		0,
		33 * time.Millisecond,
		10001 * time.Millisecond,
		4 * hour32,
	} {
		t0 := epoch.Add(off)
		ts := ToTransport(t0)
		got := FromTransport(ts)
		diff := got.Sub(t0)
		if diff < 0 {
			diff = -diff
		}
		// 90 kHz granularity is about 11.1 us, so we allow one tick of slack.
		// This only holds for times within one transport wrap of the epoch,
		// which is what TestTransportWrap covers separately.
		if diff > 12*time.Microsecond {
			t.Fatalf("round trip error %v (ts=%d, want %v)", diff, ts, t0)
		}
	}
}

func TestTransportWrap(t *testing.T) {
	// 2^32 ticks at 90 kHz is 47721.86 s, about 13.26 h. The period is expressed
	// in seconds rather than as (1<<32) ticks times the clock rate, because that
	// product would overflow the int64 the compiler needs for the intermediate.
	wrap := time.Duration(1<<32) * time.Second / time.Duration(Timescale)
	far := time.Unix(0, 0).UTC().Add(4 * hour32)

	// The conversion must be a stable reduction modulo 2^32: one wrap later the
	// transport timestamp is unchanged. The wrap truncates to nanoseconds, so
	// at most one tick of drift is expected.
	a := ToTransport(far)
	b := ToTransport(far.Add(wrap))
	diff := int64(a) - int64(b)
	if diff < 0 {
		diff = -diff
	}
	if diff > 1 {
		t.Fatalf("wrap not periodic: off by %d ticks, want 0", diff)
	}
}

func TestTransportHz(t *testing.T) {
	// A second at 44100 Hz must land exactly on 44100 ticks, and one second at
	// 90000 Hz must land exactly on 90000 ticks. Integer arithmetic should not
	// drift on round numbers.
	one := time.Unix(0, 0).UTC().Add(time.Second)
	if got, want := ToTransportHz(one, 44100), uint32(44100); got != want {
		t.Fatalf("44100 Hz: got %d, want %d", got, want)
	}
	if got, want := ToTransport(one), uint32(90000); got != want {
		t.Fatalf("90000 Hz: got %d, want %d", got, want)
	}
	if FromTransportHz(44100, 44100).Sub(one) != 0 {
		t.Fatal("44100 Hz inverse off")
	}
	if FromTransport(90000).Sub(one) != 0 {
		t.Fatal("90000 Hz inverse off")
	}
}

func TestDurationToTransport(t *testing.T) {
	if got, want := DurationToTransport(10*time.Millisecond), uint32(900); got != want {
		t.Fatalf("got %d, want %d", got, want)
	}
	if got := DurationToTransport(-time.Second); got != 0 {
		t.Fatalf("negative duration gave %d", got)
	}
	if got := DurationToTransport(0); got != 0 {
		t.Fatalf("zero duration gave %d", got)
	}
}

func TestAnnexBRoundTrip(t *testing.T) {
	for name, payload := range map[string][]byte{"spspps": testSPSPPS(), "idr": testIDR()} {
		b := AnnexB(payload)
		back := ParseAnnexB(b)
		if !bytes.Equal(back, payload) {
			t.Errorf("%s: annex-b round trip mismatch\n got %x\nwant %x", name, back, payload)
		}
	}
}

func TestAnnexBEmpty(t *testing.T) {
	if b := AnnexB(nil); b != nil {
		t.Fatalf("AnnexB(nil) = %x, want nil", b)
	}
}
