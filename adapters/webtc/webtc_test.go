// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package webtc

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yejinlei/quickmedia/container/h264"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// --- SPS scanning ----------------------------------------------------------

// Parameter sets captured from libx264 (ffmpeg 8.0.1) encoding a 128x96
// 25 fps yuv420p source at level 3.0, each as one Annex-B NAL. They are
// verified by ffprobe frame order, not by the flags they were asked for:
//
//     ffmpeg -f lavfi -i testsrc=size=128x96:rate=25 -t 2 -pix_fmt yuv420p \
//            -profile:v baseline -bf 0      -c:v libx264 -level:v 30 \
//            -g 250 -keyint_min 250 -sc_threshold 0 ...
//
// The parser is tested against what an encoder actually emits rather than a
// hand-assembled parameter set, because emulation prevention only shows up in
// the former and a parser that ignores it looks perfectly fine on hand-built
// bytes.
//
// Baseline is I P P P ... — no B frames, which is what a browser needs. Main
// and high are I B B B P ... at -bf 3, which declares a second reference
// list and is the case the refusal exists for. High also exercises the chroma
// branch, which is where a reader falls out of alignment and every later
// field reads from the wrong bit.
var (
	baselineSPS = []byte{
		0x67, 0x42, 0xc0, 0x1e, 0xd9, 0x02, 0x0d, 0xb0, 0x11, 0x00, 0x00,
		0x03, 0x00, 0x01, 0x00, 0x00, 0x03, 0x00, 0x32, 0x0f, 0x16, 0x2e, 0x48,
	}
	baselinePPS = []byte{0x68, 0xcb, 0x83, 0xcb, 0x20}

	mainBFSPS = []byte{
		0x67, 0x4d, 0x40, 0x1e, 0xec, 0xa1, 0x06, 0xd8, 0x08, 0x80, 0x00, 0x00,
		0x03, 0x00, 0x80, 0x00, 0x00, 0x19, 0x07, 0x8b, 0x16, 0xcb,
	}
	mainBFPPS = []byte{0x68, 0xeb, 0xe3, 0xcb, 0x20}

	highBFSPS = []byte{
		0x67, 0x64, 0x00, 0x1e, 0xac, 0xd9, 0x42, 0x0d, 0xb0, 0x11, 0x00, 0x00,
		0x03, 0x00, 0x01, 0x00, 0x00, 0x03, 0x00, 0x32, 0x0f, 0x16, 0x2d, 0x96,
	}
)

// TestSPSScanRealEncoders asserts the parser reads a real SPS correctly, for
// each of the three profiles a browser is likely to meet.
func TestSPSScanRealEncoders(t *testing.T) {
	for _, c := range []struct {
		name    string
		sps     []byte
		bframes bool
	}{
		{"baseline, no bframes", baselineSPS, false},
		{"main, bframes", mainBFSPS, true},
		{"high, bframes", highBFSPS, true},
	} {
		got, err := h264HasBframes(c.sps)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.bframes {
			t.Fatalf("%s: bframes=%v, want %v", c.name, got, c.bframes)
		}
	}
}

// TestSPSScanBareBody asserts the headerless form is read too, which is the
// form the track parameters use after RTMP or RTSP has stripped the NAL
// header.
func TestSPSScanBareBody(t *testing.T) {
	for _, c := range []struct {
		name    string
		sps     []byte
		bframes bool
	}{
		{"baseline bare", baselineSPS[1:], false},
		{"main bare", mainBFSPS[1:], true},
		{"high bare", highBFSPS[1:], true},
	} {
		got, err := h264HasBframes(c.sps)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got != c.bframes {
			t.Fatalf("%s: bframes=%v, want %v", c.name, got, c.bframes)
		}
	}
}

// TestSPSScanEmulationPrevention pins the decapsulation against a stream that
// needs it: three consecutive zero bytes in the RBSP, which is what the
// scaling list section above produces. A rule that strips the wrong zero
// byte drops a data byte and shifts every bit position after it, so the
// profile and level are the first things that go wrong.
func TestSPSScanEmulationPrevention(t *testing.T) {
	rbsp := append([]byte(nil), baselineSPS[1:]...)
	out := emulationPrevention(rbsp)
	// Two EP patterns are present: 00 00 03 appears twice in the body.
	if len(out) != len(rbsp)-2 {
		t.Fatalf("removed %d bytes, want 2: %x", len(rbsp)-len(out), out)
	}

	r := &bitReader{data: out}
	prof, ok := r.bits(8)
	if !ok || prof != 0x42 {
		t.Fatalf("profile %x ok=%v, want 0x42", prof, ok)
	}
	if !skip(r, 8) {
		t.Fatal("constraint flags")
	}
	lev, ok := r.bits(8)
	if !ok || lev != 0x1e {
		t.Fatalf("level 0x%x ok=%v, want 0x1e", lev, ok)
	}
	id, ok := ue(r)
	if !ok || id != 0 {
		t.Fatalf("sps id %d ok=%v, want 0", id, ok)
	}
}

// TestSPSScanRefusesUnreadable asserts a parse failure is a refusal, not a
// silent approval. The alternative is accepting a stream this tree will not
// play and only finding out from the missing pictures.
func TestSPSScanRefusesUnreadable(t *testing.T) {
	for _, sps := range [][]byte{
		nil,
		{0x67},
		{0x00, 0x00, 0x01, 0x02, 0x03},
		{0x42, 0xc0},
		{0x67, 0x42},
		{0x00, 0x00},
	} {
		if _, err := h264HasBframes(sps); err == nil {
			t.Fatalf("sps %x was accepted", sps)
		}
	}
}

// --- fmtp ------------------------------------------------------------------

func TestH264Fmtp(t *testing.T) {
	for _, c := range []struct {
		name string
		sps  []byte
		pps  []byte
	}{
		{"synthetic fixture", h264.SyntheticSPS(), h264.SyntheticPPS()},
		{"encoder output", baselineSPS, baselinePPS},
	} {
		got := parseH264Fmtp("sps=" + hex.EncodeToString(c.sps) + ";pps=" + hex.EncodeToString(c.pps))
		if !got.ok || !bytesEq(got.sps, c.sps) || !bytesEq(got.pps, c.pps) {
			t.Fatalf("%s: fmtp ok=%v sps=%x pps=%x", c.name, got.ok, got.sps, got.pps)
		}
	}
	if parseH264Fmtp("").ok {
		t.Fatal("empty fmtp must not parse")
	}
	if parseH264Fmtp("sps=" + hex.EncodeToString(baselineSPS)).ok {
		t.Fatal("sps without pps must be refused")
	}
	if parseH264Fmtp("sps=not-hex;pps=alsobad").ok {
		t.Fatal("non-hex params must be refused")
	}
}

func TestH264ProfileID(t *testing.T) {
	cases := []struct {
		sps  []byte
		want string
		ok   bool
	}{
		{baselineSPS, "42C01E", true},
		{baselineSPS[1:], "42C01E", true},
		{mainBFSPS, "4D401E", true},
		{highBFSPS, "64001E", true},
		{h264.SyntheticSPS(), "42C00A", true},
		// profile_idc 0xc0 is not a valid H.264 profile, so the id must not
		// be reported. This is what a truncated or misaligned stream looks
		// like when only the first byte is shifted, and the parser should
		// not launder it into a valid profile id.
		{[]byte{0x67, 0xc0, 0x00, 0x0a, 0x00}, "", false},
	}
	for _, c := range cases {
		got, ok := h264ProfileID(c.sps)
		if ok != c.ok || got != c.want {
			t.Fatalf("sps %x: got %q ok=%v, want %q ok=%v", c.sps, got, ok, c.want, c.ok)
		}
	}
}

func bytesEq(a, b []byte) bool {
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

// --- offer parsing ---------------------------------------------------------

func h264SDP(extra string) []byte {
	sps := hex.EncodeToString(baselineSPS)
	pps := hex.EncodeToString(baselinePPS)
	return []byte("v=0\r\n" +
		"o=- 123 1 IN IP4 127.0.0.1\r\n" +
		"s=-\r\nt=0 0\r\n" +
		"a=group:BUNDLE 0 1\r\n" +
		"m=video 9 UDP/TLS/RTP/SAVPF 96\r\n" +
		"a=rtpmap:96 H264/90000\r\n" +
		"a=fmtp:96 packetization-mode=1;profile-level-id=42c01e;sps=" + sps + ";pps=" + pps + "\r\n" +
		"a=sendonly\r\n" +
		"a=mid:0\r\n" + extra)
}

func bframeSDP(extra string) []byte {
	return []byte("v=0\r\n" +
		"o=- 123 1 IN IP4 127.0.0.1\r\n" +
		"s=-\r\nt=0 0\r\n" +
		"m=video 9 UDP/TLS/RTP/SAVPF 96\r\n" +
		"a=rtpmap:96 H264/90000\r\n" +
		"a=fmtp:96 profile-level-id=4d401e;sps=" + hex.EncodeToString(mainBFSPS) + ";pps=" + hex.EncodeToString(mainBFPPS) + "\r\n" +
		"a=sendonly\r\n" +
		"a=mid:0\r\n" + extra)
}

func TestSdpTracksH264Opus(t *testing.T) {
	offer := h264SDP("m=audio 9 UDP/TLS/RTP/SAVPF 97\r\n" +
		"a=rtpmap:97 opus/48000/2\r\n" +
		"a=fmtp:97 minptime=10;useinbandfec=1\r\n" +
		"a=sendonly\r\n" +
		"a=mid:1\r\n")
	trks, err := sdpTracks(offer)
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	if len(trks) != 2 {
		t.Fatalf("tracks: %d", len(trks))
	}
	if trks[0].Codec != codecH264 || trks[1].Codec != codecOpus {
		t.Fatalf("track table %v", trks)
	}
}

// TestSdpTracksRefusesBframes is the browser rule end to end: the offer is
// refused, not accepted and played badly.
func TestSdpTracksRefusesBframes(t *testing.T) {
	if _, err := sdpTracks(bframeSDP("")); err == nil {
		t.Fatal("b-frame offer was accepted")
	}
}

func TestSdpTracksRefusesH265(t *testing.T) {
	b := []byte("v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nt=0 0\r\n" +
		"m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=rtpmap:96 H265/90000\r\n" +
		"a=fmtp:96 level-id=93\r\na=sendonly\r\na=mid:0\r\n")
	if _, err := sdpTracks(b); err == nil {
		t.Fatal("h265 offer was accepted")
	}
}

func TestSdpTracksRefusesEmpty(t *testing.T) {
	if _, err := sdpTracks(nil); err == nil {
		t.Fatal("empty offer was accepted")
	}
}

// --- outbound track table --------------------------------------------------

func TestBuildOutboundRefusesBframes(t *testing.T) {
	if _, err := buildOutbound([]*stream.Track{{
		Codec: codecH264, Kind: stream.KindVideo, Timescale: h264Rate,
		Params: map[string]string{
			paramSPS: hex.EncodeToString(mainBFSPS),
			paramPPS: hex.EncodeToString(mainBFPPS),
		},
	}}); err == nil {
		t.Fatal("a b-frame path was made playable")
	}
}

func TestBuildOutboundRefusesUnknownCodec(t *testing.T) {
	if _, err := buildOutbound([]*stream.Track{{Codec: "av1", Kind: stream.KindVideo}}); err == nil {
		t.Fatal("an unknown codec was made playable")
	}
}

// --- server endpoints ------------------------------------------------------

func TestServerWHEPRefusesWithoutPath(t *testing.T) {
	s := NewServer(nil, Options{})
	mux := http.NewServeMux()
	s.Handle(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/whep", nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("code %d", w.Code)
	}
}

func TestServerWhipRequiresAuth(t *testing.T) {
	s := NewServer(nil, Options{AuthToken: "secret"})
	mux := http.NewServeMux()
	s.Handle(mux)

	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/whip?path=a", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauth code %d", w.Code)
	}

	r := httptest.NewRequest(http.MethodPost, "/whip?path=a", nil)
	r.Header.Set("Authorization", "Bearer secret")
	w2 := httptest.NewRecorder()
	mux.ServeHTTP(w2, r)
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("authed code %d", w2.Code)
	}
}

func TestServerCountAndClose(t *testing.T) {
	s := NewServer(nil, Options{})
	if p, pl := s.Count(); p != 0 || pl != 0 {
		t.Fatalf("count %d %d", p, pl)
	}
	s.Close()
	s.Close()
}

func TestCredentialRoundTrip(t *testing.T) {
	s := NewServer(nil, Options{})
	id := s.nextID("publish")
	tok := s.credential(id)
	got, ok := s.sessionOf(tok)
	if !ok || got != id {
		t.Fatalf("credential resolved to %q ok=%v", got, ok)
	}
	if _, ok := s.sessionOf("not-a-token"); ok {
		t.Fatal("bogus token resolved")
	}
}
