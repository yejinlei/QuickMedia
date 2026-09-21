// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package opus

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

func selectModule(t *testing.T) *module {
	t.Helper()
	m, err := registry.SelectCodec(CodecID)
	if err != nil {
		t.Fatalf("module %q is not registered: %v", CodecID, err)
	}
	return m.(*module)
}

// unitOf builds one audio unit at the given PTS.
func unitOf(payload []byte, pts time.Time) *stream.Unit {
	return stream.NewUnit(&stream.Unit{
		Codec: CodecID, Kind: stream.KindAudio, Payload: payload,
		PTS: pts, DTS: pts, Duration: 20 * time.Millisecond,
	})
}

// --- module registration ---------------------------------------------------

func TestModuleRegistered(t *testing.T) {
	m := selectModule(t)
	if m.ID() != CodecID {
		t.Errorf("ID = %s, want %s", m.ID(), CodecID)
	}
	if m.Kind() != stream.KindAudio {
		t.Errorf("Kind = %s, want audio", m.Kind())
	}
	if m.Timescale() != opusTimescale {
		t.Errorf("Timescale = %d, want %d", m.Timescale(), opusTimescale)
	}
	if len(m.Formats()) != 1 || m.Formats()[0] != registry.FormatOpus {
		t.Errorf("Formats = %v", m.Formats())
	}
	if !m.SupportsFormat(registry.FormatOpus) {
		t.Errorf("SupportsFormat(opus) = false")
	}
	if m.SupportsFormat(registry.FormatFLV) {
		t.Errorf("SupportsFormat(flv) = true, want false")
	}

	if _, err := m.NewContainerPacker(registry.FormatFLV); err == nil {
		t.Errorf("NewContainerPacker(flv) accepted an unsupported format")
	}
	if _, err := m.NewContainerUnpacker(registry.FormatFLV); err == nil {
		t.Errorf("NewContainerUnpacker(flv) accepted an unsupported format")
	}

	cp, err := m.NewContainerPacker(registry.FormatOpus)
	if err != nil {
		t.Fatalf("NewContainerPacker(opus): %v", err)
	}
	if cp.Format() != registry.FormatOpus {
		t.Errorf("Format = %s", cp.Format())
	}
	up, err := m.NewContainerUnpacker(registry.FormatOpus)
	if err != nil {
		t.Fatalf("NewContainerUnpacker(opus): %v", err)
	}
	if up.Format() != registry.FormatOpus {
		t.Errorf("Format = %s", up.Format())
	}
}

func TestSetParamsClones(t *testing.T) {
	m := selectModule(t)
	in := map[string]string{"a": "1"}
	m.SetRTPParams(in)
	in["a"] = "changed"
	if got := m.RTPParams()["a"]; got != "1" {
		t.Errorf("RTPParams was aliased: %q", got)
	}
	m.SetContainerParams(map[string]string{"b": "2"})
	if got := m.ContainerParams()["b"]; got != "2" {
		t.Errorf("ContainerParams = %v", m.ContainerParams())
	}
}

// --- RTP pack/unpack (RFC 6716) --------------------------------------------

func TestRTPSinglePacket(t *testing.T) {
	m := selectModule(t)
	p, _ := m.NewRTPPacker()
	u := unitOf([]byte("abcdef"), time.Time{})

	packs, seq, ts := p.Pack(u, 10, 960)
	if len(packs) != 1 {
		t.Fatalf("packs = %d, want 1", len(packs))
	}
	if !bytes.Equal(packs[0], []byte("abcdef")) {
		t.Errorf("payload = %q", packs[0])
	}
	if seq != 11 || ts != 960 {
		t.Errorf("seq/ts = %d/%d, want 11/960", seq, ts)
	}
}

func TestRTPEmptyPayload(t *testing.T) {
	m := selectModule(t)
	p, _ := m.NewRTPPacker()
	packs, seq, ts := p.Pack(unitOf(nil, time.Time{}), 7, 5)
	if packs != nil {
		t.Errorf("packs = %v, want nil", packs)
	}
	if seq != 7 || ts != 5 {
		t.Errorf("seq/ts = %d/%d, want unchanged", seq, ts)
	}
}

func TestRTPExactPayloadSize(t *testing.T) {
	m := selectModule(t)
	p, _ := m.NewRTPPacker()
	payload := bytes.Repeat([]byte{0xab}, maxPayload)
	u := unitOf(payload, time.Time{})

	packs, seq, _ := p.Pack(u, 0, 0)
	if len(packs) != 1 {
		t.Fatalf("packs = %d, want 1 for a payload exactly at the limit", len(packs))
	}
	if len(packs[0]) != maxPayload {
		t.Errorf("len = %d, want %d", len(packs[0]), maxPayload)
	}
	if seq != 1 {
		t.Errorf("seq = %d, want 1", seq)
	}
}

func TestRTPSplitAcrossTwoPackets(t *testing.T) {
	m := selectModule(t)
	p, _ := m.NewRTPPacker()
	// Two payloads exactly at the limit would split 50/50, so offset the size to
	// exercise the uneven case: the first fragment is shorter and the second
	// carries the remainder. Still inside the two-packet branch.
	payload := bytes.Repeat([]byte{0xcd}, maxPayload*2-3)
	u := unitOf(payload, time.Time{})

	packs, seq, _ := p.Pack(u, 100, 0)
	if len(packs) != 2 {
		t.Fatalf("packs = %d, want 2", len(packs))
	}
	var joined []byte
	for _, pk := range packs {
		if len(pk) > maxPayload {
			t.Errorf("packet %d exceeds maxPayload: %d", len(pk), maxPayload)
		}
		joined = append(joined, pk...)
	}
	if !bytes.Equal(joined, payload) {
		t.Errorf("rejoined payload differs (got %d bytes, want %d)", len(joined), len(payload))
	}
	if seq != 102 {
		t.Errorf("seq = %d, want 102", seq)
	}
}

func TestRTPLargeFrame(t *testing.T) {
	m := selectModule(t)
	p, _ := m.NewRTPPacker()
	payload := bytes.Repeat([]byte{0xef}, maxPayload*5+77)
	u := unitOf(payload, time.Time{})

	packs, seq, _ := p.Pack(u, 1000, 0)
	if len(packs) != 6 {
		t.Fatalf("packs = %d, want 6", len(packs))
	}
	var joined []byte
	for _, pk := range packs {
		if len(pk) > maxPayload {
			t.Errorf("packet %d exceeds maxPayload: %d", len(pk), maxPayload)
		}
		joined = append(joined, pk...)
	}
	if !bytes.Equal(joined, payload) {
		t.Errorf("rejoined payload differs")
	}
	if seq != 1006 {
		t.Errorf("seq = %d, want 1006", seq)
	}
}

func TestRTPPackDoesNotAliasInput(t *testing.T) {
	m := selectModule(t)
	p, _ := m.NewRTPPacker()
	payload := []byte{1, 2, 3}
	u := unitOf(payload, time.Time{})
	packs, _, _ := p.Pack(u, 0, 0)
	payload[0] = 99
	if packs[0][0] == 99 {
		t.Errorf("packer aliased the input buffer")
	}
}

func TestRTPUnpackAccumulatesUntilMarker(t *testing.T) {
	// RFC 6716 has no length prefix and no configuration subheader, so the
	// marker bit is the only frame delimiter. A frame can therefore be several
	// packets, and the marker set on the last one is what closes it.
	m := selectModule(t)
	up, _ := m.NewRTPUnpacker()

	// A frame spanning two packets must not be emitted until the marker arrives,
	// and must come out reassembled rather than as two half-frames.
	if u, err := up.Unpack([]byte("abc"), 0, 0, false); err != nil {
		t.Fatalf("Unpack: %v", err)
	} else if u != nil {
		t.Fatalf("a non-marker packet produced a frame: %q", u.Payload)
	}
	u, err := up.Unpack([]byte("def"), 1, 0, true)
	if err != nil {
		t.Fatalf("Unpack: %v", err)
	}
	if string(u.Payload) != "abcdef" {
		t.Errorf("reassembled = %q, want %q", u.Payload, "abcdef")
	}
	if upu := up.(*rtpUnpacker); len(upu.buf) != 0 {
		t.Errorf("unpacker held %d bytes after the frame completed", len(upu.buf))
	}

	// A single-packet frame is the marker case alone.
	if u, err := up.Unpack([]byte("ghi"), 2, 0, true); err != nil || string(u.Payload) != "ghi" {
		t.Errorf("single-packet frame: %q %v", u.Payload, err)
	}
}

func TestRTPUnpackEmpty(t *testing.T) {
	m := selectModule(t)
	up, _ := m.NewRTPUnpacker()
	if _, err := up.Unpack(nil, 0, 0, true); err == nil {
		t.Errorf("empty payload was accepted")
	}
}

func TestRTPRoundTrip(t *testing.T) {
	m := selectModule(t)
	p, _ := m.NewRTPPacker()
	up, _ := m.NewRTPUnpacker()

	payload := bytes.Repeat([]byte{0x42}, 3000)
	u := unitOf(payload, time.Time{})
	ts := uint32(container.ToTransportHz(time.Unix(1000, 0), opusTimescale))

	packs, _, usedTS := p.Pack(u, 0, ts)
	if usedTS != ts {
		t.Fatalf("Pack changed the timestamp")
	}
	for i, pk := range packs {
		_, err := up.Unpack(pk, uint16(i), usedTS, i == len(packs)-1)
		if err != nil {
			t.Fatalf("Unpack(%d): %v", i, err)
		}
	}
	if upu, ok := up.(*rtpUnpacker); !ok {
		t.Fatal("unpacker is not a *rtpUnpacker")
	} else if len(upu.buf) != 0 {
		t.Errorf("unpacker held %d bytes after the frame completed", len(upu.buf))
	}
}

// --- OpusHead / OpusTags (RFC 7845) ----------------------------------------

// ffmpeg writes this exact header: magic, version, channels, pre_skip,
// input_sample_rate, output_gain, mapping_family, no vendor. The comparison is
// byte exact rather than field by field so that an off-by-one in the fixed part
// shows up.
func TestOpusHeadMatchesFFmpegLayout(t *testing.T) {
	got := OpusHead(nil, 312, 2, 48000)
	want := []byte("OpusHead")
	want = append(want, 0x01, 0x02) // version, channel count
	want = append(want, 0x38, 0x01) // pre_skip 312 LE
	want = append(want, 0x80, 0xbb, 0x00, 0x00) // 48000 LE
	want = append(want, 0x00, 0x00)             // output gain
	want = append(want, 0x00)                   // mapping family
	if !bytes.Equal(got, want) {
		t.Errorf("OpusHead = % x\nwant     % x", got, want)
	}
}

func TestOpusHeadRoundTrip(t *testing.T) {
	head := OpusHead([]byte("QuickMedia"), opusDefaultPreSkip, 6, 44100)
	v, ch, skip, rate, ok := ParseOpusHead(head)
	if !ok {
		t.Fatalf("ParseOpusHead failed on its own output: % x", head)
	}
	if v != 1 || ch != 6 || skip != opusDefaultPreSkip || rate != 44100 {
		t.Errorf("got v=%d ch=%d skip=%d rate=%d", v, ch, skip, rate)
	}
}

func TestOpusHeadRejects(t *testing.T) {
	if _, _, _, _, ok := ParseOpusHead(nil); ok {
		t.Errorf("nil accepted")
	}
	if _, _, _, _, ok := ParseOpusHead([]byte("OpusHead\x02")); ok {
		t.Errorf("wrong version accepted")
	}
	bad := OpusHead(nil, 0, 2, 48000)
	bad[8] = 0x99
	if _, _, _, _, ok := ParseOpusHead(bad); ok {
		t.Errorf("corrupted version accepted")
	}
	if _, _, _, _, ok := ParseOpusHead([]byte("NotOpusHead")); ok {
		t.Errorf("wrong magic accepted")
	}
	if _, _, _, _, ok := ParseOpusHead([]byte("OpusHe")); ok {
		t.Errorf("truncated header accepted")
	}
}

func TestOpusHeadZeroArgs(t *testing.T) {
	if OpusHead(nil, 0, 0, 48000) != nil {
		t.Errorf("zero channels produced a header")
	}
	if OpusHead(nil, 0, 2, 0) != nil {
		t.Errorf("zero sample rate produced a header")
	}
}

// The ffmpeg reference for the identifier packet is:
//
//	OpusTags | 0c000000 | "Lavf62.3.100" | 01000000 | 1d000000 |
//	"encoder=Lavc62.11.100 libopus"
//
// 61 bytes total, which is the lacing value of the page it lands on. The byte
// exact check is what catches the layout mistakes: comments are length prefixed
// rather than terminated, they sit after the body rather than inside it, there
// is no vendor field, and body_length covers ident only. A comment folded into
// the body parses without erroring but drops every tag.
func TestOpusTagsMatchesFFmpegLayout(t *testing.T) {
	got := OpusTags("Lavf62.3.100", []string{"encoder=Lavc62.11.100 libopus"})
	want := []byte("OpusTags")
	want = append(want, 0x0c, 0x00, 0x00, 0x00)
	want = append(want, []byte("Lavf62.3.100")...)
	want = append(want, 0x01, 0x00, 0x00, 0x00)
	want = append(want, 0x1d, 0x00, 0x00, 0x00)
	want = append(want, []byte("encoder=Lavc62.11.100 libopus")...)
	if len(got) != 61 {
		t.Errorf("len = %d, want 61", len(got))
	}
	if !bytes.Equal(got, want) {
		t.Errorf("OpusTags = % x\nwant     % x", got, want)
	}
}

func TestOpusTagsRoundTrip(t *testing.T) {
	tags := OpusTags("QuickMedia", []string{"title=live", "version=0.2.0"})
	ident, comments, ok := ParseOpusTags(tags)
	if !ok {
		t.Fatalf("ParseOpusTags failed on its own output: % x", tags)
	}
	if ident != "QuickMedia" {
		t.Errorf("ident = %q", ident)
	}
	if len(comments) != 2 || comments[0] != "title=live" || comments[1] != "version=0.2.0" {
		t.Errorf("comments = %v", comments)
	}
}

func TestOpusTagsNoComments(t *testing.T) {
	tags := OpusTags("solo", nil)
	ident, comments, ok := ParseOpusTags(tags)
	if !ok {
		t.Fatalf("ParseOpusTags failed: % x", tags)
	}
	if ident != "solo" || comments != nil {
		t.Errorf("got %q %v", ident, comments)
	}
}

func TestOpusTagsNoIdent(t *testing.T) {
	// ident may be absent: body_length is allowed to be zero.
	tags := OpusTags("", []string{"a=1"})
	ident, comments, ok := ParseOpusTags(tags)
	if !ok {
		t.Fatalf("ParseOpusTags failed: % x", tags)
	}
	if ident != "" || len(comments) != 1 || comments[0] != "a=1" {
		t.Errorf("got %q %v", ident, comments)
	}
}

func TestParseOpusTagsRejectsCorruption(t *testing.T) {
	good := OpusTags("QuickMedia", []string{"a=1", "b=2"})

	// body_length too large: ident would overrun the comment table.
	tampered := append([]byte(nil), good...)
	tampered[8] = 0xff
	if _, _, ok := ParseOpusTags(tampered); ok {
		t.Errorf("oversized body_length accepted")
	}

	// body_length too small: ident truncates and the rest of the table misreads.
	tampered = append([]byte(nil), good...)
	tampered[8] = 0x01
	if _, _, ok := ParseOpusTags(tampered); ok {
		t.Errorf("undersized body_length accepted")
	}

	// comment_list_entries inconsistent with the table: reading three comments
	// where two were written runs off the end.
	tampered = append([]byte(nil), good...)
	tampered[24] = 0x03
	if _, _, ok := ParseOpusTags(tampered); ok {
		t.Errorf("inconsistent comment count accepted")
	}

	// A comment length that overruns the packet.
	tampered = append([]byte(nil), good...)
	tampered[29] = 0xff
	if _, _, ok := ParseOpusTags(tampered); ok {
		t.Errorf("oversized comment length accepted")
	}

	// Trailing garbage is not allowed, since it would make a longer comment
	// table look shorter.
	tampered = append([]byte(nil), good...)
	tampered = append(tampered, 0x00)
	if _, _, ok := ParseOpusTags(tampered); ok {
		t.Errorf("trailing garbage accepted")
	}

	if _, _, ok := ParseOpusTags([]byte("OpusTags\x00\x00")); ok {
		t.Errorf("short packet accepted")
	}
	if _, _, ok := ParseOpusTags([]byte("NotOggst")); ok {
		t.Errorf("wrong magic accepted")
	}
}

// --- Ogg pages -------------------------------------------------------------

// The ffmpeg reference lays a 19 byte OpusHead down as a single lacing entry of
// 0x13. Writing 255 for a short packet adds a segment the format does not want,
// so the lacing is asserted rather than just the page.
func TestLacingIsTrueSize(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	frames := []OggFrame{
		{Payload: bytes.Repeat([]byte{1}, 19), PTS: start, Duration: 20 * time.Millisecond},
	}
	pages := WritePages(7, 0, start, OpusHead(nil, 312, 2, 48000), true, frames)
	if len(pages) != 1 {
		t.Fatalf("pages = %d, want 1", len(pages))
	}
	lacing := pages[0].Data[pageHeaderSize : pageHeaderSize+int(pages[0].Data[26])]
	if !bytes.Equal(lacing, []byte{0x13, 0x13}) {
		t.Errorf("lacing = % x, want 13 13", lacing)
	}
}

// Lacing is one entry per segment, not per byte block. A segment shorter than
// 255 closes the packet and carries its true size; a full 255 byte segment
// means the packet continues. ffmpeg writes ff ff ff 08 for a 773 byte frame
// and ff 01 for a 256 byte one. The comparison is byte exact rather than a
// count, so a 255 split that omits its trailing zero shows up.
func TestLacingSplitsLargeFrame(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	for _, tc := range []struct {
		size  int
		lace  string
		bytes string
	}{
		{255, "ff 00", "ff 00"},      // exactly one segment needs the closing zero
		{256, "ff 01", "ff 01"},      // one full segment plus one byte
		{773, "ff ff ff 08", "ff ff ff 08"}, // ffmpeg's layout, byte exact
	} {
		pages := WritePages(7, 0, start, nil, false, []OggFrame{
			{Payload: bytes.Repeat([]byte{1}, tc.size), PTS: start, Duration: 20 * time.Millisecond},
		})
		if len(pages) != 1 {
			t.Fatalf("%d byte frame: pages = %d, want 1", tc.size, len(pages))
		}
		n := int(pages[0].Data[26])
		got := fmt.Sprintf("% x", pages[0].Data[pageHeaderSize:pageHeaderSize+n])
		if got != tc.lace {
			t.Errorf("%d byte frame: lacing = %s, want %s", tc.size, got, tc.lace)
		}
		// The payload comes back whole, which is what the lacing has to support.
		pkts, err := ParsePage(pages[0].Data)
		if err != nil {
			t.Fatalf("%d byte frame: ParsePage: %v", tc.size, err)
		}
		if len(pkts) != 1 || len(pkts[0]) != tc.size {
			t.Errorf("%d byte frame: packets = %d, sizes %v",
				tc.size, len(pkts), packetSizes(pkts))
		}
	}
}

// packetSizes reports the length of each packet, for diagnostics.
func packetSizes(pkts [][]byte) []int {
	out := make([]int, len(pkts))
	for i, pk := range pkts {
		out[i] = len(pk)
	}
	return out
}

func TestWritePagesHeaderPackets(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	head := OpusHead(nil, 312, 2, 48000)
	frames := []OggFrame{
		{Payload: []byte{0x01, 0x02}, PTS: start, Duration: 20 * time.Millisecond},
	}
	pages := WritePages(42, 3, start, head, true, frames)
	if len(pages) != 1 {
		t.Fatalf("pages = %d, want 1", len(pages))
	}

	pkts, err := ParsePage(pages[0].Data)
	if err != nil {
		t.Fatalf("ParsePage: %v", err)
	}
	if len(pkts) != 2 {
		t.Fatalf("packets = %d, want 2 (header plus frame)", len(pkts))
	}
	if !bytes.Equal(pkts[0], head) {
		t.Errorf("header packet = % x", pkts[0])
	}
	if !bytes.Equal(pkts[1], []byte{0x01, 0x02}) {
		t.Errorf("frame packet = % x", pkts[1])
	}

	serial, ok := PageSerial(pages[0].Data)
	if !ok || serial != 42 {
		t.Errorf("serial = %d", serial)
	}
	seq, _ := PageSequence(pages[0].Data)
	if seq != 3 {
		t.Errorf("seq = %d, want 3", seq)
	}

	// A single page carries both the beginning and the end of stream, so both
	// bits are set. That is legal and is what ffmpeg produces for a short file.
	ht, _ := PageHeaderType(pages[0].Data)
	if ht != pageBOS|pageEOS {
		t.Errorf("header type = % x, want % x", ht, pageBOS|pageEOS)
	}
}

func TestHeaderTypeBits(t *testing.T) {
	if pageHeaderType(true, false, false) != pageBOS {
		t.Errorf("first page = % x, want % x", pageHeaderType(true, false, false), pageBOS)
	}
	if pageHeaderType(false, true, false) != pageEOS {
		t.Errorf("last page = % x, want % x", pageHeaderType(false, true, false), pageEOS)
	}
	if pageHeaderType(true, true, false) != pageBOS|pageEOS {
		t.Errorf("only page = % x, want % x", pageHeaderType(true, true, false), pageBOS|pageEOS)
	}
	if pageHeaderType(false, false, false) != 0 {
		t.Errorf("middle page = % x, want 0", pageHeaderType(false, false, false))
	}
	// The continuation bit is independent of the stream-boundary bits, so a
	// continuing page that is also the first is legal. Setting BOS on a
	// continuing page is what a truncated stream looks like, which is why the
	// bit is only set for pages after the first.
	if pageHeaderType(false, false, true) != pageContPacket {
		t.Errorf("continuation page = % x, want % x",
			pageHeaderType(false, false, true), pageContPacket)
	}
	// The bits are not consecutive: BOS is 0x02 because 0x01 is already taken
	// by the continuation flag. A wrong assignment is a stream that reads as
	// truncated rather than as wrong, so it is asserted here.
	if pageContPacket != 0x01 || pageBOS != 0x02 || pageEOS != 0x04 {
		t.Errorf("header type bits = %x %x %x", pageContPacket, pageBOS, pageEOS)
	}
}

func TestPageSequenceIncrements(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	var frames []OggFrame
	for i := 0; i < 300; i++ {
		frames = append(frames, OggFrame{
			Payload: []byte{byte(i)}, PTS: start.Add(time.Duration(i) * time.Millisecond),
			Duration: 20 * time.Millisecond,
		})
	}
	pages := WritePages(1, 0, start, nil, true, frames)
	if len(pages) != 2 {
		t.Fatalf("pages = %d, want 2", len(pages))
	}
	for i, pg := range pages {
		seq, _ := PageSequence(pg.Data)
		if seq != uint32(i) {
			t.Errorf("page %d seq = %d", i, seq)
		}
	}
	ht0, _ := PageHeaderType(pages[0].Data)
	if ht0 != pageBOS {
		t.Errorf("first page = % x, want BOS only", ht0)
	}
	ht1, _ := PageHeaderType(pages[1].Data)
	if ht1 != pageEOS {
		t.Errorf("last page = % x, want EOS only", ht1)
	}
}

func TestParsePageRoundTrip(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	payloads := [][]byte{
		bytes.Repeat([]byte{1}, 19),
		bytes.Repeat([]byte{2}, 300),
		bytes.Repeat([]byte{3}, 1000),
	}
	var frames []OggFrame
	for i, pl := range payloads {
		frames = append(frames, OggFrame{Payload: pl, PTS: start.Add(time.Duration(i) * 20 * time.Millisecond), Duration: 20 * time.Millisecond})
	}
	pages := WritePages(9, 0, start, nil, false, frames)

	for _, pg := range pages {
		got, err := ParsePage(pg.Data)
		if err != nil {
			t.Fatalf("ParsePage: %v", err)
		}
		// The frame sizes must come back exactly, which is what splicing gets
		// right or wrong.
		var sizes []int
		for _, pk := range got {
			sizes = append(sizes, len(pk))
		}
		if len(got) != len(payloads) {
			t.Fatalf("packets = %d, want %d", len(got), len(payloads))
		}
		for i, pk := range got {
			if !bytes.Equal(pk, payloads[i]) {
				t.Errorf("packet %d = %d bytes, want %d", i, len(pk), len(payloads[i]))
			}
		}
	}
}

func TestParsePageRejectsBadCRC(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	pages := WritePages(1, 0, start, nil, false, []OggFrame{
		{Payload: []byte{0x10, 0x20}, PTS: start, Duration: 20 * time.Millisecond},
	})
	bad := append([]byte(nil), pages[0].Data...)
	bad[25] ^= 0xff // a payload byte
	if _, err := ParsePage(bad); err == nil {
		t.Errorf("a page with a flipped payload byte was accepted")
	}
	// Flipping a byte inside the CRC field itself is a different corruption and
	// must be caught the same way.
	bad = append([]byte(nil), pages[0].Data...)
	bad[23] ^= 0xff
	if _, err := ParsePage(bad); err == nil {
		t.Errorf("a page with a corrupted CRC was accepted")
	}
}

func TestParsePageRejectsBadStructure(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	pages := WritePages(1, 0, start, nil, false, []OggFrame{
		{Payload: []byte{0x10}, PTS: start, Duration: 20 * time.Millisecond},
	})
	pg := pages[0].Data

	if _, err := ParsePage([]byte("NotS")); err == nil {
		t.Errorf("bad magic accepted")
	}
	if _, err := ParsePage(pg[:pageHeaderSize-1]); err == nil {
		t.Errorf("short page accepted")
	}

	// A page that begins a continued packet is a half frame with no marker, so
	// it is refused rather than returned as a frame.
	c := mutatePage(pg, func(p []byte) { p[5] |= pageContPacket })
	if _, err := ParsePage(c); err == nil {
		t.Errorf("a continued page was accepted")
	}

	// Zero segments is not a page.
	c = mutatePage(pg, func(p []byte) { p[26] = 0 })
	if _, err := ParsePage(c); err == nil {
		t.Errorf("a page with no segments was accepted")
	}

	// A lacing value that overruns the page must fail rather than read past it.
	c = mutatePage(pg, func(p []byte) { p[27] = 0xff })
	if _, err := ParsePage(c); err == nil {
		t.Errorf("a page with an overrunning lacing value was accepted")
	}
}

// mutatePage changes one page and repairs its checksum so the field being
// tested is the only thing the parser sees wrong.
func mutatePage(page []byte, f func([]byte)) []byte {
	c := append([]byte(nil), page...)
	f(c)
	c[22], c[23], c[24], c[25] = 0, 0, 0, 0
	binary.LittleEndian.PutUint32(c[22:26], OggCRC(c))
	return c
}

// --- granules --------------------------------------------------------------

// A granule position names the end of the last frame on the page, so the
// duration must be added. Using the PTS alone puts the granule one frame early
// on every page, which reads as audio that plays slightly fast.
func TestGranuleIsEndOfFrame(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	frames := []OggFrame{
		{Payload: []byte{1}, PTS: start.Add(100 * time.Millisecond), Duration: 20 * time.Millisecond},
	}
	pages := WritePages(1, 0, start, nil, false, frames)
	if len(pages) != 1 {
		t.Fatalf("pages = %d", len(pages))
	}
	g, ok := PageGranule(pages[0].Data)
	if !ok {
		t.Fatal("no granule")
	}
	want := uint64(opusTimescale * 120 / 1000)
	if g != want {
		t.Errorf("granule = %d, want %d (100 ms PTS plus 20 ms duration)", g, want)
	}
}

func TestGranuleClampsBelowStart(t *testing.T) {
	start := time.Unix(1000, 0).UTC()
	g := granuleOf(start, start.Add(-time.Second), 10*time.Millisecond)
	if g != 0 {
		t.Errorf("granule = %d, want 0 for a frame before the start", g)
	}
}

func TestGranuleZeroTimes(t *testing.T) {
	if granuleOf(time.Time{}, time.Unix(1, 0), time.Second) != 0 {
		t.Errorf("zero start produced a granule")
	}
	if granuleOf(time.Unix(1, 0), time.Time{}, time.Second) != 0 {
		t.Errorf("zero PTS produced a granule")
	}
}

func TestGranuleWrapsWithoutOverflow(t *testing.T) {
	// A frame whose PTS precedes the stream start by almost a full timeline
	// wrap is the case a uint32 subtraction gets wrong: it reports a granule
	// near 2^32 and the negative guard cannot fire. The result here must be 0.
	start := time.Unix(100000, 0).UTC()
	pts := time.Unix(0, 0).UTC()
	if got := granuleOf(start, pts, time.Second); got != 0 {
		t.Errorf("granule = %d, want 0", got)
	}
}

// --- the container packer ---------------------------------------------------

func TestOggPackerConfigFrames(t *testing.T) {
	m := selectModule(t)
	cp, _ := m.NewContainerPacker(registry.FormatOpus)
	cf := cp.ConfigFrames()
	if len(cf) != 2 {
		t.Fatalf("config frames = %d, want 2", len(cf))
	}
	if !bytes.Equal(cf[0].Data, OpusHead([]byte("QuickMedia"), opusDefaultPreSkip, 2, opusTimescale)) {
		t.Errorf("frame 0 is not an OpusHead")
	}
	if _, _, _, _, ok := ParseOpusHead(cf[0].Data); !ok {
		t.Errorf("frame 0 does not parse as an OpusHead")
	}
	if _, _, ok := ParseOpusTags(cf[1].Data); !ok {
		t.Errorf("frame 1 does not parse as OpusTags")
	}
	if cp.InitData() != nil {
		t.Errorf("InitData = % x, want nil", cp.InitData())
	}
}

func TestOggPackerRoundTrip(t *testing.T) {
	m := selectModule(t)
	cp, _ := m.NewContainerPacker(registry.FormatOpus)
	up, _ := m.NewContainerUnpacker(registry.FormatOpus)

	start := time.Unix(1000, 0).UTC()
	payloads := [][]byte{
		bytes.Repeat([]byte{0x11}, 19),
		bytes.Repeat([]byte{0x22}, 256),
		bytes.Repeat([]byte{0x33}, 3000),
	}
	var got [][]byte
	for i, pl := range payloads {
		fs, err := cp.Pack(unitOf(pl, start.Add(time.Duration(i)*20*time.Millisecond)))
		if err != nil {
			t.Fatalf("Pack(%d): %v", i, err)
		}
		if len(fs) == 0 {
			t.Fatalf("Pack(%d) produced no frames", i)
		}
		for _, f := range fs {
			units, err := up.Feed(f.Data)
			if err != nil {
				t.Fatalf("Feed: %v", err)
			}
			for _, un := range units {
				got = append(got, un.Payload)
				if un.Codec != CodecID {
					t.Errorf("codec = %s", un.Codec)
				}
				if un.Kind != stream.KindAudio {
					t.Errorf("kind = %s", un.Kind)
				}
			}
		}
	}
	if len(got) != len(payloads) {
		t.Fatalf("units = %d, want %d", len(got), len(payloads))
	}
	for i, pl := range got {
		if !bytes.Equal(pl, payloads[i]) {
			t.Errorf("unit %d = %d bytes, want %d", i, len(pl), len(payloads[i]))
		}
	}
}

func TestOggPackerEmptyUnit(t *testing.T) {
	m := selectModule(t)
	cp, _ := m.NewContainerPacker(registry.FormatOpus)
	fs, err := cp.Pack(unitOf(nil, time.Time{}))
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	if fs != nil {
		t.Errorf("frames = %v, want nil", fs)
	}
}

func TestOggUnpackerIgnoresShortInput(t *testing.T) {
	m := selectModule(t)
	up, _ := m.NewContainerUnpacker(registry.FormatOpus)
	units, err := up.Feed([]byte("OggSxxxx"))
	if err != nil {
		t.Fatalf("Feed: %v", err)
	}
	if len(units) != 0 {
		t.Errorf("units = %d", len(units))
	}
}

// --- CRC -------------------------------------------------------------------


func TestOggCRCStoredMatchesRecomputed(t *testing.T) {
	// The CRC is stored where the bytes are zeroed, so a page reads back to the
	// value it was written with. That is the property ParsePage relies on, so
	// the page is produced by the writer rather than assembled by hand.
	start := time.Unix(1000, 0).UTC()
	page := WritePages(3, 7, start, nil, false, []OggFrame{
		{Payload: []byte{0x01, 0x02, 0x03}, PTS: start, Duration: time.Second},
	})[0].Data
	stored := binary.LittleEndian.Uint32(page[22:26])
	if stored == 0 {
		t.Fatal("the page carries no CRC")
	}
	z := append([]byte(nil), page...)
	z[22], z[23], z[24], z[25] = 0, 0, 0, 0
	if got := OggCRC(z); got != stored {
		t.Errorf("stored CRC %08x, recomputed %08x", stored, got)
	}

	// The CRC must actually depend on the payload. A constant function would
	// also pass the comparison above.
	other := WritePages(3, 7, start, nil, false, []OggFrame{
		{Payload: []byte{0x04, 0x05}, PTS: start, Duration: time.Second},
	})[0].Data
	oz := append([]byte(nil), other...)
	oz[22], oz[23], oz[24], oz[25] = 0, 0, 0, 0
	if OggCRC(oz) == stored {
		t.Errorf("the CRC does not depend on the payload")
	}
}
