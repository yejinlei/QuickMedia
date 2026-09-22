// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package opus is the Opus codec module.
//
// It is a registry module, not a library: nothing imports it directly. It
// registers itself under the identifier "opus" and the kernel looks it up by
// codec identifier.
//
// Opus has exactly one standard container, Ogg, so this module packs two
// things: RTP payloads for the WebRTC path, and Ogg header packets and pages
// for the file and streaming paths. The configuration travels in two header
// packets rather than in a sidecar, because Ogg has no other channel for it
// and a decoder that has not received them cannot begin.
package opus

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Version is this module's version.
const Version = "0.2.0"

// CodecID is this codec's identifier.
const CodecID stream.CodecID = "opus"

// opusTimescale is the transport clock rate. It is 48 kHz because Opus always
// works at 48 kHz internally: the encoder converts whatever sampling rate it is
// given on input, so the clock is 48 kHz regardless of source.
const opusTimescale = 48000

// maxPayload is the largest RTP payload this packer emits.
const maxPayload = 1398

// Ogg layer constants, from the Ogg specification and RFC 7845 section 4.
//
// maxSegmentBytes is the size of one Ogg segment. A packet is spread over as
// many segments as it needs, one byte past 255 per segment, and the lacing value
// of 255 marks a segment as continuing. It is a segment size and not a packet
// size: a packet may be much larger than 255 bytes, which is the only reason a
// lacing table ever contains a value of 255.
const maxSegmentBytes = 255

// maxSegmentsPerPage is the segment count limit of one Ogg page.
const maxSegmentsPerPage = 255

// pageHeaderSize is the fixed part of an Ogg page header.
const pageHeaderSize = 27

// Page header type bits. They are not consecutive, and the lowest is the
// continuation flag, so beginning-of-stream is 0x02 rather than 0x01.
const (
	pageContPacket byte = 0x01 // this page continues a packet started on the previous page
	pageBOS        byte = 0x02 // beginning of stream
	pageEOS        byte = 0x04 // end of stream
)

// opusHeadSize is the fixed part of the OpusHead packet.
const opusHeadSize = 19

// opusTagsHeaderSize is the magic plus the body length field of OpusTags.
const opusTagsHeaderSize = 12

// opusDefaultPreSkip is the encoder delay RFC 7845 recommends and ffmpeg
// writes: 80 ms at 48 kHz. The decoder discards that many samples, so omitting
// it makes a player begin 80 ms into the audio.
const opusDefaultPreSkip uint16 = 3840

// Magic strings required by RFC 7845 sections 4.2 and 4.3.
const (
	opusHeadMagic = "OpusHead"
	opusTagsMagic = "OpusTags"
)

// ModuleInfo implements registry.CodecPacker.
func (m *module) ModuleInfo() registry.ModuleInfo {
	return registry.ModuleInfo{
		Name:      "opus",
		Version:   Version,
		Type:      registry.TCodec,
		MinKernel: "0.1.0",
		Priority:  100,
		Dir:       "container/opus",
	}
}

// ID implements registry.CodecPacker.
func (m *module) ID() stream.CodecID { return CodecID }

// Kind implements registry.CodecPacker.
func (m *module) Kind() stream.CodecKind { return stream.KindAudio }

// Timescale implements registry.CodecPacker.
func (m *module) Timescale() uint32 { return opusTimescale }

// RTPParams implements registry.CodecPacker.
func (m *module) RTPParams() map[string]string { return m.rtp }

// SetRTPParams implements registry.CodecPacker.
func (m *module) SetRTPParams(p map[string]string) { m.rtp = cloneMap(p) }

// ContainerParams implements registry.CodecPacker.
func (m *module) ContainerParams() map[string]string { return m.cpar }

// SetContainerParams implements registry.CodecPacker.
func (m *module) SetContainerParams(p map[string]string) { m.cpar = cloneMap(p) }

// SupportsFormat implements registry.CodecPacker.
func (m *module) SupportsFormat(f registry.Format) bool { return f == registry.FormatOpus }

// Formats implements registry.CodecPacker.
func (m *module) Formats() []registry.Format { return []registry.Format{registry.FormatOpus} }

// NewRTPPacker implements registry.CodecPacker.
func (m *module) NewRTPPacker() (registry.RTPPacker, error) { return &rtpPacker{}, nil }

// NewRTPUnpacker implements registry.CodecPacker.
func (m *module) NewRTPUnpacker() (registry.RTPUnpacker, error) { return &rtpUnpacker{}, nil }

// NewContainerPacker implements registry.CodecPacker.
func (m *module) NewContainerPacker(f registry.Format) (registry.ContainerPacker, error) {
	if f != registry.FormatOpus {
		return nil, errors.New("opus: unsupported format " + string(f))
	}
	return newOgg(), nil
}

// NewContainerUnpacker implements registry.CodecPacker.
func (m *module) NewContainerUnpacker(f registry.Format) (registry.ContainerUnpacker, error) {
	if f != registry.FormatOpus {
		return nil, errors.New("opus: unsupported format " + string(f))
	}
	return newOggUnpacker(), nil
}

// module is the Opus codec module.
type module struct {
	rtp  map[string]string
	cpar map[string]string
}

func cloneMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func init() { registry.Register(&module{}) }

// --- RTP (RFC 6716) ---------------------------------------------------------

// rtpPacker implements the RFC 6716 packetization of this codec.
type rtpPacker struct{}

// MaxPayload implements registry.RTPPacker.
func (p *rtpPacker) MaxPayload() int { return maxPayload }

// Pack implements registry.RTPPacker.
//
// Three shapes, in order of preference: one payload per packet, one fragment per
// packet, and one packet per fragment for a frame larger than two payloads.
// This codec has no configuration subheader and no payload type, which is what
// makes each shape a plain copy.
func (p *rtpPacker) Pack(u *stream.Unit, seq uint16, ts uint32) ([][]byte, uint16, uint32) {
	if len(u.Payload) == 0 {
		return nil, seq, ts
	}
	if len(u.Payload) <= maxPayload {
		return [][]byte{append([]byte(nil), u.Payload...)}, seq + 1, ts
	}
	if len(u.Payload) <= 2*maxPayload {
		mid := len(u.Payload) / 2
		return [][]byte{
			append([]byte(nil), u.Payload[:mid]...),
			append([]byte(nil), u.Payload[mid:]...),
		}, seq + 2, ts
	}
	var out [][]byte
	for at := 0; at < len(u.Payload); {
		n := len(u.Payload) - at
		if n > maxPayload {
			n = maxPayload
		}
		out = append(out, append([]byte(nil), u.Payload[at:at+n]...))
		at += n
	}
	return out, seq + uint16(len(out)), ts
}

// rtpUnpacker reassembles RTP payloads back into access units.
type rtpUnpacker struct {
	buf []byte
}

// Unpack implements registry.RTPUnpacker.
//
// The marker bit is the only frame delimiter this codec has: there is no length
// prefix and no configuration subheader. A frame is therefore either one
// packet with the marker set, or several packets with the marker set only on
// the last one. Both shapes accumulate into one frame, so a non-empty buffer
// when the marker arrives is the normal fragmented case and not an error.
//
// The one failure this unpacker cannot detect is a sender that omits the marker
// on a whole frame. That frame merges with the next one and nothing in the
// payload can say the two were separate, so there is no check to write. The
// refusal below covers the case that is detectable, an empty payload, which
// would otherwise record a zero length frame in the stream.
func (u *rtpUnpacker) Unpack(payload []byte, seq uint16, ts uint32, marker bool) (*stream.Unit, error) {
	if len(payload) == 0 {
		return nil, errors.New("opus: empty rtp payload")
	}
	u.buf = append(u.buf, payload...)
	if !marker {
		return nil, nil
	}
	frame := u.buf
	u.buf = nil
	return stream.NewUnit(&stream.Unit{
		Codec: CodecID, Kind: stream.KindAudio,
		Payload: frame, Key: true,
		PTS: container.FromTransportHz(ts, opusTimescale),
		DTS: container.FromTransportHz(ts, opusTimescale),
	}), nil
}

// --- Ogg header packets (RFC 7845) ------------------------------------------

// OpusHead builds the header packet required by RFC 7845 section 4.2.
//
// The fixed part is 19 bytes, all little-endian:
//
//	magic(8) | version(1) | channel_count(1) | pre_skip(2) |
//	input_sample_rate(4) | output_gain(2) | channel_mapping_family(1)
//
// Everything after that is the vendor string, which is unbounded. version is 1
// and channel_mapping_family is 0, the stream-level mapping that every
// deployment uses; anything else needs the coupling table this module does not
// use.
//
// The two fields that carry a mistake into an audibly wrong stream are
// pre_skip and output_gain. pre_skip tells the decoder how many samples to
// discard, so a zero here makes playback start partway through the first frame;
// output_gain is signed, and encoding a negative gain as unsigned reads it as a
// very large positive one, which is a much louder stream rather than a quieter
// one.
func OpusHead(vendor []byte, preSkip uint16, channels uint8, inputSampleRate uint32) []byte {
	if channels == 0 || inputSampleRate == 0 {
		return nil
	}
	out := make([]byte, 0, opusHeadSize+len(vendor))
	out = append(out, []byte(opusHeadMagic)...)
	out = append(out, 0x01)
	out = append(out, channels)
	out = binary.LittleEndian.AppendUint16(out, preSkip)
	out = binary.LittleEndian.AppendUint32(out, inputSampleRate)
	out = binary.LittleEndian.AppendUint16(out, 0x0000) // output gain
	out = append(out, 0x00)                             // channel mapping family
	out = append(out, vendor...)
	return out
}

// ParseOpusHead reads the header fields out of an OpusHead packet.
//
// The fixed part is 19 bytes and its offsets are: magic 0-7, version 8,
// channel count 9, pre_skip 10-11, input sample rate 12-15, output gain 16-17,
// mapping family 18. The length check is against 19 bytes, so a vendor string of
// any length is legal and one shorter is not. Version is checked rather than
// read because there is no other version and accepting it would claim support
// for a layout this module does not understand.
func ParseOpusHead(data []byte) (version, channels uint8, preSkip uint16, sampleRate uint32, ok bool) {
	if len(data) < opusHeadSize || string(data[:8]) != opusHeadMagic {
		return 0, 0, 0, 0, false
	}
	if data[8] != 0x01 {
		return 0, 0, 0, 0, false
	}
	return data[8], data[9],
		binary.LittleEndian.Uint16(data[10:12]),
		binary.LittleEndian.Uint32(data[12:16]),
		true
}

// OpusTags builds the identifier packet required by RFC 7845 section 4.3.
//
// The layout is the part that bites, and it is not the intuitive one. Each user
// comment is length prefixed rather than terminated, the comments come after the
// body rather than inside it, and there is no vendor string:
//
//	magic(8) | body_length(4) | ident | comment_list_entries(4) |
//	user_comment_length(4) per comment | comment ...
//
// body_length covers only ident. Putting the comments inside the body is the
// tempting mistake because Vorbis's comment field is a single blob, and it
// parses without erroring: a reader that trusts body_length reads ident plus the
// first few comment bytes and stops, so the stream looks fine and every tag is
// quietly wrong. The count is likewise a count of comments only, not of
// ident plus comments, because ident is not an entry in the comment list.
//
// The length prefix is little endian, matching the rest of the format. There is
// deliberately no vendor argument, since RFC 7845 drops the vendor field that
// Vorbis had.
func OpusTags(ident string, comments []string) []byte {
	out := make([]byte, 0, opusTagsHeaderSize+len(ident)+4)
	out = append(out, []byte(opusTagsMagic)...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(ident)))
	out = append(out, []byte(ident)...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(comments)))
	for _, c := range comments {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(c)))
		out = append(out, []byte(c)...)
	}
	return out
}

// ParseOpusTags reads the identifier packet.
//
// ident runs to the comment_list_entries field, so body_length is only a bound:
// the read stops at the first complete set of length-prefixed comments, which
// is what lets a reader tolerate trailing garbage. The number of comments
// reported must equal comment_list_entries, because a mismatch means the comment
// table does not parse as the writer intended.
//
// A zero body_length is not rejected: ident is allowed to be absent, which is
// what an encoder that emits tags without an application id produces.
func ParseOpusTags(data []byte) (ident string, comments []string, ok bool) {
	if len(data) < opusTagsHeaderSize {
		return "", nil, false
	}
	if string(data[:8]) != opusTagsMagic {
		return "", nil, false
	}
	bodyLen := int(binary.LittleEndian.Uint32(data[8:12]))
	bEnd := opusTagsHeaderSize + bodyLen
	if bodyLen < 0 || bEnd > len(data) {
		return "", nil, false
	}
	if len(data) < bEnd+4 {
		return "", nil, false
	}
	ident = string(data[opusTagsHeaderSize : bEnd])
	nComments := int(binary.LittleEndian.Uint32(data[bEnd : bEnd+4]))

	// Comment lengths are bounds, so a malformed table cannot run past the
	// buffer: each read checks the length against the bytes that remain before
	// advancing.
	at := bEnd + 4
	for i := 0; i < nComments; i++ {
		if at+4 > len(data) {
			return "", nil, false
		}
		n := int(binary.LittleEndian.Uint32(data[at : at+4]))
		at += 4
		if n < 0 || at+n > len(data) {
			return "", nil, false
		}
		comments = append(comments, string(data[at:at+n]))
		at += n
	}

	// Nothing may follow the comment table. A non-empty tail is either a
	// truncated parse of a longer table or a malformed length, and returning a
	// partial result here would look like a shorter comment list.
	if at != len(data) {
		return "", nil, false
	}
	return ident, comments, true
}

// --- Ogg pages (RFC 3525) ---------------------------------------------------

// OggFrame is one Opus frame about to be packed.
type OggFrame struct {
	// Payload is the encoded frame. A frame becomes one Ogg packet; frames larger
	// than one segment are spread across several segments by the writer.
	Payload []byte
	// PTS is the presentation time.
	PTS time.Time
	// Duration is the frame length. Ogg granule positions name the end of a
	// frame rather than its start, so the duration is what places the granule.
	Duration time.Duration
}

// OggPage is one Ogg page.
type OggPage struct {
	// Data is the assembled page, header and CRC included.
	Data []byte
}

// oggSeg is one Ogg segment, 255 bytes at most.
type oggSeg struct {
	// data is the segment body.
	data []byte
	// last reports whether the segment closes its packet. A segment that is not
	// last takes the lacing value 255, which is how a reader knows the packet
	// continues into the next segment.
	last bool
	// granule is the granule position of the packet this segment belongs to.
	granule uint64
}

// WritePages splits frames into Ogg pages.
//
// serial is the stream identifier, seq the page sequence, and start the PTS of
// the first frame, which is what granule positions are measured from. Header
// packets go on the first page and that page carries the beginning-of-stream
// bit, because a decoder joining anywhere after them needs both to begin.
//
// eos says whether the final page produced here is the end of the stream. The
// caller must decide it rather than the writer, because a packer that is called
// once per frame cannot know that the frame it was given is the last. Setting
// EOS on the header page is the failure this prevents: it marks the stream as
// finished before any audio has been written, and a reader stops there.
//
// A frame is one packet and a packet is spread over as many segments as it
// needs. A segment holds 255 bytes, so a frame of 773 bytes takes four segments
// with the lacing table ff ff ff 08. Splitting a frame into several packets of
// 255 bytes each is not an equivalent encoding: a decoder reads the pieces as
// separate frames and plays them at the wrong rate. The lacing table is the
// only thing that marks where one packet ends, so it must be right.
//
// A page holds no more than 255 segments, so it ends at the last segment that
// closes a packet, or at the segment limit when the packet is larger than a
// page. In the latter case the next page carries the continuation bit, which
// tells a reader that its first segments belong to the packet started on the
// previous page.
func WritePages(serial, seq uint32, start time.Time, head []byte, eos bool, frames []OggFrame) []OggPage {
	// segs spreads one packet over as many segments as it needs. Every segment
	// but the last carries the continuation lacing value; the last carries its
	// true byte count, which is how a reader tells that the packet is complete.
	//
	// The cut is at >= rather than > because lacing value 255 means "this
	// segment is 255 bytes and the packet continues". A packet that is exactly
	// a multiple of 255 would otherwise end on a 255 lacing value and a reader
	// would keep waiting for a segment that never comes, so it gets a trailing
	// zero length segment. That is why ffmpeg writes ff 00 for 255 bytes and
	// ff 01 for 256.
	segs := func(pkt []byte, granule uint64) []oggSeg {
		out := make([]oggSeg, 0, (len(pkt)+maxSegmentBytes-1)/maxSegmentBytes)
		for len(pkt) >= maxSegmentBytes {
			out = append(out, oggSeg{data: append([]byte(nil), pkt[:maxSegmentBytes]...), granule: granule})
			pkt = pkt[maxSegmentBytes:]
		}
		out = append(out, oggSeg{data: append([]byte(nil), pkt...), last: true, granule: granule})
		return out
	}

	var all []oggSeg
	if head != nil {
		all = append(all, segs(head, 0)...)
	}
	for _, fr := range frames {
		if len(fr.Payload) == 0 {
			continue
		}
		g := granuleOf(start, fr.PTS, fr.Duration)
		all = append(all, segs(fr.Payload, g)...)
	}

	var pages []OggPage
	off := 0
	for off < len(all) {
		// A page holds up to 255 segments. The limit is on segments, not on
		// packets: a packet larger than one segment takes several, and it is
		// correct to pack as many as fit. Ending a page at the first packet
		// boundary would put one frame on a page each, which is 255 times more
		// page overhead than the stream needs.
		//
		// A page can therefore end mid-packet. That is why the continuation bit
		// exists, and it is what a reader relies on to know that the first
		// segments on the next page belong to the packet started here.
		n := len(all) - off
		if n > maxSegmentsPerPage {
			n = maxSegmentsPerPage
		}
		end := off + n

		// The continuation bit belongs on the page that follows a split, not on
		// the page that started the packet. The split point is wherever the
		// previous page ran out of segments, so the test is whether the segment
		// before this page closed its packet.
		isCont := off > 0 && !all[off-1].last

		pages = append(pages, OggPage{
			Data: makePage(serial, seq,
				pageHeaderType(off == 0, eos && end == len(all), isCont),
				// The page granule is the granule of the packet occupying the
				// last segment. A reader seeking to a page lands on that packet,
				// so it is the one the granule has to name. A page that ends
				// mid-packet names the packet that continues onto the next page.
				all[end-1].granule, all[off:end]),
		})
		seq++
		off = end
	}
	return pages
}

// pageHeaderType builds the page header_type byte.
//
// The bits are not consecutive, and the lowest is the continuation flag, so
// beginning-of-stream is 0x02 rather than 0x01. Setting 0x01 on a page that
// does not continue a packet marks it as the tail of a packet that never
// arrived, which looks like a truncated stream and is far harder to diagnose
// than the actual cause. Both the BOS and EOS bits set on one page is legal and
// is what a stream that fits on a single page produces.
func pageHeaderType(isFirst, isLast, isCont bool) byte {
	var t byte
	if isFirst {
		t |= pageBOS
	}
	if isLast {
		t |= pageEOS
	}
	if isCont {
		t |= pageContPacket
	}
	return t
}

// granuleOf converts an absolute instant into a granule position in 48 kHz
// sample ticks relative to the stream start.
//
// Ogg granule positions name the last unit in the page, which for a frame is
// the instant the frame ends rather than the instant it starts. The duration is
// added here for that reason; using the PTS alone puts the granule one frame
// early on every page, which reads as audio that plays slightly fast.
//
// The subtraction is done in int64 because both operands are 32-bit transport
// ticks: a frame whose PTS precedes the stream start would wrap a uint32
// subtraction to a value near 2^32, and the negative guard would never fire.
func granuleOf(start, pts time.Time, dur time.Duration) uint64 {
	if pts.IsZero() || start.IsZero() {
		return 0
	}
	base := int64(container.ToTransportHz(pts, opusTimescale)) - int64(container.ToTransportHz(start, opusTimescale))
	end := base + int64(dur.Nanoseconds())*int64(opusTimescale)/int64(time.Second)
	if end < 0 {
		end = 0
	}
	return uint64(end)
}

// makePage assembles one Ogg page, including its checksum.
//
// One segment becomes one lacing value. A segment that does not end its packet
// takes 255, which is the continuation marker, and the segment that closes the
// packet takes its true byte count. A packet of 773 bytes therefore produces
// ff ff ff 08. Writing the true size of every segment for a multi-segment
// packet is not the format: a reader would see the first segment as a complete
// 255 byte packet and stop reading it.
//
// The checksum covers the whole page with the four CRC bytes read as zero. That
// is what the specification defines, so the bytes are zero before hashing and
// filled in afterwards; hashing a page as received folds the stored CRC into
// itself and never matches, which makes a correct page look corrupt.
func makePage(serial, seq uint32, htype byte, granule uint64, segs []oggSeg) []byte {
	// One segment becomes one lacing entry. A segment that ends its packet
	// carries its true byte count, which is how a reader knows the packet is
	// complete; the others carry 255 and mean "continues". So a 256 byte packet
	// is ff 01 and a 773 byte packet is ff ff ff 08, matching ffmpeg.
	lacing := make([]byte, len(segs))
	for i, s := range segs {
		if s.last {
			lacing[i] = byte(len(s.data))
		} else {
			lacing[i] = maxSegmentBytes
		}
	}

	size := pageHeaderSize + len(lacing)
	for _, s := range segs {
		size += len(s.data)
	}
	out := make([]byte, size)
	copy(out, "OggS")
	out[4] = 0x00                     // version
	out[5] = htype                    // header type
	binary.LittleEndian.PutUint64(out[6:14], granule)
	binary.LittleEndian.PutUint32(out[14:18], serial)
	binary.LittleEndian.PutUint32(out[18:22], seq)
	out[26] = byte(len(lacing))
	copy(out[pageHeaderSize:pageHeaderSize+len(lacing)], lacing)

	at := pageHeaderSize + len(lacing)
	for _, s := range segs {
		copy(out[at:], s.data)
		at += len(s.data)
	}
	binary.LittleEndian.PutUint32(out[22:26], OggCRC(out))
	return out
}

// PageSerial reads the stream serial number out of a page.
func PageSerial(page []byte) (uint32, bool) {
	if len(page) < pageHeaderSize || string(page[:4]) != "OggS" {
		return 0, false
	}
	return binary.LittleEndian.Uint32(page[14:18]), true
}

// PageSequence reads the page sequence number out of a page.
func PageSequence(page []byte) (uint32, bool) {
	if len(page) < pageHeaderSize || string(page[:4]) != "OggS" {
		return 0, false
	}
	return binary.LittleEndian.Uint32(page[18:22]), true
}

// PageGranule reads the page granule position.
func PageGranule(page []byte) (uint64, bool) {
	if len(page) < pageHeaderSize || string(page[:4]) != "OggS" {
		return 0, false
	}
	return binary.LittleEndian.Uint64(page[6:14]), true
}

// PageHeaderType reads the page header_type byte.
func PageHeaderType(page []byte) (byte, bool) {
	if len(page) < pageHeaderSize || string(page[:4]) != "OggS" {
		return 0, false
	}
	return page[5], true
}

// pageBody is the validated body of one page: its segments, their lacing
// values, and whether the page begins a packet continued from the previous one.
type pageBody struct {
	// segs holds the payload of each segment, in lacing order.
	segs [][]byte
	// lace holds the lacing value of each segment. A value below
	// maxSegmentBytes marks the segment as closing its packet.
	lace []byte
	// cont reports whether the page begins a packet that started on the
	// previous page. The bit is set on the continuing page, not on the page that
	// started the packet, which is what the reader needs to know when it has no
	// memory of the previous page.
	cont bool
}

// readPageSegs validates one page and returns its body.
//
// The checksum is computed over the page with the CRC field read as zero, which
// is what the specification defines. Hashing the page as received folds the
// stored CRC into itself and never matches, so a correct page looks corrupt.
func readPageSegs(page []byte) (pageBody, error) {
	var b pageBody
	if len(page) < pageHeaderSize || string(page[:4]) != "OggS" {
		return b, errors.New("opus: bad page header")
	}
	got := binary.LittleEndian.Uint32(page[22:26])
	z := append([]byte(nil), page...)
	z[22], z[23], z[24], z[25] = 0, 0, 0, 0
	if got != OggCRC(z) {
		return b, errors.New("opus: page checksum mismatch")
	}
	b.cont = page[5]&pageContPacket != 0
	n := int(page[26])
	if n == 0 || n > maxSegmentsPerPage {
		return b, errors.New("opus: bad segment count")
	}
	b.lace = page[pageHeaderSize : pageHeaderSize+n]
	at := pageHeaderSize + n
	b.segs = make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		l := int(b.lace[i])
		if at+l > len(page) {
			return b, errors.New("opus: truncated page")
		}
		b.segs = append(b.segs, page[at:at+l])
		at += l
	}
	return b, nil
}

// assemblePackets walks a page body into whole packets. A packet is closed by
// the first segment whose lacing value is below maxSegmentBytes, so a value of
// 255 on the last segment means the packet continues onto the next page.
//
// start is a packet carried over from an earlier page. It is prepended to the
// first packet of this page, which is what the continuation bit means. Returns
// the packets completed by this page and the unfinished tail, if any.
func assemblePackets(b pageBody, start []byte) ([][]byte, []byte) {
	var out [][]byte
	cur := start
	for i, seg := range b.segs {
		cur = append(cur, seg...)
		if b.lace[i] < maxSegmentBytes {
			out = append(out, cur)
			cur = nil
		}
	}
	return out, cur
}

// ParsePage reads the packets out of one self-contained page.
//
// A page that begins a continued packet or ends mid-packet is refused. A
// stateless reader cannot splice a packet across a page boundary, and returning
// half a frame is worse than reporting the condition, so the caller uses the
// unpacker when packets may span pages.
func ParsePage(page []byte) ([][]byte, error) {
	b, err := readPageSegs(page)
	if err != nil {
		return nil, err
	}
	if b.cont {
		return nil, errors.New("opus: page begins a continued packet")
	}
	pkts, tail := assemblePackets(b, nil)
	if tail != nil {
		return nil, errors.New("opus: page ends mid-packet")
	}
	return pkts, nil
}

// crcTable is the Ogg CRC table.
var crcTable = func() [256]uint32 {
	var t [256]uint32
	for i := 0; i < 256; i++ {
		v := uint32(i) << 24
		for j := 0; j < 8; j++ {
			if v&0x80000000 != 0 {
				v = (v << 1) ^ 0x04C11DB7
			} else {
				v <<= 1
			}
		}
		t[i] = v
	}
	return t
}()

// OggCRC computes the checksum for one page, treating the CRC field as zero.
func OggCRC(data []byte) uint32 {
	crc := uint32(0)
	for _, b := range data {
		crc = (crc << 8) ^ crcTable[byte(crc>>24)^b]
	}
	return crc
}

// oggPacker writes Opus frames into Ogg pages.
//
// It owns the page sequence and the granule reference, which is what makes the
// container deterministic for a given set of frames. That is also why it is a
// per-sink writer rather than a free function: page numbers must not wrap
// between independent callers, and the header pages consume the first two of
// them.
type oggPacker struct {
	serial  uint32
	start   time.Time
	nextSeq uint32
}

// newOgg builds an Ogg packer.
func newOgg() *oggPacker { return &oggPacker{serial: 1} }

// Format implements registry.ContainerPacker.
func (p *oggPacker) Format() registry.Format { return registry.FormatOpus }

// InitData implements registry.ContainerPacker. Ogg configures itself in its
// first two packets, so there is nothing to emit out of band.
func (p *oggPacker) InitData() []byte { return nil }

// ConfigFrames implements registry.ContainerPacker.
//
// It returns the two header packets the stream needs, as raw packets rather
// than pages. OpusHead and OpusTags are codec descriptors, which is what this
// method yields for every other container in this tree: FLV returns the raw
// AVC decoder config tag and fMP4 returns nothing because its configuration
// belongs in a sample description. Wrapping them in pages here would force
// them onto a page with the beginning- and end-of-stream bits, and this call
// cannot know where the stream ends, so it cannot set EOS correctly.
//
// Without them a decoder has no channel count, pre-skip or sampling rate, which
// is a total failure rather than a degraded one. The second is a separate frame
// because it is a separate packet and has its own lacing entry.
func (p *oggPacker) ConfigFrames() []registry.Frame {
	return []registry.Frame{
		{Data: OpusHead([]byte("QuickMedia"), opusDefaultPreSkip, 2, opusTimescale), Config: true},
		{Data: OpusTags("QuickMedia", nil), Config: true},
	}
}

// Pack implements registry.ContainerPacker.
//
// One unit becomes one frame. The frame's PTS is what the granule position
// comes from, so a zero PTS yields a page with a zero granule, which reads as
// the beginning of the stream.
//
// The page sequence continues from the headers, so calling Pack before
// ConfigFrames numbers the first media page 0 and the stream starts with
// frames that no decoder has configured for. Callers that need the headers
// call ConfigFrames first, which is the order the container requires.
func (p *oggPacker) Pack(u *stream.Unit) ([]registry.Frame, error) {
	if len(u.Payload) == 0 {
		return nil, nil
	}
	start := u.PTS
	if start.IsZero() {
		start = p.start
	}
	if p.start.IsZero() {
		p.start = start
	}
	seq := p.nextSeq
	pages := WritePages(p.serial, seq, start, nil, false,
		[]OggFrame{{Payload: u.Payload, PTS: start, Duration: u.Duration}})
	p.nextSeq = seq + uint32(len(pages))
	out := make([]registry.Frame, 0, len(pages))
	for _, pg := range pages {
		out = append(out, registry.Frame{Data: pg.Data, Key: true, PTS: u.PTS, DTS: u.DTS})
	}
	return out, nil
}

// oggUnpacker reads Opus frames out of Ogg pages.
//
// It carries one unfinished packet across page boundaries. A frame whose packet
// does not fit in one page of 255 segments is split across pages, with the
// continuation bit set on the later ones. buf is the part of that packet read so
// far and is nil when no packet is mid-read, which is the state a fresh
// unpacker is in.
type oggUnpacker struct {
	// buf holds the segments of a packet that is not yet complete.
	buf []byte
}

// Format implements registry.ContainerUnpacker.
func (u *oggUnpacker) Format() registry.Format { return registry.FormatOpus }

// Feed implements registry.ContainerUnpacker.
//
// Feed takes one page at a time and returns every frame the page completes. A
// page may complete several frames and one frame may take several pages, so the
// number of units returned is not related to the number of pages fed.
func (u *oggUnpacker) Feed(data []byte) ([]*stream.Unit, error) {
	if len(data) < pageHeaderSize {
		return nil, nil
	}
	b, err := readPageSegs(data)
	if err != nil {
		return nil, err
	}

	// The continuation bit says the first packet on this page starts on the
	// previous page, so the buffer must be prepended to it. The splice happens
	// here rather than at the call site because Feed is the only seam between
	// two pages: a caller that reassembles on its own would need to know the
	// continuation semantics, which is the layer's job.
	pkts, tail := assemblePackets(b, u.buf)
	u.buf = tail
	if len(pkts) == 0 {
		return nil, nil
	}

	out := make([]*stream.Unit, 0, len(pkts))
	for _, pk := range pkts {
		out = append(out, stream.NewUnit(&stream.Unit{
			Codec: CodecID, Kind: stream.KindAudio,
			Payload: append([]byte(nil), pk...), Key: true,
		}))
	}
	return out, nil
}

// Reset implements registry.ContainerUnpacker.
func (u *oggUnpacker) Reset() { u.buf = nil }

func newOggUnpacker() *oggUnpacker { return &oggUnpacker{} }
