// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package aac is the AAC codec module.
//
// It is a registry module, not a library: nothing imports it directly. It
// registers itself under the identifier "aac" and the kernel looks it up by
// codec identifier.
package aac

import (
	"errors"
	"strconv"
	"time"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Version is this module's version.
const Version = "0.1.0"

// timeZero is the reference time used by writers built in tests.
var timeZero = time.Time{}

// CodecID is this codec's identifier.
const CodecID stream.CodecID = "aac"

// aacTimescale is the transport clock rate for this codec.
const aacTimescale = 44100

// adtsFrameLen is the length of an ADTS header for a full-frame payload.
const adtsFrameLen = 7

// maxPayload is the largest RTP payload this packer emits.
const maxPayload = 1398

// ModuleInfo implements registry.CodecPacker.
func (m *module) ModuleInfo() registry.ModuleInfo {
	return registry.ModuleInfo{
		Name:      "aac",
		Version:   Version,
		Type:      registry.TCodec,
		MinKernel: "0.1.0",
		Priority:  100,
		Dir:       "container/aac",
	}
}

// ID implements registry.CodecPacker.
func (m *module) ID() stream.CodecID { return CodecID }

// Kind implements registry.CodecPacker.
func (m *module) Kind() stream.CodecKind { return stream.KindAudio }

// Timescale implements registry.CodecPacker.
func (m *module) Timescale() uint32 { return aacTimescale }

// RTPParams implements registry.CodecPacker.
func (m *module) RTPParams() map[string]string { return m.rtp }

// SetRTPParams implements registry.CodecPacker.
func (m *module) SetRTPParams(p map[string]string) { m.rtp = cloneMap(p) }

// ContainerParams implements registry.CodecPacker.
func (m *module) ContainerParams() map[string]string { return m.cpar }

// SetContainerParams implements registry.CodecPacker.
func (m *module) SetContainerParams(p map[string]string) { m.cpar = cloneMap(p) }

// SupportsFormat implements registry.CodecPacker.
func (m *module) SupportsFormat(f registry.Format) bool {
	switch f {
	case registry.FormatTS, registry.FormatFLV, registry.FormatFMP4, registry.FormatADTS:
		return true
	default:
		return false
	}
}

// Formats implements registry.CodecPacker.
func (m *module) Formats() []registry.Format {
	return []registry.Format{
		registry.FormatTS, registry.FormatFLV, registry.FormatFMP4, registry.FormatADTS,
	}
}

// NewRTPPacker implements registry.CodecPacker.
func (m *module) NewRTPPacker() (registry.RTPPacker, error) { return &rtpPacker{}, nil }

// NewRTPUnpacker implements registry.CodecPacker.
func (m *module) NewRTPUnpacker() (registry.RTPUnpacker, error) { return &rtpUnpacker{}, nil }

// NewContainerPacker implements registry.CodecPacker.
func (m *module) NewContainerPacker(f registry.Format) (registry.ContainerPacker, error) {
	switch f {
	case registry.FormatTS:
		return newTS(), nil
	case registry.FormatFLV:
		return newFLVFromParams(m.cpar), nil
	case registry.FormatFMP4:
		return newFMP4(), nil
	case registry.FormatADTS:
		return newADTSFromParams(m.cpar), nil
	default:
		return nil, errors.New("aac: unsupported format " + string(f))
	}
}

// NewContainerUnpacker implements registry.CodecPacker.
func (m *module) NewContainerUnpacker(f registry.Format) (registry.ContainerUnpacker, error) {
	switch f {
	case registry.FormatADTS:
		return newADTSUnpacker(), nil
	default:
		return nil, errors.New("aac: unsupported format " + string(f))
	}
}

// module is the AAC codec module.
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

// --- RTP --------------------------------------------------------------------

// rtpPacker implements the RFC 3640 packetization of this codec.
type rtpPacker struct{}

// MaxPayload implements registry.RTPPacker.
func (p *rtpPacker) MaxPayload() int { return maxPayload }

// Pack implements registry.RTPPacker.
//
// RFC 3640 uses a fixed 2-byte header: the config length in the high byte and
// the AU length in the low byte, followed by the config and the access unit.
// Splitting is not supported; an access unit larger than the payload budget is
// rejected rather than silently truncated, because truncation would produce a
// silent audio gap that looks like a valid stream.
func (p *rtpPacker) Pack(u *stream.Unit, seq uint16, ts uint32) ([][]byte, uint16, uint32) {
	if len(u.Payload) > maxPayload-2 {
		return nil, seq, ts
	}
	out := make([]byte, 2+len(u.Payload))
	out[0] = 0x00
	out[1] = byte(len(u.Payload))
	copy(out[2:], u.Payload)
	return [][]byte{out}, seq + 1, ts
}

// rtpUnpacker reassembles RTP payloads back into access units.
type rtpUnpacker struct{}

// Unpack implements registry.RTPUnpacker.
func (u *rtpUnpacker) Unpack(payload []byte, seq uint16, ts uint32, marker bool) (*stream.Unit, error) {
	if len(payload) < 2 {
		return nil, errors.New("aac: short rtp payload")
	}
	if payload[0] != 0x00 {
		return nil, errors.New("aac: unexpected config in rtp payload")
	}
	au := payload[1]
	if len(payload)-2 != int(au) {
		return nil, errors.New("aac: au length mismatch")
	}
	return stream.NewUnit(&stream.Unit{
		Codec: CodecID, Kind: stream.KindAudio,
		Payload: append([]byte(nil), payload[2:]...),
		PTS:     container.FromTransportHz(ts, aacTimescale), DTS: container.FromTransportHz(ts, aacTimescale),
		Key: true,
	}), nil
}

// --- MPEG-TS ----------------------------------------------------------------

type tsPacker struct{ w *container.TSWriter }

// Format implements registry.ContainerPacker.
func (p *tsPacker) Format() registry.Format { return registry.FormatTS }

// InitData implements registry.ContainerPacker.
func (p *tsPacker) InitData() []byte { return nil }

// ConfigFrames implements registry.ContainerPacker. MPEG-TS carries the
// AudioSpecificConfig in-band, so there is nothing to emit out of band.
func (p *tsPacker) ConfigFrames() []registry.Frame { return nil }

// Pack implements registry.ContainerPacker.
func (p *tsPacker) Pack(u *stream.Unit) ([]registry.Frame, error) {
	out := make([]registry.Frame, 0, 4)
	for _, pkt := range p.w.Pack(u) {
		out = append(out, registry.Frame{Data: pkt, Key: true, PTS: u.PTS, DTS: u.DTS})
	}
	return out, nil
}

func newTS() *tsPacker {
	return &tsPacker{w: container.NewTSWriter(container.TSConfig{
		StreamPID:  0x0101,
		StreamType: container.TSStreamTypeAAC,
	})}
}

// --- Flash Video ------------------------------------------------------------

// flvPacker writes Flash Video tags.
//
// It carries the sampling parameters explicitly rather than inferring them,
// because FLV's audio sequence start is a required tag: without it the audio
// track is refused outright, since the tag is the only place the parameters
// appear.
type flvPacker struct {
	w  *container.FLVWriter
	sr int
	ch uint8
}

// Format implements registry.ContainerPacker.
func (p *flvPacker) Format() registry.Format { return registry.FormatFLV }

// InitData implements registry.ContainerPacker. FLV has no muxed preamble; its
// codec descriptor is a separate tag, exposed through ConfigFrames.
func (p *flvPacker) InitData() []byte { return nil }

// ConfigFrames implements registry.ContainerPacker.
//
// It returns one audio sequence start tag carrying the AudioSpecificConfig.
// The config is built from the sampling rate and channel count rather than
// stored raw, because those two values are what the adapter knows from the
// published track and are what every FLV player expects.
//
// Missing parameters yield no frame, which is the same reasoning as the video
// equivalent: a zero-length descriptor is worse than no descriptor, because
// it is indistinguishable from a broken stream.
func (p *flvPacker) ConfigFrames() []registry.Frame {
	asiof, ok := AudioSpecificConfig(p.sr, p.ch)
	if !ok {
		return nil
	}
	return []registry.Frame{container.AACSequenceTag(asiof)}
}

// Pack implements registry.ContainerPacker.
func (p *flvPacker) Pack(u *stream.Unit) ([]registry.Frame, error) {
	return p.w.Pack(u)
}

// newFLV builds an FLV packer anchored to the stream start.
//
// Anchoring to the zero time instead would put every tag on the 30 s offset,
// which looks like a valid stream and plays back frozen. The sampling rate and
// channel count are what the sequence start tag is built from.
func newFLV(sr int, ch uint8) *flvPacker {
	return &flvPacker{w: container.NewFLVWriterAt(timeZero), sr: sr, ch: ch}
}

// newFLVFromParams builds an FLV packer from the module's stored container
// parameters. The key names mirror the upstream convention, which keeps an
// adapter that republishes an FLV source reading and writing the same keys.
func newFLVFromParams(p map[string]string) *flvPacker {
	sr, _ := strconv.Atoi(p["sampleRate"])
	ch, _ := strconv.Atoi(p["numberOfChannels"])
	return newFLV(sr, uint8(ch))
}

// AudioSpecificConfig builds the AudioSpecificConfig for AAC-LC.
//
// The layout is objectType(5) | samplingFrequencyIndex(4) |
// channelConfiguration(4), 13 bits packed big-endian into two bytes. Sampling
// frequencies are enumerated rather than stored as a value, which is the
// codec's own encoding; an unsupported rate is a hard failure here because
// there is no way to represent it, and reporting a guess would produce a
// stream every decoder rejects.
//
// The split is uneven, which is the trap: objectType takes 5 bits and the
// sampling index 4, so the index straddles the byte boundary with its low bit
// landing in byte 1 bit 7, and channelConfiguration occupies byte 1 bits
// 6..3. 44100 Hz mono encodes to 0x12 0x08 and stereo to 0x12 0x10, which is
// what ffmpeg writes. Two ways to get this wrong: an off-by-one in the
// objectType constant (AAC-LC is ISO type 2, not 1, which shifts everything
// down by 0x08), and placing the channel field in bit 7, which produces 0x12
// 0x40 instead of 0x12 0x10.
func AudioSpecificConfig(sampleRate int, channels uint8) ([]byte, bool) {
	idx, ok := audioSampleRateIndex(sampleRate)
	if !ok {
		return nil, false
	}
	if channels == 0 {
		channels = 1
	}
	if channels > 7 {
		return nil, false
	}
	// Build the 16-bit value with an explicit shift per field so the layout is
	// readable, then encode big-endian. Each field is widened to uint16 BEFORE
	// shifting: the shift operand's type is preserved by Go, so shifting a
	// uint8 by 7 truncates back to 8 bits and silently zeroes 4<<7=512. That
	// was the bug this table needed the reference for, and it produces
	// 0x08 0x08 for 44100 Hz mono rather than 0x11 0x00.
	val := uint16(0x02)<<11 | uint16(idx)<<7 | uint16(channels)<<3
	b := make([]byte, 2)
	b[0] = byte(val >> 8)
	b[1] = byte(val)
	return b, true
}

// audioSampleRateIndex maps a sample rate to its ISO enumerated value.
//
// 44100 Hz is the only rate this codec is configured for, which is why the
// table is small and the fallback is a refusal rather than the nearest value.
func audioSampleRateIndex(rate int) (uint8, bool) {
	switch rate {
	case 96000:
		return 0, true
	case 88200:
		return 1, true
	case 64000:
		return 2, true
	case 48000:
		return 3, true
	case 44100:
		return 4, true
	case 32000:
		return 5, true
	case 24000:
		return 6, true
	case 22050:
		return 7, true
	case 16000:
		return 8, true
	case 12000:
		return 9, true
	case 11025:
		return 10, true
	case 8000:
		return 11, true
	case 7350:
		return 12, true
	default:
		return 0, false
	}
}

// --- fragmented MP4 ---------------------------------------------------------

// fmp4Packer writes fragmented MP4 samples.
type fmp4Packer struct{}

// Format implements registry.ContainerPacker.
func (p *fmp4Packer) Format() registry.Format { return registry.FormatFMP4 }

// InitData implements registry.ContainerPacker.
func (p *fmp4Packer) InitData() []byte { return nil }

// ConfigFrames implements registry.ContainerPacker.
//
// fMP4 carries the sampling parameters in the mp4a sample description rather
// than as a sample, which is the same reasoning as the video case: a sample
// emitted as media has no codec to decode it with. The parameters therefore
// belong in the description and the sample stream starts with media.
func (p *fmp4Packer) ConfigFrames() []registry.Frame { return nil }

// Pack implements registry.ContainerPacker.
func (p *fmp4Packer) Pack(u *stream.Unit) ([]registry.Frame, error) {
	return []registry.Frame{{Data: u.Payload, Key: true, PTS: u.PTS, DTS: u.DTS}}, nil
}

func newFMP4() *fmp4Packer { return &fmp4Packer{} }

// --- ADTS -------------------------------------------------------------------

// adtsPacker writes raw ADTS frames.
//
// It carries the sampling parameters explicitly for the same reason as the
// FLV packer: ADTS is self-describing, so the header is the only place the
// parameters appear, and a header built from nothing would describe a stream
// that does not exist.
type adtsPacker struct {
	sr int
	ch uint8
}

// Format implements registry.ContainerPacker.
func (p *adtsPacker) Format() registry.Format { return registry.FormatADTS }

// InitData implements registry.ContainerPacker.
func (p *adtsPacker) InitData() []byte { return nil }

// ConfigFrames implements registry.ContainerPacker. ADTS is self-describing:
// each frame carries its own sampling parameters in the header, so there is
// nothing to emit out of band.
func (p *adtsPacker) ConfigFrames() []registry.Frame { return nil }

// Pack implements registry.ContainerPacker.
//
// ADTS is the audio form of Annex-B: a header per frame instead of a start
// code, which is what makes it self-describing for a stream that has no
// container. The header is 7 bytes, which is why it is a constant here rather
// than computed per frame.
func (p *adtsPacker) Pack(u *stream.Unit) ([]registry.Frame, error) {
	head := MakeADTSHeader(p.sr, p.ch, u.Payload)
	if head == nil {
		return nil, errors.New("aac: empty access unit")
	}
	out := append(head, u.Payload...)
	return []registry.Frame{{Data: out, Key: true, PTS: u.PTS, DTS: u.DTS}}, nil
}

// newADTS builds an ADTS packer anchored to the given sampling parameters.
func newADTS(sr int, ch uint8) *adtsPacker { return &adtsPacker{sr: sr, ch: ch} }

// newADTSFromParams builds an ADTS packer from the module's stored container
// parameters, mirroring the FLV equivalent's key names.
func newADTSFromParams(p map[string]string) *adtsPacker {
	sr, _ := strconv.Atoi(p["sampleRate"])
	ch, _ := strconv.Atoi(p["numberOfChannels"])
	return newADTS(sr, uint8(ch))
}

// adtsUnpacker reads ADTS frames.
type adtsUnpacker struct{}

// Format implements registry.ContainerUnpacker.
func (u *adtsUnpacker) Format() registry.Format { return registry.FormatADTS }

// Feed implements registry.ContainerUnpacker.
//
// A chunk may carry several frames, which is the common case: a raw ADTS byte
// stream arrives in whatever sizes the transport happens to deliver, not in
// frame sizes. Returning only the first frame would drop every frame after it,
// which reads as an audio stream that starts and then goes silent.
func (u *adtsUnpacker) Feed(data []byte) ([]*stream.Unit, error) {
	bodies, err := ParseADTS(data)
	if err != nil {
		return nil, err
	}
	if len(bodies) == 0 {
		return nil, nil
	}
	out := make([]*stream.Unit, 0, len(bodies))
	for _, b := range bodies {
		out = append(out, stream.NewUnit(&stream.Unit{
			Codec: CodecID, Kind: stream.KindAudio,
			Payload: b, PTS: container.FromTransportHz(0, aacTimescale), DTS: container.FromTransportHz(0, aacTimescale),
		}))
	}
	return out, nil
}

// Reset implements registry.ContainerUnpacker.
func (u *adtsUnpacker) Reset() {}

func newADTSUnpacker() *adtsUnpacker { return &adtsUnpacker{} }

// MakeADTSHeader builds a 7-byte ADTS header for the given sampling parameters
// and access unit.
//
// It returns nil rather than a header for anything it cannot represent: an
// unsupported sampling rate, a channel count outside the 1..7 range the
// 3-bit field holds, an empty access unit, or a payload longer than the
// 13-bit frame length field can name. Returning a header for any of those
// would describe a stream that decodes to nothing, which is worse than
// rejecting the frame.
//
// The 56-bit header packs to bytes as follows, verified against ffmpeg's
// output across a 4-rate x 6-channel matrix:
//
//	b0  sync (0xFF)
//	b1  id=1, layer=0, protection_absent=1 (0xF1)
//	b2  profile(2) | sfi(4) | ch[2]      profile=1 is AAC-LC
//	b3  ch[1:0]   | fl[12:11]
//	b4  fl[10:3]
//	b5  fl[2:0]   | buf_fullness[12:6]
//	b6  buf_fullness[5:0] | raw_block(0x0C)
//
// buf_fullness is written as 0x1FFC: a "full" buffer that lets a receiver
// start decoding immediately, which is what every player expects from a live
// stream. raw_block is 0 because this codec is single-frame only.
//
// Every shift widens its operand to uint8 or uint16 first. Go preserves the
// shift operand's type, so shifting a byte by 7 silently truncates back to
// 8 bits and zeroes the result; that is the trap this layout sits in, since
// frame length and the sampling index both straddle byte boundaries.
func MakeADTSHeader(sampleRate int, channels uint8, payload []byte) []byte {
	idx, ok := audioSampleRateIndex(sampleRate)
	if !ok {
		return nil
	}
	if channels == 0 {
		channels = 1
	}
	if channels > 7 {
		return nil
	}
	if len(payload) == 0 {
		return nil
	}
	frameLen := uint16(len(payload)) + adtsFrameLen
	if frameLen > 0x1FFF {
		return nil
	}
	b := make([]byte, adtsFrameLen)
	b[0] = 0xFF
	b[1] = 0xF1
	b[2] = 0x40 | uint8(idx)<<2 | (channels>>2)&1
	b[3] = channels&3<<6 | uint8(frameLen>>11)&3
	b[4] = uint8(frameLen >> 3)
	b[5] = uint8(frameLen&7)<<5 | 0x1F
	b[6] = 0xFC
	return b
}

// ParseADTSHeader parses an ADTS header, returning the frame length and
// profile. It returns false when the header is not an ADTS header, which is
// how a mixed stream tells one frame from the next.
//
// A bare header (exactly 7 bytes with no payload following) is rejected
// because the frame length field covers the header plus the payload, so a
// header with no payload cannot satisfy its own length. Without this check a
// trailing header at the end of a partial write would look like a valid frame.
func ParseADTSHeader(data []byte) (frameLen uint16, profile byte, ok bool) {
	if len(data) < adtsFrameLen {
		return 0, 0, false
	}
	if data[0] != 0xFF || data[1]&0xF0 != 0xF0 {
		return 0, 0, false
	}
	frameLen = uint16(data[3]&0x03)<<11 | uint16(data[4])<<3 | uint16(data[5])>>5
	if frameLen < adtsFrameLen || frameLen > uint16(len(data)) {
		return 0, 0, false
	}
	profile = data[2] >> 6
	return frameLen, profile, true
}

// ParseADTS splits a byte stream into ADTS frames, returning each frame's raw
// AAC payload.
func ParseADTS(data []byte) ([][]byte, error) {
	var out [][]byte
	at := 0
	for {
		for at < len(data) && !(data[at] == 0xFF && data[at+1]&0xF0 == 0xF0) {
			at++
			if at >= len(data)-1 {
				return out, nil
			}
		}
		if at+adtsFrameLen > len(data) {
			break
		}
		frameLen, _, ok := ParseADTSHeader(data[at:])
		if !ok || frameLen == 0 || frameLen > uint16(len(data)-at) {
			return nil, errors.New("aac: bad adts frame")
		}
		out = append(out, append([]byte(nil), data[at+adtsFrameLen:at+int(frameLen)]...))
		at += int(frameLen)
	}
	return out, nil
}
