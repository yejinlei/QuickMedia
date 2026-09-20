// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Container writer for the Flash Video family of formats.
//
// FLV is used by two things in scope: the long-lived live streaming variant and
// the segment file variant. Both share the same tag framing, so one writer
// serves both.
//
// FLV timestamps are 32-bit and signed, so a stream running longer than
// 1 h 7 min 30 s wraps negative. That is a property of the format, not a bug:
// the unwrap on read is what keeps a stream continuous across the wrap point.
package container

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// flvTagVideo is the tag type for video.
const flvTagVideo byte = 0x09

// flvTagAudio is the tag type for audio.
const flvTagAudio byte = 0x08

// flvVideoAVC is the video codec id for H.264/AVC. It is 7: codec id 1 is
// MP3, so writing it emits a stream every real player treats as audio.
const flvVideoAVC byte = 0x07

// flvFrameKey is frame_type 1 (key) in the high four bits of the video tag's
// first byte. A config record is always a key frame.
const flvFrameKey byte = 0x10

// flvFrameInter is frame_type 2, an inter frame.
const flvFrameInter byte = 0x20

// AVC packet type in the second byte: 0 is a decoder configuration record, 1
// is a coded frame.
const (
	flvPacketSetup byte = 0x00
	flvPacketCoded byte = 0x01
)

// flvMaxTagSize is the largest tag body allowed.
const flvMaxTagSize = 16383

// flvTimeOffset is 30 s in milliseconds, applied so a stream that joins
// mid-live does not report a huge offset and so the timestamp is well within
// the safe positive range.
const flvTimeOffset = 30 * 1000

// FLVWriter emits Flash Video tags.
//
// It is a per-sink writer driven from one goroutine, which is the contract the
// packer interface documents. That is what lets Pack re-anchor on the first
// unit without a lock: a sink writer is never shared between connections.
type FLVWriter struct {
	anchor time.Time
}

// NewFLVWriter builds a writer anchored to now.
func NewFLVWriter() *FLVWriter { return &FLVWriter{anchor: time.Now().UTC()} }

// NewFLVWriterAt anchors the writer to a caller-supplied reference time, which
// makes output deterministic in tests.
func NewFLVWriterAt(anchor time.Time) *FLVWriter { return &FLVWriter{anchor: anchor} }

// Pack emits the tags for one unit.
//
// A unit larger than the tag limit is split across tags carrying the same
// timestamp, which is legal and what real encoders do. The first split tag
// carries the composition offset; the rest do not, so the receiver reassembles
// on frame type rather than by index.
func (w *FLVWriter) Pack(u *stream.Unit) ([]registry.Frame, error) {
	if u.Kind == stream.KindAudio {
		return []registry.Frame{w.audioTag(u)}, nil
	}
	if len(u.Payload) > flvMaxTagSize {
		return w.splitVideo(u), nil
	}
	return []registry.Frame{w.videoTag(u)}, nil
}

// videoTag builds one video tag body.
func (w *FLVWriter) videoTag(u *stream.Unit) registry.Frame {
	body := make([]byte, 0, 8+len(u.Payload))

	// frame_type occupies bits 4..7 and codec_id bits 0..3, so a key frame is
	// 0x17 and an inter frame 0x27. frame_type is one field rather than two
	// independent bits: OR-ing inter and key gives frame_type 3, which the spec
	// does not define and which no player accepts.
	head := flvVideoAVC | flvFrameInter
	if IsVideoKey(u) {
		head = flvVideoAVC | flvFrameKey
	}
	body = append(body, head)

	// AVC packet type: coded frames carry the 24-bit composition offset.
	body = append(body, flvPacketCoded)

	comp := int32(0)
	if !u.PTS.IsZero() && !u.DTS.IsZero() {
		comp = int32(u.PTS.Sub(u.DTS).Milliseconds())
		if comp > 0x7FFFFF {
			comp = 0x7FFFFF
		}
		if comp < -0x800000 {
			comp = -0x800000
		}
	}
	body = append(body, byte(comp>>16), byte(comp>>8), byte(comp))

	// AVC tags carry the payload as AVCC: a 32-bit big-endian length prefix
	// followed by the access unit. The length must come after the three
	// header bytes, in this exact order; ffmpeg's h264_mp4toannexb reverses
	// it on the way out.
	body = append(body,
		byte(len(u.Payload)>>24), byte(len(u.Payload)>>16),
		byte(len(u.Payload)>>8), byte(len(u.Payload)))
	body = append(body, u.Payload...)

	return registry.Frame{
		Data: body,
		Key:  IsVideoKey(u),
		PTS:  u.PTS,
		DTS:  u.DTS,
	}
}

// audioTag builds one audio tag body.
//
// Byte 0 is the FLV audio header: sound_format(4) | sound_rate(2) |
// sound_size(1) | sound_type(1). AAC is sound_format 10 (0xA0); the remaining
// bits are conventionally 0001, which is what ffmpeg and Adobe's FLV spec
// writer emit. Byte 1 is the AAC spec version: 0x01 = LC.
//
// A wrong value here is a hard failure rather than a degraded stream: the
// decoder cannot infer the format from the payload, so it rejects the track
// outright.
func (w *FLVWriter) audioTag(u *stream.Unit) registry.Frame {
	body := make([]byte, 0, 2+len(u.Payload))
	body = append(body, 0xA1, 0x01)
	body = append(body, u.Payload...)
	return registry.Frame{Data: body, Key: true, PTS: u.PTS, DTS: u.DTS}
}

// AVCSetupTag builds an AVC decoder configuration record (packet type 0).
//
// This tag is the reason a decoder can start at all: H.264 has no in-band
// stream syntax, so the SPS and PPS must be delivered out of band, and FLV
// delivers them as one extra tag ahead of the first coded frame. A player that
// never sees it renders nothing, so omitting it is a total failure rather than
// a degraded one.
//
// The body is frame_type 4 (key) | packet_type 0 | composition_offset(3, all
// zero for a config record) | uint32be len(SPS) | SPS | uint32be len(PPS) | PPS.
// The length prefixes are the AVCC form of the profile and parameter sets, the
// same representation the kernel already carries in stream.Unit.Payload.
func AVCSetupTag(sps, pps []byte) registry.Frame {
	body := make([]byte, 0, 9+len(sps)+len(pps))
	body = append(body, flvVideoAVC|flvFrameKey, flvPacketSetup)
	body = append(body, 0, 0, 0)
	body = append(body,
		byte(len(sps)>>24), byte(len(sps)>>16), byte(len(sps)>>8), byte(len(sps)))
	body = append(body, sps...)
	body = append(body,
		byte(len(pps)>>24), byte(len(pps)>>16), byte(len(pps)>>8), byte(len(pps)))
	body = append(body, pps...)
	return registry.Frame{Data: body, Key: true, Config: true}
}

// AACSequenceTag builds the AAC sequence start for FLV.
//
// It is byte-identical to an audio tag minus the access unit: 0xA1 0x01
// followed by the AudioSpecificConfig. Emitting it as a plain frame rather than
// a separate type keeps the FLV writer's output on one code path.
func AACSequenceTag(asiof []byte) registry.Frame {
	body := make([]byte, 0, 2+len(asiof))
	body = append(body, 0xA1, 0x01)
	body = append(body, asiof...)
	return registry.Frame{Data: body, Key: true, Config: true}
}

// ParseAVCSetupTag reads the SPS and PPS back out of an AVC decoder
// configuration record. It reports false when the body is a coded frame rather
// than a config record, which is how a reader distinguishes the two.
func ParseAVCSetupTag(tagBody []byte) (sps, pps []byte, ok bool) {
	if len(tagBody) < 8 {
		return nil, nil, false
	}
	if tagBody[0]&0x0F != flvVideoAVC || tagBody[1] != flvPacketSetup {
		return nil, nil, false
	}
	return parseAVCC(tagBody[5:])
}

// parseAVCC splits an AVCC-length-prefixed NAL sequence into (SPS, PPS).
//
// It stops after the second NAL: the profile and parameter sets are the only
// two that a FLV config record carries, so reading further would be inventing
// a field the format does not define.
func parseAVCC(data []byte) (sps, pps []byte, ok bool) {
	first, rest, more := readAVCC(data)
	if !more {
		return nil, nil, false
	}
	second, _, ok2 := readAVCC(rest)
	if !ok2 {
		return nil, nil, false
	}
	return first, second, true
}

// readAVCC reads one uint32be-length-prefixed element.
func readAVCC(data []byte) (elem, rest []byte, ok bool) {
	if len(data) < 4 {
		return nil, nil, false
	}
	n := int(binary.BigEndian.Uint32(data[:4]))
	if n == 0 || n > len(data)-4 {
		return nil, nil, false
	}
	return data[4 : 4+n], data[4+n:], true
}

// splitVideo splits an oversized unit across several tags.
func (w *FLVWriter) splitVideo(u *stream.Unit) []registry.Frame {
	var out []registry.Frame
	remain := u.Payload
	first := true
	for len(remain) > 0 {
		n := min(len(remain), flvMaxTagSize-4-8)
		frame := w.videoTag(&stream.Unit{
			Kind:    u.Kind,
			Codec:   u.Codec,
			Payload: remain[:n],
			Key:     first && u.Key,
			PTS:     u.PTS,
			DTS:     u.DTS,
		})
		remain = remain[n:]
		first = false
		out = append(out, frame)
	}
	return out
}

// TagTimestamp converts an absolute time into an FLV tag timestamp relative to
// this writer's anchor. Every FLV-family adapter uses the writer's own clock,
// which is what keeps several multiplexers on one path agreeing.
func (w *FLVWriter) TagTimestamp(t time.Time) uint32 {
	if t.IsZero() {
		return 0
	}
	d := t.Sub(w.anchor)
	if d < 0 {
		d = 0
	}
	ms := int64(d/time.Millisecond) + flvTimeOffset
	if ms < 0 {
		ms = 0
	}
	return uint32(ms)
}

// Anchor reports the reference instant timestamps are measured from.
func (w *FLVWriter) Anchor() time.Time { return w.anchor }

// EncodeTag serialises one frame into a full Flash Video tag, header and size
// included, followed by the PreviousTagSize field.
//
// FLV is little-endian throughout, which is why the writers here differ from
// every other container writer in this package. PreviousTagSize is the total
// size of the tag that precedes this one, and its fourth byte is written too:
// leaving it out shifts the stream by four bytes on every tag, which is
// invisible until the first tag and fatal from the second on.
func EncodeTag(tagType byte, ts, bodyLen uint32, body []byte) []byte {
	head := make([]byte, 0, 11+len(body)+4)
	head = append(head, tagType)
	head = append(head, byte(bodyLen), byte(bodyLen>>8), byte(bodyLen>>16))
	head = append(head, byte(ts), byte(ts>>8), byte(ts>>16), byte(ts>>24))
	head = append(head, 0, 0, 0)
	head = append(head, body...)
	tagSize := bodyLen + 11
	head = append(head, byte(tagSize), byte(tagSize>>8), byte(tagSize>>16), byte(tagSize>>24))
	return head
}

// ParseTagBody extracts the access-unit payload from an AVC video tag body.
//
// An AVC coded-frame body is frame_type(1) | packet_type(1) |
// composition_offset(3) | AVCC length(4) | access unit. The length prefix is
// part of the framing, not media, so the payload starts 8 bytes in; skipping
// only 5 leaves the length bytes glued to the NAL data and every downstream
// parse is off by four.
func ParseTagBody(tagBody []byte) ([]byte, bool) {
	if len(tagBody) < 8 {
		return nil, false
	}
	codecID := tagBody[0] & 0x0F
	if codecID != flvVideoAVC {
		return nil, false
	}
	if tagBody[1] != flvPacketCoded {
		return nil, false
	}
	n := int(binary.BigEndian.Uint32(tagBody[5:9]))
	if n == 0 || n > len(tagBody)-9 {
		return nil, false
	}
	return tagBody[9 : 9+n], true
}

// ParseAudioTagBody extracts the raw AAC payload from an audio tag body.
func ParseAudioTagBody(tagBody []byte) ([]byte, bool) {
	if len(tagBody) < 3 {
		return nil, false
	}
	return tagBody[2:], true
}

// ParseTagType reports whether a tag body is a video, audio, or script tag.
//
// It is inferred from the body because a raw stream may reach the parser with
// the tag header already consumed. The audio rule comes first: a video body's
// high nibble is 0x1 or 0x4 (frame_type in bits 5..6), which never overlaps
// the audio sound_format range, so the high nibble is a clean discriminator.
// Putting the video rule first misclassifies every real AAC stream, whose first
// byte is 0xA1 and whose low nibble is coincidentally the AVC codec id.
func ParseTagType(tagBody []byte) byte {
	if len(tagBody) == 0 {
		return 0
	}
	switch {
	case tagBody[0]>>4 >= 0x08:
		return flvTagAudio
	case tagBody[0]>>4 == 0x01 || tagBody[0]>>4 == 0x02:
		return flvTagVideo
	default:
		return 0
	}
}

// ParseFLVTags splits a byte stream into tag bodies and their types.
//
// The header, when present, is consumed first. Two tolerances apply:
//
//   - A leading tag with an empty body is skipped. FFmpeg emits one before any
//     media so that the tag header's PreviousTagSize field has something to
//     point at; it carries no media and returning it would confuse a caller
//     that maps bodies onto access units.
//   - A 3-byte PreviousTagSize is accepted. The spec defines four bytes, and
//     ffmpeg writes four, but some producers write three. Skipping a tag that
//     does not have them would drop the rest of the stream, so the short form
//     is tried second.
//
// The function stops at the first incomplete tag rather than erroring, so it
// can be fed from a live stream.
func ParseFLVTags(data []byte) ([][]byte, []byte, error) {
	var bodies [][]byte
	var types []byte

	at := 0
	if len(data) >= 4 && data[0] == 'F' && data[1] == 'L' && data[2] == 'V' {
		if len(data) < 13 {
			return nil, nil, errors.New("flv: truncated header")
		}
		at = 13
	}
	for at+11 <= len(data) {
		tagType := data[at]
		bodySize := uint32(data[at+1]) | uint32(data[at+2])<<8 | uint32(data[at+3])<<16
		if at+11+int(bodySize) > len(data) {
			break
		}
		body := data[at+11 : at+11+int(bodySize)]
		if len(body) > 0 {
			bodies = append(bodies, body)
			types = append(types, tagType)
		}
		nxt := at + 11 + int(bodySize)
		if nxt+4 <= len(data) {
			at = nxt + 4
		} else if nxt+3 <= len(data) {
			at = nxt + 3
		} else {
			break
		}
	}
	return bodies, types, nil
}
