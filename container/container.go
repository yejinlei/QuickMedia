// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package container is layer L2: codec packing.
//
// It converts Elementary-Stream units into transport payloads and back, and
// units into container frames and back. It is the only layer that may reason
// about codec identity and container structure, which is what keeps the media
// plane above it free of both.
//
// Payload convention, normative for every packer in this tree:
//
//   - Video (H.264): stream.Unit.Payload is one access unit expressed as a
//     sequence of AVCC-length-prefixed NAL units, i.e. [uint32be length][NAL]*.
//     That is the form every protocol library in scope hands down, so the
//     adapter layer needs no conversion on ingest.
//   - Audio (AAC): stream.Unit.Payload is one raw AAC access unit, with the
//     sampling parameters carried on stream.Track.Params rather than in-band.
//
// Keeping configuration on the track and media in the payload is deliberate:
// a unit can then be re-packed for any container format without re-parsing,
// and a packer never has to guess which part of a buffer is media.
package container

import (
	"encoding/binary"
	"time"

	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Timescale is the H.264 transport clock rate in Hz.
const Timescale = 90000

// Nalu is one H.264 NAL unit as carried in an AVCC-length-prefixed access unit.
type Nalu struct {
	// Type is the 5-bit NAL reference indicator, 1..31.
	Type byte
	// Data is the NAL body including the NAL header byte.
	Data []byte
}

// NAL reference indicators referenced by the packing rules.
const (
	NalNonIDR = 1
	NalFUA    = 28
	NalFUB    = 29
	NalSTAPA  = 24
	NalSTAPB  = 25
	NalSEI    = 6
	NalIDR    = 5
	NalSPS    = 7
	NalPPS    = 8
	NalPrefix = 14
)

// ParseAU splits an access unit into its NAL units.
//
// An empty or malformed access unit yields nil rather than an error: a track
// that announces configuration but carries no media yet is legal, and a
// corrupt stream must degrade to silence rather than tear down a session.
func ParseAU(payload []byte) []Nalu {
	var out []Nalu
	for len(payload) > 0 {
		if len(payload) < 4 {
			return nil
		}
		n := int(binary.BigEndian.Uint32(payload[:4]))
		payload = payload[4:]
		if n == 0 || n > len(payload) {
			return nil
		}
		out = append(out, Nalu{Type: payload[0] & 0x1F, Data: payload[:n]})
		payload = payload[n:]
	}
	return out
}

// EncodeAU repacks NAL bodies into an AVCC-length-prefixed access unit, the
// inverse of ParseAU.
func EncodeAU(nals [][]byte) []byte {
	total := 0
	for _, n := range nals {
		total += 4 + len(n)
	}
	buf := make([]byte, total)
	at := 0
	for _, n := range nals {
		binary.BigEndian.PutUint32(buf[at:], uint32(len(n)))
		at += 4
		copy(buf[at:], n)
		at += len(n)
	}
	return buf
}

// IsIDR reports whether an access unit contains an IDR slice, which is the
// definition of a decodable picture for this codec.
func IsIDR(payload []byte) bool {
	for _, n := range ParseAU(payload) {
		if n.Type == NalIDR {
			return true
		}
	}
	return false
}

// IsConfig reports whether an access unit carries codec configuration, which
// container writers use to decide where to emit a codec descriptor.
func IsConfig(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	for _, n := range ParseAU(payload) {
		if n.Type == NalSPS || n.Type == NalPPS {
			return true
		}
	}
	return false
}

// IsConfigOnly reports whether an access unit is nothing but parameter sets.
//
// It is a stricter reading of the same question IsConfig asks, and it is the one
// the kernel needs: it decides whether a unit is a config frame at all, where a
// config frame is a unit the kernel does not have to deliver to subscribers
// because it carries no picture. Conflating the two makes every IDR that an
// encoder ships alongside a parameter-set retransmission count as a config
// frame, which silently drops the first picture of every keyframe group and
// leaves a player showing a frozen frame with no error anywhere in the logs.
//
// The name stays separate from IsConfig rather than changing it, because the
// loose reading is the right one for a writer choosing where to place a codec
// descriptor: any unit that carries parameter sets is a valid place for one.
func IsConfigOnly(payload []byte) bool {
	nals := ParseAU(payload)
	if len(nals) == 0 {
		return false
	}
	for _, n := range nals {
		if n.Type != NalSPS && n.Type != NalPPS {
			return false
		}
	}
	return true
}

// IsVideoKey reports whether a unit is a key picture for any codec. Audio is
// always key, because it has no reference pictures.
func IsVideoKey(u *stream.Unit) bool {
	if u.Key {
		return true
	}
	return u.Kind == stream.KindVideo && IsIDR(u.Payload)
}

// --- transport timestamps ---------------------------------------------------

// ToTransport converts an absolute time into a 32-bit transport timestamp at
// the H.264 clock rate.
//
// The result wraps at 2^32, which is the transport contract: consumers are
// expected to use the stream sequence number for reordering rather than
// assuming a monotonic transport clock.
func ToTransport(t time.Time) uint32 { return ToTransportHz(t, Timescale) }

// ToTransportHz converts an absolute time into a 32-bit transport timestamp at
// a caller-chosen clock rate. Different codecs use different rates, which is
// why the rate is a parameter rather than a constant.
func ToTransportHz(t time.Time, hz uint32) uint32 {
	if t.IsZero() || hz == 0 {
		return 0
	}
	return uint32(t.UnixNano() * int64(hz) / int64(time.Second))
}

// FromTransport is the inverse of ToTransport.
func FromTransport(ts uint32) time.Time { return FromTransportHz(ts, Timescale) }

// FromTransportHz is the inverse of ToTransportHz.
//
// Transport timestamps are absolute, not relative, which is what makes them
// usable across a relay that restarts: the receiver can place a frame on the
// same clock the sender measured on. ToTransportHz divides and therefore
// discards the sub-tick remainder, so the inverse has to undo that division
// rather than reconstruct it. Using the current time would turn every
// timestamp into a small offset from "now", which is unambiguously wrong.
func FromTransportHz(ts uint32, hz uint32) time.Time {
	if hz == 0 {
		return time.Time{}
	}
	ns := time.Duration(ts) * time.Second / time.Duration(hz)
	return time.Unix(0, int64(ns)).UTC()
}

// DurationToTransport converts a duration into transport-clock ticks at the
// H.264 clock rate.
func DurationToTransport(d time.Duration) uint32 { return DurationToTransportHz(d, Timescale) }

// DurationToTransportHz converts a duration into transport-clock ticks at a
// caller-chosen clock rate. The rate is a parameter because every codec has its
// own: the same duration is 3600 ticks on the H.264 clock and 1764 on the AAC
// clock. A conversion that assumed one codec's rate turns another codec's
// timeline into a different one, which is a stream that plays at the wrong
// speed rather than a stream that fails.
func DurationToTransportHz(d time.Duration, hz uint32) uint32 {
	if d <= 0 || hz == 0 {
		return 0
	}
	return uint32(d.Nanoseconds() * int64(hz) / int64(time.Second))
}

// --- byte helpers -----------------------------------------------------------

// WriteU32 appends a big-endian uint32.
func WriteU32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// WriteU16 appends a big-endian uint16.
func WriteU16(b []byte, v uint16) []byte {
	return append(b, byte(v>>8), byte(v))
}

// WriteU24 appends a 24-bit big-endian value.
func WriteU24(b []byte, v uint32) []byte {
	return append(b, byte(v>>16), byte(v>>8), byte(v))
}

// ReadU32 reads a big-endian uint32, returning false when the buffer is short.
func ReadU32(b []byte) (uint32, bool) {
	if len(b) < 4 {
		return 0, false
	}
	return binary.BigEndian.Uint32(b), true
}

// ReadU16 reads a big-endian uint16.
func ReadU16(b []byte) (uint16, bool) {
	if len(b) < 2 {
		return 0, false
	}
	return binary.BigEndian.Uint16(b), true
}

// ReadU8 reads one byte.
func ReadU8(b []byte) (byte, bool) {
	if len(b) < 1 {
		return 0, false
	}
	return b[0], true
}

// AppendU8 is the one-byte form of WriteU32/WriteU16.
func AppendU8(b []byte, v byte) []byte { return append(b, v) }
