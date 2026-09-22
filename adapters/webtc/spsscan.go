// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// SPS scanning: the one piece of H.264 syntax a browser needs that the fmtp
// line does not carry.
//
// A browser decodes B frames, it cannot reorder pictures. A stream that was
// encoded with B frames therefore renders as a moving picture whose frames
// arrive out of order, which the decoder drops or delivers late. The SPS says
// whether B frames are permitted, and the SPS is not summarized in the fmtp,
// so the only way to refuse one honestly is to read it.
//
// The SPS is parsed by bluenviron mediacommon's H.264 reader. Maintaining a
// second SPS parser here would be a second implementation of the same
// syntax — and H.264 SPS has a variable-length scaling list section, where
// the one field a reader can fall out of alignment on. We want the vetted
// reader, not a second one.

package webtc

import (
	"errors"
	"fmt"

	h264 "github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
)

// emulationPrevention removes the bytes an encoder inserted so that the
// reader sees RBSP.
//
// The byte that is inserted is 0x03, not a zero: H.264 7.4.1 emits 0x00 0x00
// 0x03 0x01 in the byte stream for the RBSP 0x00 0x00 0x01, because 0x00 0x00
// 0x01 would be mistaken for the start code that terminates a NAL. So the
// stream carries an extra 0x03 wedged between two zeros and a byte of value
// 1, 2 or 3, and removal is what strips that 0x03 out.
//
// The 0x03 is identified by looking behind: 0x00 0x00 0x03 anywhere in the
// stream is an insertion marker, since the RBSP cannot itself contain that
// sequence. Dropping the zero instead of the 0x03 is a failure this exists
// to prevent: it leaves 0x00 0x03 where 0x00 0x00 should be.
//
// Kept for tests of decapsulation behaviour; the SPS parser above uses
// bluenviron's own EP step.
func emulationPrevention(nalu []byte) []byte {
	out := make([]byte, 0, len(nalu))
	for i := 0; i < len(nalu); i++ {
		if i >= 2 && nalu[i-2] == 0 && nalu[i-1] == 0 && nalu[i] == 0x03 {
			continue // drop the emulation prevention byte
		}
		out = append(out, nalu[i])
	}
	return out
}

// bitReader reads RBSP as a bit stream. Kept as the small reader the EP
// test walks to confirm the decapsulated bytes put the profile and level
// back where the encoder wrote them.
type bitReader struct {
	data []byte
	pos  int
	buf  uint32
	have int
}

func (r *bitReader) fill() bool {
	if r.have > 0 {
		return true
	}
	if r.pos >= len(r.data) {
		return false
	}
	r.buf = uint32(r.data[r.pos])
	r.pos++
	r.have = 8
	return true
}

func (r *bitReader) bit() (int, bool) {
	if !r.fill() {
		return 0, false
	}
	v := int(r.buf >> 7)
	r.buf = (r.buf << 1) & 0xff
	r.have--
	return v, true
}

func (r *bitReader) bits(n int) (int, bool) {
	v := 0
	for i := 0; i < n; i++ {
		x, ok := r.bit()
		if !ok {
			return 0, false
		}
		v = (v << 1) | x
	}
	return v, true
}

// ue is one unsigned exp-golomb code.
func ue(r *bitReader) (int, bool) {
	z := 0
	for {
		x, ok := r.bit()
		if !ok {
			return 0, false
		}
		if x == 1 {
			break
		}
		z++
		if z > 31 {
			return 0, false
		}
	}
	v, ok := r.bits(z)
	return v + (1 << z) - 1, ok
}

// skip reads bits without inspecting them.
func skip(r *bitReader, n int) bool { _, ok := r.bits(n); return ok }

// spsBody returns the SPS as NAL bytes, ready for the bluenviron parser.
//
// The bluenviron reader strips its own NAL header, so we pass the whole NAL
// through. The two forms an encoder hands us are the Annex-B form with the
// 0x67 header and the bare body without it; the header is required by the
// parser, so the bare form gets it back on.
//
// Detecting the header is unambiguous by looking at the top 5 bits: 0x67
// makes the NAL type 7 (SPS), and the profile_idc 0x42 (baseline) cannot
// itself be mistaken for one.
func spsBody(sps []byte) []byte {
	if len(sps) < 4 {
		return sps
	}
	if sps[0]&0x1F != 7 {
		return append([]byte{0x67}, sps...)
	}
	return sps
}

// h264HasBframes reports whether a stream is encoded with B frames.
//
// The answer is only ever "yes" or "no" from the SPS alone. Baseline
// profile_idc 66 forbids B frames entirely by the spec; every other
// profile permits them, so the honest answer for the rest is that they
// may be present. A caller who needs to know for sure has to read the PPS
// field num_ref_idx_l1_active_minus1, which the fmtp also carries, but
// the SPS refusal is the one that catches the case where the encoder
// forgot to advertise it and the browser never finds out until a picture
// is missing.
//
// It refuses when the SPS cannot be parsed to its end. A refusal is the
// failure mode that costs one rejected stream; an approval is the one
// that costs a stream a browser cannot play and only discovers from the
// missing pictures.
func h264HasBframes(sps []byte) (bool, error) {
	body := spsBody(sps)
	if len(body) < 4 {
		return false, errors.New("webrtc: sps too short")
	}
	s := h264.SPS{}
	if err := s.Unmarshal(body); err != nil {
		return false, fmt.Errorf("webrtc: sps unreadable — %w", err)
	}
	return s.ProfileIdc != 66, nil
}
