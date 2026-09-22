// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package h264

import (
	"encoding/binary"

	"github.com/yejinlei/quickmedia/container"
)

// SyntheticAU builds a minimal H.264 access unit in AVCC form: length-prefixed
// NAL bodies, no start codes.
//
// The bytes come from a real encoder (libx264, 64x48 baseline) rather than being
// hand-assembled. That is not a matter of the pipeline's taste: an HLS muxer
// hands the SPS to a bitstream parser, which refuses a synthetic run, and a
// hand-built parameter set makes the whole test suite unable to exercise that
// path at all. What the fixture buys is the pipeline's contract — the first
// byte's type field, the NRI bits, the length prefix — without a dependency on
// any one encoder's slice content.
//
// The unit always carries SPS, PPS and one picture. A single NAL per unit
// exercises the fragmentation path; several exercise STAP-A. Both are worth
// testing, because they take different branches of the packer.
func SyntheticAU(idr bool) []byte {
	au := [][]byte{syntheticSPS(), syntheticPPS()}
	if idr {
		au = append(au, syntheticIDR())
	} else {
		au = append(au, syntheticNonIDR())
	}
	return container.EncodeAU(au)
}

// SyntheticPicture returns one picture NAL, with no parameter sets.
func SyntheticPicture(idr bool) []byte {
	if idr {
		return syntheticIDR()
	}
	return syntheticNonIDR()
}

// syntheticSPS is the SPS of a 64x48, level 1.2, baseline stream.
func syntheticSPS() []byte {
	return []byte{
		0x67, 0x42, 0xc0, 0x0a, 0xd9, 0x04, 0x7b, 0x01, 0x10, 0x00, 0x00, 0x03,
		0x00, 0x10, 0x00, 0x00, 0x03, 0x03, 0x20, 0xf1, 0x22, 0x64, 0x80,
	}
}

// syntheticPPS is the matching PPS.
func syntheticPPS() []byte {
	return []byte{0x68, 0xcb, 0x83, 0xcb, 0x20}
}

// syntheticIDR is one IDR slice from the same stream.
func syntheticIDR() []byte {
	return []byte{
		0x65, 0xff, 0xff, 0x6d, 0xdc, 0x45, 0xe9, 0xbd, 0xe6, 0xd9, 0x48,
		0xb7, 0x96, 0x2c, 0xd8, 0x20, 0x6d, 0xd9, 0x23, 0xee, 0xef,
	}
}

// syntheticNonIDR is one non-IDR slice from the same stream.
func syntheticNonIDR() []byte {
	return []byte{
		0x41, 0x11, 0xe3, 0x13, 0x63, 0x49, 0x64, 0x68, 0xb5, 0x57, 0x43,
		0x74, 0x76, 0xd1, 0x9e, 0x60, 0x21, 0x2c, 0x22,
	}
}

// SyntheticSPS returns the parameter set body used by the fixtures.
//
// It is exported so the protocol adapters can publish a stream whose track
// description matches what the container tests feed, which is how an adapter
// test gets a codec configuration without depending on a real encoder.
func SyntheticSPS() []byte { return syntheticSPS() }

// SyntheticPPS returns the parameter set body used by the fixtures.
func SyntheticPPS() []byte { return syntheticPPS() }

// SyntheticAU4ByteLength wraps one picture in AVCC form using the
// four-byte length prefix some containers require.
func SyntheticAU4ByteLength(idr bool) []byte {
	n := SyntheticPicture(idr)
	l := make([]byte, 4)
	binary.BigEndian.PutUint32(l, uint32(len(n)))
	return append(l, n...)
}
