// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package h264 is the H.264 codec module.
//
// It is a registry module, not a library: nothing imports it directly. It
// registers itself under the identifier "h264" and the kernel looks it up by
// codec identifier. That is the property that lets a codec be added or removed
// by changing one import in the entry point rather than by touching the kernel.
package h264

import (
	"errors"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Version is this module's version.
const Version = "0.1.0"

// CodecID is this codec's identifier.
const CodecID stream.CodecID = "h264"

// maxPayload is the largest RTP payload this packer emits.
//
// It is well under the 1400-byte safe path MTU so that fragmentation does not
// depend on the network agreeing to jumbo frames.
const maxPayload = 1398

// ModuleInfo implements registry.CodecPacker.
func (m *module) ModuleInfo() registry.ModuleInfo {
	return registry.ModuleInfo{
		Name:      "h264",
		Version:   Version,
		Type:      registry.TCodec,
		MinKernel: "0.1.0",
		Priority:  100,
		Dir:       "container/h264",
	}
}

// ID implements registry.CodecPacker.
func (m *module) ID() stream.CodecID { return CodecID }

// Kind implements registry.CodecPacker.
func (m *module) Kind() stream.CodecKind { return stream.KindVideo }

// Timescale implements registry.CodecPacker.
func (m *module) Timescale() uint32 { return container.Timescale }

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
	case registry.FormatTS, registry.FormatFLV, registry.FormatFMP4, registry.FormatAnnexB:
		return true
	default:
		return false
	}
}

// Formats implements registry.CodecPacker.
func (m *module) Formats() []registry.Format {
	return []registry.Format{
		registry.FormatTS, registry.FormatFLV, registry.FormatFMP4, registry.FormatAnnexB,
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
		// The SPS and PPS are the inputs to the AVC decoder configuration
		// record, and a packer built without them cannot emit one. An FLV sink
		// without a descriptor is a stream no player renders, so the factory
		// is the right place to refuse the configuration rather than emitting
		// a broken tag.
		return newFLVFromParams(m.cpar), nil
	case registry.FormatFMP4:
		return newFMP4(), nil
	case registry.FormatAnnexB:
		return newAnnexB(), nil
	default:
		return nil, errors.New("h264: unsupported format " + string(f))
	}
}

// NewContainerUnpacker implements registry.CodecPacker.
func (m *module) NewContainerUnpacker(f registry.Format) (registry.ContainerUnpacker, error) {
	switch f {
	case registry.FormatTS:
		return newTSUnpacker(), nil
	case registry.FormatFLV:
		return newFLVUnpacker(), nil
	case registry.FormatFMP4:
		return newFMP4Unpacker(), nil
	case registry.FormatAnnexB:
		return newAnnexBUnpacker(), nil
	default:
		return nil, errors.New("h264: unsupported format " + string(f))
	}
}

// module is the H.264 codec module.
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

// ---------------------------------------------------------------------------
// RTP: RFC 6182
// ---------------------------------------------------------------------------

// rtpPacker implements the RTP packetization of this codec.
type rtpPacker struct{}

// MaxPayload implements registry.RTPPacker.
func (p *rtpPacker) MaxPayload() int { return maxPayload }

// Pack implements registry.RTPPacker.
//
// Three shapes are used, in order of preference:
//
//  1. STAP-A, one packet for several NALs. Worthwhile only when there is more
//     than one NAL to aggregate, which is the common case at the start of a
//     stream (SPS + PPS + IDR in one picture).
//  2. A single NAL that already fits, emitted as-is.
//  3. FU-A fragmentation for NALs that exceed the payload budget.
func (p *rtpPacker) Pack(u *stream.Unit, seq uint16, ts uint32) ([][]byte, uint16, uint32) {
	nals := container.ParseAU(u.Payload)
	if len(nals) == 0 {
		return nil, seq, ts
	}
	if len(nals) > 1 {
		if pkt, ok := stapA(nals); ok {
			return [][]byte{pkt}, seq + 1, ts
		}
	}

	var out [][]byte
	for _, n := range nals {
		out = append(out, frag(n.Data)...)
	}
	return out, seq + uint16(len(out)), ts
}

// stapA aggregates several NALs into one STAP-A packet. It reports false when
// the aggregate would not fit, which is how the caller falls back.
func stapA(nals []container.Nalu) ([]byte, bool) {
	body := 0
	for _, n := range nals {
		if len(n.Data) > 16383 {
			return nil, false
		}
		body += 2 + len(n.Data)
	}
	if body > maxPayload {
		return nil, false
	}
	out := container.AppendU8(nil, container.NalSTAPA)
	for _, n := range nals {
		out = container.WriteU16(out, uint16(len(n.Data)))
		out = append(out, n.Data...)
	}
	return out, true
}

// frag emits the packets for one NAL, splitting it into FU-A fragments when it
// does not fit a single payload.
func frag(data []byte) [][]byte {
	if len(data) <= maxPayload {
		return [][]byte{append([]byte{}, data...)}
	}

	ftype := data[0] & 0x1F
	nri := data[0] & 0xE0
	remain := data[1:]
	capN := maxPayload - 2 // FU indicator + FU header
	var out [][]byte
	for len(remain) > 0 {
		n := len(remain)
		if n > capN {
			n = capN
		}
		last := n == len(remain)

		// S=1 marks the first fragment, E=1 the last. Reassembly needs both,
		// and neither marker bit may be set on a middle fragment.
		fuh := ftype
		if last {
			fuh |= 0x40
		} else {
			fuh |= 0x80
		}

		pkt := make([]byte, 2+n)
		pkt[0] = nri | container.NalFUA
		pkt[1] = fuh
		copy(pkt[2:], remain[:n])
		out = append(out, pkt)
		remain = remain[n:]
	}
	return out
}

// rtpUnpacker reassembles RTP payloads back into access units.
type rtpUnpacker struct {
	// buf holds the NAL being reassembled from FU-A fragments.
	buf []byte
}

// Unpack implements registry.RTPUnpacker.
func (u *rtpUnpacker) Unpack(payload []byte, seq uint16, ts uint32, marker bool) (*stream.Unit, error) {
	if len(payload) < 1 {
		return nil, errors.New("h264: empty rtp payload")
	}
	switch payload[0] & 0x1F {
	case container.NalSTAPA:
		return u.stapA(payload[1:], ts)
	case container.NalFUA, container.NalFUB:
		if len(payload) < 2 {
			return nil, errors.New("h264: short fu-a payload")
		}
		nri := payload[0] & 0xE0
		fuh := payload[1]
		ftype := fuh & 0x1F
		start := (fuh>>7)&1 == 1
		end := (fuh>>6)&1 == 1

		if start || u.buf == nil {
			u.buf = container.AppendU8(nil, nri|ftype)
		}
		u.buf = append(u.buf, payload[2:]...)

		if !end && !marker {
			return nil, nil
		}
		if u.buf == nil {
			return nil, nil
		}
		data := u.buf
		u.buf = nil
		return unit(data, ts), nil
	default:
		if !marker {
			return nil, nil
		}
		return unit(payload, ts), nil
	}
}

// stapA consumes one STAP-A packet and returns the unit it aggregates.
func (u *rtpUnpacker) stapA(body []byte, ts uint32) (*stream.Unit, error) {
	var nals [][]byte
	for len(body) > 0 {
		size, ok := container.ReadU16(body)
		if !ok {
			return nil, errors.New("h264: truncated stap-a")
		}
		body = body[2:]
		if int(size) > len(body) {
			return nil, errors.New("h264: truncated stap-a")
		}
		nals = append(nals, append([]byte{}, body[:size]...))
		body = body[size:]
	}
	return unitMulti(nals, ts), nil
}

// unit builds a unit from one reassembled NAL.
func unit(data []byte, ts uint32) *stream.Unit {
	return unitMulti([][]byte{data}, ts)
}

// unitMulti builds a unit from reassembled NALs.
func unitMulti(nals [][]byte, ts uint32) *stream.Unit {
	payload := container.EncodeAU(nals)
	un := &stream.Unit{
		Codec:   CodecID,
		Kind:    stream.KindVideo,
		Payload: payload,
		PTS:     container.FromTransport(ts),
		DTS:     container.FromTransport(ts),
		Key:     container.IsIDR(payload),
	}
	if container.IsConfigOnly(payload) {
		un.Flags |= stream.FlagConfig
	}
	return stream.NewUnit(un)
}
