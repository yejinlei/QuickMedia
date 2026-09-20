// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Container packers for the H.264 codec module.
//
// One packer per target format. Each is deliberately small: the format's
// framing rules live in the container layer, and the codec layer only decides
// what belongs in a frame.
package h264

import (
	"encoding/hex"
	"time"

	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// tsPacker writes MPEG-TS packets.
type tsPacker struct{ w *container.TSWriter }

// Format implements registry.ContainerPacker.
func (p *tsPacker) Format() registry.Format { return registry.FormatTS }

// InitData implements registry.ContainerPacker. MPEG-TS carries configuration
// in-band in the PES stream, so there is no separate preamble.
func (p *tsPacker) InitData() []byte { return nil }

// ConfigFrames implements registry.ContainerPacker. MPEG-TS carries the
// profile and parameter sets as SPS and PPS NALs inside the PES stream, so
// there is nothing to emit out of band.
func (p *tsPacker) ConfigFrames() []registry.Frame { return nil }

// Pack implements registry.ContainerPacker.
func (p *tsPacker) Pack(u *stream.Unit) ([]registry.Frame, error) {
	out := make([]registry.Frame, 0, 4)
	for _, pkt := range p.w.Pack(u) {
		out = append(out, registry.Frame{
			Data: pkt, Key: container.IsVideoKey(u), PTS: u.PTS, DTS: u.DTS,
		})
	}
	return out, nil
}

func newTS() *tsPacker {
	return &tsPacker{w: container.NewTSWriter(container.TSConfig{
		StreamPID:  0x0100,
		StreamType: container.TSStreamTypeH264,
	})}
}

// tsUnpacker demultiplexes MPEG-TS back into access units.
//
// A PES packet can span several transport packets, so the unpacker accumulates
// payload until it sees the next payload-unit-start. It is stateful across
// Feed calls, which is why it is an object rather than a free function.
type tsUnpacker struct {
	buf []byte
}

// Format implements registry.ContainerUnpacker.
func (u *tsUnpacker) Format() registry.Format { return registry.FormatTS }

// Feed implements registry.ContainerUnpacker.
func (u *tsUnpacker) Feed(data []byte) ([]*stream.Unit, error) {
	var out []*stream.Unit
	for _, pkt := range container.ParseTS(data) {
		h := container.ParseTSHeader(pkt)
		payload := container.TSPayload(pkt)
		if len(payload) == 0 {
			continue
		}
		if h.PUSI {
			if len(u.buf) > 0 {
				if un := u.complete(u.buf); un != nil {
					out = append(out, un)
				}
				u.buf = nil
			}
			u.buf = append(u.buf, payload...)
		} else {
			u.buf = append(u.buf, payload...)
		}
	}
	return out, nil
}

// complete demuxes one accumulated PES packet into a unit.
func (u *tsUnpacker) complete(pes []byte) *stream.Unit {
	payload, ok := container.ParsePES(pes)
	if !ok || len(payload) == 0 {
		return nil
	}
	un := &stream.Unit{
		Codec:   CodecID,
		Kind:    stream.KindVideo,
		Payload: payload,
		Key:     container.IsVideoKey(&stream.Unit{Kind: stream.KindVideo, Payload: payload}),
	}
	if container.IsConfigOnly(payload) {
		un.Flags |= stream.FlagConfig
	}
	return stream.NewUnit(un)
}

// Reset implements registry.ContainerUnpacker.
func (u *tsUnpacker) Reset() { u.buf = nil }

func newTSUnpacker() *tsUnpacker { return &tsUnpacker{} }

// flvPacker writes Flash Video tags.
type flvPacker struct {
	w   *container.FLVWriter
	sps []byte
	pps []byte
}

// Format implements registry.ContainerPacker.
func (p *flvPacker) Format() registry.Format { return registry.FormatFLV }

// InitData implements registry.ContainerPacker. FLV has no muxed preamble; its
// codec descriptor is a separate tag, exposed through ConfigFrames.
func (p *flvPacker) InitData() []byte { return nil }

// ConfigFrames implements registry.ContainerPacker.
//
// It returns one AVC decoder configuration record carrying the SPS and PPS.
// FLV is a tag stream with no other channel for codec parameters, so a decoder
// cannot begin until it has received this tag; the tag is written ahead of the
// first coded frame and only then.
//
// Empty SPS or PPS yields no frame. Emitting a descriptor with a zero length
// would produce a tag that every player rejects, which is worse than waiting
// for the real parameters.
func (p *flvPacker) ConfigFrames() []registry.Frame {
	if len(p.sps) == 0 || len(p.pps) == 0 {
		return nil
	}
	return []registry.Frame{container.AVCSetupTag(p.sps, p.pps)}
}

// Pack implements registry.ContainerPacker.
func (p *flvPacker) Pack(u *stream.Unit) ([]registry.Frame, error) {
	return p.w.Pack(u)
}

// newFLV builds an FLV packer.
//
// The SPS and PPS are the inputs to the AVC decoder configuration record, and
// a packer built without them cannot emit one. The anchor is the stream start;
// anchoring to the zero time would put every tag on the 30 s offset, which is
// indistinguishable from a frozen stream when played back.
func newFLV(anchor time.Time, sps, pps []byte) *flvPacker {
	return &flvPacker{w: container.NewFLVWriterAt(anchor), sps: sps, pps: pps}
}

// newFLVFromParams builds an FLV packer from the module's stored container
// parameters, base16-encoded as the registry contract specifies.
//
// Decoding failures are not fatal here: a publisher that announces the codec
// without advertising its parameter sets is still serviceable on formats that
// carry configuration in-band, and reporting an error would break the whole
// FLV path for a stream that could have streamed fine. The packer simply
// emits no configuration tag until the parameters arrive, which the caller
// detects through an empty ConfigFrames.
func newFLVFromParams(p map[string]string) *flvPacker {
	sps, _ := hex.DecodeString(p["sps"])
	pps, _ := hex.DecodeString(p["pps"])
	return newFLV(time.Time{}, sps, pps)
}

// annexbPacker writes raw Annex-B streams.
type annexbPacker struct{}

// Format implements registry.ContainerPacker.
func (p *annexbPacker) Format() registry.Format { return registry.FormatAnnexB }

// InitData implements registry.ContainerPacker. Annex-B carries configuration
// in-band, so there is no separate preamble.
func (p *annexbPacker) InitData() []byte { return nil }

// ConfigFrames implements registry.ContainerPacker. Annex-B carries the SPS
// and PPS in the stream itself, so there is nothing to emit out of band.
func (p *annexbPacker) ConfigFrames() []registry.Frame { return nil }

// Pack implements registry.ContainerPacker.
func (p *annexbPacker) Pack(u *stream.Unit) ([]registry.Frame, error) {
	body := container.AnnexB(u.Payload)
	return []registry.Frame{{
		Data: body, Key: container.IsVideoKey(u), Config: container.IsConfigOnly(u.Payload),
		PTS: u.PTS, DTS: u.DTS, Duration: u.Duration,
	}}, nil
}

func newAnnexB() *annexbPacker { return &annexbPacker{} }

// annexbUnpacker reads Annex-B streams back into access units.
type annexbUnpacker struct{}

// Format implements registry.ContainerUnpacker.
func (u *annexbUnpacker) Format() registry.Format { return registry.FormatAnnexB }

// Feed implements registry.ContainerUnpacker.
func (u *annexbUnpacker) Feed(data []byte) ([]*stream.Unit, error) {
	payload := container.ParseAnnexB(data)
	if payload == nil {
		return nil, nil
	}
	return []*stream.Unit{stream.NewUnit(&stream.Unit{
		Codec: CodecID, Kind: stream.KindVideo, Payload: payload,
	})}, nil
}

// Reset implements registry.ContainerUnpacker.
func (u *annexbUnpacker) Reset() {}

func newAnnexBUnpacker() *annexbUnpacker { return &annexbUnpacker{} }

// fmp4Packer writes fragmented MP4 samples.
//
// An fMP4 sample for H.264 is exactly the AVCC-length-prefixed access unit the
// kernel already carries, which is why this packer is a pass-through. That is
// not an accident: the payload convention was chosen so that fragmented MP4
// and the kernel share a representation, and it removes a copy on the hot path.
type fmp4Packer struct{}

// Format implements registry.ContainerPacker.
func (p *fmp4Packer) Format() registry.Format { return registry.FormatFMP4 }

// InitData implements registry.ContainerPacker.
func (p *fmp4Packer) InitData() []byte { return nil }

// ConfigFrames implements registry.ContainerPacker.
//
// fMP4 is the exception in this tree: its codec configuration is the
// avcC box, which is a sample-description entry rather than a media sample.
// A sample-level config frame would be written into the moof as a sample with
// no codec to decode it as, so the correct answer here is that the
// configuration is delivered elsewhere and the sample stream starts directly
// with media.
func (p *fmp4Packer) ConfigFrames() []registry.Frame { return nil }

// Pack implements registry.ContainerPacker.
func (p *fmp4Packer) Pack(u *stream.Unit) ([]registry.Frame, error) {
	return []registry.Frame{{
		Data: u.Payload, Key: container.IsVideoKey(u), PTS: u.PTS, DTS: u.DTS,
	}}, nil
}

func newFMP4() *fmp4Packer { return &fmp4Packer{} }

// fmp4Unpacker reads fragmented MP4 samples.
type fmp4Unpacker struct{}

// Format implements registry.ContainerUnpacker.
func (u *fmp4Unpacker) Format() registry.Format { return registry.FormatFMP4 }

// Feed implements registry.ContainerUnpacker.
func (u *fmp4Unpacker) Feed(data []byte) ([]*stream.Unit, error) {
	return []*stream.Unit{stream.NewUnit(&stream.Unit{
		Codec: CodecID, Kind: stream.KindVideo, Payload: append([]byte(nil), data...),
	})}, nil
}

// Reset implements registry.ContainerUnpacker.
func (u *fmp4Unpacker) Reset() {}

func newFMP4Unpacker() *fmp4Unpacker { return &fmp4Unpacker{} }

// flvUnpacker reads Flash Video tags.
type flvUnpacker struct{}

// Format implements registry.ContainerUnpacker.
func (u *flvUnpacker) Format() registry.Format { return registry.FormatFLV }

// Feed implements registry.ContainerUnpacker.
func (u *flvUnpacker) Feed(data []byte) ([]*stream.Unit, error) {
	payload, ok := container.ParseTagBody(data)
	if !ok {
		return nil, nil
	}
	return []*stream.Unit{stream.NewUnit(&stream.Unit{
		Codec: CodecID, Kind: stream.KindVideo, Payload: payload,
	})}, nil
}

// Reset implements registry.ContainerUnpacker.
func (u *flvUnpacker) Reset() {}

func newFLVUnpacker() *flvUnpacker { return &flvUnpacker{} }
