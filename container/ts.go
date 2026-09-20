// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// MPEG-TS multiplexer, shared by every codec that ships in this container.
//
// MPEG-TS is the one container format that is inherently multiplexed: a single
// transport stream carries a program association table, a program map table,
// and one PES stream per elementary stream. Writing it correctly means writing
// the tables and the PCR, which is why this is a shared writer rather than a
// per-codec copy.
//
// PCR is carried on every keyframe. Real encoders reinsert PCR every 100 ms or
// per keyframe; this reinserts per keyframe, which is the conservative choice
// and costs a few bytes.
package container

import (
	"time"

	"github.com/yejinlei/quickmedia/kernel/stream"
)

// TSStreamTypeH264 is the MPEG-TS stream type for H.264.
const TSStreamTypeH264 byte = 0x1b

// TSStreamTypeAAC is the MPEG-TS stream type for AAC.
const TSStreamTypeAAC byte = 0x0f

// tsPacketSize is the transport-stream packet size.
const tsPacketSize = 188

// crcTable is the MPEG-2 CRC32 table, polynomial 0x04C11DB7.
//
// This is a non-reflected CRC, which the Go standard library does not implement,
// so the table is built by hand.
var crcTable = func() [256]uint32 {
	var t [256]uint32
	const poly = 0x04C11DB7
	for i := 0; i < 256; i++ {
		c := uint32(i) << 24
		for j := 0; j < 8; j++ {
			if c&0x80000000 != 0 {
				c = (c << 1) ^ poly
			} else {
				c <<= 1
			}
		}
		t[i] = c
	}
	return t
}()

// crc32mpeg computes the MPEG-2 CRC32.
func crc32mpeg(data []byte) uint32 {
	crc := uint32(0xFFFFFFFF)
	for _, b := range data {
		crc = (crc << 8) ^ crcTable[byte(crc>>24)^b]
	}
	return crc
}

// TSConfig configures a TSWriter.
type TSConfig struct {
	// PATPID is the PID of the program association table. Zero selects 0x0001.
	PATPID uint16
	// PMTPID is the PID of the program map table. Zero selects 0x0101.
	PMTPID uint16
	// StreamPID is the PID of this elementary stream. Zero selects 0x0200.
	StreamPID uint16
	// StreamType is the MPEG-TS stream type.
	StreamType byte
}

// TSWriter multiplexes one elementary stream into MPEG-TS packets.
type TSWriter struct {
	patPID     uint16
	pmtPID     uint16
	streamPID  uint16
	streamType byte
	sync       [][]byte
}

// NewTSWriter builds a multiplexer for one elementary stream.
func NewTSWriter(cfg TSConfig) *TSWriter {
	if cfg.PATPID == 0 {
		cfg.PATPID = 0x0001
	}
	if cfg.PMTPID == 0 {
		cfg.PMTPID = 0x0101
	}
	if cfg.StreamPID == 0 {
		cfg.StreamPID = 0x0200
	}
	w := &TSWriter{
		patPID:     cfg.PATPID,
		pmtPID:     cfg.PMTPID,
		streamPID:  cfg.StreamPID,
		streamType: cfg.StreamType,
	}
	w.sync = w.programTables()
	return w
}

// PATPMT returns the PAT and PMT packets. Emit one set at stream start and
// again after each keyframe, which is how a receiver that joined mid-stream
// finds this elementary stream.
func (w *TSWriter) PATPMT() [][]byte { return w.sync }

// StreamPID reports this stream's PID.
func (w *TSWriter) StreamPID() uint16 { return w.streamPID }

// Pack multiplexes one access unit into MPEG-TS packets.
//
// A zero DTS means the caller has no decode time, which is legal for audio and
// is emitted as a PTS-only PES header.
func (w *TSWriter) Pack(u *stream.Unit) [][]byte {
	streamID := byte(0xE0)
	if u.Kind == stream.KindAudio {
		streamID = byte(0xC0)
	}
	key := IsVideoKey(u)
	body := w.pesBody(streamID, u.DTS, u.PTS, key, len(u.Payload))
	body = append(body, u.Payload...)
	var out [][]byte
	for _, p := range w.pack(w.streamPID, body, true) {
		out = append(out, p)
	}
	return out
}

// programTables builds the PAT and PMT sync packets.
func (w *TSWriter) programTables() [][]byte {
	var out [][]byte
	// Table packets are never the start of a PES, which is why both are
	// emitted with the payload-unit-start indicator clear.
	for _, p := range w.pack(w.patPID, w.patSection(), false) {
		out = append(out, p)
	}
	for _, p := range w.pack(w.pmtPID, w.pmtSection(), false) {
		out = append(out, p)
	}
	return out
}

// patSection is the PAT payload: one program entry pointing at the PMT.
//
// Layout: marker(0x00), table_id(0x00), syntax+sec_len_high(0xB0), sec_len_low,
// program_number(2), reserved_network(1), PMT_PID(2), CRC(4).
// section_length covers everything after itself, including the CRC.
func (w *TSWriter) patSection() []byte {
	b := []byte{0x00, 0x00, 0xB0, 0x00}
	b = WriteU16(b, 0x0001)   // program_number = 1
	b = AppendU8(b, 0x40)     // reserved(1) | network_PID_flag(1) | network_PID_high(2)
	b = WriteU16(b, w.pmtPID) // PMT_PID
	b[3] = byte(len(b))       // section_length = bytes after sec_len field (includes CRC)
	b = WriteU32(b, crc32mpeg(b))
	return b
}

// pmtSection is the PMT payload: one elementary stream entry.
//
// The section header points at this stream's PMT PID; the entry points at the
// elementary stream PID. Two different PIDs, which is the whole point of the
// program map.
func (w *TSWriter) pmtSection() []byte {
	b := []byte{0x00, 0x02, 0xB0, 0x00}
	b = WriteU16(b, 0xEFFF|w.pmtPID)    // reserved(3) | pmt_program_number(13)
	b = WriteU16(b, 0x0000)             // version(5)=0 | current_next=1 | reserved(3) | section_number=0
	b = WriteU16(b, 0x0000)             // last_section_number = 0
	b = WriteU16(b, 0xEFFF|w.streamPID) // reserved(3) | PCR_PID(13)
	b = WriteU16(b, 0x0000)             // reserved(4) | program_info_length(12)=0
	b = AppendU8(b, w.streamType)
	b = AppendU8(b, 0xE0)                   // reserved(4) | elementary_PID(13) hi
	b = AppendU8(b, byte(w.streamPID&0xFF)) // elementary_PID lo
	b = WriteU16(b, 0x0000)                 // reserved(4) | elementary_stream_info_length(12) = 0
	b[3] = byte(len(b))                     // section_length, written last so it covers the CRC
	b = WriteU32(b, crc32mpeg(b))
	return b
}

// pesBody builds a PES packet: start code, length, flags, and the optional
// extension data.
//
// The PES header is nine bytes before any optional data: the three-byte start
// code, the stream identifier, the two-byte PES length, the flag byte, the
// reserved/flag byte, and the one-byte data-unit length. Skipping the
// reserved byte is the single most common cause of an otherwise correct muxer
// producing an unreadable stream, so the length is accounted for in bytes
// rather than assembled by feel.
//
// PCR is inserted on keyframes only, which is the minimum that lets a receiver
// build a presentation clock after joining mid-stream.
func (w *TSWriter) pesBody(streamID byte, dts, pts time.Time, key bool, payloadLen int) []byte {
	hasPTS := !pts.IsZero()
	hasDTS := !dts.IsZero()
	hasPCR := key && hasDTS

	// The data-unit length covers the optional header and everything that
	// follows it: P-STD_buffer_size(2) + PCR_base(6) + PTS(5) + DTS(5).
	const optionalHeader = 3 // reserved(8) marker(1) | data_alignment..scrambling | rate..I/P
	dataLen := optionalHeader + 2
	if hasPCR {
		dataLen += 6
	}
	if hasPTS {
		dataLen += 5
	}
	if hasDTS {
		dataLen += 5
	}

	// PES length covers the flag byte, the reserved byte, the data-unit length,
	// and the whole data unit, i.e. everything after the length field itself.
	// Zero is legal and means the stream continues to the next start code or
	// the end of the stream, which is the right choice for a live muxer that
	// emits one PES per access unit.
	length := 3 + dataLen + payloadLen
	if length > 0xFFFF {
		length = 0
	}

	b := []byte{0x00, 0x00, 0x01, streamID}
	b = WriteU16(b, uint16(length))

	flags := byte(0x80) // reserved(3)='100' + marker(1) + optional_extension_flag
	if hasDTS {
		flags |= 0x10
	}
	if hasPTS {
		flags |= 0x08
	}
	if hasPCR {
		flags |= 0x04
	}
	b = append(b, flags)
	b = append(b, 0x00) // EOP, esrate_flag, DSM_flag, PES_CRC_flag, reserved, datalen_hi
	b = append(b, byte(dataLen))

	// optional header: marker bit set, data alignment set, everything else zero.
	b = append(b, 0x80, 0x5E, 0x00)

	b = WriteU16(b, 0x0000) // P-STD_buffer_size
	if hasPCR {
		b = append(b, pcrBase(dts)...)
	}
	if hasPTS {
		b = appendPESField(b, pts, 0x30)
	}
	if hasDTS {
		b = appendPESField(b, dts, 0x40)
	}
	return b
}

// appendPESField appends a PTS or DTS field, encoding the 33-bit value.
func appendPESField(b []byte, t time.Time, marker byte) []byte {
	v := int64(ToTransport(t))
	// '0010' pts33_30 | pts29_15(15) | '01' pts14_0(15)
	b = append(b, marker|(byte(v>>30)&0x0F))
	b = append(b, byte((v>>22)&0xFF))
	b = append(b, byte(((v>>14)&0xFF)<<1)|0x01)
	b = append(b, byte(((v>>7)&0xFF)<<1)|0x01)
	b = append(b, byte((v&0x7F)<<1)|0x01)
	return b
}

// pcrBase builds the 6-byte PCR_base field, 33 bits in 90 kHz ticks.
func pcrBase(dts time.Time) []byte {
	v := int64(ToTransport(dts))
	// '00000011' scr33_30 | scr29_15(15) | '01' scr14_0(15)
	return []byte{
		0x00 | byte((v>>30)&0x0F),
		byte((v >> 22) & 0xFF),
		byte(((v>>14)&0xFF)<<1) | 0x01,
		byte(((v>>7)&0xFF)<<1) | 0x01,
		byte((v&0x7F)<<1) | 0x01,
		0x00,
	}
}

// pack splits payload into transport packets, padding the final packet with an
// adaptation field so every packet is exactly tsPacketSize.
//
// pusi marks the first packet of a new PES. It is a parameter rather than
// computed inside the loop because the caller is the only one that knows where
// a PES boundary falls; the tables and the PES stream share this helper.
func (w *TSWriter) pack(pid uint16, payload []byte, pusi bool) [][]byte {
	if len(payload) == 0 {
		return [][]byte{w.emptyPacket(pid)}
	}
	var out [][]byte
	for len(payload) > 0 {
		pkt := make([]byte, tsPacketSize)
		pkt[0] = 0x47
		// Byte 1: TSI(1) | PUSI(1, 0x40) | transport_priority(1) | PID_high(5, 0x1F)
		pkt[1] = byte((pid >> 8) & 0x1F)
		if pusi {
			pkt[1] |= 0x40
		}
		pkt[2] = byte(pid & 0xFF)

		// Byte 3: transport_priority(1) | reserved(1) | AFC(2, 0x30) | CC(4)
		// AFC 00=payload only, 01=AF only, 10=AF+payload, 11=reserved.
		c := len(payload)
		if c > tsPacketSize-4 {
			c = tsPacketSize - 4
		}
		alen := tsPacketSize - 5 - c
		if alen > 0 {
			pkt[3] |= 0x20 // AFC=2 (bits 5-4, value 0b10): adaptation field and payload
			pkt[4] = byte(alen)
			for i := 5; i < 5+alen; i++ {
				pkt[i] = 0xFF
			}
			copy(pkt[5+alen:], payload[:c])
		} else {
			copy(pkt[4:], payload[:c])
		}
		payload = payload[c:]
		out = append(out, pkt)
		pusi = false
	}
	return out
}

// emptyPacket emits a single packet carrying only an adaptation field.
func (w *TSWriter) emptyPacket(pid uint16) []byte {
	pkt := make([]byte, tsPacketSize)
	pkt[0] = 0x47
	pkt[1] = byte((pid >> 8) & 0x1F)
	pkt[2] = byte(pid & 0xFF)
	pkt[3] |= 0x10 // AFC=1 (bits 5-4, value 0b01): adaptation field only
	pkt[4] = byte(tsPacketSize - 5)
	for i := 5; i < tsPacketSize; i++ {
		pkt[i] = 0xFF
	}
	return pkt
}

// ParseTS splits a byte stream into 188-byte transport packets, dropping any
// packet that does not begin with the sync byte. Real streams arrive on a raw
// byte-oriented transport and may start mid-packet, which is what alignment
// recovery is for.
func ParseTS(data []byte) [][]byte {
	var out [][]byte
	at := 0
	for at+1 < len(data) {
		if data[at] != 0x47 {
			at++
			continue
		}
		if at+tsPacketSize > len(data) {
			break
		}
		out = append(out, data[at:at+tsPacketSize])
		at += tsPacketSize
	}
	return out
}

// TSHeader is the header fields of one transport packet.
type TSHeader struct {
	// PID is the 13-bit packet identifier.
	PID uint16
	// PUSI is the payload-unit-start indicator, set on the first packet of a
	// PES packet.
	PUSI bool
	// AUF is the adaptation-field-present flag.
	AUF bool
}

// ParseTSHeader parses the 4-byte transport packet header.
func ParseTSHeader(pkt []byte) TSHeader {
	if len(pkt) < 4 {
		return TSHeader{}
	}
	// Byte 1: TSI(1) | PUSI(1, 0x40) | transport_priority(1) | PID_high(5, 0x1F)
	// Byte 2: PID_low(8)
	// Byte 3: transport_priority(1) | reserved(1) | AFC(2, 0x30) | CC(4)
	// AFC: 0=payload only, 1=AF only, 2=AF+payload, 3=reserved.
	pid := uint16(pkt[1]&0x1F)<<8 | uint16(pkt[2])
	return TSHeader{
		PID:  pid,
		PUSI: pkt[1]&0x40 != 0,
		AUF:  (pkt[3]>>4)&0x3 != 0,
	}
}

// TSPayload returns the payload of a transport packet, skipping the
// adaptation field when present. A packet with an adaptation field may carry
// both an adaptation field and payload, which is why the flag bit is what
// decides rather than the presence of an adaptation field.
func TSPayload(pkt []byte) []byte {
	if len(pkt) < 4 {
		return nil
	}
	// AFC is 2 bits (byte 3, positions 5-4).
	// 00 = payload only, 01 = adaptation field only,
	// 10 = payload with adaptation field, 11 = reserved.
	afc := (pkt[3] >> 4) & 0x3
	switch afc {
	case 0: // payload only
		return pkt[4:]
	case 2: // payload with adaptation field: the field precedes the payload
		if len(pkt) < 5 {
			return nil
		}
		alen := int(pkt[4])
		if 5+alen > len(pkt) {
			return nil
		}
		return pkt[5+alen:]
	}
	// 1 = adaptation field only, 3 = reserved.
	return nil
}

// ParsePES extracts the elementary stream payload from a PES packet, returning
// false when the data does not begin with a PES start code.
//
// PES data is variable-length and a full PES packet may arrive across several
// transport packets, so this returns whatever follows the header rather than
// validating the length field, which the muxer sets to zero.
func ParsePES(data []byte) ([]byte, bool) {
	if len(data) < 9 {
		return nil, false
	}
	if data[0] != 0x00 || data[1] != 0x00 || data[2] != 0x01 {
		return nil, false
	}
	// PES header layout:
	// [0-2] start code 00 00 01
	// [3] stream_id
	// [4-5] PES length
	// [6] flags
	// [7] reserved byte
	// [8] data_unit_length (optional header size, 0 if no optional header)
	// [9+dataLen:] payload

	dataLen := int(data[8])
	start := 9 + dataLen
	if start > len(data) {
		return nil, false
	}
	return data[start:], true
}
