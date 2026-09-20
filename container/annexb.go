// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Annex-B, the byte-stream form of an access unit.
//
// Annex-B wraps every NAL unit with a zero-length start code, which is the
// form a raw stream uses. Converting between it and the AVCC-length-prefixed
// form the kernel carries is mechanical, but it is worth keeping in one place
// because the start code delimiters are ambiguous when a payload contains a
// long run of zeros.
package container

// scf4 is the 4-byte start code delimiter.
var scf4 = []byte{0x00, 0x00, 0x00, 0x01}

// scf3 is the 3-byte start code delimiter.
var scf3 = []byte{0x00, 0x00, 0x01}

// AnnexB converts an AVCC-length-prefixed access unit into Annex-B.
func AnnexB(payload []byte) []byte {
	nals := ParseAU(payload)
	if len(nals) == 0 {
		return nil
	}
	var out []byte
	for _, n := range nals {
		out = append(out, scf4...)
		out = append(out, n.Data...)
	}
	return out
}

// ParseAnnexB converts an Annex-B stream into an AVCC-length-prefixed access
// unit. It returns nil when the stream is empty or malformed, matching the
// convention used by every other parser in this package: a corrupt stream must
// degrade to silence, not tear down a session.
func ParseAnnexB(data []byte) []byte {
	var nals [][]byte
	for {
		idx := indexOf(data, scf4)
		start := -1
		if idx >= 0 {
			start = idx + len(scf4)
		} else {
			idx = indexOf(data, scf3)
			if idx < 0 {
				return nil
			}
			start = idx + len(scf3)
		}
		next := len(data)
		if i := indexOf(data[start:], scf4); i >= 0 {
			next = start + i
		} else if i := indexOf(data[start:], scf3); i >= 0 {
			next = start + i
		}
		nal := data[start:next]
		// Strip trailing emulation-prevention 0x03 bytes.
		for len(nal) > 0 && nal[len(nal)-1] == 0x03 {
			nal = nal[:len(nal)-1]
		}
		if len(nal) == 0 {
			return nil
		}
		nals = append(nals, append([]byte(nil), nal...))
		data = data[next:]
		if len(data) == 0 {
			break
		}
	}
	return EncodeAU(nals)
}

// indexOf returns the byte index of pat in data, or -1.
func indexOf(data, pat []byte) int {
	if len(pat) > len(data) {
		return -1
	}
	for i := 0; i+len(pat) <= len(data); i++ {
		same := true
		for j := 0; j < len(pat); j++ {
			if data[i+j] != pat[j] {
				same = false
				break
			}
		}
		if same {
			return i
		}
	}
	return -1
}
