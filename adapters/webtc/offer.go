// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Offer parsing: the track table a WHIP publisher announces.
//
// Media arrives over RTP, so the SDP is the only place a publisher tells the
// server what it is going to send. Everything downstream of Begin is derived
// from this file, which is why its refusals are hard ones.

package webtc

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/pion/sdp/v3"

	"github.com/yejinlei/quickmedia/kernel/stream"
)

// h264Fmtp is one parsed H.264 fmtp attribute.
type h264Fmtp struct {
	sps  []byte
	pps  []byte
	id   string
	ok   bool
}

// parseH264Fmtp pulls SPS and PPS out of an fmtp line.
//
// The parameters are base16 and semicolon separated, and their names are
// case-insensitive. A line with neither SPS nor PPS is refused rather than
// accepted: a decoder without both renders nothing, so an accepted empty
// configuration is a stream that is up but never plays.
func parseH264Fmtp(f string) h264Fmtp {
	out := h264Fmtp{}
	for _, kv := range strings.Split(f, ";") {
		k, v, ok := strings.Cut(strings.TrimSpace(kv), "=")
		if !ok {
			continue
		}
		b, err := hex.DecodeString(strings.TrimSpace(v))
		if err != nil {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "sps":
			if len(b) == 0 {
				return out
			}
			out.sps = b
		case "pps":
			if len(b) == 0 {
				return out
			}
			out.pps = b
		case "profile-level-id":
			s := strings.TrimSpace(v)
			if len(s) == 6 {
				out.id = strings.ToUpper(s)
			}
		}
	}
	out.ok = out.sps != nil && out.pps != nil
	return out
}

// sdpTracks derives the publisher's track table from its offer.
//
// It refuses codecs a browser cannot decode, which is the negotiation rule that
// makes the seven-protocol set honest: nothing here converts, so a refused
// track is named rather than dropped.
func sdpTracks(offer []byte) ([]*stream.Track, error) {
	var sd sdp.SessionDescription
	if err := sd.Unmarshal(offer); err != nil {
		return nil, fmt.Errorf("webrtc: bad offer: %w", err)
	}

	codecMap := sd.GetCodecMap()
	var tracks []*stream.Track
	var refused []stream.CodecID

	for _, md := range sd.MediaDescriptions {
		for _, f := range md.MediaName.Formats {
			pt, err := strconv.ParseUint(f, 10, 8)
			if err != nil {
				continue
			}
			c, ok := codecMap[uint8(pt)]
			if !ok {
				continue
			}

			name := strings.ToLower(strings.TrimSpace(c.Name))
			switch {
			case name == "h264":
				fmtp := parseH264Fmtp(c.Fmtp)
				if !fmtp.ok {
					return nil, noCommon("h264 offered without sps and pps", []stream.CodecID{codecH264})
				}
				// B frames are refused here rather than at decode time, because
				// the offer is the only place to say so without accepting a
				// stream this tree will not play. The SPS is read, not guessed:
				// the fmtp carries the profile but not the reference lists.
				if bframes, err := h264HasBframes(fmtp.sps); err != nil {
					return nil, fmt.Errorf("webrtc: sps unreadable — %w", err)
				} else if bframes {
					return nil, noCommon("h264 offered with b-frames, which browsers cannot reorder", []stream.CodecID{codecH264})
				}
				tracks = append(tracks, &stream.Track{
					Codec: codecH264, Kind: stream.KindVideo, Timescale: h264Rate,
					Params: map[string]string{
						paramSPS: hex.EncodeToString(fmtp.sps),
						paramPPS: hex.EncodeToString(fmtp.pps),
					},
				})
			case name == "opus":
				tracks = append(tracks, &stream.Track{
					Codec: codecOpus, Kind: stream.KindAudio, Timescale: opusRate,
					Params: map[string]string{
						"sampleRate":       strconv.Itoa(opusRate),
						"numberOfChannels": strconv.Itoa(2),
						"preSkip":          strconv.Itoa(0),
					},
				})
			default:
				// VP8, H.265 and every other offer is refused. A publisher that
				// sends a video codec this tree has no browser-compatible
				// decoder for is not degraded: it is told so.
				refused = append(refused, stream.CodecID(name))
			}
		}
	}

	if len(tracks) == 0 {
		return nil, noCommon("no track is playable in a browser", refused)
	}
	if len(refused) > 0 {
		return nil, noCommon(fmt.Sprintf("refused %v", refused), refused)
	}
	return tracks, nil
}
