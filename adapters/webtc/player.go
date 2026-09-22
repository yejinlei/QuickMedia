// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// WHEP sessions: one per browser that is playing a path.
//
// The session owns the peer connection and the subscription together, because
// they die together. When the browser closes the tab the subscription must go,
// or the kernel keeps filling a ring nobody reads; and when the path's
// publisher leaves the browser must see EOF rather than a frozen picture.

package webtc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/yejinlei/quickmedia/adapters"
	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// gatherTimeout bounds ICE gathering on the play side. It is longer than the
// server default because a peer with a public STUN server takes real time to
// reach convergence, and the wait is bounded and visible rather than open-ended.
const gatherTimeout = 15 * time.Second

// --- session ---------------------------------------------------------------

// playerSession is one WHEP viewer.
type playerSession struct {
	id   string
	srv  *Server
	ctx  context.Context
	path string

	pc  *webrtc.PeerConnection
	// tracks holds one sender per playable codec. The payload type is filled
	// after the answer is negotiated, because the answer is where the number
	// comes from and the writer needs it before the first packet leaves.
	tracks map[stream.CodecID]*trackOut

	local *webrtc.SessionDescription
	sess  *adapters.Session

	once chan struct{}
	err  error
}

// trackOut is one outbound media line: the pion track, its packer, and the
// per-track clock state. The packer is stateless, so the sequence and
// timestamp counters belong to the session, which is where the clock is.
type trackOut struct {
	trk    *webrtc.TrackLocalStaticRTP
	sender *webrtc.RTPSender
	pack   registry.RTPPacker
	codec  stream.CodecID
	pt     webrtc.PayloadType
	hz     uint32

	seq uint16
	ts  uint32
}

func newPlayerSession(ctx context.Context, srv *Server, p, remote string) *playerSession {
	return &playerSession{
		ctx:    ctx,
		srv:    srv,
		path:   p,
		tracks: make(map[stream.CodecID]*trackOut),
		once:   make(chan struct{}),
	}
}

// end tears the session down. It is idempotent and safe to call from the
// read loop, from the kernel's Done channel, or from the server on shutdown.
func (pl *playerSession) end(err error) {
	if err != nil {
		pl.err = err
	}
	select {
	case <-pl.once:
	default:
		close(pl.once)
	}
	if pl.pc != nil {
		_ = pl.pc.Close()
	}
	pl.srv.remove(pl.id)
}

// Done is closed when the session has ended.
func (pl *playerSession) Done() <-chan struct{} { return pl.once }

// Err reports the terminal error.
func (pl *playerSession) Err() error { return pl.err }

// Close tears the session down. It is idempotent.
func (pl *playerSession) Close() { pl.end(nil) }

// --- negotiation -----------------------------------------------------------

// negotiateSend runs the WHEP answer. It blocks until ICE gathering is
// complete, which is the non-trickle path: one response carries every
// candidate, which is what WHEP exchanges.
func (pl *playerSession) negotiateSend(trks []*stream.Track, offer []byte) error {
	out, err := buildOutbound(trks)
	if err != nil {
		return err
	}
	pl.tracks = out

	pc, err := pl.srv.newPeerConnection()
	if err != nil {
		return fmt.Errorf("webrtc: peer: %w", err)
	}
	pl.pc = pc

	for _, t := range pl.tracks {
		sender, err := pc.AddTrack(t.trk)
		if err != nil {
			return fmt.Errorf("webrtc: add track %s: %w", t.codec, err)
		}
		t.sender = sender
	}

	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: string(offer),
	}); err != nil {
		return fmt.Errorf("webrtc: offer: %w", err)
	}

	ans, err := pc.CreateAnswer(nil)
	if err != nil {
		return fmt.Errorf("webrtc: answer: %w", err)
	}
	if err := pc.SetLocalDescription(ans); err != nil {
		return fmt.Errorf("webrtc: local description: %w", err)
	}

	select {
	case <-gathered:
	case <-time.After(gatherTimeout):
		return errors.New("webrtc: ICE gathering timed out")
	case <-pl.ctx.Done():
		return pl.ctx.Err()
	}

	cur := pc.CurrentLocalDescription()
	if cur == nil {
		return errors.New("webrtc: no local description")
	}
	pl.local = cur
	return pl.bindPayloadTypes()
}

// bindPayloadTypes reads each sender's negotiated payload type.
func (pl *playerSession) bindPayloadTypes() error {
	for _, t := range pl.tracks {
		p := t.sender.GetParameters()
		if len(p.Encodings) == 0 {
			return fmt.Errorf("webrtc: no encoding for %s", t.codec)
		}
		t.pt = p.Encodings[0].PayloadType
	}
	return nil
}

// --- media flow ------------------------------------------------------------

// pump pushes one kernel unit out over its sender.
func (pl *playerSession) pump(u *stream.Unit) error {
	t, ok := pl.tracks[u.Codec]
	if !ok {
		return fmt.Errorf("webrtc: no track for %s", u.Codec)
	}
	ts := container.ToTransportHz(u.PTS, t.hz)
	payloads, seq, _ := t.pack.Pack(u, t.seq, ts)
	if len(payloads) == 0 {
		return nil
	}
	dur := container.DurationToTransportHz(u.Duration, t.hz)
	for i, payload := range payloads {
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    uint8(t.pt),
				SequenceNumber: seq + uint16(i),
				Timestamp:      ts + uint32(i)*dur,
				Marker:         i == len(payloads)-1,
			},
			Payload: payload,
		}
		if err := t.trk.WriteRTP(pkt); err != nil {
			return err
		}
	}
	t.seq = seq
	return nil
}

// readLoop drains the kernel into the peer. It is the only consumer of this
// subscription, so the ring cannot fill for want of a reader.
func (pl *playerSession) readLoop(sub stream.Subscription) {
	for {
		u, err := sub.ReadUnit(pl.ctx)
		if err != nil {
			pl.end(err)
			return
		}
		if err := pl.pump(u); err != nil {
			pl.end(err)
			return
		}
		u.Release()
	}
}

// --- helpers ---------------------------------------------------------------

// buildOutbound turns a track table into sender tracks.
func buildOutbound(trks []*stream.Track) (map[stream.CodecID]*trackOut, error) {
	out := make(map[stream.CodecID]*trackOut)
	for _, t := range trks {
		switch t.Codec {
		case codecH264:
			pc, err := registry.SelectCodec(codecH264)
			if err != nil {
				return nil, err
			}
			pack, err := pc.NewRTPPacker()
			if err != nil {
				return nil, err
			}
			// The source's own SPS decides this too. A path published over RTSP
			// or RTMP carries its SPS in its params, and the same rule applies:
			// a browser cannot reorder B frames, so the refusal happens at the
			// answer rather than as a stream that plays badly.
			if t.Params[paramSPS] != "" {
				if bframes, err := h264HasBframes(hexKeep(t.Params[paramSPS])); err != nil {
					return nil, fmt.Errorf("webrtc: sps unreadable — %w", err)
				} else if bframes {
					return nil, noCommon("path carries b-frames, which browsers cannot reorder", []stream.CodecID{codecH264})
				}
			}
			trk, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
				MimeType: webrtc.MimeTypeH264, ClockRate: h264Rate, Channels: 0,
				SDPFmtpLine: h264FmtpFor(t.Params),
			}, trackVideo, streamID)
			if err != nil {
				return nil, fmt.Errorf("webrtc: h264 track: %w", err)
			}
			out[codecH264] = &trackOut{
				trk: trk, pack: pack, codec: codecH264, hz: h264Rate,
			}

		case codecOpus:
			pc, err := registry.SelectCodec(codecOpus)
			if err != nil {
				return nil, err
			}
			pack, err := pc.NewRTPPacker()
			if err != nil {
				return nil, err
			}
			trk, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
				MimeType: webrtc.MimeTypeOpus, ClockRate: opusRate, Channels: 2,
				SDPFmtpLine: opusFmtp,
			}, trackAudio, streamID)
			if err != nil {
				return nil, fmt.Errorf("webrtc: opus track: %w", err)
			}
			out[codecOpus] = &trackOut{
				trk: trk, pack: pack, codec: codecOpus, hz: opusRate,
			}

		default:
			return nil, noCommon(fmt.Sprintf("unplayable codec %s", t.Codec), []stream.CodecID{t.Codec})
		}
	}
	if len(out) == 0 {
		return nil, errors.New("webrtc: no playable track")
	}
	return out, nil
}

// h264FmtpFor builds the fmtp line from the track's SPS, so the answer carries
// the same profile the source actually produces.
func h264FmtpFor(p map[string]string) string {
	id, ok := h264ProfileID(hexKeep(p[paramSPS]))
	if !ok {
		return "level-asymmetry-allowed=1;packetization-mode=1"
	}
	return "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=" + id
}
