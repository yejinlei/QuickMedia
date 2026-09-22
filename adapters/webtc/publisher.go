// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// WHIP sessions: one per peer that is publishing a path.
//
// The session owns the peer connection and the kernel writer together, because
// they die together. When the browser stops, the writer must close with it, or
// the path keeps advertising a publisher that sends nothing.

package webtc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/yejinlei/quickmedia/adapters"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// --- session ---------------------------------------------------------------

// publisherSession is one WHIP publisher.
type publisherSession struct {
	id     string
	srv    *Server
	ctx    context.Context
	path   string
	remote string

	pc     *webrtc.PeerConnection
	writer stream.StreamWriter
	// tracks pairs each negotiated track with its unpacker and its track id.
	tracks map[stream.CodecID]*trackIn

	sess  *adapters.Session
	local *webrtc.SessionDescription

	once chan struct{}
	err  error
}

// trackIn is one inbound media line.
type trackIn struct {
	id    stream.TrackID
	codec stream.CodecID
	kind  stream.CodecKind
	up    registry.RTPUnpacker
}

func newPublisherSession(ctx context.Context, srv *Server, p, remote string) *publisherSession {
	return &publisherSession{
		ctx: ctx, srv: srv, path: p, remote: remote,
		tracks: make(map[stream.CodecID]*trackIn),
		once:   make(chan struct{}),
	}
}

// end tears the session down. It is idempotent and safe to call from the
// packet loop, from the kernel, or from the HTTP DELETE handler.
func (ps *publisherSession) end(err error) {
	if err != nil {
		ps.err = err
	}
	select {
	case <-ps.once:
	default:
		close(ps.once)
	}
	if ps.pc != nil {
		_ = ps.pc.Close()
	}
	if ps.sess != nil {
		_ = ps.sess.Close()
	}
	ps.srv.remove(ps.id)
}

// Done is closed when the session has ended.
func (ps *publisherSession) Done() <-chan struct{} { return ps.once }

// Err reports the terminal error.
func (ps *publisherSession) Err() error { return ps.err }

// Close tears the session down. It is idempotent.
func (ps *publisherSession) Close() { ps.end(nil) }

// --- negotiation -----------------------------------------------------------

// negotiateRecv runs the WHIP answer, creating a recvonly transceiver per
// offered track and returning once ICE gathering is complete.
func (ps *publisherSession) negotiateRecv(trks []*stream.Track, offer []byte) error {
	pc, err := ps.srv.newPeerConnection()
	if err != nil {
		return fmt.Errorf("webrtc: peer: %w", err)
	}
	ps.pc = pc

	for _, t := range trks {
		if t.Codec == codecH264 {
			if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
				return fmt.Errorf("webrtc: video transceiver: %w", err)
			}
		} else {
			if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
				return fmt.Errorf("webrtc: audio transceiver: %w", err)
			}
		}
	}

	ps.pc.OnTrack(func(tr *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		ps.onTrack(tr)
	})

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
	case <-ps.ctx.Done():
		return ps.ctx.Err()
	}

	cur := pc.CurrentLocalDescription()
	if cur == nil {
		return errors.New("webrtc: no local description")
	}
	ps.local = cur
	return nil
}

// bind maps the kernel's tracks onto the local transceivers and starts reading
// each one. It runs after Begin, because the kernel assigns the track ids.
func (ps *publisherSession) bind(trks []*stream.Track) error {
	for _, t := range trks {
		pc, err := registry.SelectCodec(t.Codec)
		if err != nil {
			return err
		}
		up, err := pc.NewRTPUnpacker()
		if err != nil {
			return err
		}
		ps.tracks[t.Codec] = &trackIn{
			id: t.ID, codec: t.Codec, kind: t.Kind, up: up,
		}
	}
	return nil
}

// onTrack starts reading one remote track.
//
// pion invokes this from its own goroutine, so the unpacker must be installed
// by then; bind runs before the answer is written, which is before a remote
// track can exist.
func (ps *publisherSession) onTrack(tr *webrtc.TrackRemote) {
	ti, ok := ps.tracks[trackCodec(tr)]
	if !ok {
		return
	}
	ps.ingest(tr, ti)
}

// ingest reads one remote track and writes the reconstructed units out.
func (ps *publisherSession) ingest(tr *webrtc.TrackRemote, ti *trackIn) {
	go func() {
		for {
			pkt, _, err := tr.ReadRTP()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				ps.end(err)
				return
			}
			u, err := ti.up.Unpack(pkt.Payload, pkt.SequenceNumber, pkt.Timestamp, pkt.Marker)
			if err != nil || u == nil {
				return
			}
			u.TrackID = ti.id
			u.Codec = ti.codec
			u.Kind = ti.kind
			// No Release here: WriteUnit takes the reference on every path,
			// success included.
			if err := ps.writer.WriteUnit(u); err != nil {
				if errors.Is(err, stream.ErrCanceled) || errors.Is(err, stream.ErrEOF) {
					return
				}
				ps.end(err)
				return
			}
		}
	}()
}

// --- helpers ---------------------------------------------------------------

// trackCodec maps a remote track to the codec id it carries.
//
// The kind is used rather than the MIME type because the kind is what the
// offer's media line is, and it is stable across payload types.
func trackCodec(tr *webrtc.TrackRemote) stream.CodecID {
	switch tr.Kind() {
	case webrtc.RTPCodecTypeVideo:
		return codecH264
	case webrtc.RTPCodecTypeAudio:
		return codecOpus
	}
	return ""
}
