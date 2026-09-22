// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package rtsp is the RTSP protocol adapter, built on
// github.com/bluenviron/gortsplib/v5 for the whole control and transport
// plane: the RTSP state machine, DESCRIBE/ANNOUNCE/SETUP/PLAY/RECORD
// handling, RTP multiplexing over TCP or UDP, and NTP synchronization.
//
// What is QuickMedia's is the boundary crossing, in both directions. Inbound,
// an announced SDP is turned into a kernel track table through the registry,
// and RTP media clocks are turned back into the absolute instants the kernel
// carries. Outbound, a kernel subscription is turned into RTP payloads
// through the codec layer's own packers rather than a second, hand-derived
// packetization.
//
// The publish side is the one that has to be careful about ordering, not size.
// A publisher's packets arrive on ServerSession.OnPacketRTP after RECORD,
// routed from the RTP channel allocated at SETUP; the adapter keeps one
// unpacker per media, reconstructs access units, and writes them into the
// kernel writer in packet order. RTP media clocks are relative and wrap every
// 4.5 hours, so they are anchored against the publisher's own clock rather
// than used as an absolute time.
//
// The play side is the one that owns real state. A ServerStream lives once per
// path and is shared by every player of that path, so the subscription that
// feeds it must start when the first player arrives and keep running after
// that player leaves. The push loop is keyed by path, not by session, which is
// the same discipline the HLS muxer uses.
package rtsp

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	gortsplib "github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	gsp "github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	"github.com/pion/rtp"

	"github.com/yejinlei/quickmedia/adapters"
	"github.com/yejinlei/quickmedia/container"
	_ "github.com/yejinlei/quickmedia/container/aac"
	_ "github.com/yejinlei/quickmedia/container/h264"
	"github.com/yejinlei/quickmedia/kernel/path"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Version is this module's version.
const Version = "0.1.0"

const (
	codecH264 stream.CodecID = "h264"
	codecAAC  stream.CodecID = "aac"

	// Payload types. Any dynamic-range value works; these are the ones the
	// upstream examples and most media servers use, which is what a client
	// expects to see in the fmtp lines.
	videoPayloadTyp = 96
	audioPayloadTyp = 97

	// publishAnchorOffset shifts the inbound clock so the first frame lands at
	// a plausible instant instead of the Unix epoch. Every downstream adapter
	// re-anchors on its first frame anyway, so this only affects the absolute
	// timestamps the control plane reports.
	publishAnchorOffset = 30 * time.Second
)

// Server is the RTSP control listener.
//
// It owns one gortsplib.Server and two tables keyed by path name. Player
// streams are the resource: several players of one path share one ServerStream
// and one subscription, and a late player must not start a stream that
// re-runs from the head. Publishers are keyed the same way because a path has
// one publisher, which is also the kernel's invariant.
//
// The gortsplib.Server is stored on this struct rather than local to Start
// because ServerStream.Initialize needs a pointer to it: a stream built from a
// nil server fails, and the handlers that build streams do not run inside
// Start.
type Server struct {
	mgr *path.Manager

	ctx    context.Context
	cancel context.CancelFunc

	gortsrv *gortsplib.Server

	mu        sync.Mutex
	pubs      map[string]*pubSession
	plays     map[string]*playStream
	bySession map[*gortsplib.ServerSession]*pubSession

	rtspAddr, rtpAddr, rtcpAddr, multicast string
	// tls is the RTSPS credential set; nil means the control port stays
	// plaintext RTSP only.
	tls *tls.Config
}

// pubSession is one inbound stream: the client's source, the kernel writer it
// feeds, and the per-media unpackers that turn RTP into units.
type pubSession struct {
	path   string
	src    registry.Source
	w      stream.StreamWriter
	media  map[*description.Media]*pubMedia
	anchor time.Time
}

// pubMedia is the unpacker state of one published media.
type pubMedia struct {
	unpacker  registry.RTPUnpacker
	track     *stream.Track
	clockRate uint32
}

// playStream is one outbound stream: the shared ServerStream, the subscription
// feeding it, the push loop's handles, and the per-media packetization state.
//
// Sequence and timestamp counters are per-media rather than global, because the
// RTP media clock is per-media too. Sharing them across tracks would make a
// player that tracks its clock per track see a jump whenever the other track
// moved.
type playStream struct {
	st      *gortsplib.ServerStream
	byMedia map[*description.Media]*playMedia
	sub     stream.Subscription
	cancel  context.CancelFunc
	done    <-chan struct{}
}

// playMedia is the packetization state of one played media.
type playMedia struct {
	track *stream.Track
	// pt is this media's RTP payload type. The packer builds the packet header,
	// but gortsplib picks the per-media writer by the payload type in that
	// header, so a zero header finds no format and dereferences nil.
	pt  uint8
	seq uint16
}

// Options configures the RTSP listeners.
type Options struct {
	// RTSPAddress is the RTSP control port. Defaults to ":8554".
	RTSPAddress string
	// UDPRTPAddress and UDPRTCPAddress are the RTP and RTCP UDP ports. Either
	// being empty means the corresponding port is auto-assigned, which keeps
	// the server usable behind NAT where the control port is the only one a
	// client can reach. RTP must be even: RTCP is RTP+1.
	UDPRTPAddress  string
	UDPRTCPAddress string
	// MulticastIPRange allows multicast transports. Empty disables it.
	MulticastIPRange string
	// TLS is the tls.Config for RTSPS on the same control port. gortsplib
	// multiplexes RTSP and RTSPS on one socket: an RTSPS client negotiates a
	// TLS handshake, an RTSP client does not. A nil config keeps the port
	// plaintext-only, which is the default because a self-signed cert is not
	// the right default for a production listener.
	TLS *tls.Config
}

// NewServer builds the listener. It does not bind; Start does.
func NewServer(m *path.Manager, opts Options) *Server {
	s := &Server{
		mgr:       m,
		pubs:      make(map[string]*pubSession),
		plays:     make(map[string]*playStream),
		bySession: make(map[*gortsplib.ServerSession]*pubSession),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	if opts.RTSPAddress != "" {
		s.rtspAddr = opts.RTSPAddress
	} else {
		s.rtspAddr = ":8554"
	}
	s.rtpAddr, s.rtcpAddr, s.multicast = opts.UDPRTPAddress, opts.UDPRTCPAddress, opts.MulticastIPRange
	s.tls = opts.TLS
	return s
}

// Start binds the listeners and returns. It never blocks, so a caller that
// wants shutdown waits on the context rather than on Start.
func (s *Server) Start(ctx context.Context) error {
	if s.gortsrv != nil {
		return nil
	}
	s.gortsrv = &gortsplib.Server{
		Handler:          s,
		RTSPAddress:      s.rtspAddr,
		UDPRTPAddress:    s.rtpAddr,
		UDPRTCPAddress:   s.rtcpAddr,
		MulticastIPRange: s.multicast,
		TLSConfig:        s.tls,
	}
	// Start is non-blocking and, once it returns, sets the session table that
	// ServerStream.Initialize checks. Binding before returning is what makes a
	// DESCRIBE that races Start fail closed rather than building an unusable
	// stream.
	if err := s.gortsrv.Start(); err != nil {
		return err
	}
	go func() {
		select {
		case <-ctx.Done():
		case <-s.ctx.Done():
		}
		s.gortsrv.Close()
	}()
	return nil
}

// Addr reports the address the RTSP control listener actually bound to.
//
// When the server was started with an empty address, as the composition root
// does in tests, the listener picked an ephemeral port and the configured
// string no longer describes reality. A caller that dials the configured
// address would connect to nothing, so the real address must be readable after
// binding rather than only inferable from it.
func (s *Server) Addr() string {
	if s.gortsrv == nil {
		return ""
	}
	if ln := s.gortsrv.NetListener(); ln != nil {
		return ln.Addr().String()
	}
	return s.rtspAddr
}

// Close stops the listeners and every attached session.
func (s *Server) Close() {
	s.cancel()
	if s.gortsrv != nil {
		s.gortsrv.Close()
	}
	s.mu.Lock()
	pubs, plays := s.pubs, s.plays
	s.pubs, s.plays = make(map[string]*pubSession), make(map[string]*playStream)
	s.bySession = make(map[*gortsplib.ServerSession]*pubSession)
	s.mu.Unlock()
	for _, ps := range pubs {
		_ = ps.src.Close()
	}
	for _, p := range plays {
		if p.cancel != nil {
			p.cancel()
		}
		if p.sub != nil {
			p.sub.Cancel()
		}
		if p.st != nil {
			p.st.Close()
		}
	}
}

// --- gortsplib handlers ----------------------------------------------------

// OnSessionClose is the only place a session can be tied back to the resource
// it claimed, so all teardown happens here rather than scattered through each
// request handler.
func (s *Server) OnSessionClose(ctx *gortsplib.ServerHandlerOnSessionCloseCtx) {
	s.mu.Lock()
	ps := s.bySession[ctx.Session]
	if ps == nil {
		s.mu.Unlock()
		return
	}
	delete(s.bySession, ctx.Session)
	delete(s.pubs, ps.path)
	s.mu.Unlock()
	// Ending the source with a nil error is what reaps the path through the
	// kernel's observer. A clean publisher loss keeps the path alive for the
	// retain window instead of dropping it, which is the behavior a camera
	// restarting over a flaky link needs.
	_ = ps.src.Close()
}

// OnDescribe answers a player's SDP request.
func (s *Server) OnDescribe(ctx *gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	st, err := s.streamFor(pathNameFrom(ctx.Path))
	if err != nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	return &base.Response{StatusCode: base.StatusOK}, st, nil
}

// OnSetup answers the transport negotiation.
//
// The Transport response header is built by gortsplib from the request header.
// A handler that rebuilt it would advertise ports the server is not actually
// bound to, and the client would then time out on a stream that looks healthy.
func (s *Server) OnSetup(ctx *gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	// SETUP is used by both readers and publishers. A publisher announces first
	// and arrives here in PreRecord, where it has no stream to return and
	// returning one would make gortsplib panic.
	if ctx.Session.State() == gortsplib.ServerSessionStatePreRecord {
		return &base.Response{StatusCode: base.StatusOK}, nil, nil
	}
	st, err := s.streamFor(pathNameFrom(ctx.Path))
	if err != nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	return &base.Response{StatusCode: base.StatusOK}, st, nil
}

// OnPlay starts the push loop for a player.
//
// The subscription is claimed here rather than in OnDescribe, because a
// DESCRIBE is a probe: a client that describes a stream it never plays would
// otherwise pin a path's ring for the process lifetime.
func (s *Server) OnPlay(ctx *gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	if _, err := s.startPlay(pathNameFrom(ctx.Path)); err != nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil
	}
	return &base.Response{StatusCode: base.StatusOK}, nil
}

// OnRecord is where a publisher's RTP arrives. Registering the callbacks is
// all that is needed: the library routes the channels negotiated at SETUP here,
// in packet order, so no additional sequencing is required.
func (s *Server) OnRecord(ctx *gortsplib.ServerHandlerOnRecordCtx) (*base.Response, error) {
	s.mu.Lock()
	ps := s.bySession[ctx.Session]
	s.mu.Unlock()
	if ps == nil {
		return &base.Response{StatusCode: base.StatusMethodNotValidInThisState}, nil
	}

	ss := ctx.Session
	for _, medi := range ss.Medias() {
		pm := ps.media[medi]
		if pm == nil {
			return &base.Response{StatusCode: base.StatusUnsupportedMediaType}, nil
		}
		for _, forma := range medi.Formats {
			ss.OnPacketRTP(medi, forma, func(pkt *rtp.Packet) {
				ps.handlePacket(ss, medi, pkt)
			})
		}
	}
	return &base.Response{StatusCode: base.StatusOK}, nil
}

// OnAnnounce is the publish negotiation: the publisher announces its tracks
// and this adapter declares them through the kernel before any media arrives.
//
// The call into Manager.Publish comes back out through Adapter.Publish with a
// real PublishSession, which is where the writer is opened. That round trip is
// what keeps the track-ID assignment in the kernel and makes RTSP announce a
// shape the kernel can accept without a protocol-specific backdoor.
func (s *Server) OnAnnounce(ctx *gortsplib.ServerHandlerOnAnnounceCtx) (*base.Response, error) {
	remote := ctx.Conn.NetConn().RemoteAddr().String()
	a := &Adapter{remote: remote, srv: s}

	desc := ctx.Description
	if desc == nil {
		return &base.Response{StatusCode: base.StatusUnsupportedMediaType}, nil
	}
	path := pathNameFrom(ctx.Path)

	byMedia, err := a.kernelTracks(desc)
	if err != nil {
		return &base.Response{StatusCode: base.StatusUnsupportedMediaType}, nil
	}
	a.byMedia = byMedia

	src, err := s.mgr.Publish(s.ctx, a, registry.SinkRequest{
		Path:   path,
		Query:  ctx.Query,
		Remote: remote,
	})
	if err != nil {
		return &base.Response{StatusCode: base.StatusNotAcceptable}, nil
	}

	// The writer the adapter opened is the thing OnRecord needs, and OnRecord
	// runs later. Storing it here keyed by the session is what closes that gap.
	s.mu.Lock()
	s.pubs[path] = &pubSession{
		path:   path,
		src:    src,
		w:      a.writer,
		media:  byMedia,
		anchor: time.Now().UTC().Add(-publishAnchorOffset),
	}
	s.bySession[ctx.Session] = s.pubs[path]
	s.mu.Unlock()

	return &base.Response{StatusCode: base.StatusOK}, nil
}

// OnPause is answered to acknowledge. The kernel keeps a publisher's session
// live across RTSP pauses, so there is nothing to suspend.
func (s *Server) OnPause(_ *gortsplib.ServerHandlerOnPauseCtx) (*base.Response, error) {
	return &base.Response{StatusCode: base.StatusOK}, nil
}

// --- publish ---------------------------------------------------------------

// handlePacket unpacks one RTP payload and writes the completed unit, if any.
//
// The unpacker does not set TrackID: the codec layer has no way to know the
// kernel's identifier assignment, and the writer rejects a unit whose track is
// not in its own table. Assigning it here keeps the mapping local to the one
// place that sees both the SDP and the kernel's table.
func (ps *pubSession) handlePacket(ss *gortsplib.ServerSession, medi *description.Media, pkt *rtp.Packet) {
	pm := ps.media[medi]
	if pm == nil {
		return
	}
	u, err := pm.unpacker.Unpack(pkt.Payload, pkt.SequenceNumber, pkt.Timestamp, pkt.Marker)
	if err != nil || u == nil {
		return
	}

	u.PTS = ps.instant(pm, ss, medi, pkt)
	u.DTS = u.PTS
	u.TrackID = pm.track.ID
	u.Codec = pm.track.Codec
	u.Kind = pm.track.Kind

	// No Release here: WriteUnit takes the reference on every return path,
	// success included. A defer in this hot callback would also pin a buffer for
	// the whole session rather than until the next packet.
	if err := ps.w.WriteUnit(u); err != nil {
		if !errors.Is(err, stream.ErrCanceled) && !errors.Is(err, stream.ErrEOF) {
			return
		}
	}
}

// instant converts one RTP packet's media clock into an absolute instant.
//
// The RTP clock is relative and wraps every 4.5 hours, so it is anchored
// against the publisher's own clock rather than used as an absolute time.
// PacketNTP returns the instant gortsplib derived from RTCP sender reports,
// which is the absolute reference the RTP clock was synchronized to; falling
// back to PacketPTS and then to wall clock keeps a publisher that never sends
// RTCP usable at the cost of per-frame jitter in the reported timestamps.
func (ps *pubSession) instant(pm *pubMedia, ss *gortsplib.ServerSession, medi *description.Media, pkt *rtp.Packet) time.Time {
	if ntp, ok := ss.PacketNTP(medi, pkt); ok && !ntp.IsZero() {
		return ntp.UTC()
	}
	if pts, ok := ss.PacketPTS(medi, pkt); ok {
		return time.Unix(0, pts*int64(time.Second)/int64(pm.clockRate)).UTC()
	}
	return time.Now().UTC()
}

// --- play ------------------------------------------------------------------

// streamFor returns the path's ServerStream, building one if it does not exist.
//
// It is the single place a ServerStream comes from, which is what keeps a
// DESCRIBE and a SETUP from racing to build two, and what a new player reuses
// rather than starting fresh. Initialize is not idempotent, so it runs exactly
// once, here, before the stream is shared.
func (s *Server) streamFor(name string) (*gortsplib.ServerStream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.plays[name]; p != nil && p.st != nil {
		return p.st, nil
	}
	if s.gortsrv == nil {
		return nil, errors.New("rtsp: server not started")
	}
	return s.buildLocked(name)
}

// buildLocked creates a path's ServerStream from the kernel's track table. It
// must be called with s.mu held, which is what makes the build-or-reuse check
// atomic across the two request handlers that can reach it.
func (s *Server) buildLocked(name string) (*gortsplib.ServerStream, error) {
	tracks, err := s.tracksFor(name)
	if err != nil {
		return nil, err
	}
	desc, err := sdpFor(tracks)
	if err != nil {
		return nil, err
	}
	st := &gortsplib.ServerStream{Server: s.gortsrv, Desc: desc}
	if err := st.Initialize(); err != nil {
		return nil, err
	}
	byMedia := make(map[*description.Media]*playMedia, len(tracks))
	for i, medi := range desc.Medias {
		if i >= len(tracks) {
			continue
		}
		pm := &playMedia{track: tracks[i]}
		if len(medi.Formats) > 0 {
			pm.pt = medi.Formats[0].PayloadType()
		}
		byMedia[medi] = pm
	}
	// Claim the slot before the stream is exposed: if the subscription is
	// claimed later and lost, a second SETUP still finds the stream rather than
	// building a second one.
	s.plays[name] = &playStream{st: st, byMedia: byMedia}
	return st, nil
}

// tracksFor returns a published path's track table.
//
// A path's tracks are only reachable through its stats snapshot: a
// subscription is the media flow and deliberately does not carry the
// description. Asking for a path with no publisher is the case a DESCRIBE must
// refuse, so this fails fast rather than blocking on one that may never arrive.
func (s *Server) tracksFor(name string) ([]*stream.Track, error) {
	for _, st := range s.mgr.Paths() {
		if st.Name != name {
			continue
		}
		if st.Tracks == nil {
			return nil, errors.New("rtsp: path has no tracks")
		}
		return st.Tracks, nil
	}
	return nil, errors.New("rtsp: no such path")
}

// startPlay starts a path's push loop once, and returns the shared stream.
func (s *Server) startPlay(name string) (*gortsplib.ServerStream, error) {
	st, err := s.streamFor(name)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	p := s.plays[name]
	if p == nil || p.cancel != nil {
		s.mu.Unlock()
		return st, nil
	}
	sub, err := s.mgr.Subscribe(s.ctx, name, 0)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	fctx, fcancel := context.WithCancel(s.ctx)
	ch := make(chan struct{})
	p.sub, p.cancel, p.done = sub, fcancel, ch
	s.mu.Unlock()

	go func() {
		defer close(ch)
		if err := s.drive(fctx, p, sub); err != nil {
			s.teardownPlay(name)
		}
	}()
	return st, nil
}

// playDone returns the channel that closes when a path's push loop stops.
//
// gortsplib's ServerStream exposes no completion signal, so the adapter keeps
// one per path. A path with no loop returns an already-closed channel, which
// lets a caller's select proceed immediately rather than blocking forever.
func (s *Server) playDone(name string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.plays[name]; p != nil && p.done != nil {
		return p.done
	}
	ch := make(chan struct{})
	close(ch)
	return ch
}

// teardownPlay stops a path's push loop and drops its stream.
func (s *Server) teardownPlay(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.plays[name]
	if p == nil {
		return
	}
	if p.cancel != nil {
		p.cancel()
	}
	if p.sub != nil {
		p.sub.Cancel()
	}
	if p.st != nil {
		p.st.Close()
	}
	delete(s.plays, name)
}

// drive reads units from the subscription and writes them to the stream.
func (s *Server) drive(ctx context.Context, p *playStream, sub stream.Subscription) error {
	var anchor time.Time
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		u, err := sub.ReadUnit(ctx)
		if err != nil {
			// The publisher is gone. The caller tears the stream down so a
			// player does not keep reading a stream that will never advance.
			return nil
		}
		if anchor.IsZero() {
			if !u.PTS.IsZero() {
				anchor = u.PTS
			} else {
				anchor = time.Now().UTC()
			}
		}
		err = s.writeRTP(p, anchor, u)
		// Release runs before the write returns, never as a defer: a defer in a
		// loop body runs at return, which would pin every payload buffer for the
		// life of the session.
		u.Release()
		if err != nil {
			return err
		}
	}
}

// writeRTP packs one kernel unit into RTP payloads and writes them.
//
// The packer returns only payloads, so the marker bit is set here on the last
// packet of an access unit: the h264 packer fragments an oversized NAL, and
// the marker is what tells the receiver the unit is complete.
func (s *Server) writeRTP(p *playStream, anchor time.Time, u *stream.Unit) error {
	if p == nil || p.st == nil || p.st.Desc == nil {
		return errors.New("rtsp: no stream")
	}
	for _, medi := range p.st.Desc.Medias {
		pm := p.byMedia[medi]
		if pm == nil || pm.track == nil || pm.track.Codec != u.Codec {
			continue
		}
		return s.writePlay(p, medi, pm, u, anchor)
	}
	return fmt.Errorf("rtsp: no media for codec %s", u.Codec)
}

// writePlay writes one unit for one media, using the codec module's own packer.
//
// Timestamps are re-derived at the media's real clock rate rather than taken
// from the unit's absolute instant: gortsplib applies its own SSRC and NTP
// bookkeeping, and feeding it an absolute instant would let the receiver
// compare it against its own epoch.
func (s *Server) writePlay(p *playStream, medi *description.Media, pm *playMedia, u *stream.Unit, anchor time.Time) error {
	pc, err := registry.SelectCodec(u.Codec)
	if err != nil {
		return fmt.Errorf("rtsp: no codec %s: %w", u.Codec, err)
	}
	packer, err := pc.NewRTPPacker()
	if err != nil {
		return err
	}
	// The media timestamp comes from the unit's PTS at this media's own clock
	// rate. The packer does not advance the clock for the caller: both
	// implementations return the timestamp they were given, so taking the next
	// frame's clock from the previous return value stamps every frame with the
	// first frame's value. A receiver whose clock never moves has nothing to
	// pace against, which reads as a stream that starts and then freezes.
	ts := container.ToTransportHz(u.PTS, uint32(pm.track.Timescale))
	// The inter-fragment offset is on the same clock. It matters only when a
	// unit spans more than one packet, and the unit duration is the measure of
	// how long it occupies.
	dur := container.DurationToTransportHz(u.Duration, uint32(pm.track.Timescale))
	payloads, seq, _ := packer.Pack(u, pm.seq, ts)
	if len(payloads) == 0 {
		return fmt.Errorf("rtsp: empty packetization for codec %s", u.Codec)
	}
	ntp := anchor.Add(u.PTS.Sub(anchor)).UTC()
	// The payload type must match the one in this stream's own description: the
	// library finds the per-media writer by that value, and a wrong one is a nil
	// dereference inside gortsplib rather than an error. SSRC is left unset, the
	// library replaces it with the media's local SSRC.
	for i, pl := range payloads {
		pkt := &rtp.Packet{
			Header:  rtp.Header{Version: 2, PayloadType: pm.pt, SequenceNumber: seq + uint16(i), Timestamp: ts + uint32(i)*dur, Marker: i == len(payloads)-1},
			Payload: pl,
		}
		if err := p.st.WritePacketRTPWithNTP(medi, pkt, ntp); err != nil {
			return err
		}
	}
	pm.seq = seq
	return nil
}

// --- registry.Adapter ------------------------------------------------------

// ModuleInfo implements registry.Adapter.
func (a *Adapter) ModuleInfo() registry.ModuleInfo {
	return registry.ModuleInfo{
		Name: "rtsp", Version: Version, Type: registry.TAdapter,
		MinKernel: "0.1.0", Priority: 100, Dir: "adapters/rtsp",
	}
}

// Schemes implements registry.Adapter.
func (a *Adapter) Schemes() []string { return []string{"rtsp", "rtsps"} }

// SupportsScheme implements registry.Adapter.
func (a *Adapter) SupportsScheme(scheme string) bool {
	return scheme == "rtsp" || scheme == "rtsps"
}

// CanPublish implements registry.Adapter.
func (a *Adapter) CanPublish() bool { return true }

// CanPlay implements registry.Adapter.
func (a *Adapter) CanPlay() bool { return true }

func init() { registry.Register(&Adapter{}) }

// Adapter is one RTSP session's registry adapter. The Server owns the state;
// this instance carries only what a single negotiation needs.
type Adapter struct {
	remote string
	srv    *Server

	// writer is the kernel writer opened by Publish, stored so OnRecord can
	// reach it after the ANNOUNCE response has gone back to the client.
	writer stream.StreamWriter
	// byMedia pairs each announced media with its track and unpacker.
	byMedia map[*description.Media]*pubMedia
}

// Publish implements registry.Adapter for one RTSP publisher.
//
// It runs inside Manager.Publish, which is called from OnAnnounce, so the SDP
// that announced the tracks is already in this adapter's byMedia table. The
// writer it opens here is returned to OnAnnounce by reference.
func (a *Adapter) Publish(ctx context.Context, s registry.PublishSession) (registry.Source, error) {
	tracks := a.tracksFromMedia()
	if len(tracks) == 0 {
		return nil, errors.New("rtsp: no tracks to publish")
	}
	w, err := s.Begin(tracks)
	if err != nil {
		return nil, err
	}
	a.writer = w

	src := adapters.NewSession(a.remote, nil)
	go func() {
		select {
		case <-ctx.Done():
			src.End(ctx.Err())
		case <-src.Done():
		}
	}()
	return src, nil
}

// tracksFromMedia returns the track table this adapter negotiated.
func (a *Adapter) tracksFromMedia() []*stream.Track {
	out := make([]*stream.Track, 0, len(a.byMedia))
	for _, pm := range a.byMedia {
		out = append(out, pm.track)
	}
	return out
}

// Play implements registry.Adapter for one RTSP player.
//
// Reaching this path means a client entered through the kernel's Manager rather
// than through the RTSP control connection, so the push loop and SDP are still
// driven by this package and the subscription comes from the session.
func (a *Adapter) Play(ctx context.Context, s registry.PlaySession) (registry.Sink, error) {
	if a.srv == nil {
		return nil, errors.New("rtsp: no server")
	}
	sub, err := s.Subscribe(ctx)
	if err != nil {
		return nil, err
	}
	st, err := a.srv.startPlay(s.Request().Path)
	if err != nil {
		sub.Cancel()
		return nil, err
	}
	sink := adapters.NewSession(a.remote, nil)
	sink.SetSub(sub)
	_ = st
	go func() {
		<-a.srv.playDone(s.Request().Path)
		sink.End(nil)
	}()
	return sink, nil
}

// --- codec mapping ---------------------------------------------------------

// kernelTracks maps an announced SDP into a kernel track table. It also stores
// the per-media unpackers, since the same description is what OnRecord unpacks
// against.
func (a *Adapter) kernelTracks(desc *description.Session) (map[*description.Media]*pubMedia, error) {
	out := make(map[*description.Media]*pubMedia)
	for _, medi := range desc.Medias {
		for _, forma := range medi.Formats {
			var kt *stream.Track
			var clockRate uint32
			switch f := forma.(type) {
			case *gsp.H264:
				if len(f.SPS) == 0 || len(f.PPS) == 0 {
					return nil, errors.New("rtsp: h264 missing sps/pps")
				}
				kt = &stream.Track{
					Codec: codecH264, Kind: stream.KindVideo, Timescale: container.Timescale,
					Params: map[string]string{
						"sps": hex.EncodeToString(f.SPS),
						"pps": hex.EncodeToString(f.PPS),
					},
				}
				clockRate = container.Timescale
			case *gsp.MPEG4Audio:
				if f.Config == nil || f.Config.SampleRate == 0 {
					return nil, errors.New("rtsp: aac missing config")
				}
				kt = &stream.Track{
					Codec: codecAAC, Kind: stream.KindAudio, Timescale: uint64(f.Config.SampleRate),
					Params: map[string]string{
						"sampleRate":       strconv.Itoa(f.Config.SampleRate),
						"numberOfChannels": strconv.Itoa(int(f.Config.ChannelConfig)),
					},
				}
				clockRate = uint32(f.Config.SampleRate)
			default:
				return nil, fmt.Errorf("rtsp: unsupported format %T", forma)
			}
			pc, err := registry.SelectCodec(kt.Codec)
			if err != nil {
				return nil, err
			}
			up, err := pc.NewRTPUnpacker()
			if err != nil {
				return nil, err
			}
			out[medi] = &pubMedia{unpacker: up, track: kt, clockRate: clockRate}
		}
	}
	if len(out) == 0 {
		return nil, errors.New("rtsp: no tracks")
	}
	return out, nil
}

// sdpFor builds the SDP a player receives for a track table.
func sdpFor(tracks []*stream.Track) (*description.Session, error) {
	s := &description.Session{Title: "QuickMedia"}
	for _, t := range tracks {
		m := &description.Media{Type: description.MediaTypeVideo}
		switch t.Codec {
		case codecH264:
			sps, err := hex.DecodeString(t.Params["sps"])
			if err != nil || len(sps) == 0 {
				return nil, fmt.Errorf("rtsp: bad sps: %w", err)
			}
			pps, err := hex.DecodeString(t.Params["pps"])
			if err != nil || len(pps) == 0 {
				return nil, fmt.Errorf("rtsp: bad pps: %w", err)
			}
			m.Formats = []gsp.Format{&gsp.H264{
				PayloadTyp: videoPayloadTyp, SPS: sps, PPS: pps, PacketizationMode: 1,
			}}
		case codecAAC:
			rate, _ := strconv.Atoi(t.Params["sampleRate"])
			ch, _ := strconv.Atoi(t.Params["numberOfChannels"])
			if rate == 0 {
				return nil, errors.New("rtsp: aac track missing sampleRate")
			}
			m.Type = description.MediaTypeAudio
			m.Formats = []gsp.Format{&gsp.MPEG4Audio{
				PayloadTyp:     audioPayloadTyp,
				ProfileLevelID: 1,
				// sizelength is mandatory in an mpeg4-generic fmtp: it tells a
				// receiver how many bits the AU length field occupies. Omitting
				// it makes the whole media line unparsable, which is how a
				// stream with valid audio can fail to play with no error in the
				// log. 13 covers every AU size the packer can emit.
				SizeLength: 13,
				Config: &mpeg4audio.AudioSpecificConfig{
					Type:          mpeg4audio.ObjectTypeAACLC,
					SampleRate:    rate,
					ChannelConfig: uint8(ch),
				},
			}}
		default:
			return nil, fmt.Errorf("rtsp: unsupported codec %s", t.Codec)
		}
		s.Medias = append(s.Medias, m)
	}
	return s, nil
}

// pathNameFrom normalizes the path a request carries. The library hands back a
// URL whose Path keeps its leading slash, which is what makes RTSP differ from
// RTMP in this respect and the one place the mismatch lived: leaving it in
// would make /live/stream a name the kernel's validator refuses, and every
// publisher would get a 406 back for a stream that was fine everywhere else.
func pathNameFrom(raw string) string {
	return strings.TrimPrefix(raw, "/")
}
