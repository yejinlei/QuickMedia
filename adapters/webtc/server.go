// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package webtc is the WebRTC adapter, covering the WHIP and WHEP signaling
// endpoints.
//
// It reuses github.com/pion/webrtc for everything that is protocol state rather
// than media: SDP negotiation, ICE, DTLS, SRTP and the RTCP pipeline. What is
// QuickMedia's own is the mapping between a negotiated media section and a
// kernel track table, and the translation between the kernel's absolute instants
// and a per-connection RTP clock.
//
// WHIP and WHEP each exchange one SDP round trip, so the answer carries every
// ICE candidate and ICE gathering completes before it is written. The client
// waits for the HTTP response, which is what makes the non-trickle path usable.
//
// --- Negotiation runs in the adapter ---
//
// The kernel owns the negotiation call: the HTTP handler builds an Adapter,
// hands its offer to the kernel through Manager, and reads the answer back out
// afterwards. The offer and the answer live on the Adapter, the way RTSP carries
// its announced SDP: they are per-connection state, not server state, and they
// have to be reachable from both sides of the round trip.
//
// --- Browser reachability ---
//
// A browser plays only codecs it is willing to negotiate, which for this
// deployment is H.264 for video and Opus for audio. There is no implicit
// transcoding, so a path whose codec set has no common profile with that pair is
// refused with stream.ErrNoCommonProfile, which names the codecs on both sides.
// That is a harder failure than a stream that plays badly: a refused player can
// switch paths, and a silently downgraded one cannot say so.
//
// Two refusals are worth naming because they are the ones operators hit. H.264
// with B frames is refused, because browsers decode it but cannot reorder
// pictures. H.265 is refused outright, because most browsers have no decoder.
// Neither is fixed by the packer; both are fixed at the source.

package webtc

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/yejinlei/quickmedia/adapters"
	"github.com/yejinlei/quickmedia/kernel/path"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Version is this module's version.
const Version = "0.1.0"

// --- codec identity and parameters -----------------------------------------

const (
	codecH264 stream.CodecID = "h264"
	codecOpus stream.CodecID = "opus"

	paramSPS = "sps"
	paramPPS = "pps"

	opusRate = 48000
	h264Rate = 90000
	streamID = "quickmedia"

	trackVideo = "video"
	trackAudio = "audio"

	opusFmtp = "minptime=10;useinbandfec=1"
)

// --- options ----------------------------------------------------------------

// Options is the server configuration. Zero values select the defaults.
type Options struct {
	// WHIPPath is the publish endpoint. It is also the URL the server tells a
	// publisher to DELETE in order to end its own session, so it must be
	// reachable.
	WHIPPath string
	// WHEPPath is the play endpoint.
	WHEPPath string
	// STUNServer is an optional STUN URL. Empty means host candidates only,
	// which is how the endpoints are tested: a loopback connectivity check needs
	// no STUN, and a public server turns a local test into a network test.
	STUNServer string
	// MaxPublishers and MaxPlayers bound the session tables.
	MaxPublishers int
	MaxPlayers    int
	// MaxOfferBytes bounds one SDP offer.
	MaxOfferBytes int64
	// ICETimeout bounds ICE gathering.
	ICETimeout time.Duration
	// AuthToken, when non-empty, requires "Authorization: Bearer <token>" on
	// every request. A WHIP DELETE uses the session's own credential instead.
	AuthToken string
}

// --- server -----------------------------------------------------------------

// Server holds the state both endpoints share. It owns no sockets: the
// composition root mounts it on an existing HTTP mux, which is how it shares one
// listener with every other HTTP adapter.
type Server struct {
	mgr    *path.Manager
	opts   Options
	closer chan struct{}

	mu         sync.Mutex
	publishers map[string]*publisherSession
	players    map[string]*playerSession
	tokens     map[string]string
	seq        uint32
}

// NewServer builds a server against the kernel manager.
func NewServer(mgr *path.Manager, opts Options) *Server {
	if opts.WHIPPath == "" {
		opts.WHIPPath = "/whip"
	}
	if opts.WHEPPath == "" {
		opts.WHEPPath = "/whep"
	}
	if opts.MaxPublishers == 0 {
		opts.MaxPublishers = 1024
	}
	if opts.MaxPlayers == 0 {
		opts.MaxPlayers = 4096
	}
	if opts.MaxOfferBytes == 0 {
		opts.MaxOfferBytes = 512 << 10
	}
	if opts.ICETimeout == 0 {
		opts.ICETimeout = 5 * time.Second
	}
	return &Server{
		mgr: mgr, opts: opts, closer: make(chan struct{}),
		publishers: make(map[string]*publisherSession),
		players:    make(map[string]*playerSession),
		tokens:     make(map[string]string),
	}
}

// Close tears down every live session. It is idempotent. The tables are
// swapped out under the lock and the sessions closed after it is released,
// because teardown calls remove, which takes the same lock.
func (s *Server) Close() {
	select {
	case <-s.closer:
	default:
		close(s.closer)
	}
	s.mu.Lock()
	pubs, plays := s.publishers, s.players
	s.publishers = make(map[string]*publisherSession)
	s.players = make(map[string]*playerSession)
	s.tokens = make(map[string]string)
	s.mu.Unlock()
	for _, ps := range pubs {
		ps.Close()
	}
	for _, pl := range plays {
		pl.Close()
	}
}

// Handle mounts both endpoints on mux. Each path is registered with and without
// a trailing slash: a mux pattern without one matches only the exact path.
func (s *Server) Handle(mux *http.ServeMux) {
	mux.Handle(s.opts.WHEPPath, s)
	mux.Handle(s.opts.WHEPPath+"/", s)
	mux.Handle(s.opts.WHIPPath, s)
	mux.Handle(s.opts.WHIPPath+"/", s)
}

// Count reports how many sessions each direction holds.
func (s *Server) Count() (publishers, players int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.publishers), len(s.players)
}

// --- sessions --------------------------------------------------------------

// newPublisher builds an empty WHIP session and registers it.
func (s *Server) newPublisher(ctx context.Context, p, remote string) *publisherSession {
	ps := newPublisherSession(ctx, s, p, remote)
	ps.id = s.nextID("publish")
	s.mu.Lock()
	s.publishers[ps.id] = ps
	s.mu.Unlock()
	return ps
}

// newPlayer builds an empty WHEP session and registers it.
func (s *Server) newPlayer(ctx context.Context, p, remote string) *playerSession {
	pl := newPlayerSession(ctx, s, p, remote)
	pl.id = s.nextID("play")
	s.mu.Lock()
	s.players[pl.id] = pl
	s.mu.Unlock()
	return pl
}

// remove drops one session from both tables and its token.
func (s *Server) remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.publishers, id)
	delete(s.players, id)
	delete(s.tokens, id)
}

// --- registry.Adapter ------------------------------------------------------

// Adapter is one WebRTC session. It carries the offer that started it and, after
// the kernel call, the answer it produced, because both live outside the kernel
// and neither belongs to the server.
type Adapter struct {
	remote string
	srv    *Server
	offer  []byte

	// local is the negotiated answer, filled by Play and Publish.
	local *webrtc.SessionDescription
}

// ModuleInfo implements registry.Adapter.
func (a *Adapter) ModuleInfo() registry.ModuleInfo {
	return registry.ModuleInfo{
		Name: "webrtc", Version: Version, Type: registry.TAdapter,
		MinKernel: "0.1.0", Priority: 100, Dir: "adapters/webtc",
	}
}

// Schemes implements registry.Adapter.
func (a *Adapter) Schemes() []string { return []string{"webrtc", "whep", "whip"} }

// SupportsScheme implements registry.Adapter.
func (a *Adapter) SupportsScheme(scheme string) bool {
	switch scheme {
	case "webrtc", "whep", "whip":
		return true
	}
	return false
}

// CanPublish implements registry.Adapter.
func (a *Adapter) CanPublish() bool { return true }

// CanPlay implements registry.Adapter.
func (a *Adapter) CanPlay() bool { return true }

func init() { registry.Register(&Adapter{}) }

// Publish owns one WHIP publisher's whole lifetime: it negotiates the answer,
// declares the offer's tracks through the kernel, and returns the source the
// kernel will hold. The offer arrives in RTP only, so the track table is derived
// from the SDP rather than read from the source.
func (a *Adapter) Publish(ctx context.Context, s registry.PublishSession) (registry.Source, error) {
	if a.srv == nil {
		return nil, errors.New("webrtc: no server")
	}
	trks, err := sdpTracks(a.offer)
	if err != nil {
		return nil, err
	}

	ps := a.srv.newPublisher(ctx, s.Request().Path, a.remote)
	defer func() {
		// On any error before the answer is returned the session is not live.
		if ps.pc != nil {
			_ = ps.pc.Close()
		}
	}()
	if err := ps.negotiateRecv(trks, a.offer); err != nil {
		return nil, err
	}

	// Begin assigns the track ids; bind needs them, so the call order matters.
	w, err := s.Begin(trks)
	if err != nil {
		return nil, err
	}
	ps.writer = w
	if err := ps.bind(trks); err != nil {
		return nil, err
	}

	sess := adapters.NewSession(a.remote, func() { ps.end(stream.ErrEOF) })
	ps.sess = sess
	go a.srv.watchSource(ctx, sess, ps)

	a.local = ps.local
	return sess, nil
}

// Play owns one WHEP viewer's whole lifetime. The kernel's session is only
// valid for the duration of this call, so the tracks are captured here and
// handed to the session that outlives it.
func (a *Adapter) Play(ctx context.Context, s registry.PlaySession) (registry.Sink, error) {
	if a.srv == nil {
		return nil, errors.New("webrtc: no server")
	}
	sub, err := s.Subscribe(ctx)
	if err != nil {
		return nil, err
	}
	trks := cloneTracks(s.Tracks())

	pl := a.srv.newPlayer(ctx, s.Request().Path, a.remote)
	defer func() {
		if pl.pc != nil {
			_ = pl.pc.Close()
		}
	}()
	if err := pl.negotiateSend(trks, a.offer); err != nil {
		sub.Cancel()
		return nil, err
	}

	sess := adapters.NewSession(a.remote, func() { pl.end(nil) })
	sess.SetSub(sub)
	pl.sess = sess
	go a.srv.watchSink(ctx, sess, pl, sub)

	a.local = pl.local
	return sess, nil
}

// watchSource ends a publish session when the kernel drops it, or when the
// context is canceled. Whichever side notices first wins; the Done channel is
// what the kernel observes, so a dead peer shows up as a closed source.
func (s *Server) watchSource(ctx context.Context, sess *adapters.Session, ps *publisherSession) {
	select {
	case <-ctx.Done():
		ps.end(ctx.Err())
	case <-sess.Done():
		ps.end(sess.Err())
	}
}

// watchSink pulls the subscription into the viewer and watches for the session
// ending. The kernel reaps the subscription on Close, so a browser that closes
// its tab cannot leave a ring filling in the background.
func (s *Server) watchSink(ctx context.Context, sess *adapters.Session, pl *playerSession, sub stream.Subscription) {
	go pl.readLoop(sub)
	select {
	case <-ctx.Done():
		pl.end(ctx.Err())
	case <-sess.Done():
		pl.end(sess.Err())
	}
}

// --- http.ServeHTTP --------------------------------------------------------

// ServeHTTP dispatches one WHIP or WHEP request.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.cors(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !s.authed(w, r) {
		return
	}

	switch r.URL.Path {
	case s.opts.WHEPPath, s.opts.WHEPPath + "/":
		s.whep(w, r)
	case s.opts.WHIPPath, s.opts.WHIPPath + "/":
		s.whip(w, r)
	default:
		http.NotFound(w, r)
	}
}

// authed reports whether the request carries a usable credential.
func (s *Server) authed(w http.ResponseWriter, r *http.Request) bool {
	want := strings.TrimSpace(s.opts.AuthToken)
	if want == "" {
		return true
	}
	if bearer(r) != want {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	if v, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func (s *Server) cors(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "POST, DELETE, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	h.Set("Access-Control-Expose-Headers", "Location")
}

// readOffer reads a bounded body and the path it names.
func (s *Server) readOffer(r *http.Request) ([]byte, string, error) {
	if r.Body == nil {
		return nil, "", errors.New("missing body")
	}
	defer func() { _ = r.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(r.Body, s.opts.MaxOfferBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("read offer: %w", err)
	}
	if int64(len(body)) > s.opts.MaxOfferBytes {
		return nil, "", errors.New("offer too large")
	}
	if strings.TrimSpace(string(body)) == "" {
		return nil, "", errors.New("empty offer")
	}
	return body, pathFrom(r.URL.Query().Get("path")), nil
}

// pathFrom keeps the URL's leading slash off the kernel, whose path names never
// carry one.
func pathFrom(p string) string { return strings.TrimPrefix(strings.TrimSpace(p), "/") }

// writeAnswer sends the SDP the WHIP and WHEP specs require: 201 with a
// Location header naming the session endpoint.
func writeAnswer(w http.ResponseWriter, answer *webrtc.SessionDescription, loc string) {
	w.Header().Set("Location", loc)
	w.Header().Set("Content-Type", "application/sdp")
	w.WriteHeader(http.StatusCreated)
	_, _ = io.WriteString(w, answer.SDP)
}

// noCommon wraps a refusal in the kernel's error and names every codec that
// took part, so a client can say which one caused the miss.
func noCommon(why string, codes []stream.CodecID) error {
	var names []string
	for _, c := range codes {
		names = append(names, string(c))
	}
	sort.Strings(names)
	return fmt.Errorf("%w: have %v — %s", stream.ErrNoCommonProfile, names, why)
}

func (s *Server) failPlay(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, stream.ErrNoCommonProfile):
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
	case errors.Is(err, path.ErrNoSuchPath):
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) failPublish(w http.ResponseWriter, err error) {
	if errors.Is(err, stream.ErrNoCommonProfile) {
		http.Error(w, err.Error(), http.StatusUnsupportedMediaType)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// --- WHEP (playback) -------------------------------------------------------

func (s *Server) whep(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "whep: POST only", http.StatusMethodNotAllowed)
		return
	}
	offer, name, err := s.readOffer(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if name == "" {
		http.Error(w, "whep: missing ?path=", http.StatusBadRequest)
		return
	}

	ad := &Adapter{remote: r.RemoteAddr, srv: s, offer: offer}
	sink, err := s.mgr.Play(r.Context(), ad, registry.SrcRequest{Path: name, Remote: r.RemoteAddr})
	if err != nil {
		s.failPlay(w, err)
		return
	}
	if ad.local == nil {
		_ = sink.Close()
		http.Error(w, "webrtc: no answer produced", http.StatusInternalServerError)
		return
	}
	writeAnswer(w, ad.local, s.opts.WHEPPath)
}

// --- WHIP (publish) --------------------------------------------------------

func (s *Server) whip(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodOptions:
		w.WriteHeader(http.StatusNoContent)
		return
	case http.MethodDelete:
		s.endSession(w, r)
		return
	case http.MethodPost:
	default:
		http.Error(w, "whip: POST or DELETE only", http.StatusMethodNotAllowed)
		return
	}

	offer, name, err := s.readOffer(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if name == "" {
		http.Error(w, "whip: missing ?path=", http.StatusBadRequest)
		return
	}

	// After the server has issued a session credential a publisher may use it
	// instead of the server-wide token. That is what lets a client end its own
	// session: it cannot have known the id in advance.
	if tok := bearer(r); tok != "" && tok != strings.TrimSpace(s.opts.AuthToken) {
		if _, ok := s.sessionOf(tok); !ok {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	ad := &Adapter{remote: r.RemoteAddr, srv: s, offer: offer}
	src, err := s.mgr.Publish(r.Context(), ad, registry.SinkRequest{Path: name, Remote: r.RemoteAddr})
	if err != nil {
		s.failPublish(w, err)
		return
	}
	if ad.local == nil {
		_ = src.Close()
		http.Error(w, "webrtc: no answer produced", http.StatusInternalServerError)
		return
	}
	ps, err := s.lookupPublisher(src)
	if err != nil {
		_ = src.Close()
		http.Error(w, "webrtc: session not registered", http.StatusInternalServerError)
		return
	}
	writeAnswer(w, ad.local, s.opts.WHIPPath+"/"+s.credential(ps.id))
}

// endSession stops one publish session, addressed by its own credential.
func (s *Server) endSession(w http.ResponseWriter, r *http.Request) {
	id, ok := s.sessionOf(bearer(r))
	if !ok {
		w.Header().Set("WWW-Authenticate", "Bearer")
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	s.mu.Lock()
	ps := s.publishers[id]
	delete(s.publishers, id)
	delete(s.tokens, id)
	s.mu.Unlock()
	if ps != nil {
		ps.Close()
	}
	w.WriteHeader(http.StatusNoContent)
}

// lookupPublisher finds the session that owns a source the kernel returned.
func (s *Server) lookupPublisher(src registry.Source) (*publisherSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ps := range s.publishers {
		if ps.sess == src {
			return ps, nil
		}
	}
	return nil, errors.New("webrtc: source has no session")
}

// --- ICE configuration -----------------------------------------------------

// iceConfig builds the ICE server list. An empty list makes a peer host-only,
// which is the correct default for clients that connect to this server directly
// and the only configuration that works in a sandboxed test.
func (s *Server) iceConfig() webrtc.Configuration {
	cfg := webrtc.Configuration{}
	if u := strings.TrimSpace(s.opts.STUNServer); u != "" {
		cfg.ICEServers = []webrtc.ICEServer{{URLs: []string{u}}}
	}
	return cfg
}

// newPeerConnection builds one connection. The default codec set is used rather
// than a hand-built one: it carries Opus and every H.264 profile a browser
// offers, so the answer can echo back whichever variant the client chose. A
// hand-built set with a single profile would reject offers that name any other
// variant, which some browsers do.
func (s *Server) newPeerConnection() (*webrtc.PeerConnection, error) {
	return webrtc.NewPeerConnection(s.iceConfig())
}

// --- helpers ---------------------------------------------------------------

// nextID hands out monotonic ids. They are map keys and connection labels only.
func (s *Server) nextID(prefix string) string {
	s.seq++
	return fmt.Sprintf("%s-%d", prefix, s.seq)
}

// credential issues the per-session token a WHIP publisher uses to end its own
// session. The server-wide token cannot serve that purpose: a client cannot know
// the id in advance.
func (s *Server) credential(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	tok := hex.EncodeToString([]byte("quickmedia-" + id))
	s.tokens[id] = tok
	return tok
}

// sessionOf resolves a credential to the id of the session it names.
func (s *Server) sessionOf(tok string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, t := range s.tokens {
		if t != "" && t == tok {
			return id, true
		}
	}
	return "", false
}

// --- codec helpers ---------------------------------------------------------

// h264ProfileID extracts the profile-level-id from an SPS: profile_idc, then
// the constraint flags, then level_idc.
//
// The fmtp value may or may not carry the NAL header; the profile-level-id
// field is the first three bytes of the body either way, so the header is
// peeled when it is present.
// h264ProfileID renders the profile-level-id fmtp parameter.
//
// profile-level-id is profile_idc, constraint flags, level_idc: the level is
// the third byte, so reading the fourth skips past it. That is the difference
// between "42C002" and "42C01E" for the same stream.
//
// It refuses when the profile byte is not one the spec assigns, which is the
// case for a slice that lost its first byte: 0xc0 0x0a ... would otherwise be
// reported as profile 0x0a, a level of 4.1 and a stream that cannot exist.
func h264ProfileID(sps []byte) (string, bool) {
	if len(sps) < 4 {
		return "", false
	}
	if sps[0] == 0x67 {
		sps = sps[1:]
	}
	if len(sps) < 3 {
		return "", false
	}
	switch sps[0] {
	case 66, 77, 88, 100, 110, 118, 122, 132, 134, 135, 137, 138, 139, 144, 244:
	default:
		return "", false
	}
	return fmt.Sprintf("%02X%02X%02X", sps[0], sps[1], sps[2]), true
}

// hexKeep decodes a base16 codec parameter, yielding nil on failure.
func hexKeep(s string) []byte {
	if s == "" {
		return nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

// atoi parses a codec parameter, defaulting to zero.
func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// cloneTracks snapshots the kernel's track table. The kernel assigns ids to the
// very pointers it hands out, so a caller that keeps its own copy cannot see
// those ids drift under it.
func cloneTracks(in []*stream.Track) []*stream.Track {
	if in == nil {
		return nil
	}
	out := make([]*stream.Track, len(in))
	for i, t := range in {
		if t == nil {
			continue
		}
		p := make(map[string]string, len(t.Params))
		for k, v := range t.Params {
			p[k] = v
		}
		out[i] = &stream.Track{
			ID: t.ID, Codec: t.Codec, Kind: t.Kind,
			Params: p, Bandwidth: t.Bandwidth, Timescale: t.Timescale,
		}
	}
	return out
}

// cloneParams snapshots the kernel's codec parameter table.
func cloneParams(in map[stream.CodecID]map[string]string) map[stream.CodecID]map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[stream.CodecID]map[string]string, len(in))
	for k, v := range in {
		p := make(map[string]string, len(v))
		for kk, vv := range v {
			p[kk] = vv
		}
		out[k] = p
	}
	return out
}
