// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package httpflv is the HTTP-FLV protocol adapter: live FLV over a plain HTTP
// GET, with no upgrade and no chunked transfer.
//
// It is the one protocol in this tree with no upstream library, which follows
// from its shape. HTTP-FLV is a FLV tag stream behind a nine-byte file header
// and nothing negotiable above the transport, so there is no handshake to hand
// to a library and no framing state of its own. The container layer owns the
// tag format; this adapter owns request routing, descriptor emission, and
// disconnect detection.
//
// Because the wire is a request and not a connection, the session's lifetime
// is the request context. The handler blocks for the life of the stream, which
// is how every HTTP-FLV implementation behaves: net/http cancels the context
// when the client drops, and ctx-cancelled reads are the disconnect signal.
// They are preferred over watching Write, which can succeed into a buffer
// after the client is already gone.
//
// The streaming loop runs inside the handler's own stack. Returning from it
// would make net/http consider the response finished and close the connection,
// so a background writer would race the connection's own teardown.
package httpflv

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yejinlei/quickmedia/adapters"
	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/container/aac"
	"github.com/yejinlei/quickmedia/kernel/path"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Version is this module's version.
const Version = "0.1.0"

const (
	codecH264 stream.CodecID = "h264"
	codecAAC  stream.CodecID = "aac"

	flvTagVideo byte = 0x09
	flvTagAudio byte = 0x08

	// flvSuffix identifies a live FLV request in the URL path.
	flvSuffix = ".flv"
	// tagLimit is the largest FLV tag body. A unit over it is dropped rather
	// than split: split tags share one timestamp, which a tag-level reader with
	// no reassembly state treats as several frames.
	tagLimit = 16383
	// tagOffset is the first-tag timestamp. A late join must not report a
	// timestamp measured from the epoch, for the same reason the container
	// layer applies it to file output.
	tagOffset = 30 * 1000
)

// header is the FLV file header: magic, version, the file-type-flags byte, then
// the initial PreviousTagSize of nine, which is the header's own byte length.
//
// The trailing zero tag and the size of nine are where this header is usually
// wrong. A player computes its read offset from PreviousTagSize, so writing
// zero shifts every read by nine bytes: the stream parses up to the second tag
// and then fails with a decode error that gives no hint at the cause.
var header = []byte{'F', 'L', 'V', 0x01, 0x00, 0, 0, 0, 9, 0, 0, 0, 0}

// Server serves live FLV for the paths one kernel manager knows.
type Server struct{ mgr *path.Manager }

// NewServer returns the http.Handler. It is an http.Handler rather than a
// net.Listener so the caller can mount it under any mux and behind any TLS
// terminator, which is the same constraint the kernel places on L1.
func NewServer(m *path.Manager) http.Handler { return &Server{mgr: m} }

// ServeHTTP runs one live FLV request.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), flvSuffix)
	if name == "" {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	ad := &Adapter{
		remote: r.RemoteAddr,
		rw:     w,
		// r.Context() is the whole of the session's lifetime: the client going
		// away cancels it, and Subscribe and ReadUnit both observe it.
		ctx: r.Context(),
	}
	if _, err := s.mgr.Play(r.Context(), ad, registry.SrcRequest{
		Path:   name,
		Query:  r.URL.RawQuery,
		Remote: r.RemoteAddr,
		// Zero means wait for the request context, the right bound for a client
		// that connects before its source is published: it keeps waiting until
		// it gives up rather than learning the path is empty.
		SubscribeTimeout: 0,
	}); err != nil {
		http.Error(w, fmt.Sprintf("stream unavailable: %v", err), http.StatusNotFound)
	}
}

// --- registry.Adapter ------------------------------------------------------

// ModuleInfo implements registry.Adapter.
func (a *Adapter) ModuleInfo() registry.ModuleInfo {
	return registry.ModuleInfo{
		Name: "http-flv", Version: Version, Type: registry.TAdapter,
		MinKernel: "0.1.0", Priority: 100, Dir: "adapters/httpflv",
	}
}

// Schemes implements registry.Adapter.
func (a *Adapter) Schemes() []string { return []string{"http", "https"} }

// SupportsScheme implements registry.Adapter.
func (a *Adapter) SupportsScheme(scheme string) bool {
	return scheme == "http" || scheme == "https"
}

// CanPublish implements registry.Adapter. HTTP-FLV carries a live stream to a
// player and nothing back, so the refusal belongs here rather than in Play.
func (a *Adapter) CanPublish() bool { return false }

// CanPlay implements registry.Adapter.
func (a *Adapter) CanPlay() bool { return true }

func init() { registry.Register(&Adapter{}) }

// Adapter is the HTTP-FLV registry adapter, one instance per request.
//
// It carries the response writer and the request context beside the remote
// address because PlaySession exposes only path and codec data: the kernel
// hands an adapter a session, never an HTTP response. Holding them here is
// what lets the listener and a test each construct an Adapter without
// reaching into the other.
type Adapter struct {
	remote string
	rw     http.ResponseWriter
	ctx    context.Context
}

// Publish implements registry.Adapter.
func (a *Adapter) Publish(context.Context, registry.PublishSession) (registry.Source, error) {
	return nil, errors.New("http-flv: publishing is not supported")
}

// Play implements registry.Adapter. It blocks, streaming until the client
// disconnects, the publisher is gone, or the kernel closes the stream.
//
// The sink is built before the loop and is ended when it leaves, because the
// kernel's Play observes sink.Done() to reap the subscription. Returning a nil
// sink would defer that to the handler exiting, which drops the reap entirely.
func (a *Adapter) Play(ctx context.Context, s registry.PlaySession) (registry.Sink, error) {
	if a.rw == nil {
		return nil, errors.New("httpflv: no response writer")
	}
	sub, err := s.Subscribe(ctx)
	if err != nil {
		return nil, err
	}

	// Tracks is non-nil only once a publisher exists, which Subscribe has now
	// guaranteed. Checking it before subscribing would report a healthy path as
	// empty to a client that connected ahead of its source.
	tm, err := newTrackMap(s.Tracks(), s.CodecParams())
	if err != nil {
		return nil, err
	}

	sink := adapters.NewSession(a.remote, nil)
	sink.SetSub(sub)

	flusher, _ := a.rw.(http.Flusher)
	a.rw.Header().Set("Content-Type", "video/x-flv")
	a.rw.Header().Set("Cache-Control", "no-store")
	// No Transfer-Encoding and no chunked marker: some players see either and
	// treat the stream as a file download. Their absence is what marks this
	// stream as live.
	a.rw.Header().Set("Connection", "close")
	if _, err := a.rw.Write(header); err != nil {
		sink.End(nil)
		return sink, nil
	}
	doFlush(flusher)

	// The codec descriptor goes out ahead of the first frame of its codec, and
	// it carries that frame's timestamp. Writing it at zero makes a player that
	// keys off the tag timestamp render the first frame early.
	seen := make(map[stream.CodecID]struct{})
	var anchor time.Time

	for {
		u, err := sub.ReadUnit(ctx)
		if err != nil {
			// Normal termination: the client went away or the publisher did.
			// The path must survive a player disconnecting, so this is not an
			// error and the sink ends clean.
			sink.End(nil)
			return sink, nil
		}
		id := tm.codecOf(u)
		if id == "" {
			u.Release()
			continue
		}
		if anchor.IsZero() {
			if !u.PTS.IsZero() {
				anchor = u.PTS
			} else {
				anchor = time.Now().UTC()
			}
		}
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			if cf := tm.configOf(id); cf != nil {
				if _, werr := a.rw.Write(container.EncodeTag(
					tm.tagTypeOf(id), tagTimestamp(anchor, u.DTS),
					uint32(len(cf.Data)), cf.Data)); werr != nil {
					u.Release()
					sink.End(nil)
					return sink, nil
				}
				doFlush(flusher)
			}
		}
		tags := tm.pack(u, anchor)
		// Release precedes the write rather than running as a defer. A defer
		// inside a loop body runs when the function returns, which would hold
		// every payload buffer for the life of the session.
		u.Release()
		for _, tg := range tags {
			if _, err := a.rw.Write(container.EncodeTag(
				tg.tagType, tg.ts, uint32(len(tg.Data)), tg.Data)); err != nil {
				sink.End(nil)
				return sink, nil
			}
		}
		doFlush(flusher)
	}
}

// --- descriptor and tag building ------------------------------------------

// trackMap holds the per-codec state a play session needs.
//
// It holds one video and one audio codec because that is the stream QuickMedia
// serves today. A second track of the same codec would need a per-track key,
// which is a change to this struct rather than to the protocol.
type trackMap struct {
	hasVideo bool
	hasAudio bool
	sps      []byte
	pps      []byte
	asiof    []byte
}

// newTrackMap derives descriptor state from a path's track table.
func newTrackMap(trks []*stream.Track, params map[stream.CodecID]map[string]string) (*trackMap, error) {
	if len(trks) == 0 {
		return nil, errors.New("http-flv: path has no tracks")
	}
	tm := &trackMap{}
	for _, t := range trks {
		p := t.Params
		if p == nil {
			p = params[t.Codec]
		}
		switch t.Codec {
		case codecH264:
			if tm.hasVideo {
				return nil, errors.New("http-flv: more than one h264 track")
			}
			sps, err := hex.DecodeString(p["sps"])
			if err != nil || len(sps) == 0 {
				return nil, fmt.Errorf("http-flv: h264 track missing sps: %w", err)
			}
			pps, err := hex.DecodeString(p["pps"])
			if err != nil || len(pps) == 0 {
				return nil, fmt.Errorf("http-flv: h264 track missing pps: %w", err)
			}
			tm.hasVideo, tm.sps, tm.pps = true, sps, pps
		case codecAAC:
			if tm.hasAudio {
				return nil, errors.New("http-flv: more than one aac track")
			}
			rate, _ := strconv.Atoi(p["sampleRate"])
			ch, _ := strconv.Atoi(p["numberOfChannels"])
			if rate == 0 {
				return nil, errors.New("http-flv: aac track missing sampleRate")
			}
			asiof, ok := aac.AudioSpecificConfig(rate, uint8(ch))
			if !ok {
				return nil, fmt.Errorf(
					"http-flv: aac rate %d channels %d not representable", rate, ch)
			}
			tm.hasAudio, tm.asiof = true, asiof
		default:
			// Refusing rather than dropping the track is deliberate: a stream
			// missing one codec plays back as video with no audio, which looks
			// to the viewer like a working feed with a broken source.
			return nil, fmt.Errorf("http-flv: unsupported codec %s", t.Codec)
		}
	}
	return tm, nil
}

// codecOf reports which of this session's codecs a unit belongs to. The codec
// field wins; the kind is a fallback for a unit that arrived without it, which
// the kernel can produce when it has to interpolate PTS.
func (tm *trackMap) codecOf(u *stream.Unit) stream.CodecID {
	if u.Codec != "" {
		if id := tm.codecOfID(u.Codec); id != "" {
			return id
		}
	}
	if u.Kind == stream.KindAudio {
		if tm.hasAudio {
			return codecAAC
		}
		return ""
	}
	if tm.hasVideo {
		return codecH264
	}
	return ""
}

func (tm *trackMap) codecOfID(id stream.CodecID) stream.CodecID {
	switch id {
	case codecH264:
		if tm.hasVideo {
			return id
		}
	case codecAAC:
		if tm.hasAudio {
			return id
		}
	}
	return ""
}

// tagTypeOf reports whether a codec's FLV tags are video or audio.
func (tm *trackMap) tagTypeOf(id stream.CodecID) byte {
	if id == codecAAC {
		return flvTagAudio
	}
	return flvTagVideo
}

// configOf returns the codec descriptor, or nil when there is none.
func (tm *trackMap) configOf(id stream.CodecID) *registry.Frame {
	switch id {
	case codecH264:
		if tm.sps != nil && tm.pps != nil {
			f := container.AVCSetupTag(tm.sps, tm.pps)
			return &f
		}
	case codecAAC:
		if tm.asiof != nil {
			f := container.AACSequenceTag(tm.asiof)
			return &f
		}
	}
	return nil
}

// tagOut is one FLV tag with its type and timestamp resolved.
type tagOut struct {
	tagType byte
	ts      uint32
	Data    []byte
}

// pack turns one unit into FLV tag bodies.
func (tm *trackMap) pack(u *stream.Unit, anchor time.Time) []tagOut {
	w := container.NewFLVWriterAt(time.Time{})
	frames, err := w.Pack(u)
	if err != nil {
		return nil
	}
	tt := tm.tagTypeOf(tm.codecOf(u))
	ts := tagTimestamp(anchor, u.DTS)
	out := make([]tagOut, 0, len(frames))
	for _, f := range frames {
		if len(f.Data) == 0 || len(f.Data) > tagLimit {
			continue
		}
		out = append(out, tagOut{tagType: tt, ts: ts, Data: f.Data})
	}
	return out
}

// tagTimestamp converts an absolute instant into an FLV tag timestamp.
func tagTimestamp(anchor, t time.Time) uint32 {
	if t.IsZero() || anchor.IsZero() {
		return tagOffset
	}
	ms := t.Sub(anchor).Milliseconds() + tagOffset
	if ms < 0 {
		ms = 0
	}
	return uint32(ms)
}

func doFlush(f http.Flusher) {
	if f != nil {
		f.Flush()
	}
}
