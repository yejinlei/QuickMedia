// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package rtmp is the RTMP protocol adapter.
//
// It reuses github.com/bluenviron/gortmplib for the wire protocol — handshake,
// chunk framing, AMF0 negotiation, and the per-codec Writer/Reader. What is
// QuickMedia's own is the mapping between a gortmplib codec object and a
// kernel track table, the NAL repackaging, and the translation between RTMP's
// relative clock and the kernel's absolute instants.
//
// RTMP timestamps are 32-bit and signed, so a stream running longer than
// 1 h 7 min 30 s wraps negative. That is a property of the wire format; the
// kernel sees absolute instants and never reasons about the wrap, which is why
// the conversion lives here rather than in L5.
//
// The adapter is constructed once per client connection. The listener passes
// that instance to the kernel's Manager, which calls back into Publish or Play
// with a session; the session supplies the path-side writer or subscription,
// and this adapter supplies the client side. That split is what lets the kernel
// own the path and the adapter own the connection without either knowing the
// other's internals.
package rtmp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	gortmplib "github.com/bluenviron/gortmplib"
	"github.com/bluenviron/gortmplib/pkg/codecs"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"

	"github.com/yejinlei/quickmedia/adapters"
	"github.com/yejinlei/quickmedia/container"
	"github.com/yejinlei/quickmedia/kernel/path"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/kernel/stream"
)

// Version is this module's version.
const Version = "0.1.0"

// Timescale is the transport clock rate for this adapter's codecs.
const Timescale uint64 = 90000

// readTimeout bounds a single read on a client connection. RTMP publishers send
// user-control pings every second or two, so silence for this long means the
// publisher is gone rather than merely quiet.
const readTimeout = 10 * time.Second

// publishAnchorOffset shifts the published clock so the first frame lands at a
// plausible instant rather than the epoch. Every RTMP player treats the first
// tag as the reference, so anchoring at first-frame time keeps the published
// clock aligned with the playback clock.
const publishAnchorOffset = 30 * time.Second

// Codec identifiers this adapter speaks.
const (
	codecH264 stream.CodecID = "h264"
	codecAAC  stream.CodecID = "aac"
)

// Adapter is the RTMP registry adapter. One instance per client connection.
type Adapter struct {
	conn   net.Conn
	sc     *gortmplib.ServerConn
	remote string
}

// ModuleInfo implements registry.Adapter.
func (a *Adapter) ModuleInfo() registry.ModuleInfo {
	return registry.ModuleInfo{
		Name: "rtmp", Version: Version, Type: registry.TAdapter,
		MinKernel: "0.1.0", Priority: 100, Dir: "adapters/rtmp",
	}
}

// Schemes implements registry.Adapter.
func (a *Adapter) Schemes() []string { return []string{"rtmp", "rtmps"} }

// SupportsScheme implements registry.Adapter.
func (a *Adapter) SupportsScheme(scheme string) bool {
	return scheme == "rtmp" || scheme == "rtmps"
}

// CanPublish implements registry.Adapter.
func (a *Adapter) CanPublish() bool { return true }

// CanPlay implements registry.Adapter.
func (a *Adapter) CanPlay() bool { return true }

func init() { registry.Register(&Adapter{}) }

// ServeConn runs one RTMP connection that was already accepted, from the
// handshake through to the end of the session. It blocks.
//
// It does not listen. L1 is the only layer that opens sockets; this adapter
// owns everything from the handshake onward. The composition root accepts the
// connection, decides it is RTMP by the bytes it read first, and hands it here
// — which is also the shape M3's shared port needs, since one accept loop can
// then dispatch to any protocol adapter by the same function.
//
// A connection accepted for the wrong protocol fails here with an error rather
// than being accepted and then refused mid-stream.
func ServeConn(ctx context.Context, conn net.Conn, m *path.Manager) error {
	sc := &gortmplib.ServerConn{RW: conn}
	if err := sc.Initialize(); err != nil {
		return err
	}
	if err := sc.Accept(); err != nil {
		return err
	}
	ad := &Adapter{conn: conn, sc: sc, remote: conn.RemoteAddr().String()}
	// gortmplib hands back a URL whose Path keeps its leading slash. The kernel
	// validates path names, and every other adapter strips it first, so RTMP
	// must too: leaving it in makes /live/test a name nothing else can name.
	name := strings.TrimPrefix(sc.URL.Path, "/")
	if sc.Publish {
		src, err := m.Publish(ctx, ad, registry.SinkRequest{
			Path:   name,
			Query:  sc.URL.RawQuery,
			Remote: ad.remote,
		})
		if err != nil {
			return err
		}
		// Publish returns when the adapter has registered its source, which is
		// before its read loop has pulled a single message. The publisher holds
		// its connection for the life of the stream, so the source is the
		// connection's real lifetime here too; returning early would tear it
		// down mid-handshake.
		<-src.Done()
		return src.Err()
	}
	sink, err := m.Play(ctx, ad, registry.SrcRequest{
		Path:   name,
		Query:  sc.URL.RawQuery,
		Remote: ad.remote,
	})
	if err != nil {
		return err
	}
	// Play returns as soon as the adapter has registered its sink, which is
	// before the adapter's egress goroutine has written a single frame. The
	// manager's own goroutine closes the subscription once the sink is Done,
	// so this is the connection's real lifetime: returning here would hand the
	// listener a connection to close a few microseconds into the stream, and
	// every player would read the setup tags, one frame of media, and EOF.
	<-sink.Done()
	return sink.Err()
}

// --- publish ---------------------------------------------------------------

// Publish implements registry.Adapter for one RTMP publisher.
func (a *Adapter) Publish(ctx context.Context, s registry.PublishSession) (registry.Source, error) {
	r := &gortmplib.Reader{Conn: a.sc}
	if err := r.Initialize(); err != nil {
		return nil, err
	}

	// Declare the track table through the kernel now that the codecs are known.
	// Begin assigns track identifiers and returns the writer; the kernel owns
	// the path, this adapter owns the client.
	ktrk, byTrack, err := toKernelTracks(r.Tracks())
	if err != nil {
		return nil, err
	}
	w, err := s.Begin(ktrk)
	if err != nil {
		return nil, err
	}

	src := adapters.NewSession(a.remote, func() {
		a.conn.SetReadDeadline(time.Time{})
		_ = a.conn.Close()
	})

	// Wire each supported codec to the kernel writer. The map is keyed by the
	// gortmplib track pointer rather than by codec id, so two tracks of the same
	// codec cannot alias onto one another.
	anchor := time.Now().UTC().Add(-publishAnchorOffset)

	for _, t := range r.Tracks() {
		k := byTrack[t]
		switch c := t.Codec.(type) {
		case *codecs.H264:
			r.OnDataH264(t, func(pts, dts time.Duration, au [][]byte) {
				if err := writeUnit(w, k, container.EncodeAU(au),
					addRel(anchor, pts), addRel(anchor, dts), auKey(au)); err != nil {
					src.End(err)
				}
			})
		case *codecs.MPEG4Audio:
			r.OnDataMPEG4Audio(t, func(pts time.Duration, au []byte) {
				// The callback hands back the library's internal buffer, which
				// may be reused on the next message. Copying before the kernel
				// keeps it makes a unit's payload survive its arrival.
				payload := append([]byte(nil), au...)
				if err := writeUnit(w, k, payload, addRel(anchor, pts), addRel(anchor, pts), true); err != nil {
					src.End(err)
				}
			})
		default:
			// Unreachable through toKernelTracks, which refuses the same codec,
			// but the refusal belongs here too: a publisher that negotiated a
			// codec we cannot carry would otherwise get a stream missing one
			// track, which plays back as video with no audio.
			return nil, fmt.Errorf("rtmp: unsupported codec %T", c)
		}
	}

	// The Reader delivers media through the callbacks above, but its internal
	// state still advances via Read, which the connection must be drained for.
	go func() {
		for {
			if src.DoneClosed() {
				return
			}
			a.conn.SetReadDeadline(time.Now().Add(readTimeout))
			if err := r.Read(); err != nil {
				// A stalled or closed RTMP connection is the normal end of a
				// publish. Reporting it as a protocol failure would close the
				// whole path, killing every subscriber on a mere network
				// hiccup; a graceful loss keeps the path available for a
				// republish inside the retain window.
				src.End(nil)
				return
			}
		}
	}()

	go func() {
		<-ctx.Done()
		src.End(ctx.Err())
	}()

	return src, nil
}

// writeUnit converts one access unit into a kernel unit and writes it.
//
// Ownership: NewUnit returns one reference and WriteUnit takes it on every
// return path, error included. So this function releases nothing. Releasing the
// unit here too would be a second release of the pooled buffer: the pool hands
// that memory to the next AcquireSlice while the path worker is still
// broadcasting it to every subscriber, and a live frame payload is recycled
// under its own reader. The race detector reports it as a pool write racing a
// Release read on the same bytes.

// The callback is not called for every arriving frame, so there is no
// loop-level defer to hold the release in, and there must not be one.
//
// Two failures are not a reason to end the session, and are folded to nil so
// that they cannot be mistaken for one: ErrCanceled is back-pressure, meaning
// this one unit was dropped while the session lives on, and ErrEOF means the
// kernel already closed the writer, in which case the read loop notices through
// the session's Done channel. Anything else means the declared track table and
// the arriving media disagree, which is a real fault.
func writeUnit(w stream.StreamWriter, t *stream.Track, payload []byte, pts, dts time.Time, key bool) error {
	if t == nil || len(payload) == 0 {
		return nil
	}
	u := stream.NewUnit(&stream.Unit{
		TrackID: t.ID, Codec: t.Codec, Kind: t.Kind,
		Payload: payload, PTS: pts, DTS: dts, Key: key,
	})
	err := w.WriteUnit(u)
	if err != nil {
		if errors.Is(err, stream.ErrCanceled) || errors.Is(err, stream.ErrEOF) {
			return nil
		}
		return err
	}
	return nil
}

// --- play ------------------------------------------------------------------

// Play implements registry.Adapter for one RTMP player.
func (a *Adapter) Play(ctx context.Context, s registry.PlaySession) (registry.Sink, error) {
	sub, err := s.Subscribe(ctx)
	if err != nil {
		return nil, err
	}

	// Tracks is authoritative only once a publisher exists, which Subscribe has
	// now guaranteed. Reading it before subscribing would return nil for a
	// client that connects ahead of its source and reject a perfectly good path.
	rtrk, err := toRTMPTracks(s.Tracks(), s.CodecParams())
	if err != nil {
		sub.Cancel()
		return nil, err
	}

	w := &gortmplib.Writer{Conn: a.sc, Tracks: rtrk}
	if err := w.Initialize(); err != nil {
		sub.Cancel()
		return nil, err
	}

	sink := adapters.NewSession(a.remote, func() {
		a.conn.SetReadDeadline(time.Time{})
		_ = a.conn.Close()
	})
	sink.SetSub(sub)

	go func() {
		var anchor time.Time
		for {
			u, err := sub.ReadUnit(ctx)
			if err != nil {
				sink.End(nil)
				return
			}
			if anchor.IsZero() {
				if !u.PTS.IsZero() {
					anchor = u.PTS
				} else {
					anchor = time.Now().UTC()
				}
			}
			werr := writeRTMP(w, rtrk, anchor, u)
			// Release runs unconditionally rather than as a defer: a defer in a
			// loop body does not run until the function returns, which would pin
			// every payload buffer for the life of the session.
			u.Release()
			if werr != nil {
				// A failed write is either the player disconnecting or a chunk
				// framing fault. Either ends the session, and the adapter
				// reports it up so the manager can close the subscription.
				sink.End(werr)
				return
			}
		}
	}()

	return sink, nil
}

// writeRTMP converts one kernel unit into the matching RTMP write call.
//
// H.264 is the one codec needing a repack: the kernel carries AVCC-length-
// prefixed access units, while gortmplib takes a slice of NAL bodies. The split
// is mechanical and lossless in both directions.
func writeRTMP(w *gortmplib.Writer, trks []*gortmplib.Track, anchor time.Time, u *stream.Unit) error {
	pts := rel(anchor, u.PTS)
	dts := rel(anchor, u.DTS)
	for _, t := range trks {
		switch t.Codec.(type) {
		case *codecs.H264:
			if u.Codec != codecH264 {
				continue
			}
			var au [][]byte
			for _, n := range container.ParseAU(u.Payload) {
				au = append(au, n.Data)
			}
			if len(au) == 0 {
				continue
			}
			return w.WriteH264(t, pts, dts, au)
		case *codecs.MPEG4Audio:
			if u.Codec != codecAAC {
				continue
			}
			return w.WriteMPEG4Audio(t, pts, u.Payload)
		}
	}
	return fmt.Errorf("rtmp: no track for codec %s", u.Codec)
}

// --- codec mapping ---------------------------------------------------------

// toKernelTracks maps a gortmplib track table into kernel tracks, and returns a
// second map from each gortmplib track to the kernel track it became. The second
// map is what the data callbacks use: keyed by pointer it cannot collapse two
// tracks of one codec onto each other, and it cannot drift out of step with the
// kernel's own identifier assignment, which mutates the very pointers it holds.
func toKernelTracks(trks []*gortmplib.Track) ([]*stream.Track, map[*gortmplib.Track]*stream.Track, error) {
	out := make([]*stream.Track, 0, len(trks))
	byTrack := make(map[*gortmplib.Track]*stream.Track, len(trks))
	for _, t := range trks {
		var kt *stream.Track
		switch c := t.Codec.(type) {
		case *codecs.H264:
			if len(c.SPS) == 0 || len(c.PPS) == 0 {
				return nil, nil, errors.New("rtmp: h264 track missing sps or pps")
			}
			// SPS and PPS are the whole of H.264's in-band syntax. A player that
			// never receives them renders nothing, so their absence is a refusal
			// rather than a degraded stream.
			kt = &stream.Track{
				Codec: codecH264, Kind: stream.KindVideo, Timescale: Timescale,
				Params: map[string]string{
					"sps": hex.EncodeToString(c.SPS),
					"pps": hex.EncodeToString(c.PPS),
				},
			}
		case *codecs.MPEG4Audio:
			kt = &stream.Track{
				Codec: codecAAC, Kind: stream.KindAudio, Timescale: Timescale,
				Params: map[string]string{
					"sampleRate":       strconv.Itoa(c.Config.SampleRate),
					"numberOfChannels": strconv.Itoa(int(c.Config.ChannelConfig)),
				},
			}
		default:
			return nil, nil, fmt.Errorf("rtmp: unsupported codec %T", t.Codec)
		}
		out = append(out, kt)
		byTrack[t] = kt
	}
	return out, byTrack, nil
}

// toRTMPTracks is the inverse of toKernelTracks, used to advertise a path's
// description to a player. CodecParams is consulted as a fallback because the
// descriptor must be built from parameters, never from the codec identifier
// itself: two codecs can share an identifier across module versions.
func toRTMPTracks(trks []*stream.Track, params map[stream.CodecID]map[string]string) ([]*gortmplib.Track, error) {
	out := make([]*gortmplib.Track, 0, len(trks))
	for _, t := range trks {
		p := t.Params
		if p == nil {
			p = params[t.Codec]
		}
		switch t.Codec {
		case codecH264:
			sps, err := hex.DecodeString(p["sps"])
			if err != nil {
				return nil, fmt.Errorf("rtmp: bad sps: %w", err)
			}
			pps, err := hex.DecodeString(p["pps"])
			if err != nil {
				return nil, fmt.Errorf("rtmp: bad pps: %w", err)
			}
			if len(sps) == 0 || len(pps) == 0 {
				return nil, errors.New("rtmp: h264 track missing sps/pps")
			}
			out = append(out, &gortmplib.Track{Codec: &codecs.H264{SPS: sps, PPS: pps}})
		case codecAAC:
			rate, _ := strconv.Atoi(p["sampleRate"])
			ch, _ := strconv.Atoi(p["numberOfChannels"])
			if rate == 0 {
				return nil, errors.New("rtmp: aac track missing sampleRate")
			}
			out = append(out, &gortmplib.Track{
				Codec: &codecs.MPEG4Audio{Config: &mpeg4audio.AudioSpecificConfig{
					Type:          mpeg4audio.ObjectTypeAACLC,
					SampleRate:    rate,
					ChannelConfig: uint8(ch),
				}},
			})
		default:
			return nil, fmt.Errorf("rtmp: unsupported codec %s", t.Codec)
		}
	}
	return out, nil
}

// auKey reports whether an access unit is a key picture.
func auKey(au [][]byte) bool {
	for _, n := range au {
		if len(n) > 0 && n[0]&0x1F == container.NalIDR {
			return true
		}
	}
	return false
}

// addRel converts a relative duration to an absolute instant against an anchor.
func addRel(anchor time.Time, d time.Duration) time.Time { return anchor.Add(d) }

// rel converts an absolute instant to a duration relative to an anchor.
func rel(anchor, t time.Time) time.Duration {
	if t.IsZero() || anchor.IsZero() {
		return 0
	}
	d := t.Sub(anchor)
	if d < 0 {
		d = 0
	}
	return d
}
