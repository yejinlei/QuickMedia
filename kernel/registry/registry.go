// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package registry is the single plugin contract of QuickMedia. It defines the
// three module kinds the kernel loads (adapter / codec / capability), the
// type-safe registry they register into, and the negotiation helpers the
// kernel uses to pick a module by name.
//
// The kernel keeps no backdoor field for any concrete protocol: a protocol
// becomes a module only by implementing Adapter and calling Register from an
// init() in its own package. Adding a protocol must not change this package.
// That property is what makes "kernel diff = 0 lines" checkable.
package registry

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/yejinlei/quickmedia/kernel/stream"
)

// ErrNotFound is returned by a selector when nothing matches.
var ErrNotFound = errors.New("registry: module not found")

// ModuleType enumerates the kinds of module the kernel accepts.
type ModuleType int

const (
	// TAdapter is a protocol adaptor: it turns a wire protocol into Units and
	// back. Implementations live in the adapters/ tree.
	TAdapter ModuleType = iota
	// TCodec is a codec packing pair: it turns Units into transport payloads
	// and back. Implementations live in the container/ tree.
	TCodec
	// TCapability is a horizontal capability attached to a subscription or a
	// path. Implementations live in the capabilities/ tree.
	TCapability
)

// String implements fmt.Stringer.
func (t ModuleType) String() string {
	switch t {
	case TAdapter:
		return "adapter"
	case TCodec:
		return "codec"
	case TCapability:
		return "capability"
	default:
		return "unknown"
	}
}

// ModuleInfo is the metadata every module must report. The kernel only ever
// reads this struct; the module owns its own version semantics.
type ModuleInfo struct {
	Name      string // globally unique, e.g. "rtsp", "h264", "recorder"
	Version   string // module version, semver
	Type      ModuleType
	MinKernel string // minimum kernel version this module needs
	Priority  int    // higher wins when several modules of one kind claim a scheme
	Dir       string // directory of the implementing module, for diagnostics
}

// IsValid reports whether the info is usable in a registry.
func (m ModuleInfo) IsValid() bool {
	return m.Name != "" && m.Type >= TAdapter && m.Type <= TCapability
}

// Adapter is the protocol plugin contract (layer L3).
//
// An adapter is both a publisher and a player, but each direction is declared
// separately so that a module which only publishes, or only plays, is legal.
// The kernel never passes protocol objects to the adapter: it passes sessions,
// which is what lets the same registry serve a plugin and a process-external
// gateway.
type Adapter interface {
	ModuleInfo() ModuleInfo
	// Schemes returns the URL schemes this adapter handles, e.g. "rtsp",
	// "rtmp", "http". The server routes an incoming connection by scheme.
	Schemes() []string
	// SupportsScheme reports whether the adapter handles a given scheme.
	SupportsScheme(scheme string) bool
	// CanPublish reports whether the adapter can ingest a stream from a client.
	CanPublish() bool
	// CanPlay reports whether the adapter can feed a stream to a client.
	CanPlay() bool
	// Publish negotiates with a client and returns the Source that drives it.
	Publish(ctx context.Context, s PublishSession) (Source, error)
	// Play negotiates with a client and returns the Sink that drives it.
	Play(ctx context.Context, s PlaySession) (Sink, error)
}

// PublishSession is what the kernel hands a publishing adapter. The adapter owns
// the client connection; the kernel owns the path.
//
// It is an interface and not a struct because it mixes data (Request) with
// callbacks (Begin): a struct cannot hold a callback, so the kernel must
// implement both directions of the conversation. Adapters hold only the
// interface, which is what keeps them free of kernel internals.
type PublishSession interface {
	// Request returns the incoming publish request.
	Request() SinkRequest
	// Begin is called once the adapter has negotiated with the client and
	// learned the track table. It allocates the path's track identifiers and
	// returns the writer the adapter writes units into.
	//
	// Call Begin exactly once. Writing units into the returned writer before
	// the client has fully negotiated is legal and is buffered, which is what
	// lets an adapter negotiate lazily.
	Begin([]*stream.Track) (stream.StreamWriter, error)
	// Relay reports whether this path already has a live publisher whose
	// output the adapter should forward instead of negotiating a fresh one.
	Relay() bool
	// PathName returns the path the kernel assigned.
	PathName() string
}

// PlaySession is what the kernel hands a playing adapter. See PublishSession
// for why this is an interface rather than a struct.
type PlaySession interface {
	// Request returns the incoming play request.
	Request() SrcRequest
	// Tracks returns the path's description as of now, or nil when the path has
	// no publisher yet. An adapter that only supports known descriptions must
	// then return ErrNoCommonProfile rather than guessing.
	Tracks() []*stream.Track
	// CodecParams returns the codec-specific parameters of every track, keyed
	// by codec ID. Adapters use these to build their own container headers;
	// they must not rely on the codec identifier itself.
	CodecParams() map[stream.CodecID]map[string]string
	// Subscribe claims a subscription, blocking until the path is published or
	// the context is canceled. It is called at the moment the client is ready,
	// which is what keeps a client that connects before its source exists from
	// being rejected.
	Subscribe(ctx context.Context) (stream.Subscription, error)
}

// SinkRequest describes the path a client wants to publish to.
type SinkRequest struct {
	// Path is the path the client is writing into; it is the URL path segment.
	Path string
	// Query is the raw query string, for adapters that read request hints.
	Query string
	// Remote is the client address, for logging and access control.
	Remote string
	// Relay is set when the kernel asks this adapter to forward an existing
	// path instead of accepting a fresh publisher from the client.
	Relay bool
	// RelayPath is the source path of a relay, meaningful only when Relay is
	// set. This is how one protocol republishes a path that already exists.
	RelayPath string
	// WaitPublisherTimeout bounds how long a relay waits for its source to be
	// published before giving up. Zero means wait for the context.
	WaitPublisherTimeout time.Duration
}

// SrcRequest describes the path a client wants to play.
type SrcRequest struct {
	// Path is the path the client is reading from.
	Path string
	// Query is the raw query string, for adapters that read request hints.
	Query string
	// Remote is the client address, for logging and access control.
	Remote string
	// SubscribeTimeout bounds how long Play may wait for a publisher before
	// returning ErrNotPublished. Zero means wait for the context.
	SubscribeTimeout time.Duration
}

// Source is what a publishing adapter returns. The adapter keeps driving the
// client connection in its own goroutine, so the kernel only needs to observe
// the session and tear it down.
//
// Tracks are not reported here: the adapter declared them through
// PublishSession.Begin, and the path's own track table is authoritative.
type Source interface {
	// Err reports the adapter's terminal error, if any.
	Err() error
	// Done is closed when the publishing session has ended, normally or with
	// an error.
	Done() <-chan struct{}
	// Close tears down the client connection. It is idempotent.
	Close() error
	// RemoteAddr returns the client address, for logging.
	RemoteAddr() string
}

// Sink is what a playing adapter returns.
//
// The kernel does not read units out of a Sink: the adapter claims a
// subscription through PlaySession.Subscribe and reads from it directly, which
// is what lets an adapter batch, resegment, or reorder per protocol. The kernel
// only needs the subscription handle, to reap it when the session ends.
type Sink interface {
	// Sub returns the subscription the adapter claimed, or nil when it has not
	// claimed one yet. The kernel calls this when the session ends so the
	// subscription can be released.
	Sub() stream.Subscription
	// Err reports the adapter's terminal error, if any.
	Err() error
	// Done is closed when the play session has ended.
	Done() <-chan struct{}
	// Close tears down the client connection. It is idempotent.
	Close() error
	// RemoteAddr returns the client address, for logging.
	RemoteAddr() string
}

// Capability is the horizontal plugin contract (layer L4).
type Capability interface {
	ModuleInfo() ModuleInfo
	// OnAttach installs the capability on a subscription and returns an opaque
	// handle that OnDetach receives.
	OnAttach(stream.Subscription, CapConfig) (CapHandle, error)
	// OnDetach removes the capability.
	OnDetach(CapHandle) error
	// ConfigSchema returns the config keys this capability accepts, so the
	// control plane can validate config without knowing the capability.
	ConfigSchema() []string
}

// CapConfig is an untyped capability configuration, validated against
// Capability.ConfigSchema().
type CapConfig map[string]any

// CapHandle is an opaque capability instance.
type CapHandle any

// CodecPacker is the codec plugin contract (layer L2). It is the only thing
// that knows how a codec's payload becomes transport packets, container
// frames, and back.
//
// Two orthogonal families are exposed because they have opposite lifetimes:
// an RTP packer is stateless per unit and lives as long as a peer connection,
// while a container packer is a per-sink writer that owns segment state.
// Keeping them separate means an adapter that only transports never pays for a
// multiplexer.
//
// Payloads are elementary-stream level: elements are NAL units for video
// codecs and access units for audio codecs.
type CodecPacker interface {
	ModuleInfo() ModuleInfo
	// ID returns the codec identifier.
	ID() stream.CodecID
	// Kind returns the track kind this codec carries.
	Kind() stream.CodecKind
	// Timescale returns the transport clock rate for this codec, in Hz.
	Timescale() uint32
	// RTPParams returns the codec-specific transport parameters (SPS/PPS style)
	// as base16 strings, for use in fmtp lines and codec descriptors.
	RTPParams() map[string]string
	// SetRTPParams stores codec-specific parameters learned from a source.
	SetRTPParams(map[string]string)
	// ContainerParams returns the codec-specific parameters in the key/value
	// form carried by stream.Track.Params.
	ContainerParams() map[string]string
	// SetContainerParams stores parameters learned from a source.
	SetContainerParams(map[string]string)
	// SupportsFormat reports whether this codec can be packed for a container.
	SupportsFormat(Format) bool
	// Formats returns the container formats this codec supports.
	Formats() []Format
	// NewRTPPacker builds a packer bound to this codec instance.
	NewRTPPacker() (RTPPacker, error)
	// NewRTPUnpacker builds an unpacker bound to this codec instance.
	NewRTPUnpacker() (RTPUnpacker, error)
	// NewContainerPacker builds a container writer bound to this codec.
	NewContainerPacker(f Format) (ContainerPacker, error)
	// NewContainerUnpacker builds a container reader bound to this codec.
	NewContainerUnpacker(f Format) (ContainerUnpacker, error)
}

// Format identifies a container format this codec can be packed for. It is a
// string, not an enum, so that a codec module can advertise a format without
// this package having to know about it.
type Format string

const (
	// FormatTS is MPEG-TS.
	FormatTS Format = "ts"
	// FormatFLV is Flash Video.
	FormatFLV Format = "flv"
	// FormatFMP4 is fragmented MP4.
	FormatFMP4 Format = "fmp4"
	// FormatADTS is raw ADTS audio access units.
	FormatADTS Format = "adts"
	// FormatAnnexB is raw Annex-B video NAL units.
	FormatAnnexB Format = "annexb"
	// FormatOpus is Ogg-multiplexed Opus. The name names the container rather
	// than the codec because a codec identifier never appears in this package:
	// it would couple the contract to one module. Opus is the only codec that
	// has exactly one standard container, so the two are interchangeable in
	// practice, but the direction of the dependency matters.
	FormatOpus Format = "opus"
)

// RTPPacker turns one unit into transport payloads.
type RTPPacker interface {
	// Pack emits the transport payloads for one unit. It returns the new
	// sequence number and timestamp to use for the next unit, so the adapter
	// owns the running counters and the packer stays stateless enough to test.
	Pack(*stream.Unit, uint16, uint32) (packs [][]byte, nextSequence uint16, nextTimestamp uint32)
	// MaxPayload reports the largest payload size the packer supports.
	MaxPayload() int
}

// RTPUnpacker consumes transport payloads and reassembles units.
type RTPUnpacker interface {
	// Unpack consumes one payload. A nil unit means the packet was consumed but
	// the access unit is not complete yet.
	Unpack(payload []byte, sequence uint16, timestamp uint32, marker bool) (*stream.Unit, error)
}

// Frame is one container frame, the unit container writers produce.
//
// It is defined here so that the contract and the packers that emit it stay in
// one direction: the container layer imports this package, never the other way
// around. The muxer-level meaning of Data is format-specific: a PES packet for
// MPEG-TS, one tag payload for the Flash Video family, one sample for
// fragmented MP4.
type Frame struct {
	// Data is the encoded frame body, excluding any stream-level framing.
	Data []byte
	// Key reports whether the frame is an access unit a decoder can start from.
	Key bool
	// Config reports whether the frame carries codec configuration rather than
	// media, which container writers use to emit a codec descriptor.
	Config bool
	// Disc reports whether the frame must be preceded by a discontinuity, which
	// is the frame-level marker a receiver needs to re-derive its state.
	Disc bool
	// PTS and DTS are absolute times; zero means the muxer should choose.
	PTS time.Time
	DTS time.Time
	// Duration is this frame's duration, for duration-based segmenters.
	Duration time.Duration
}

// ContainerPacker turns units into container frames.
type ContainerPacker interface {
	// Format reports which container this packer emits.
	Format() Format
	// InitData returns the stream-level preamble for this codec and format, if
	// one exists. Empty for MPEG-TS, which carries configuration in-stream.
	InitData() []byte
	// ConfigFrames returns the codec descriptors this format needs before a
	// decoder can start, in the order they must be written.
	//
	// It is separate from InitData because the two live at different levels:
	// InitData is one muxed byte stream (MPEG-TS program tables), while these
	// are individual tags, each with its own timestamp. Formats without a
	// separate descriptor return an empty slice.
	//
	// A packer that returns nil here produces a stream no player will render:
	// H.264 has no in-band stream syntax and AAC has no in-band sampling
	// parameters, so a missing descriptor is a total failure rather than a
	// degraded one.
	ConfigFrames() []Frame
	// Pack converts one unit into frames. It may return several: a single
	// access unit that exceeds a fragment size is split, and codecs that keep
	// configuration in-band emit a config frame ahead of the media frame.
	Pack(*stream.Unit) ([]Frame, error)
}

// ContainerUnpacker turns container bytes back into units.
type ContainerUnpacker interface {
	// Format reports which container this unpacker reads.
	Format() Format
	// Feed consumes one container-level frame body and returns any units it
	// completes. Units assembled across several calls are emitted when the last
	// one arrives, which is how fragmented packets and split access units work.
	Feed(data []byte) ([]*stream.Unit, error)
	// Reset discards partial state so the unpacker can be reused on a new
	// stream.
	Reset()
}

// registryEntry is one registered module.
type registryEntry struct {
	info ModuleInfo
	mod  any
}

var (
	mu     sync.RWMutex
	byName = make(map[string]*registryEntry)
	byKind = make(map[ModuleType][]*registryEntry)
)

// Register adds a module to the registry. It is meant to be called from an
// init() function so registration is compile-time. It panics on duplicate
// names, which is correct: a silent overwrite here would mask a build error.
// moduleInfoReporter is the minimal surface every module must expose. It is
// declared as a named type rather than an anonymous interface because
// Go 1.24 forbids the anonymous form on generic methods.
type moduleInfoReporter interface{ ModuleInfo() ModuleInfo }

func Register(mod any) ModuleInfo {
	info := mod.(moduleInfoReporter).ModuleInfo()
	if !info.IsValid() {
		panic("registry: invalid module info " + info.Name)
	}

	mu.Lock()
	defer mu.Unlock()
	if _, exists := byName[info.Name]; exists {
		panic("registry: duplicate module name " + info.Name)
	}
	e := &registryEntry{info: info, mod: mod}
	byName[info.Name] = e
	byKind[info.Type] = append(byKind[info.Type], e)
	return info
}

// Lookup returns the module registered under name.
func Lookup(name string) (any, bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := byName[name]
	if !ok {
		return nil, false
	}
	return e.mod, true
}

// Info returns the registered info for name.
func Info(name string) (ModuleInfo, bool) {
	mu.RLock()
	defer mu.RUnlock()
	e, ok := byName[name]
	if !ok {
		return ModuleInfo{}, false
	}
	return e.info, true
}

// All returns every registered module of a kind, sorted by priority (highest
// first) then name. This order is the negotiation order used by Select*.
func All(t ModuleType) []ModuleInfo {
	mu.RLock()
	ents := make([]*registryEntry, len(byKind[t]))
	copy(ents, byKind[t])
	mu.RUnlock()

	sort.Slice(ents, func(i, j int) bool {
		if ents[i].info.Priority != ents[j].info.Priority {
			return ents[i].info.Priority > ents[j].info.Priority
		}
		return ents[i].info.Name < ents[j].info.Name
	})

	out := make([]ModuleInfo, 0, len(ents))
	for _, e := range ents {
		out = append(out, e.info)
	}
	return out
}

// Count returns how many modules are registered of a kind.
func Count(t ModuleType) int {
	mu.RLock()
	defer mu.RUnlock()
	return len(byKind[t])
}

// Names returns the module names of a kind in negotiation order.
func Names(t ModuleType) []string {
	infs := All(t)
	out := make([]string, 0, len(infs))
	for _, i := range infs {
		out = append(out, i.Name)
	}
	return out
}

// SelectAdapter returns the adapter that claims a scheme, in negotiation
// order. It prefers modules that support both the requested direction and the
// scheme, then falls back to any module that supports the scheme.
func SelectAdapter(scheme string, publish, play bool) (Adapter, error) {
	mu.RLock()
	var matched []Adapter
	for _, e := range byKind[TAdapter] {
		ad, ok := e.mod.(Adapter)
		if !ok || !ad.SupportsScheme(scheme) {
			continue
		}
		if (publish && ad.CanPublish()) || (play && ad.CanPlay()) {
			matched = append(matched, ad)
		}
	}
	mu.RUnlock()

	if len(matched) == 0 {
		return nil, ErrNotFound
	}
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].ModuleInfo().Priority != matched[j].ModuleInfo().Priority {
			return matched[i].ModuleInfo().Priority > matched[j].ModuleInfo().Priority
		}
		return matched[i].ModuleInfo().Name < matched[j].ModuleInfo().Name
	})
	return matched[0], nil
}

// SelectCodec returns the packer registered for a codec identifier.
func SelectCodec(id stream.CodecID) (CodecPacker, error) {
	mu.RLock()
	defer mu.RUnlock()
	for _, e := range byKind[TCodec] {
		if e.info.Name == string(id) {
			if cp, ok := e.mod.(CodecPacker); ok {
				return cp, nil
			}
		}
	}
	return nil, ErrNotFound
}

// Codecs returns every registered codec identifier.
func Codecs() []stream.CodecID {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]stream.CodecID, 0, len(byKind[TCodec]))
	for _, e := range byKind[TCodec] {
		out = append(out, stream.CodecID(e.info.Name))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Reset clears the registry. Test use only.
func Reset() {
	mu.Lock()
	byName = make(map[string]*registryEntry)
	byKind = make(map[ModuleType][]*registryEntry)
	mu.Unlock()
}
