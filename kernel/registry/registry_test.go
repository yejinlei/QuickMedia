// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package registry

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/yejinlei/quickmedia/kernel/stream"
)

// TestRegisterPanicsOnDuplicateName pins the one invariant a registry exists to
// enforce: two modules cannot claim one name.
//
// A silent overwrite here is worse than a panic — the binary would start, serve
// traffic on the wrong adapter, and produce no error anywhere. The panic is the
// build failure that tells a developer which module was imported twice.
func TestRegisterPanicsOnDuplicateName(t *testing.T) {
	defer Reset()

	Register(&testAdapter{name: "dup", prio: 10})
	defer func() {
		if recover() == nil {
			t.Fatal("Register did not panic on a duplicate name")
		}
	}()
	Register(&testAdapter{name: "dup", prio: 20})
}

// TestRegisterPanicsOnInvalidInfo is the mirror: metadata that cannot name or
// classify a module is refused at compile time rather than at request time.
func TestRegisterPanicsOnInvalidInfo(t *testing.T) {
	defer Reset()

	cases := []ModuleInfo{
		{Name: "", Type: TAdapter},
		{Name: "x", Type: ModuleType(-1)},
		{Name: "x", Type: TCapability + 1},
	}
	for i, info := range cases {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("case %d: Register accepted invalid info %+v", i, info)
				}
			}()
			Register(&testAdapter{info: info, infoSet: true})
		}()
	}
}

// TestNamesAreNegotiationOrder pins the ordering the whole server depends on:
// priority descends, ties break on name so the order is deterministic across
// machines. A nondeterministic order here means two binaries pick different
// adapters for the same URL.
func TestNamesAreNegotiationOrder(t *testing.T) {
	defer Reset()

	Register(&testAdapter{name: "low", prio: 1})
	Register(&testAdapter{name: "high", prio: 50})
	Register(&testAdapter{name: "aa", prio: 20})
	Register(&testAdapter{name: "bb", prio: 20})

	want := []string{"high", "aa", "bb", "low"}
	got := Names(TAdapter)
	if len(got) != len(want) {
		t.Fatalf("Names = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Names[%d] = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}
	if got := Count(TAdapter); got != len(want) {
		t.Fatalf("Count = %d, want %d", got, len(want))
	}
	if got := Count(TCodec); got != 0 {
		t.Fatalf("Count(TCodec) = %d, want 0", got)
	}
}

// TestSelectAdapterPrefersADirectionMatch is the routing decision a listener
// makes: a module that advertises the requested direction must beat one that
// only matches the scheme. Falling back to the scheme match is still legal,
// which is what lets a publish-only module be listed as a player at all.
func TestSelectAdapterPrefersADirectionMatch(t *testing.T) {
	defer Reset()

	Register(&testAdapter{name: "scheme-only", prio: 100, schemes: []string{"p"}, pub: false, play: false})
	Register(&testAdapter{name: "pub-and-play", prio: 1, schemes: []string{"p"}, pub: true, play: true})
	Register(&testAdapter{name: "no-scheme", prio: 50, schemes: []string{"q"}, pub: true, play: true})

	ad, err := SelectAdapter("p", true, true)
	if err != nil {
		t.Fatalf("SelectAdapter: %v", err)
	}
	if ad.ModuleInfo().Name != "pub-and-play" {
		t.Fatalf("selected %s, want pub-and-play", ad.ModuleInfo().Name)
	}

	// A direction nobody claims must not be invented: it is refused rather than
	// served by a module that cannot honour it.
	if _, err = SelectAdapter("p", false, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SelectAdapter(p, no direction) = %v, want ErrNotFound", err)
	}
	if _, err = SelectAdapter("no-such-scheme", true, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SelectAdapter(no-such-scheme) = %v, want ErrNotFound", err)
	}
}

// TestLookupAndInfoExposeRegisteredData is the read side of the registry: the
// control plane resolves a module by name rather than by type assertion.
func TestLookupAndInfoExposeRegisteredData(t *testing.T) {
	defer Reset()

	Register(&testAdapter{name: "found", prio: 3, version: "1.2.3"})

	mod, ok := Lookup("found")
	if !ok {
		t.Fatal("Lookup did not find the module")
	}
	if _, isAd := mod.(Adapter); !isAd {
		t.Fatalf("Lookup returned %T, want an Adapter", mod)
	}
	info, ok := Info("found")
	if !ok || info.Name != "found" || info.Version != "1.2.3" || info.Type != TAdapter || info.Priority != 3 {
		t.Fatalf("Info = %+v, ok = %v", info, ok)
	}
	if _, ok = Lookup("absent"); ok {
		t.Fatal("Lookup found an absent module")
	}
	if _, ok = Info("absent"); ok {
		t.Fatal("Info found an absent module")
	}
}

// TestSelectCodecByIdentifier checks codec resolution by identifier and the
// not-found path, which an adapter must surface as an error rather than as an
// empty packer.
func TestSelectCodecByIdentifier(t *testing.T) {
	defer Reset()

	cp := &testCodec{id: "h264"}
	Register(cp)

	got, err := SelectCodec("h264")
	if err != nil {
		t.Fatalf("SelectCodec: %v", err)
	}
	if got != cp {
		t.Fatal("SelectCodec returned a different instance")
	}
	if _, err = SelectCodec("vp9"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SelectCodec(vp9) = %v, want ErrNotFound", err)
	}
	if got := Codecs(); len(got) != 1 || got[0] != "h264" {
		t.Fatalf("Codecs = %v, want [h264]", got)
	}
}

// TestResetClearsEveryKind confirms the test hook really empties the registry,
// which is what keeps the registrations below from leaking into other packages'
// tests in a parallel build.
func TestResetClearsEveryKind(t *testing.T) {
	Register(&testAdapter{name: "a", prio: 1})
	Register(&testCodec{id: "b"})

	Reset()

	if got := Count(TAdapter); got != 0 {
		t.Fatalf("Count(TAdapter) after Reset = %d, want 0", got)
	}
	if got := Count(TCodec); got != 0 {
		t.Fatalf("Count(TCodec) after Reset = %d, want 0", got)
	}
	if _, ok := Lookup("a"); ok {
		t.Fatal("Lookup still found a module after Reset")
	}
}

// --- test doubles ----------------------------------------------------------

// testAdapter is a minimal registry.Adapter whose behaviour the tests set.
type testAdapter struct {
	name    string
	version string
	prio    int
	schemes []string
	pub     bool
	play    bool
	info    ModuleInfo // used verbatim when infoSet
	infoSet bool
}

func (a *testAdapter) ModuleInfo() ModuleInfo {
	if a.infoSet {
		return a.info
	}
	return ModuleInfo{Name: a.name, Version: a.version, Type: TAdapter, Priority: a.prio, Dir: "test"}
}

func (a *testAdapter) Schemes() []string { return a.schemes }
func (a *testAdapter) CanPublish() bool  { return a.pub }
func (a *testAdapter) CanPlay() bool     { return a.play }
func (a *testAdapter) SupportsScheme(s string) bool {
	return slices.Contains(a.schemes, s)
}

func (a *testAdapter) Publish(context.Context, PublishSession) (Source, error) {
	return nil, nil
}

func (a *testAdapter) Play(context.Context, PlaySession) (Sink, error) {
	return nil, nil
}

var _ Adapter = (*testAdapter)(nil)

// testCodec is a minimal CodecPacker carrying nothing but an identifier.
type testCodec struct{ id stream.CodecID }

func (c *testCodec) ModuleInfo() ModuleInfo {
	return ModuleInfo{Name: string(c.id), Version: "0.0.0", Type: TCodec, Dir: "test"}
}
func (c *testCodec) ID() stream.CodecID                   { return c.id }
func (c *testCodec) Kind() stream.CodecKind               { return stream.KindVideo }
func (c *testCodec) Timescale() uint32                    { return 90000 }
func (c *testCodec) RTPParams() map[string]string         { return nil }
func (c *testCodec) SetRTPParams(map[string]string)       {}
func (c *testCodec) ContainerParams() map[string]string   { return nil }
func (c *testCodec) SetContainerParams(map[string]string) {}
func (c *testCodec) SupportsFormat(Format) bool           { return false }
func (c *testCodec) Formats() []Format                    { return nil }
func (c *testCodec) NewRTPPacker() (RTPPacker, error)     { return nil, nil }
func (c *testCodec) NewRTPUnpacker() (RTPUnpacker, error) { return nil, nil }
func (c *testCodec) NewContainerPacker(Format) (ContainerPacker, error) {
	return nil, nil
}
func (c *testCodec) NewContainerUnpacker(Format) (ContainerUnpacker, error) {
	return nil, nil
}

var _ CodecPacker = (*testCodec)(nil)
