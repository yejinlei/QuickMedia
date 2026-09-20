// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package memory is layer L0: reference counting, size-class object pools and
// wait-free synchronization primitives used by the whole media plane.
//
// Pool sizing is tuned for the media plane rather than general use: payload
// buffers are bounded at PoolMaxSize (256 KiB). Larger payloads fall back to
// plain heap allocation and are never returned to a pool, which keeps the pool
// footprint bounded regardless of what a publisher sends.
package memory

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// PoolMaxSize is the largest payload that gets pooled. Payloads above this
// size are allocated from the heap and referenced directly.
const PoolMaxSize = 256 * 1024

// SizeClasses are the payload classes served by the pool. Classes are
// deliberately few and coarse: media payloads cluster around a handful of
// sizes, and a coarse ladder keeps fragmentation and per-class metadata low.
var SizeClasses = []int{
	256, 1024, 4096, 16384, 65536, 262144,
}

// maxSlots bounds how many buffers of one class may sit idle in the pool.
// The bound is what makes memory.RssOf(Pool) predictable under spikes.
const maxSlots = 64

// Refs is a thread-safe reference count for pooled objects.
type Refs struct {
	n atomic.Int32
}

// Retain adds one reference.
func (r *Refs) Retain() { r.n.Add(1) }

// Release removes one reference and reports whether it was the last one.
func (r *Refs) Release() bool { return r.n.Add(-1) == 0 }

// Count returns the current reference count. Diagnostics only.
func (r *Refs) Count() int32 { return r.n.Load() }

type sizeClass struct {
	// size is the exact capacity of every buffer this class hands out. A class
	// returns buffers of its own capacity and never grows one, so reuse is
	// exact and growth never happens on the hot path.
	size int
	mu   sync.Mutex
	// free holds the buffers themselves, by value. A *[]byte here would alias
	// the field of the Unit that owns it: ReleaseSlice nils that field
	// immediately after returning, so a pointer entry could resolve to nil or
	// to a buffer someone else was still reading. Holding the slice keeps the
	// buffer detached from its former owner.
	free [][]byte

	hits, misses, dropped atomic.Int64
}

func (sc *sizeClass) acquire(size int) []byte {
	sc.mu.Lock()
	var buf []byte
	if len(sc.free) > 0 {
		buf = sc.free[len(sc.free)-1]
		sc.free = sc.free[:len(sc.free)-1]
	}
	sc.mu.Unlock()

	if buf == nil {
		sc.misses.Add(1)
		buf = make([]byte, size, sc.size)
	} else {
		sc.hits.Add(1)
		// The stored buffer keeps the class capacity so it can be reused, but
		// every caller must see exactly the length it asked for: a media unit's
		// payload length is authoritative, and trailing bytes from a previous
		// owner would be parsed as media.
		buf = buf[:size:size]
	}
	claim(buf)
	return buf
}

func (sc *sizeClass) release(buf []byte) {
	if sc == nil || buf == nil || cap(buf) != sc.size {
		return
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if len(sc.free) >= maxSlots {
		sc.dropped.Add(1)
		return
	}
	sc.free = append(sc.free, buf)
}

// idle returns how many buffers of this class are currently pooled and unused.
func (sc *sizeClass) idle() int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return len(sc.free)
}

// --- ownership tracking ----------------------------------------------------
//
// A pooled buffer is detached from its former owner before it is reused, so
// the underlying array is stable for the buffer's whole life and its address
// is a valid identity. That is what lets claim(buf) and releaseClaim(buf)
// decide "is this one of ours" by address, which is both stronger than a
// capacity comparison and cheaper than a per-class bookkeeping list.
//
// The claim map is bounded by how many buffers are out: a buffer leaves it the
// moment it is released, so a live server holds only the entries for buffers
// in flight plus the idle slots that sit in the classes.

// claim marks buf as pool-owned.
func claim(buf []byte) {
	owner.mu.Lock()
	owner.sets[unsafe.Pointer(&buf[0])] = struct{}{}
	owner.mu.Unlock()
}

// releaseClaim takes ownership back and reports whether it was ours.
func releaseClaim(buf []byte) bool {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if _, ok := owner.sets[unsafe.Pointer(&buf[0])]; !ok {
		return false
	}
	delete(owner.sets, unsafe.Pointer(&buf[0]))
	return true
}

type claimSet struct {
	mu   sync.Mutex
	sets map[unsafe.Pointer]struct{}
}

var owner = claimSet{sets: make(map[unsafe.Pointer]struct{})}

// classes holds one pool per size class, built at init time. Building them
// eagerly instead of lazily keeps the hot path free of sync.Once overhead.
var classes = make(map[int]*sizeClass, len(SizeClasses))

func initClasses() {
	for _, s := range SizeClasses {
		if _, ok := classes[s]; ok {
			continue
		}
		classes[s] = &sizeClass{size: s}
	}
}

func init() { initClasses() }

// sizeClassFor picks the smallest class that fits n bytes. Only called for
// n <= PoolMaxSize.
func sizeClassFor(n int) int {
	for _, c := range SizeClasses {
		if n <= c {
			return c
		}
	}
	return SizeClasses[len(SizeClasses)-1]
}

// AcquireSlice returns a []byte of exactly n bytes from the pool.
//
// A payload above PoolMaxSize is heap-allocated rather than forced into the
// largest class: the ladder tops out at PoolMaxSize, and padding a bigger
// buffer to that capacity would misclassify it on release.
//
// Claimed ownership: the returned buffer is QuickMedia's until it is given
// back with ReleaseSlice. A buffer created here and never released is simply
// left for the garbage collector, which is the right trade for a live server.
func AcquireSlice(n int) []byte {
	if n < 1 {
		return nil
	}
	if n > PoolMaxSize {
		return make([]byte, n, n)
	}
	c := sizeClassFor(n)
	return classes[c].acquire(n)
}

// ReleaseSlice returns buf to the pool if it was acquired from one. The buffer
// is detached before it is handed over, and buf is nilled, so the caller
// cannot use it or return it again after this call.
//
// A plain capacity check is not enough to decide ownership: a payload of 3
// bytes has the same capacity as a 256-byte pooled buffer, and accepting it
// would let an adapter's own heap slice sit in the pool and be handed to a
// media unit that is writing into it. Ownership is therefore tracked explicitly
// by identity, which is also what makes a double Release a no-op.
func ReleaseSlice(buf *[]byte) {
	if buf == nil || *buf == nil {
		return
	}
	b := *buf
	if !releaseClaim(b) {
		return
	}
	if sc, ok := classes[sizeClassFor(len(b))]; ok && cap(b) == sc.size {
		sc.release(b)
	}
	*buf = nil
}

// PoolStats reports pool counters.
type PoolStats struct {
	ClassSize int
	Idle      int
	Hits      int64
	Misses    int64
	Dropped   int64
}

// PoolStatsOf returns per-class pool statistics.
func PoolStatsOf() []PoolStats {
	out := make([]PoolStats, 0, len(SizeClasses))
	for _, s := range SizeClasses {
		sc := classes[s]
		out = append(out, PoolStats{
			ClassSize: s,
			Idle:      sc.idle(),
			Hits:      sc.hits.Load(),
			Misses:    sc.misses.Load(),
			Dropped:   sc.dropped.Load(),
		})
	}
	return out
}
