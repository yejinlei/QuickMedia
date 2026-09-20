// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package path is layer L5: the session and stream core.
//
// This package is the only place in QuickMedia that owns a path. Everything
// else — protocol adapters, capabilities, the control plane — talks to a
// path through a Manager or through the subscription it was granted.
package path

import "time"

// defaultRetain is how long the kernel holds a path's content after the
// publisher disconnects. Long enough that a media device restarting over a
// flaky link recovers seamlessly; short enough that a permanently dead
// publisher does not pin a broadcast worker.
const defaultRetain = 5 * time.Second

// defaultHeartbeatTimeout drops a subscriber that has stopped reading. A
// subscriber that is alive but slow is handled by the fill-based policy; this
// is the rule for a subscriber that simply stopped, whose ring would otherwise
// fill and stall the broadcaster's fan-out.
const defaultHeartbeatTimeout = 10 * time.Second

// Config is the kernel's tunable surface for paths. Every field here is a
// knob an operator may set; nothing in Config is a protocol name, which is
// what keeps the media plane protocol-independent.
type Config struct {
	// RingSize is the per-subscriber queue depth in units. 512 at 30 fps is
	// roughly 17 s of content, which is generous enough to absorb real
	// rebuffering without needing a separate "live edge" concept.
	RingSize int `json:"ring_size"`

	// WriterInRing bounds the publisher-to-broadcaster channel.
	WriterInRing int `json:"writer_in_ring"`

	// MaxSubscriptions caps how many readers one path may feed. Hitting the
	// cap rejects new readers rather than silently degrading existing ones.
	MaxSubscriptions int `json:"max_subscriptions"`

	// Retain is the publisher-disconnect retain window.
	Retain time.Duration `json:"retain"`

	// HeartbeatTimeout is the slow-consumer read-gap limit.
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout"`

	// BackPressure is the drop policy for stalled subscribers.
	BackPressure BackPressure `json:"back_pressure"`

	// EvictInterval is how often the manager sweeps for slow subscribers.
	EvictInterval time.Duration `json:"evict_interval"`

	// MaxPaths caps the number of concurrent paths.
	MaxPaths int `json:"max_paths"`
}

// DefaultConfig returns the baseline configuration.
func DefaultConfig() Config {
	return Config{
		RingSize:         subRingSize,
		WriterInRing:     writerInRing,
		MaxSubscriptions: maxSubscriptions,
		Retain:           defaultRetain,
		HeartbeatTimeout: defaultHeartbeatTimeout,
		BackPressure:     NewDefaultBackPressure(),
		EvictInterval:    250 * time.Millisecond,
		MaxPaths:         100000,
	}
}

// normalize fills in any unset field with its default and clamps anything
// illegal, so a partial config from the control plane never produces a kernel
// that would divide by zero or allocate an unbounded queue.
func (c *Config) normalize() {
	if c.RingSize < 1 {
		c.RingSize = subRingSize
	}
	if c.WriterInRing < 1 {
		c.WriterInRing = writerInRing
	}
	if c.MaxSubscriptions < 1 {
		c.MaxSubscriptions = maxSubscriptions
	}
	if c.Retain < 0 {
		c.Retain = defaultRetain
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if c.EvictInterval <= 0 {
		c.EvictInterval = 250 * time.Millisecond
	}
	if c.MaxPaths < 1 {
		c.MaxPaths = 100000
	}
	if c.BackPressure.WriterInRing < 1 {
		c.BackPressure.WriterInRing = c.WriterInRing
	}
	if c.BackPressure.SubRingSize < 1 {
		c.BackPressure.SubRingSize = c.RingSize
	}
	c.BackPressure.clampPolicy()
}
