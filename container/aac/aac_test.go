// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package aac

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/yejinlei/quickmedia/container"
)

// TestAudioSpecificConfig pins the byte encoding against the AAC-LC profile.
//
// AAC-LC is ISO objectType 2, which is why the high byte is 0x10-0x15 across
// the supported sample rates. An off-by-one on the objectType (1, i.e. AAC
// Main) would produce 0x0A-0x0D instead and a decoder reading the object type
// would refuse or mis-handle every frame.
//
// 44100 Hz stereo is 0x12 0x10, which is what ffmpeg writes for a standard
// AAC-LC stream. A wrong channel configuration is a hard failure: the decoder
// reads the channel count from these two bytes and has no other source, so
// every frame after the sequence start would decode with the wrong layout.
func TestAudioSpecificConfig(t *testing.T) {
	cases := []struct {
		rate     int
		channels uint8
		want     []byte
		ok       bool
	}{
		{44100, 1, []byte{0x12, 0x08}, true},
		{44100, 2, []byte{0x12, 0x10}, true},
		{48000, 1, []byte{0x11, 0x88}, true},
		{48000, 2, []byte{0x11, 0x90}, true},
		{8000, 1, []byte{0x15, 0x88}, true},
		{96000, 2, []byte{0x10, 0x10}, true},
		{0, 0, nil, false},     // unsupported rate is a refusal
		{44100, 8, nil, false}, // more than 7 channels is unrepresentable
	}
	for _, c := range cases {
		got, ok := AudioSpecificConfig(c.rate, c.channels)
		if ok != c.ok {
			t.Fatalf("rate=%d ch=%d ok=%v, want %v", c.rate, c.channels, ok, c.ok)
		}
		if c.ok && !bytes.Equal(got, c.want) {
			t.Fatalf("rate=%d ch=%d = % x, want % x", c.rate, c.channels, got, c.want)
		}
	}
}

// TestAACSequenceTag checks that the sequence start tag ffmpeg writes is the
// tag this packer writes. Without it the audio track is refused outright.
func TestAACSequenceTag(t *testing.T) {
	p := newFLVFromParams(map[string]string{
		"sampleRate":       "44100",
		"numberOfChannels": "2",
	})

	frames := p.ConfigFrames()
	if len(frames) != 1 {
		t.Fatalf("got %d config frames, want 1", len(frames))
	}
	f := frames[0]
	if !f.Config || !f.Key {
		t.Fatal("sequence start must be marked Config and Key")
	}

	// ffmpeg writes A1 01 12 10 for 44100 Hz stereo.
	want := []byte{0xA1, 0x01, 0x12, 0x10}
	if !bytes.Equal(f.Data, want) {
		t.Fatalf("sequence tag = % x, want % x", f.Data, want)
	}

	payload, ok := container.ParseAudioTagBody(f.Data)
	if !ok || !bytes.Equal(payload, want[2:]) {
		t.Fatalf("payload = % x (ok=%v), want % x", payload, ok, want[2:])
	}

	// No parameters, no tag. Emitting a descriptor with no content is worse
	// than emitting none, because it is indistinguishable from a broken stream.
	if got := newFLVFromParams(nil).ConfigFrames(); len(got) != 0 {
		t.Fatalf("empty params produced %d config frames, want 0", len(got))
	}
}

// TestADTSFrameLength checks the frame length field round trips, which is what
// keeps a raw ADTS stream frame-aligned.
//
// The parser needs the whole frame because it refuses a header it cannot
// validate: the length field covers the header plus the payload, so a bare
// header never satisfies its own length. Feeding only the header would be
// testing the guard rather than the field.
func TestADTSFrameLength(t *testing.T) {
	payload := []byte{0x21, 0x6f, 0x84, 0x03, 0x80}
	head := MakeADTSHeader(44100, 2, payload)
	if len(head) != adtsFrameLen {
		t.Fatalf("header length = %d, want %d", len(head), adtsFrameLen)
	}

	frame := append(append([]byte{}, head...), payload...)
	frameLen, profile, ok := ParseADTSHeader(frame)
	if !ok {
		t.Fatal("ParseADTSHeader rejected its own output")
	}
	if int(frameLen) != len(payload)+adtsFrameLen {
		t.Fatalf("frame length = %d, want %d", frameLen, len(payload)+adtsFrameLen)
	}
	// ADTS's profile field encodes AAC-LC as 1 (0 = Main, 2 = SSR, 3 = LTP),
	// which is why ffmpeg writes 0x50 in byte 2 for an AAC-LC stream. 0x50>>6
	// is 1, not 2. AAC-LC is the only profile QuickMedia emits, so anything
	// else means the header was built wrong.
	if profile != 1 {
		t.Fatalf("profile = %d, want 1", profile)
	}

	if _, _, ok := ParseADTSHeader(head); ok {
		t.Fatal("a bare header must not satisfy its own length field")
	}
}

// TestADTSHeaderMatchesFFmpeg pins the header bytes against ffmpeg's actual
// output.
//
// These are the first frames of `ffmpeg -f lavfi -i sine -ac N -c:a aac
// -b:a 64k -f adts` for a rate x channel matrix. The payload length is the
// frame length minus the 7-byte header, so each case reconstructs the exact
// payload size ffmpeg produced. Without this the encoder could be self-consistent
// while disagreeing with every real stream, which is how a header can look
// correct in its own round trip and still fail to play.
//
// The 3-channel case is deliberately omitted: ffmpeg's own encoder writes
// channelConfiguration 0 for -ac 3, which ffprobe then reports as 3 channels
// anyway. Reproducing ffmpeg's quirk would mean writing a header that
// describes a channel layout the stream does not have, so QuickMedia emits the
// honest value instead.
func TestADTSHeaderMatchesFFmpeg(t *testing.T) {
	cases := []struct {
		rate    int
		ch      uint8
		payload int
		want    string
	}{
		{96000, 1, 124, "ff f1 40 40 10 7f fc"},
		{96000, 2, 91, "ff f1 40 80 0c 5f fc"},
		{96000, 4, 70, "ff f1 41 00 09 bf fc"},
		{48000, 1, 192, "ff f1 4c 40 18 ff fc"},
		{48000, 2, 132, "ff f1 4c 80 11 7f fc"},
		{44100, 1, 280, "ff f1 50 40 23 ff fc"},
		{44100, 2, 133, "ff f1 50 80 11 9f fc"},
		{44100, 4, 148, "ff f1 51 00 13 7f fc"},
		{8000, 1, 601, "ff f1 6c 40 4c 1f fc"},
		{8000, 2, 702, "ff f1 6c 80 58 bf fc"},
	}
	for _, c := range cases {
		payload := make([]byte, c.payload)
		for i := range payload {
			payload[i] = byte(i)
		}
		head := MakeADTSHeader(c.rate, c.ch, payload)
		if head == nil {
			t.Fatalf("rate=%d ch=%d: got nil header", c.rate, c.ch)
		}
		if got := fmt.Sprintf("% x", head); got != c.want {
			t.Fatalf("rate=%d ch=%d len=%d: header = %s, want %s", c.rate, c.ch, c.payload, got, c.want)
		}
		// The length field must round trip through the parser.
		frameLen, _, ok := ParseADTSHeader(append(append([]byte{}, head...), payload...))
		if !ok || int(frameLen) != c.payload+adtsFrameLen {
			t.Fatalf("rate=%d ch=%d: parsed frameLen=%d ok=%v, want %d", c.rate, c.ch, frameLen, ok, c.payload+adtsFrameLen)
		}
	}
}

// TestADTSHeaderRejectsUnrepresentable checks that the encoder refuses rather
// than emitting a header it cannot back.
func TestADTSHeaderRejectsUnrepresentable(t *testing.T) {
	if h := MakeADTSHeader(0, 2, []byte{1, 2, 3}); h != nil {
		t.Fatalf("unsupported rate produced header % x", h)
	}
	if h := MakeADTSHeader(44100, 8, []byte{1, 2, 3}); h != nil {
		t.Fatalf("too many channels produced header % x", h)
	}
	if h := MakeADTSHeader(44100, 2, nil); h != nil {
		t.Fatal("empty payload produced a header")
	}
}
