// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Command quickmedia is the single binary of QuickMedia.
//
// It is the only file in the tree that may compose every layer at once: L1
// transport, L2 codecs, L3 protocol adapters, and L5 kernel. Every other
// package composes a strict subset of them, which is the architecture's
// one-directional dependency rule in executable form.
//
// Nothing here implements a protocol. Adapters are registered in init() and
// looked up through the registry; this file only decides which port each
// listener binds to and which handlers share one HTTP mux. Adding a protocol is
// therefore a new package plus one import line here, not a change to the kernel.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/yejinlei/quickmedia/adapters/hls"
	_ "github.com/yejinlei/quickmedia/adapters/httpflv"
	_ "github.com/yejinlei/quickmedia/adapters/rtmp"
	_ "github.com/yejinlei/quickmedia/adapters/rtsp"
	_ "github.com/yejinlei/quickmedia/container/aac"
	_ "github.com/yejinlei/quickmedia/container/h264"

	"github.com/yejinlei/quickmedia/adapters/hls"
	hhttpflv "github.com/yejinlei/quickmedia/adapters/httpflv"
	"github.com/yejinlei/quickmedia/adapters/rtmp"
	"github.com/yejinlei/quickmedia/adapters/rtsp"
	"github.com/yejinlei/quickmedia/kernel/path"
	"github.com/yejinlei/quickmedia/kernel/registry"
	"github.com/yejinlei/quickmedia/transport"
)

// Config is the whole command-line surface.
type Config struct {
	RTSPAddr     string
	RTMPAddr     string
	HTTPAddr     string
	RTPRTP       string
	RTPRTCP      string
	HLSDir       string
	SegmentDur   time.Duration
	SegmentCount int
	RingSize     int
	Retain       time.Duration
	Heartbeat    time.Duration
}

// LoadConfig applies the defaults and then the flags. An empty address binds an
// ephemeral port, which is what makes the binary testable in CI without
// knowing which ports happen to be free.
func LoadConfig() *Config {
	c := &Config{
		RTSPAddr:     ":8554",
		RTMPAddr:     ":1935",
		HTTPAddr:     ":8080",
		RTPRTP:       ":8000",
		RTPRTCP:      ":8001",
		SegmentDur:   time.Second,
		SegmentCount: 4,
		RingSize:     512,
		Retain:       5 * time.Second,
		Heartbeat:    10 * time.Second,
	}
	flag.StringVar(&c.RTSPAddr, "rtsp", c.RTSPAddr, "RTSP control port")
	flag.StringVar(&c.RTMPAddr, "rtmp", c.RTMPAddr, "RTMP control port")
	flag.StringVar(&c.HTTPAddr, "http", c.HTTPAddr, "HTTP port for HLS and HTTP-FLV")
	flag.StringVar(&c.RTPRTP, "rtp", c.RTPRTP, "UDP RTP port, even")
	flag.StringVar(&c.RTPRTCP, "rtcp", c.RTPRTCP, "UDP RTCP port, rtp+1")
	flag.StringVar(&c.HLSDir, "hlsdir", "", "segment directory; empty means RAM")
	flag.DurationVar(&c.SegmentDur, "segdur", c.SegmentDur, "HLS segment duration")
	flag.IntVar(&c.SegmentCount, "segcount", c.SegmentCount, "HLS segments kept")
	flag.IntVar(&c.RingSize, "ring", c.RingSize, "per-subscriber queue depth")
	flag.DurationVar(&c.Retain, "retain", c.Retain, "path retain window after a publisher leaves")
	flag.DurationVar(&c.Heartbeat, "heartbeat", c.Heartbeat, "slow-consumer timeout")
	flag.Parse()
	return c
}

// runResult reports what run() actually bound. Addresses come back from the
// listeners rather than being echoed from the config, because an empty
// configured address binds an ephemeral port: the test that starts the binary
// needs to know which port it will dial, and no other caller needs this at all.
type runResult struct {
	// RTSP, RTMP and HTTP are the addresses to connect to, host:port.
	RTSP, RTMP, HTTP string
	// Closers tear everything down, newest listener first.
	Closers []func()
}

// run builds the manager, mounts every adapter, and returns once they are all
// listening. The caller owns the lifetime of the returned closers.
func run(c *Config) (runResult, error) {
	var nilResult runResult
	cfg := path.DefaultConfig()
	cfg.RingSize = c.RingSize
	cfg.Retain = c.Retain
	cfg.HeartbeatTimeout = c.Heartbeat

	mgr := path.NewManager(cfg)
	mgr.Open()

	rtspSrv := rtsp.NewServer(mgr, rtsp.Options{
		RTSPAddress:    c.RTSPAddr,
		UDPRTPAddress:  c.RTPRTP,
		UDPRTCPAddress: c.RTPRTCP,
	})
	if err := rtspSrv.Start(context.Background()); err != nil {
		return nilResult, fmt.Errorf("rtsp: %w", err)
	}

	hlsSrv := hls.NewServer(mgr, hls.Options{
		Dir:             c.HLSDir,
		SegmentDuration: c.SegmentDur,
		SegmentCount:    c.SegmentCount,
	})

	// HLS playlists and HTTP-FLV both speak HTTP, so one mux and one listener
	// serve both. HTTP-FLV owns the tail and HLS owns every other path, which
	// keeps the two adapters from having to know about each other.
	mux := http.NewServeMux()
	mux.Handle("/", hhttpflv.NewServer(mgr))
	// HLS is keyed by its first path segment, so the mount prefix has to be
	// removed before the adapter sees the URL: otherwise /live/stream/index.m3u8
	// would serve the path named "live" rather than "stream", and the server
	// could only ever offer one HLS path per mount. The path is what an operator
	// puts in the URL, exactly as HTTP-FLV takes it from its own.
	mux.Handle("/live/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r2 := *r
		r2.URL.Path = strings.TrimPrefix(r.URL.Path, "/live")
		hlsSrv.ServeHTTP(w, &r2)
	}))

	ln, err := transport.NewListener(context.Background(), c.HTTPAddr, nil)
	if err != nil {
		return nilResult, fmt.Errorf("http listener: %w", err)
	}
	httpSrv := &http.Server{Handler: mux}
	go func() {
		_ = httpSrv.Serve(&httpListener{ln: ln})
	}()

	// RTMP needs a raw TCP listener rather than an http.Handler, so it gets a
	// transport.Listener and an accept loop that hands each connection to the
	// adapter's ServeConn.
	rtmpLn, err := transport.NewListener(context.Background(), c.RTMPAddr, nil)
	if err != nil {
		return nilResult, fmt.Errorf("rtmp listener: %w", err)
	}
	go acceptRTMP(context.Background(), rtmpLn, mgr)

	// Every address here is what the listener bound to, not what was configured.
	log.Printf("QuickMedia ready: rtsp %s, rtmp %s, http %s (adapters: %s)",
		rtspSrv.Addr(), rtmpLn.Addr(), ln.Addr(), registry.Names(registry.TAdapter))

	return runResult{
		RTSP: rtspSrv.Addr(),
		RTMP: rtmpLn.Addr(),
		HTTP: ln.Addr(),
		Closers: []func(){
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = httpSrv.Shutdown(ctx)
			},
			rtspSrv.Close,
			func() { _ = rtmpLn.Close() },
			hlsSrv.Close,
			mgr.Close,
		},
	}, nil
}

// httpListener adapts transport.Listener to net.Listener so it can be handed to
// http.Server. It is the one place in the tree that crosses L1 into stdlib HTTP:
// net/http has no context, so the server's lifetime is the context here.
type httpListener struct {
	ln *transport.Listener
}

func (l *httpListener) Accept() (net.Conn, error) {
	return l.ln.Accept(context.Background())
}

func (l *httpListener) Close() error   { return l.ln.Close() }
func (l *httpListener) Addr() net.Addr { return netAddr(l.ln.Addr()) }

// netAddr keeps the listener's address from having to know about net.Addr.
type netAddr string

func (a netAddr) Network() string { return "tcp" }
func (a netAddr) String() string  { return string(a) }

// acceptRTMP runs the RTMP listener. Each connection is one session, so a new
// goroutine per connection is the correct shape: an RTMP publisher holds its
// connection for the life of the stream.
func acceptRTMP(ctx context.Context, ln *transport.Listener, mgr *path.Manager) {
	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		go func() {
			if err := rtmp.ServeConn(ctx, conn, mgr); err != nil {
				log.Printf("rtmp: %v", err)
			}
			_ = conn.Close()
		}()
	}
}

func main() {
	res, err := run(LoadConfig())
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	log.Printf("shutting down")
	for i := len(res.Closers) - 1; i >= 0; i-- {
		res.Closers[i]()
	}
}
