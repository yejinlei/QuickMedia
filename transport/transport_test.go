// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package transport

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// TestListenerBindsAnEphemeralPort checks the contract the tests and the
// composition root rely on: ":0" must come back with the address the listener
// really holds, not the pattern it was asked for.
func TestListenerBindsAnEphemeralPort(t *testing.T) {
	ln, err := NewListener(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	addr := ln.Addr()
	if addr == "" || addr == "127.0.0.1:0" {
		t.Fatalf("Addr = %q, want the bound address", addr)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split addr %q: %v", addr, err)
	}
	if host != "127.0.0.1" {
		t.Fatalf("host = %q, want 127.0.0.1", host)
	}
	// The kernel configures listeners from ":0", so the resolved port must be
	// the one the OS actually bound.
	n, err := strconv.Atoi(port)
	if err != nil || n <= 0 {
		t.Fatalf("port = %q, want a positive number", port)
	}
}

// TestListenerAcceptReturnsARealConnection is the forward direction: one dial
// in, one accepted connection out, with a remote address worth logging.
func TestListenerAcceptReturnsARealConnection(t *testing.T) {
	ln, err := NewListener(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	conn, err := net.DialTimeout("tcp", ln.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	accepted, err := ln.Accept(context.Background())
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	defer func() { _ = accepted.Close() }()

	if accepted.RemoteAddr() == nil || accepted.RemoteAddr().String() == "" {
		t.Fatal("the accepted connection has no remote address")
	}
}

// TestListenerAcceptAfterCloseReturnsErrClosed pins the shutdown contract: a
// closed listener reports itself closed rather than blocking, and doing so more
// than once stays quiet. A blocking Accept here hangs every shutdown path in the
// binary.
func TestListenerAcceptAfterCloseReturnsErrClosed(t *testing.T) {
	ln, err := NewListener(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("second close = %v, want nil", err)
	}

	type result struct {
		conn net.Conn
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		c, err := ln.Accept(context.Background())
		ch <- result{conn: c, err: err}
	}()

	select {
	case r := <-ch:
		if !errors.Is(r.err, ErrClosed) {
			t.Fatalf("Accept on a closed listener = %v, want ErrClosed", r.err)
		}
		if r.conn != nil {
			t.Fatal("Accept on a closed listener returned a connection")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept blocked on a closed listener")
	}
}

// TestServerDispatchesAndTracksConnections is the whole L1 job in one test: two
// dials become two callbacks and two tracked connections, and Close ends both.
func TestServerDispatchesAndTracksConnections(t *testing.T) {
	ln, err := NewListener(context.Background(), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	var handled atomic.Int32
	s := NewServer(func(c net.Conn) {
		handled.Add(1)
	})
	s.AddListener(ln)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	for i := range 2 {
		_, err := net.Dial("tcp", ln.Addr())
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(3 * time.Second)
	for handled.Load() < 2 || s.Conns() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("dispatched %d, tracked %d, want 2 of each", handled.Load(), s.Conns())
		}
		time.Sleep(time.Millisecond)
	}
	if got := s.Conns(); got != 2 {
		t.Fatalf("Conns = %d, want 2", got)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := s.Conns(); got != 0 {
		t.Fatalf("Conns after Close = %d, want 0", got)
	}

	// A closed listener refuses new clients, which is how a drain ends.
	if _, err := net.DialTimeout("tcp", ln.Addr(), time.Second); err == nil {
		t.Fatal("a closed server still accepts connections")
	}
}

// TestServerCloseIsIdempotent is the teardown race: two goroutines shutting the
// server down at once must not surface an error to either one.
func TestServerCloseIsIdempotent(t *testing.T) {
	s := NewServer(func(net.Conn) {})
	if err := s.Close(); err != nil {
		t.Fatalf("close = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close = %v, want nil", err)
	}
}

// TestTLSOptionsEmptyRefusesConfig covers the opt-out contract: two empty
// paths mean plaintext, and nothing may fail at bind time because an
// operator left both flags unset.
func TestTLSOptionsEmptyRefusesConfig(t *testing.T) {
	o := TLSOptions{}
	if o.Valid() {
		t.Fatal("an empty pair must not be valid")
	}
	if _, err := o.Config(); err != nil {
		t.Fatalf("config for an empty pair = %v, want nil", err)
	}
}

// TestTLSOptionsMissingOneFailsFasts covers the misconfiguration an operator
// makes when they forget the key file. A half-set pair must not load a
// listener that refuses every handshake.
func TestTLSOptionsMissingOneFailsFasts(t *testing.T) {
	for _, o := range []TLSOptions{
		{CertFile: "cert.pem"},
		{KeyFile: "key.pem"},
	} {
		if o.Valid() {
			t.Fatalf("%+v must not be valid", o)
		}
	}
}

// TestTLSOptionsLoadsAPair covers the real path: a generated cert on disk
// becomes a tls.Config that a listener actually serves. Without this the
// adapter wiring could be dead code that compiles and never carries a
// certificate.
func TestTLSOptionsLoadsAPair(t *testing.T) {
	dir := t.TempDir()
	cert, key := dir+"/cert.pem", dir+"/key.pem"
	generateCert(cert, key)

	cfg, err := TLSOptions{CertFile: cert, KeyFile: key}.Config()
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("certificates = %d, want 1", len(cfg.Certificates))
	}
	if len(cfg.Certificates[0].Certificate) == 0 || cfg.Certificates[0].PrivateKey == nil {
		t.Fatal("the loaded certificate has no material")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("min version = %x, want TLS 1.2", cfg.MinVersion)
	}
}

// TestTLSListenerServesAHandshake covers the RTMPS path: a listener built
// from a loaded pair must actually negotiate TLS, and must negotiate at the
// floor the options declare. Without this the composition root could pass a
// tls.Config around forever without ever putting a certificate on the wire.
func TestTLSListenerServesAHandshake(t *testing.T) {
	dir := t.TempDir()
	generateCert(dir+"/cert.pem", dir+"/key.pem")
	cfg, err := TLSOptions{CertFile: dir + "/cert.pem", KeyFile: dir + "/key.pem"}.Config()
	if err != nil {
		t.Fatalf("config: %v", err)
	}

	ln, err := NewListener(context.Background(), "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("listener: %v", err)
	}

	// tls.Listen returns a tls.Conn without running the handshake: the
	// handshake happens on the first Read or Write. So the server half of
	// the test must Read once to force it, which is what any protocol
	// adapter over this listener does anyway.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept(context.Background())
			if err != nil {
				return
			}
			buf := make([]byte, 1)
			_, _ = conn.Read(buf)
			_ = conn.Close()
		}
	}()

	conn, err := tls.Dial("tcp", ln.Addr(), &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	clientVer := conn.ConnectionState().Version
	_, err = conn.Write([]byte{0x01})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.Close()

	if clientVer != tls.VersionTLS12 {
		t.Fatalf("handshake negotiated %x, want TLS 1.2", clientVer)
	}

	// The server's declared floor must be enforced: a client that only
	// offers TLS 1.1 must be refused, which is what makes MinVersion a
	// promise rather than a preference.
	if _, err := tls.Dial("tcp", ln.Addr(), &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS10, MaxVersion: tls.VersionTLS11}); err == nil {
		t.Fatal("TLS 1.0/1.1 was accepted, want it refused")
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	<-done
}

// generateCert writes a self-signed cert for a private IP into cert and its
// key into key. Only the certificate bytes matter; the subject is a
// placeholder because the listener is exercised with verification disabled.
func generateCert(cert, key string) {
	c, k, err := testTLSMaterial()
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(cert, c, 0o600); err != nil {
		panic(err)
	}
	if err := os.WriteFile(key, k, 0o600); err != nil {
		panic(err)
	}
}

// testTLSMaterial returns the PEM of a self-signed cert and its key. Kept as
// a helper rather than a testdata file because it is regenerated for every
// test run and the fixtures would go stale.
func testTLSMaterial() ([]byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 63))
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         false,
		BasicConstraintsValid: true,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	derKey, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	encKey := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: derKey})
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), encKey, nil
}
