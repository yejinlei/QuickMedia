// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

package testtls

import (
	"crypto/tls"
	"os"
	"testing"
)

// TestGenerateRoundTrips proves the pair Generate writes is a pair
// tls.LoadX509KeyPair will accept. Without this a cert that only looks
// like PEM would still satisfy every caller that just hands the paths to
// the transport layer, and the failure would show up as a bind error at
// startup rather than as a test failure here.
func TestGenerateRoundTrips(t *testing.T) {
	dir := t.TempDir()
	cert, key := dir+"/cert.pem", dir+"/key.pem"

	cfg, err := Generate(cert, key)
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	if _, err := os.Stat(cert); err != nil {
		t.Fatalf("cert not written: %v", err)
	}
	if _, err := os.Stat(key); err != nil {
		t.Fatalf("key not written: %v", err)
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

	// The second load is the real check: it proves the on-disk PEM parses,
	// which is the property an operator relies on when they point QuickMedia
	// at a file they were given.
	if _, err := tls.LoadX509KeyPair(cert, key); err != nil {
		t.Fatalf("second load: %v", err)
	}
}

// TestGenerateWritesFailsFasts covers the path an operator hits when they
// point QuickMedia at a path they cannot create. The failure must be an
// error here, not a listener that later refuses every handshake.
func TestGenerateWritesFailsFasts(t *testing.T) {
	missing := t.TempDir() + "/does-not-exist"
	if _, err := Generate(missing+"/cert.pem", missing+"/key.pem"); err == nil {
		t.Fatal("generate into a missing directory succeeded, want an error")
	}
}
