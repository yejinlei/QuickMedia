// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// tlsconfig loads a certificate pair into a tls.Config. It is the only place
// in the tree that touches disk to build TLS material, so a caller that has
// PEM paths in a flag string does not have to import crypto/tls either to
// hand them here, and an adapter that already has a tls.Config does not have
// to reach through the flag layer to get one.
//
// CertFile and KeyFile both refer to PEM files. CertFile may bundle the
// chain as one file, which is the layout most CAs hand out.
package transport

import (
	"crypto/tls"
	"fmt"
)

// TLSOptions is a cert and key on disk. Both are required to build TLS.
type TLSOptions struct {
	CertFile string
	KeyFile  string
}

// Valid reports whether both paths are present. An empty pair is the
// documented way for a listener to opt out of TLS rather than to be handed
// half a certificate and fail at bind time.
func (o TLSOptions) Valid() bool { return o.CertFile != "" && o.KeyFile != "" }

// Config loads the certificate pair into a tls.Config. A fully empty pair
// returns (nil, nil) — that is the opt-out form a listener uses for a
// plaintext port, and it keeps a caller from having to special-case the
// flag strings themselves. A half-set pair is refused rather than handed
// over, so a misconfiguration fails at startup rather than at the first
// handshake.
func (o TLSOptions) Config() (*tls.Config, error) {
	if o.CertFile == "" && o.KeyFile == "" {
		return nil, nil
	}
	if !o.Valid() {
		return nil, fmt.Errorf("transport: tls requires both a certificate and a key")
	}
	cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("transport: tls keypair: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, nil
}
