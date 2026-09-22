// Copyright 2026 QuickMedia contributors
// SPDX-License-Identifier: Apache-2.0

// Package testtls is the shared test certificate generator used by the TLS
// variant tests in the protocol adapters. The cert is regenerated for every
// test run rather than committed as a fixture, because a stale fixture would
// silently stop exercising the code it was there to exercise.
package testtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"time"
)

// Generate writes a self-signed cert and key for a private IP into cert and
// key, and returns the tls.Config they load into. The listener is exercised
// with verification disabled, so the subject is a placeholder: the point of
// the fixture is the on-disk PEM and the config it produces, not identity.
func Generate(cert, key string) (*tls.Config, error) {
	c, k, err := material()
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(cert, c, 0o600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(key, k, 0o600); err != nil {
		return nil, err
	}
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// InsecureClient is the client config that pairs with Generate: the test
// cert is self-signed, so a client that verified it would fail on trust
// rather than on the wire. The server still enforces its own floor.
func InsecureClient() *tls.Config {
	return &tls.Config{InsecureSkipVerify: true}
}

// material returns the PEM of a self-signed cert and its key.
func material() ([]byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 63))
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  false,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	derKey, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: derKey})
	return certPEM, keyPEM, nil
}
