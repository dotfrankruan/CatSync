package daemon

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

const (
	// ServerName is the fixed SAN used for every ephemeral certificate. The
	// peer is identified by its exact DER pin, not by a DNS name.
	ServerName = "catsync-ephemeral"
	// DefaultCertTTL covers long transfers that keep opening new data
	// connections; identity is pinned per run, so the window is not the
	// security boundary.
	DefaultCertTTL = 7 * 24 * time.Hour
	// certClockSkew backdates NotBefore so modest clock differences between
	// the two hosts do not reject a valid run.
	certClockSkew = 10 * time.Minute
)

type certRole int

const (
	roleServer certRole = iota
	roleClient
)

// certPair is one in-memory ephemeral identity: an ECDSA P-256 key, a
// self-signed leaf certificate, and its DER pin.
type certPair struct {
	role certRole
	cert *x509.Certificate
	der  []byte
	key  *ecdsa.PrivateKey
	pem  []byte
	pin  [32]byte
}

// newCertPair generates a fresh identity for one run.
func newCertPair(role certRole, ttl time.Duration) (*certPair, error) {
	if ttl <= 0 {
		ttl = DefaultCertTTL
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("daemon: generate key: %w", err)
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, fmt.Errorf("daemon: generate serial: %w", err)
	}
	now := time.Now()
	usage := x509.ExtKeyUsageServerAuth
	if role == roleClient {
		usage = x509.ExtKeyUsageClientAuth
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: ServerName},
		DNSNames:              []string{ServerName},
		NotBefore:             now.Add(-certClockSkew),
		NotAfter:              now.Add(ttl),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{usage},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("daemon: create certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("daemon: parse certificate: %w", err)
	}
	return &certPair{
		role: role,
		cert: cert,
		der:  der,
		key:  key,
		pem:  encodeCertPEM(der),
		pin:  sha256.Sum256(der),
	}, nil
}

// tlsCertificate returns the identity in the form crypto/tls expects.
func (p *certPair) tlsCertificate() tls.Certificate {
	return tls.Certificate{
		Certificate: [][]byte{p.der},
		PrivateKey:  p.key,
		Leaf:        p.cert,
	}
}

// encodeCertPEM wraps one DER certificate in PEM.
func encodeCertPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// parseSingleCertPEM parses exactly one certificate from PEM bytes.
func parseSingleCertPEM(data []byte) (*x509.Certificate, []byte, error) {
	var (
		cert *x509.Certificate
		der  []byte
	)
	rest := data
	for {
		block, r := pem.Decode(rest)
		if block == nil {
			break
		}
		rest = r
		if block.Type != "CERTIFICATE" {
			return nil, nil, protocol.NewError(protocol.CodeTLSCertInvalid, "unexpected PEM block %q", block.Type)
		}
		if cert != nil {
			return nil, nil, protocol.NewError(protocol.CodeTLSCertInvalid, "more than one certificate in message")
		}
		parsed, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, nil, protocol.NewError(protocol.CodeTLSCertInvalid, "parse certificate: %v", err)
		}
		cert, der = parsed, block.Bytes
	}
	if cert == nil {
		return nil, nil, protocol.NewError(protocol.CodeTLSCertInvalid, "no certificate in message")
	}
	if len(rest) != 0 && len(bytesTrimSpace(rest)) != 0 {
		return nil, nil, protocol.NewError(protocol.CodeTLSCertInvalid, "unexpected data after certificate")
	}
	return cert, der, nil
}

// bytesTrimSpace trims ASCII whitespace without importing bytes elsewhere.
func bytesTrimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\n' || b[0] == '\r' || b[0] == '\t') {
		b = b[1:]
	}
	for len(b) > 0 {
		c := b[len(b)-1]
		if c != ' ' && c != '\n' && c != '\r' && c != '\t' {
			break
		}
		b = b[:len(b)-1]
	}
	return b
}

// pinOf returns the SHA-256 of a certificate DER.
func pinOf(der []byte) [32]byte { return sha256.Sum256(der) }

// verifyPeerCert checks the structural expectations an ephemeral peer
// certificate must satisfy. Standard TLS verification also enforces EKU and
// validity; this gives an earlier, clearer error and rejects a mismatched
// role outright.
func verifyPeerCert(cert *x509.Certificate, role certRole, now time.Time) error {
	if cert == nil {
		return protocol.NewError(protocol.CodeTLSCertInvalid, "peer certificate is missing")
	}
	want := x509.ExtKeyUsageServerAuth
	if role == roleClient {
		want = x509.ExtKeyUsageClientAuth
	}
	okEKU := false
	for _, u := range cert.ExtKeyUsage {
		if u == want {
			okEKU = true
			break
		}
	}
	if !okEKU {
		return protocol.NewError(protocol.CodeTLSCertInvalid, "peer certificate has the wrong extended key usage")
	}
	if now.Add(certClockSkew).Before(cert.NotBefore) {
		return protocol.NewError(protocol.CodeTLSCertInvalid, "peer certificate is not yet valid")
	}
	if now.Add(-certClockSkew).After(cert.NotAfter) {
		return protocol.NewError(protocol.CodeTLSCertInvalid, "peer certificate has expired")
	}
	if !cert.BasicConstraintsValid || cert.IsCA {
		return protocol.NewError(protocol.CodeTLSCertInvalid, "peer certificate must not be a CA")
	}
	return nil
}

// certPool returns a pool containing exactly one trust anchor.
func certPool(cert *x509.Certificate) *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return pool
}

// verifyPinCallback returns a VerifyConnection callback that enforces the
// exact peer leaf DER pin. Chain, EKU, SAN, and validity are enforced by the
// standard verifier through RootCAs/ClientCAs and ServerName.
func verifyPinCallback(expected [32]byte, code string) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return protocol.NewError(code, "peer sent no certificate")
		}
		got := pinOf(cs.PeerCertificates[0].Raw)
		if got != expected {
			return protocol.NewError(code, "peer certificate pin does not match this run")
		}
		return nil
	}
}

// serverTLSConfig builds the server-side mTLS configuration pinned to the
// client certificate exchanged during bootstrap.
func serverTLSConfig(server *certPair, clientCert *x509.Certificate, protos []string) *tls.Config {
	return &tls.Config{
		MinVersion:             tls.VersionTLS13,
		MaxVersion:             tls.VersionTLS13,
		Certificates:           []tls.Certificate{server.tlsCertificate()},
		ClientAuth:             tls.RequireAndVerifyClientCert,
		ClientCAs:              certPool(clientCert),
		NextProtos:             protos,
		SessionTicketsDisabled: true,
		VerifyConnection:       verifyPinCallback(pinOf(clientCert.Raw), protocol.CodeTLSPeerMismatch),
	}
}

// clientTLSConfig builds the client-side mTLS configuration pinned to the
// server certificate received during bootstrap.
func clientTLSConfig(client *certPair, serverCert *x509.Certificate, protos []string) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
		Certificates:       []tls.Certificate{client.tlsCertificate()},
		RootCAs:            certPool(serverCert),
		ServerName:         ServerName,
		NextProtos:         protos,
		ClientSessionCache: nil,
		VerifyConnection:   verifyPinCallback(pinOf(serverCert.Raw), protocol.CodeTLSPeerMismatch),
	}
}
