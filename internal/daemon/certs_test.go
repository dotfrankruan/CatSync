package daemon

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
)

// handshake runs one loopback TLS 1.3 mTLS handshake and returns the
// server-side connection state.
func handshake(t *testing.T, server, client *certPair, serverProtos, clientProtos []string) error {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	done := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			done <- err
			return
		}
		tc := tls.Server(conn, serverTLSConfig(server, client.cert, serverProtos))
		done <- tc.Handshake()
	}()
	d, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	conn := tls.Client(d, clientTLSConfig(client, server.cert, clientProtos))
	if err := conn.Handshake(); err != nil {
		<-done
		return err
	}
	return <-done
}

func TestHandshakePinnedSuccess(t *testing.T) {
	server, _ := newCertPair(roleServer, time.Hour)
	client, _ := newCertPair(roleClient, time.Hour)
	if err := handshake(t, server, client, []string{protocol.ALPNHTTP}, []string{protocol.ALPNHTTP}); err != nil {
		t.Fatalf("handshake: %v", err)
	}
}

func TestHandshakeRejectsWrongServerPin(t *testing.T) {
	server, _ := newCertPair(roleServer, time.Hour)
	other, _ := newCertPair(roleServer, time.Hour)
	client, _ := newCertPair(roleClient, time.Hour)
	// Trust the real server certificate in the pool but pin the other one.
	cfg := clientTLSConfig(client, server.cert, []string{protocol.ALPNHTTP})
	cfg.RootCAs = certPool(other.cert)
	cfg.VerifyConnection = verifyPinCallback(pinOf(other.cert.Raw), protocol.CodeTLSPeerMismatch)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		tls.Server(c, serverTLSConfig(server, client.cert, []string{protocol.ALPNHTTP})).Handshake()
	}()
	d, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := tls.Client(d, cfg).Handshake(); err == nil {
		t.Fatal("handshake succeeded with a wrong pin")
	}
}

func TestHandshakeRejectsWrongEKU(t *testing.T) {
	server, _ := newCertPair(roleServer, time.Hour)
	client, _ := newCertPair(roleClient, time.Hour)
	// Server certificate as the client's pin would have client EKU; verify
	// role enforcement directly.
	wrong, _ := newCertPair(roleClient, time.Hour)
	if err := verifyPeerCert(wrong.cert, roleServer, time.Now()); err == nil {
		t.Fatal("client-EKU certificate accepted as a server identity")
	}
	if err := verifyPeerCert(server.cert, roleClient, time.Now()); err == nil {
		t.Fatal("server-EKU certificate accepted as a client identity")
	}
	if err := verifyPeerCert(client.cert, roleClient, time.Now()); err != nil {
		t.Fatalf("client identity rejected: %v", err)
	}
}

func TestHandshakeRejectsExpired(t *testing.T) {
	// A pair with a past window, used only for verifyPeerCert.
	expired, _ := newCertPair(roleServer, time.Hour)
	expired.cert.NotAfter = time.Now().Add(-time.Hour)
	if err := verifyPeerCert(expired.cert, roleServer, time.Now()); err == nil {
		t.Fatal("expired certificate accepted")
	}
}

func TestParseSingleCertPEM(t *testing.T) {
	p, _ := newCertPair(roleServer, time.Hour)
	cert, der, err := parseSingleCertPEM(p.pem)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if string(der) != string(p.der) || cert == nil {
		t.Fatal("parsed certificate does not round-trip")
	}
	two := append(append([]byte{}, p.pem...), p.pem...)
	if _, _, err := parseSingleCertPEM(two); err == nil {
		t.Fatal("two certificates accepted")
	}
	if _, _, err := parseSingleCertPEM([]byte("not a pem")); err == nil {
		t.Fatal("garbage accepted")
	}
}
