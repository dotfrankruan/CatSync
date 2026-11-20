//go:build unix

// Package rsh is the local Unix-socket helper that lets rsync's -e
// transport reach the daemon exec channel without a second ssh process.
// The socket lives in a private 0700 directory, the peer must have the same
// uid, and a per-run random token authenticates the helper. The token is
// passed through the environment, never argv or logs.
package rsh

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon"
)

// Environment variables consumed by `catsync rsh`.
const (
	EnvSocket = "CATSYNC_RSH_SOCKET"
	EnvToken  = "CATSYNC_RSH_TOKEN"
	EnvHost   = "CATSYNC_RSH_HOST"
	EnvUser   = "CATSYNC_RSH_USER"
)

// Response frame types (agent -> helper).
const (
	frameStdout byte = 1
	frameStderr byte = 2
	frameExit   byte = 4
)

const maxAgentLine = 1 << 20

// Agent is one run-scoped rsh listener.
type Agent struct {
	dir    string
	sock   string
	token  string
	host   string
	user   string
	client *daemon.Client
	ln     net.Listener
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	once   sync.Once
}

// Start creates the private socket and begins accepting helpers.
func Start(ctx context.Context, host, user string, client *daemon.Client) (*Agent, error) {
	dir, err := os.MkdirTemp("", "catsync-rsh-")
	if err != nil {
		return nil, fmt.Errorf("rsh: create socket directory: %w", err)
	}
	sock := filepath.Join(dir, "rsh.sock")
	if len(sock) > 100 {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("rsh: socket path %q is too long", sock)
	}
	ln, err := net.Listen("unix", sock)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("rsh: listen: %w", err)
	}
	if err := os.Chmod(sock, 0o600); err != nil {
		_ = ln.Close()
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("rsh: chmod socket: %w", err)
	}
	tok := make([]byte, 32)
	if _, err := rand.Read(tok); err != nil {
		_ = ln.Close()
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("rsh: token: %w", err)
	}
	agentCtx, cancel := context.WithCancel(ctx)
	a := &Agent{
		dir: dir, sock: sock,
		token: base64.RawURLEncoding.EncodeToString(tok),
		host:  host, user: user, client: client,
		ln: ln, ctx: agentCtx, cancel: cancel,
	}
	a.wg.Add(1)
	go a.acceptLoop()
	return a, nil
}

// Env returns the environment variables rsync (and therefore the helper it
// spawns) needs.
func (a *Agent) Env() []string {
	return []string{
		EnvSocket + "=" + a.sock,
		EnvToken + "=" + a.token,
		EnvHost + "=" + a.host,
		EnvUser + "=" + a.user,
	}
}

// Close stops the listener, removes the socket directory, and waits for
// in-flight helpers.
func (a *Agent) Close() {
	a.once.Do(func() {
		a.cancel()
		_ = a.ln.Close()
		a.wg.Wait()
		_ = os.RemoveAll(a.dir)
	})
}

func (a *Agent) acceptLoop() {
	defer a.wg.Done()
	for {
		conn, err := a.ln.Accept()
		if err != nil {
			return
		}
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.handle(conn)
		}()
	}
}

func (a *Agent) handle(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	uid, err := peerUID(conn)
	if err != nil || uid != os.Getuid() {
		return
	}
	br := bufio.NewReaderSize(conn, 4096)
	tokenLine, err := readAgentLine(br)
	if err != nil {
		return
	}
	if subtle.ConstantTimeCompare([]byte(tokenLine), []byte(a.token)) != 1 {
		return
	}
	reqLine, err := readAgentLine(br)
	if err != nil {
		return
	}
	var req helperRequest
	dec := json.NewDecoder(strings.NewReader(string(reqLine)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return
	}
	if req.Host != a.host || req.User != a.user {
		return
	}
	if filepath.Base(req.Program) != "rsync" {
		return
	}
	_ = conn.SetDeadline(time.Time{})
	exec, err := a.client.Exec(a.ctx, "rsync", req.Args)
	if err != nil {
		a.writeExit(conn, 255)
		return
	}
	defer exec.Close()
	// helper stdin -> remote rsync stdin (raw bytes)
	go func() {
		_, _ = io.Copy(exec.Stdin(), br)
		_ = exec.CloseStdin()
	}()
	var wmu sync.Mutex
	writeFrame := func(typ byte, payload []byte) error {
		wmu.Lock()
		defer wmu.Unlock()
		var hdr [5]byte
		hdr[0] = typ
		binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
		if _, err := conn.Write(hdr[:]); err != nil {
			return err
		}
		_, err := conn.Write(payload)
		return err
	}
	var copies sync.WaitGroup
	copies.Add(2)
	copyOut := func(r io.Reader, typ byte) {
		defer copies.Done()
		buf := make([]byte, 32*1024)
		for {
			n, rerr := r.Read(buf)
			if n > 0 {
				if writeFrame(typ, buf[:n]) != nil {
					return
				}
			}
			if rerr != nil {
				return
			}
		}
	}
	go copyOut(exec.Stdout(), frameStdout)
	go copyOut(exec.Stderr(), frameStderr)
	code, werr := exec.Wait(a.ctx)
	copies.Wait()
	if werr != nil && code == 0 {
		code = 255
	}
	a.writeExit(conn, code)
}

func (a *Agent) writeExit(conn net.Conn, code int) {
	var hdr [5]byte
	hdr[0] = frameExit
	binary.BigEndian.PutUint32(hdr[1:], 4)
	_, _ = conn.Write(hdr[:])
	_, _ = conn.Write([]byte{byte(code >> 24), byte(code >> 16), byte(code >> 8), byte(code)})
}

type helperRequest struct {
	Host    string   `json:"host"`
	User    string   `json:"user"`
	Program string   `json:"program"`
	Args    []string `json:"args"`
}

func readAgentLine(br *bufio.Reader) (string, error) {
	var out []byte
	for {
		frag, err := br.ReadSlice('\n')
		out = append(out, frag...)
		if len(out) > maxAgentLine {
			return "", errors.New("rsh: line too long")
		}
		switch {
		case err == nil:
			return strings.TrimRight(string(out), "\r\n"), nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			return "", io.EOF
		default:
			return "", err
		}
	}
}

// RunHelper is the hidden `catsync rsh` command executed by local rsync as
// its -e transport. It never re-splits its argv: it maps [ -l USER ] HOST
// PROGRAM ARGS... to one agent request and exits with the remote code.

func RunHelper(args []string) int {
	sock, token := os.Getenv(EnvSocket), os.Getenv(EnvToken)
	host, user := os.Getenv(EnvHost), os.Getenv(EnvUser)
	if sock == "" || token == "" || host == "" {
		fmt.Fprintln(os.Stderr, "catsync rsh: this command is internal to catsync's daemon transport")
		return 255
	}
	i := 0
	if i+1 < len(args) && args[i] == "-l" {
		i += 2
		if i > len(args) {
			fmt.Fprintln(os.Stderr, "catsync rsh: malformed -l user")
			return 255
		}
	}
	if i >= len(args) {
		fmt.Fprintln(os.Stderr, "catsync rsh: missing host")
		return 255
	}
	if args[i] != host {
		fmt.Fprintf(os.Stderr, "catsync rsh: unexpected host %q\n", args[i])
		return 255
	}
	i++
	if i >= len(args) {
		fmt.Fprintln(os.Stderr, "catsync rsh: missing remote program")
		return 255
	}
	if filepath.Base(args[i]) != "rsync" {
		fmt.Fprintf(os.Stderr, "catsync rsh: remote program %q is not allowed\n", args[i])
		return 255
	}
	i++
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.Dial("unix", sock)
	if err != nil {
		fmt.Fprintf(os.Stderr, "catsync rsh: connect: %v\n", err)
		return 255
	}
	defer conn.Close()
	req, _ := json.Marshal(helperRequest{Host: host, User: user, Program: "rsync", Args: args[i:]})
	if _, err := fmt.Fprintf(conn, "%s\n%s\n", token, req); err != nil {
		fmt.Fprintf(os.Stderr, "catsync rsh: handshake: %v\n", err)
		return 255
	}
	go func() {
		_, _ = io.Copy(conn, os.Stdin)
		if cw, ok := conn.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
	}()
	br := bufio.NewReaderSize(conn, 16*1024)
	var hdr [5]byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			fmt.Fprintf(os.Stderr, "catsync rsh: agent closed: %v\n", err)
			return 255
		}
		n := binary.BigEndian.Uint32(hdr[1:])
		payload := make([]byte, n)
		if _, err := io.ReadFull(br, payload); err != nil {
			return 255
		}
		switch hdr[0] {
		case frameStdout:
			_, _ = os.Stdout.Write(payload)
		case frameStderr:
			_, _ = os.Stderr.Write(payload)
		case frameExit:
			if len(payload) != 4 {
				return 255
			}
			code := int(int32(uint32(payload[0])<<24 | uint32(payload[1])<<16 | uint32(payload[2])<<8 | uint32(payload[3])))
			return code
		}
	}
}
