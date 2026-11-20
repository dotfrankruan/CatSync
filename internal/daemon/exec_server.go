package daemon

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/daemon/strictjson"
)

// execFrameWriter serializes frames from the child-output goroutines and the
// exit/error paths onto one TLS stream.
type execFrameWriter struct {
	mu   sync.Mutex
	w    io.Writer
	dead bool
}

func (fw *execFrameWriter) write(typ byte, payload []byte) error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if fw.dead {
		return net.ErrClosed
	}
	if len(payload) > protocol.MaxExecFrameBytes {
		return protocol.NewError(protocol.CodeTooLarge, "exec frame exceeds %d bytes", protocol.MaxExecFrameBytes)
	}
	var hdr [5]byte
	hdr[0] = typ
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := fw.w.Write(hdr[:]); err != nil {
		fw.dead = true
		return err
	}
	if len(payload) > 0 {
		if _, err := fw.w.Write(payload); err != nil {
			fw.dead = true
			return err
		}
	}
	return nil
}

func (fw *execFrameWriter) fail() {
	fw.mu.Lock()
	fw.dead = true
	fw.mu.Unlock()
}

// readExecFrame reads one bounded frame.
func readExecFrame(r *bufio.Reader) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > protocol.MaxExecFrameBytes {
		return 0, nil, protocol.NewError(protocol.CodeTooLarge, "exec frame length %d exceeds %d", n, protocol.MaxExecFrameBytes)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return hdr[0], payload, nil
}

// handleExecConn runs one validated rsync server child and bridges its
// stdin/stdout/stderr over the framed exec protocol.
func (s *server) handleExecConn(conn net.Conn) {
	tc := newTrackedConn(conn, s)
	defer tc.Close()
	if s.cfg.RsyncPath == "" {
		s.writeExecError(tc, protocol.NewError(protocol.CodeFeatureUnavailable, "rsync exec is not available on this server"))
		return
	}
	s.counters.execActive.Add(1)
	defer s.counters.execActive.Add(-1)

	br := bufio.NewReaderSize(tc, 16*1024)
	line, err := readExecStartLine(br)
	if err != nil {
		s.writeExecError(tc, err)
		return
	}
	var start protocol.ExecStart
	if err := strictjson.Decode(line, &start, strictjson.Limits{MaxBytes: protocol.MaxExecStartBytes, MaxDepth: 8}); err != nil {
		s.writeExecError(tc, protocol.NewError(protocol.CodeJSONTypeMismatch, "exec start: %v", err))
		return
	}
	if start.Type != "start" {
		s.writeExecError(tc, protocol.NewError(protocol.CodeExecRejected, "exec start type is %q", start.Type))
		return
	}
	if err := protocol.ValidateExecStart(start.Program, start.Args); err != nil {
		s.writeExecError(tc, err)
		return
	}

	cmd := exec.Command(s.cfg.RsyncPath, start.Args...)
	cmd.Env = sanitizedExecEnv()
	configureChild(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		s.writeExecError(tc, protocol.NewError(protocol.CodeInternal, "exec stdin: %v", err))
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.writeExecError(tc, protocol.NewError(protocol.CodeInternal, "exec stdout: %v", err))
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		s.writeExecError(tc, protocol.NewError(protocol.CodeInternal, "exec stderr: %v", err))
		return
	}
	if err := cmd.Start(); err != nil {
		s.writeExecError(tc, protocol.NewError(protocol.CodeRemoteProcessFailed, "start rsync: %v", err))
		return
	}
	child := &childProcess{cmd: cmd, done: make(chan struct{})}
	s.mu.Lock()
	s.children[child] = struct{}{}
	s.mu.Unlock()
	s.counters.children.Add(1)
	go func() {
		child.err = cmd.Wait()
		close(child.done)
		s.counters.children.Add(-1)
		s.mu.Lock()
		delete(s.children, child)
		s.mu.Unlock()
	}()

	fw := &execFrameWriter{w: tc}
	var copies sync.WaitGroup
	copyFrames := func(r io.Reader, typ byte) {
		defer copies.Done()
		buf := make([]byte, 32*1024)
		for {
			n, rerr := r.Read(buf)
			if n > 0 {
				if werr := fw.write(typ, buf[:n]); werr != nil {
					return
				}
			}
			if rerr != nil {
				return
			}
		}
	}
	copies.Add(2)
	go copyFrames(stdout, protocol.ExecFrameStdout)
	go copyFrames(stderr, protocol.ExecFrameStderr)

	// Read client frames until the child exits or the client goes away.
	clientGone := make(chan struct{})
	go func() {
		defer close(clientGone)
		for {
			typ, payload, rerr := readExecFrame(br)
			if rerr != nil {
				return
			}
			switch typ {
			case protocol.ExecFrameStdin:
				if _, werr := stdin.Write(payload); werr != nil {
					return
				}
			case protocol.ExecFrameStdinEOF:
				_ = stdin.Close()
			case protocol.ExecFrameCancel:
				return
			default:
				return
			}
		}
	}()

	select {
	case <-child.done:
	case <-clientGone:
		terminateChild(cmd, 3*time.Second)
		<-child.done
	case <-s.ctx.Done():
		terminateChild(cmd, 3*time.Second)
		<-child.done
	}
	// Stop the frame reader if the child exited first.
	fw.mu.Lock()
	alreadyDead := fw.dead
	fw.mu.Unlock()
	_ = alreadyDead
	copies.Wait()
	code := childExitCode(cmd)
	_ = fw.write(protocol.ExecFrameExit, protocol.ExecExitPayload(code))
	fw.fail()
	tc.Close()
	<-clientGone
}

func (s *server) writeExecError(w io.Writer, err error) {
	code := protocol.CodeInternal
	msg := err.Error()
	var pe *protocol.Error
	if errors.As(err, &pe) {
		code, msg = pe.Code, pe.Message
	}
	payload := []byte(code + "\x00" + msg)
	limit := protocol.MaxExecFrameBytes
	if len(payload) > limit {
		code = protocol.CodeTooLarge
		payload = []byte(code + "\x00error message too large")
	}
	fw := &execFrameWriter{w: w}
	_ = fw.write(protocol.ExecFrameError, payload)
}

func readExecStartLine(br *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		frag, err := br.ReadSlice('\n')
		out = append(out, frag...)
		if len(out) > protocol.MaxExecStartBytes {
			return nil, protocol.NewError(protocol.CodeTooLarge, "exec start line exceeds %d bytes", protocol.MaxExecStartBytes)
		}
		switch {
		case err == nil:
			return trimNewline(out), nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			return nil, io.EOF
		default:
			return nil, err
		}
	}
}

func childExitCode(cmd *exec.Cmd) int {
	if cmd.ProcessState == nil {
		return 255
	}
	return cmd.ProcessState.ExitCode()
}

// sanitizedExecEnv is the minimal environment for the remote rsync server:
// no inherited dynamic-loader hooks or other side-effectful variables.
func sanitizedExecEnv() []string {
	safe := map[string]bool{
		"PATH": true, "HOME": true, "TMPDIR": true, "TZ": true,
		"LANG": true, "LC_ALL": true, "USER": true, "LOGNAME": true,
	}
	var out []string
	for _, kv := range os.Environ() {
		name := kv
		if i := strings.IndexByte(kv, '='); i >= 0 {
			name = kv[:i]
		}
		if safe[name] {
			out = append(out, kv)
		}
	}
	if len(out) == 0 {
		out = append(out, "PATH=/usr/local/bin:/usr/bin:/bin")
	}
	return out
}
