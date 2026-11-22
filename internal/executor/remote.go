package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dotfrankruan/CatSync/internal/backend"
	"github.com/dotfrankruan/CatSync/internal/backend/multistream"
	"github.com/dotfrankruan/CatSync/internal/daemon"
	"github.com/dotfrankruan/CatSync/internal/daemon/protocol"
	"github.com/dotfrankruan/CatSync/internal/discovery"
	"github.com/dotfrankruan/CatSync/internal/partition"
	"github.com/dotfrankruan/CatSync/internal/planner"
	"github.com/dotfrankruan/CatSync/internal/process"
	"github.com/dotfrankruan/CatSync/internal/rsh"
	"github.com/dotfrankruan/CatSync/internal/state"
	"github.com/dotfrankruan/CatSync/internal/target"
)

// remoteHashCapability reports whether remote content hashing is available.
type remoteHashCapability struct {
	Available bool
	Tool      remoteHashTool
}

// remoteOps abstracts every remote operation the executor performs, so the
// SSH and daemon transports share planning, stages, verification, and
// recovery code paths.
type remoteOps interface {
	DestManifest(ctx context.Context) (map[string]discovery.RemoteEntry, bool, error)
	SourceManifest(ctx context.Context) (root, file string, entries []discovery.Entry, err error)
	MkdirParents(ctx context.Context, dirs []string) error
	// EnsureDestRoot makes the directory-source destination root exist as a
	// real directory before the first transfer write. Single-file runs never
	// call it.
	EnsureDestRoot(ctx context.Context) error
	HashCapability(ctx context.Context) (remoteHashCapability, error)
	Hashes(ctx context.Context, tool remoteHashTool, tasks []*state.Task) (map[string]string, error)
	SFTPConfig() multistream.SFTPConfig
	RequireStage(ctx context.Context) error
	Close(ctx context.Context) error
}

// effectiveTransport resolves the request/run transport ("ssh" default).
func (e *Executor) effectiveTransport() string {
	t := e.cfg.Transport
	if e.run != nil {
		t = e.run.Config.Transport
	}
	if t == "" {
		return "ssh"
	}
	return t
}

func (e *Executor) daemonPort() int {
	if e.run != nil {
		return e.run.Config.DaemonPort
	}
	return e.cfg.DaemonPort
}

func (e *Executor) daemonTransport() string {
	if e.run != nil && e.run.Config.DaemonTransport != "" {
		return e.run.Config.DaemonTransport
	}
	if e.cfg.DaemonTransport != "" {
		return e.cfg.DaemonTransport
	}
	return "tcp"
}

func (e *Executor) daemonBootstrap() string {
	if e.run != nil && e.run.Config.DaemonBootstrap != "" {
		return e.run.Config.DaemonBootstrap
	}
	if e.cfg.DaemonBootstrap != "" {
		return e.cfg.DaemonBootstrap
	}
	return "direct"
}

func (e *Executor) remoteCatsyncdName() string {
	if e.run != nil && e.run.Config.RemoteCatsyncd != "" {
		return e.run.Config.RemoteCatsyncd
	}
	if e.cfg.RemoteCatsyncd != "" {
		return e.cfg.RemoteCatsyncd
	}
	return "catsyncd"
}

func (e *Executor) daemonListen() string {
	if e.run != nil && e.run.Config.DaemonListen != "" {
		return e.run.Config.DaemonListen
	}
	if e.cfg.DaemonListen != "" {
		return e.cfg.DaemonListen
	}
	return fmt.Sprintf(":%d", e.daemonPort())
}

func (e *Executor) daemonAddr() string {
	if e.run != nil && e.run.Config.DaemonAddr != "" {
		return e.run.Config.DaemonAddr
	}
	return e.cfg.DaemonAddr
}

func (e *Executor) quicQLogPath() string {
	if e.cfg.QUICQLogPath != "" {
		return e.cfg.QUICQLogPath
	}
	return ""
}

func (e *Executor) remoteQUICQLogPath() string {
	return e.cfg.RemoteQUICQLogPath
}

// manifestDestTarget is the destination endpoint for a manifest call in both
// planning and execution.
func (e *Executor) manifestDestTarget() (target.Destination, bool) {
	if e.run != nil {
		return e.destTarget(), e.run.Config.IPv6
	}
	return e.cfg.Dest, e.cfg.IPv6
}

// manifestSourceTarget is the remote source operand for discovery.
func (e *Executor) manifestSourceTarget() (target.Destination, bool) {
	if e.run != nil {
		return e.remoteSourceOperand(), e.run.Config.IPv6
	}
	return e.cfg.Dest, e.cfg.IPv6
}

// remoteEndpoint returns the remote host/user for the bootstrap, independent
// of direction, in planning and execution.
func (e *Executor) remoteEndpoint() target.Destination {
	if e.run != nil {
		if e.run.Config.Direction.Effective() == state.DirectionRemoteToLocal {
			return target.Destination{User: e.run.Source.User, Host: e.run.Source.Host, Path: e.run.Source.Path}
		}
		return target.Destination{User: e.run.Destination.User, Host: e.run.Destination.Host, Path: e.run.Destination.Path}
	}
	return e.cfg.Dest
}

// sshRemote keeps the historical SSH implementation. The bodies live in the
// executor methods they always used.
type sshRemote struct{ e *Executor }

func (r sshRemote) DestManifest(ctx context.Context) (map[string]discovery.RemoteEntry, bool, error) {
	dest, ipv6 := r.e.manifestDestTarget()
	return r.e.collectManifest(ctx, dest, ipv6)
}

func (r sshRemote) SourceManifest(ctx context.Context) (string, string, []discovery.Entry, error) {
	src, ipv6 := r.e.manifestSourceTarget()
	return r.e.collectRemoteSource(ctx, src, ipv6)
}

func (r sshRemote) MkdirParents(ctx context.Context, dirs []string) error {
	dest, ipv6 := r.e.manifestDestTarget()
	return r.e.mkdirRemoteParents(ctx, dest, ipv6, dirs)
}

// EnsureDestRoot validates/creates the remote destination root as a real
// directory in one ssh invocation. A symlink or regular file at that path is
// rejected before any transfer write, so rsync cannot treat it as a file.
func (r sshRemote) EnsureDestRoot(ctx context.Context) error {
	if r.e.run == nil {
		return errors.New("executor: destination root is only ensured during execution")
	}
	dest, ipv6 := r.e.manifestDestTarget()
	argv, err := discovery.EnsureDirCommandProgPort(r.e.progs.ssh(), dest, ipv6, r.e.sshPort(), dest.Path)
	if err != nil {
		return err
	}
	var stderr strings.Builder
	if err := r.e.withConnection(func() error {
		return process.Run(ctx, process.Spec{Name: argv[0], Args: argv[1:], Env: r.e.cfg.Env, Stderr: &stderr})
	}); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return fmt.Errorf("executor: destination root %q: %s", dest.Path, firstLine(detail))
		}
		return fmt.Errorf("executor: destination root %q: %w", dest.Path, err)
	}
	return nil
}

func (r sshRemote) HashCapability(ctx context.Context) (remoteHashCapability, error) {
	tool, err := r.e.detectRemoteHashTool(ctx)
	if err != nil {
		return remoteHashCapability{}, err
	}
	return remoteHashCapability{Available: tool != "", Tool: tool}, nil
}

func (r sshRemote) Hashes(ctx context.Context, tool remoteHashTool, tasks []*state.Task) (map[string]string, error) {
	return r.e.remoteHashes(ctx, tool, tasks)
}

func (r sshRemote) SFTPConfig() multistream.SFTPConfig { return r.e.sshSFTPConfig() }

func (r sshRemote) RequireStage(ctx context.Context) error { return r.e.requireSSH() }

func (r sshRemote) Close(ctx context.Context) error { return nil }

// daemonRemote implements the same operations over the pinned mTLS RPC and
// SFTP data connections of one bootstrap session.
type daemonRemote struct {
	e      *Executor
	client *daemon.Client
}

func (r daemonRemote) DestManifest(ctx context.Context) (map[string]discovery.RemoteEntry, bool, error) {
	dest, _ := r.e.manifestDestTarget()
	res, err := r.client.Manifest(ctx, protocol.ModeDestTree, dest.Path)
	if err != nil {
		if isProtocolCode(err, protocol.CodeNotFound) {
			// Authoritative empty destination, matching the SSH exit-42 path.
			return map[string]discovery.RemoteEntry{}, true, nil
		}
		return nil, false, err
	}
	out := make(map[string]discovery.RemoteEntry, len(res.Entries))
	for _, en := range res.Entries {
		out[en.Path] = discovery.RemoteEntry{Path: en.Path, Size: en.Size, ModTime: time.Unix(0, en.MtimeNS).UTC()}
	}
	return out, true, nil
}

func (r daemonRemote) SourceManifest(ctx context.Context) (string, string, []discovery.Entry, error) {
	src, _ := r.e.manifestSourceTarget()
	tree := func() ([]discovery.Entry, error) {
		res, err := r.client.Manifest(ctx, protocol.ModeSourceTree, src.Path)
		if err != nil {
			return nil, err
		}
		entries := make([]discovery.Entry, 0, len(res.Entries))
		for _, en := range res.Entries {
			entries = append(entries, entryFromProtocol(en))
		}
		return entries, nil
	}
	file := func() ([]discovery.Entry, error) {
		res, err := r.client.Manifest(ctx, protocol.ModeSourceFile, src.Path)
		if err != nil {
			return nil, err
		}
		entries := make([]discovery.Entry, 0, len(res.Entries))
		for _, en := range res.Entries {
			entries = append(entries, entryFromProtocol(en))
		}
		return entries, nil
	}
	entries, err := tree()
	root, name := src.Path, ""
	if err != nil {
		if !isProtocolCode(err, protocol.CodeNotDir) {
			return "", "", nil, err
		}
		entries, err = file()
		if err != nil {
			return "", "", nil, err
		}
		root, name = path.Dir(src.Path), path.Base(src.Path)
		if len(entries) != 1 || entries[0].Path != name {
			return "", "", nil, fmt.Errorf("executor: daemon single-file source %q reported an unexpected entry set", src.Path)
		}
	}
	return root, name, entries, nil
}

func entryFromProtocol(en protocol.ManifestEntryRecord) discovery.Entry {
	kind := discovery.KindSpecial
	switch en.Kind {
	case protocol.KindFile:
		kind = discovery.KindFile
	case protocol.KindSymlink:
		kind = discovery.KindSymlink
	}
	return discovery.Entry{
		Path: en.Path, Size: en.Size, ModTime: time.Unix(0, en.MtimeNS).UTC(),
		Kind: kind,
	}
}

func (r daemonRemote) MkdirParents(ctx context.Context, dirs []string) error {
	_, err := r.client.Mkdir(ctx, dirs)
	return err
}

// EnsureDestRoot validates/creates the remote destination root over RPC. A
// symlink, regular file, or other non-directory is rejected before writes.
func (r daemonRemote) EnsureDestRoot(ctx context.Context) error {
	dest, _ := r.e.manifestDestTarget()
	if err := protocol.ValidateAbsPath(dest.Path); err != nil {
		return err
	}
	st, err := r.client.Stat(ctx, dest.Path)
	if err != nil {
		if !isProtocolCode(err, protocol.CodeNotFound) {
			return fmt.Errorf("executor: destination root %q: %w", dest.Path, err)
		}
		if _, merr := r.client.Mkdir(ctx, []string{dest.Path}); merr != nil {
			return fmt.Errorf("executor: destination root %q: %w", dest.Path, merr)
		}
		st, err = r.client.Stat(ctx, dest.Path)
		if err != nil {
			return fmt.Errorf("executor: destination root %q: %w", dest.Path, err)
		}
	}
	if st.Type != protocol.StatDir {
		return fmt.Errorf("%w: destination root %q exists but is not a directory (type %s)", ErrInvalidConfig, dest.Path, st.Type)
	}
	return nil
}

func (r daemonRemote) HashCapability(ctx context.Context) (remoteHashCapability, error) {
	if !r.client.Features().Hash {
		return remoteHashCapability{}, nil
	}
	return remoteHashCapability{Available: true, Tool: hashSHA256Sum}, nil
}

func (r daemonRemote) Hashes(ctx context.Context, tool remoteHashTool, tasks []*state.Task) (map[string]string, error) {
	dest, _ := r.e.manifestDestTarget()
	abs := make([]string, 0, len(tasks))
	relOf := make(map[string]string, len(tasks))
	for _, t := range tasks {
		p := backend.RemotePath(dest.Path, t.Path)
		abs = append(abs, p)
		relOf[p] = t.Path
	}
	// The RPC stream contract is byte-order increasing paths; sort the
	// request here rather than making every caller do it.
	sort.Strings(abs)
	hashes, err := r.client.Hash(ctx, abs)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(hashes))
	for p, h := range hashes {
		rel, ok := relOf[p]
		if !ok {
			return nil, fmt.Errorf("executor: daemon hash returned an unexpected path %q", p)
		}
		out[rel] = h
	}
	if len(out) != len(tasks) {
		return nil, fmt.Errorf("executor: daemon hash returned %d records for %d files", len(out), len(tasks))
	}
	return out, nil
}

func (r daemonRemote) SFTPConfig() multistream.SFTPConfig {
	dest, _ := r.e.manifestDestTarget()
	client := r.client
	cfg := multistream.SFTPConfig{
		Dest: dest,
		OnSession: func(delta int) {
			r.e.progressConnDelta(delta)
		},
		Dial: func(ctx context.Context) (io.ReadWriteCloser, func(), error) {
			conn, err := client.DialSFTP(ctx)
			if err != nil {
				return nil, nil, err
			}
			return conn, func() { _ = conn.Close() }, nil
		},
		SetMtime: func(ctx context.Context, p string, t time.Time) error {
			return client.MetadataMtime(ctx, p, nil, t.UnixNano())
		},
		SameFile: func(ctx context.Context, a, b string) (bool, error) {
			return client.SameFile(ctx, a, b)
		},
		RemoteIdentity: func(ctx context.Context, p string) (uint64, time.Time, error) {
			st, err := client.Stat(ctx, p)
			if err != nil {
				return 0, time.Time{}, err
			}
			if st.Type != protocol.StatFile {
				return 0, time.Time{}, fmt.Errorf("executor: remote source %q is not a regular file", p)
			}
			return uint64(st.Size), time.Unix(0, st.MtimeNS), nil
		},
	}
	return cfg
}

func (r daemonRemote) RequireStage(ctx context.Context) error {
	if r.client == nil {
		return errors.New("executor: daemon session is not ready")
	}
	return nil
}

func (r daemonRemote) Close(ctx context.Context) error { return r.client.Close() }

// isProtocolCode reports whether err carries a protocol error code.
func isProtocolCode(err error, code string) bool {
	var pe *protocol.Error
	return errors.As(err, &pe) && pe.Code == code
}

// sshSFTPConfig is the historical SSH transport configuration.
func (e *Executor) sshSFTPConfig() multistream.SFTPConfig {
	return multistream.SFTPConfig{
		Program: e.progs.sshProgram(),
		Dest:    e.destTarget(),
		IPv6:    e.run.Config.IPv6,
		Port:    e.run.Config.Port,
		Env:     e.cfg.Env,
		Stderr:  e.stderr,
		OnSession: func(delta int) {
			e.progressConnDelta(delta)
		},
	}
}

// daemonTransportOptions carries the SSH bootstrap command inputs. It is a
// struct so tests can replace the transport factory.
type daemonTransportOptions struct {
	Program            string
	User               string
	Host               string
	Port               int
	IPv6               bool
	RemoteCatsyncd     string
	Listen             string
	Env                []string
	Stderr             io.Writer
	ForwardLocalPort   int
	DaemonTransport    string
	QUICQLogPath       string
	RemoteQUICQLogPath string
	Detach             bool
}

// newDaemonControlTransport builds the production SSH bootstrap transport.
// Tests replace it with an in-process transport.
var newDaemonControlTransport = func(o daemonTransportOptions) daemon.ControlTransport {
	return daemon.SSHTransport{
		Program: o.Program, User: o.User, Host: o.Host, Port: o.Port,
		IPv6: o.IPv6, RemoteCatsyncd: o.RemoteCatsyncd, Listen: o.Listen,
		Env: o.Env, Stderr: o.Stderr, ForwardLocalPort: o.ForwardLocalPort,
		DaemonTransport: o.DaemonTransport,
		RemoteQUICQLog:  o.RemoteQUICQLogPath,
		Detach:          o.Detach,
	}
}

// ensureRemote creates the transport implementation once per executor.
func (e *Executor) ensureRemote(ctx context.Context) (remoteOps, error) {
	if e.remote != nil {
		return e.remote, nil
	}
	switch e.effectiveTransport() {
	case "ssh":
		if err := e.requireSSH(); err != nil {
			return nil, err
		}
		e.remote = sshRemote{e: e}
		return e.remote, nil
	case "daemon":
	default:
		return nil, fmt.Errorf("%w: unknown transport %q", ErrInvalidConfig, e.effectiveTransport())
	}
	switch e.daemonTransport() {
	case "tcp", "quic":
	default:
		return nil, fmt.Errorf("%w: unknown daemon transport %q", ErrInvalidConfig, e.daemonTransport())
	}
	endpoint := e.remoteEndpoint()
	listen, daemonAddr, forwardLocalPort, err := e.daemonBootstrapTargets()
	if err != nil {
		return nil, err
	}
	opts := daemonTransportOptions{
		Program: e.progs.sshProgram(), User: endpoint.User, Host: endpoint.Host,
		Port: e.effectivePort(), IPv6: e.effectiveIPv6(),
		RemoteCatsyncd: e.remoteCatsyncdName(),
		Listen:         listen,
		Env:            e.cfg.Env, Stderr: e.stderr,
		ForwardLocalPort:   forwardLocalPort,
		DaemonTransport:    e.daemonTransport(),
		QUICQLogPath:       e.quicQLogPath(),
		RemoteQUICQLogPath: e.remoteQUICQLogPath(),
		Detach:             e.daemonBootstrap() == "direct",
	}
	client, err := daemon.Bootstrap(ctx, daemon.BootstrapConfig{
		Transport:      newDaemonControlTransport(opts),
		Version:        "catsync",
		Host:           endpoint.Host,
		DaemonAddr:     daemonAddr,
		DataTransport:  e.daemonTransport(),
		Stderr:         e.stderr,
		QUICQLogPath:   e.quicQLogPath(),
		RequestHandoff: e.daemonBootstrap() == "direct",
	})
	if err != nil {
		return nil, fmt.Errorf("executor: daemon bootstrap: %w", err)
	}
	e.daemonClient = client
	e.remote = daemonRemote{e: e, client: client}
	return e.remote, nil
}

func (e *Executor) effectivePort() int {
	if e.run != nil {
		return e.run.Config.Port
	}
	return e.cfg.Port
}

func (e *Executor) effectiveIPv6() bool {
	if e.run != nil {
		return e.run.Config.IPv6
	}
	return e.cfg.IPv6
}

// closeRemote shuts the daemon session down and forgets it.
func (e *Executor) closeRemote(ctx context.Context) {
	if e.rshAgent != nil {
		e.rshAgent.Close()
		e.rshAgent = nil
		e.rshEnv = nil
		e.rshCommand = ""
	}
	if e.daemonClient != nil {
		_ = e.daemonClient.Close()
		e.daemonClient = nil
	}
	e.remote = nil
}

// plannedBudget computes the daemon resource peak of a plan.
func (e *Executor) plannedBudget(out *PlanOutcome, jobs, streams, threshold int, largeThreshold int) (sfwPeak, execPeak int) {
	if out == nil || out.Result == nil {
		return 0, 0
	}
	var large, smallSFTP, smallBatch int
	var fpsyncItems []partition.Item
	var conservative int
	for _, d := range out.Result.Decisions {
		if d.Status != planner.StatusPending {
			continue
		}
		switch d.Backend {
		case state.BackendMSCP:
			large++
		case state.BackendSFTP:
			smallSFTP++
		case state.BackendFPSync:
			fpsyncItems = append(fpsyncItems, partition.Item{Path: d.Path, Size: d.Size})
		case state.BackendRsync:
			conservative++
		}
	}
	batches, err := partition.Pack(fpsyncItems, partition.Config{MaxFiles: jobs, MaxBytes: uint64(largeThreshold)})
	if err == nil {
		smallBatch = len(batches)
	}
	sfwPeak = min(large, jobs)*streams + min(smallSFTP, jobs)
	execPeak = min(smallBatch, jobs)
	if conservative > 0 {
		execPeak++
	}
	return sfwPeak, execPeak
}

// precheckDaemon enforces features, encoding, and capacity before any run
// state or remote write is created.
func (e *Executor) precheckDaemon(out *PlanOutcome) error {
	if e.effectiveTransport() != "daemon" {
		return nil
	}
	client := e.daemonClient
	if client == nil {
		return errors.New("executor: daemon session is not ready")
	}
	jobs, streams := e.cfg.Jobs, e.cfg.Streams
	if jobs < 1 {
		jobs = 1
	}
	if streams < 1 {
		streams = 1
	}
	sfwPeak, execPeak := e.plannedBudget(out, jobs, streams, 0, int(e.cfg.LargeThreshold))
	needExec := e.cfg.Mirror
	if out.Result != nil {
		for _, d := range out.Result.Decisions {
			if d.Status == planner.StatusPending && (d.Backend == state.BackendFPSync || d.Backend == state.BackendRsync) {
				needExec = true
			}
		}
	}
	if e.cfg.Mirror {
		execPeak++
	}
	lim := client.Limits()
	sftpLimit, execLimit := lim.MaxSFTPConns, lim.MaxExecConns
	if e.daemonTransport() == "quic" {
		sftpLimit, execLimit = lim.MaxSFTPStreams, lim.MaxExecStreams
	}
	if err := e.checkCapacity("SFTP", sfwPeak, sftpLimit); err != nil {
		return err
	}
	if sfwPeak > 0 && !client.Features().SFTP {
		return fmt.Errorf("%w: the remote daemon has no sftp-server; install OpenSSH's sftp-server on the remote host", protocolError(protocol.CodeFeatureUnavailable))
	}
	if needExec {
		if err := e.checkCapacity("exec", execPeak, execLimit); err != nil {
			return err
		}
		if !client.Features().Exec {
			return fmt.Errorf("%w: the remote daemon cannot run rsync (exec channel disabled); install rsync on the remote host or use --transport ssh", protocolError(protocol.CodeFeatureUnavailable))
		}
	}
	return e.validateDaemonPaths(out)
}

// precheckDaemonRun is the resume counterpart of precheckDaemon.
func (e *Executor) precheckDaemonRun(run *state.Run) error {
	if e.effectiveTransport() != "daemon" {
		return nil
	}
	client := e.daemonClient
	if client == nil {
		return errors.New("executor: daemon session is not ready")
	}
	jobs, streams := run.Config.Jobs, run.Config.Streams
	if jobs < 1 {
		jobs = 1
	}
	if streams < 1 {
		streams = 1
	}
	var large, smallSFTP int
	var fpsync []partition.Item
	var conservative int
	for _, t := range run.Tasks {
		switch t.Status {
		case state.StatusPending, state.StatusFailed, state.StatusRunning:
		default:
			continue
		}
		switch t.Backend {
		case state.BackendMSCP:
			large++
		case state.BackendSFTP:
			smallSFTP++
		case state.BackendFPSync:
			fpsync = append(fpsync, partition.Item{Path: t.Path, Size: t.Size})
		case state.BackendRsync:
			conservative++
		}
	}
	batches, _ := partition.Pack(fpsync, partition.Config{MaxFiles: jobs, MaxBytes: run.Config.LargeThreshold})
	sfwPeak := min(large, jobs)*streams + min(smallSFTP, jobs)
	execPeak := min(len(batches), jobs) + btoi(conservative > 0)
	needExec := run.Config.Mirror
	if len(batches) > 0 || conservative > 0 {
		needExec = true
	}
	if run.Config.Mirror {
		execPeak++
	}
	lim := client.Limits()
	sftpLimit, execLimit := lim.MaxSFTPConns, lim.MaxExecConns
	if e.daemonTransport() == "quic" {
		sftpLimit, execLimit = lim.MaxSFTPStreams, lim.MaxExecStreams
	}
	if err := e.checkCapacity("SFTP", sfwPeak, sftpLimit); err != nil {
		return err
	}
	if sfwPeak > 0 && !client.Features().SFTP {
		return fmt.Errorf("%w: the remote daemon has no sftp-server", protocolError(protocol.CodeFeatureUnavailable))
	}
	if needExec {
		if err := e.checkCapacity("exec", execPeak, execLimit); err != nil {
			return err
		}
		if !client.Features().Exec {
			return fmt.Errorf("%w: the remote daemon cannot run rsync (exec channel disabled)", protocolError(protocol.CodeFeatureUnavailable))
		}
	}
	for _, t := range run.Tasks {
		if !utf8.ValidString(t.Path) {
			return fmt.Errorf("%w: run contains a non-UTF-8 path, which daemon mode does not support", protocolError(protocol.CodeUnsupportedPathEncoding))
		}
	}
	return nil
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (e *Executor) checkCapacity(kind string, required, limit int) error {
	if required <= limit {
		return nil
	}
	return fmt.Errorf("%w: the plan needs up to %d concurrent %s connections but the remote daemon allows %d; lower --jobs/--streams or raise the server budget",
		protocolError(protocol.CodeConnectionLimit), required, kind, limit)
}

// validateDaemonPaths rejects paths the daemon protocol cannot represent.
func (e *Executor) validateDaemonPaths(out *PlanOutcome) error {
	if out == nil || out.Result == nil {
		return nil
	}
	for _, d := range out.Result.Decisions {
		if !utf8.ValidString(d.Path) {
			return fmt.Errorf("%w: %q is not valid UTF-8; daemon mode requires UTF-8 names", protocolError(protocol.CodeUnsupportedPathEncoding), d.Path)
		}
	}
	return nil
}

// protocolError builds a coded executor error compatible with daemon codes.
func protocolError(code string) error {
	return &protocol.Error{Code: code}
}

// min returns the smaller of two ints.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// validateTransportConfig checks the daemon transport option values.
func (c *Config) validateTransportConfig() error {
	switch c.Transport {
	case "", "ssh", "daemon":
	default:
		return fmt.Errorf("%w: unknown transport %q", ErrInvalidConfig, c.Transport)
	}
	if c.DaemonPort < 0 || c.DaemonPort > 65535 {
		return fmt.Errorf("%w: daemon port %d is outside 0..65535", ErrInvalidConfig, c.DaemonPort)
	}
	switch c.DaemonTransport {
	case "", "tcp":
	case "quic":
		// Parsed but rejected by ensureRemote until the QUIC milestone.
	default:
		return fmt.Errorf("%w: unknown daemon transport %q", ErrInvalidConfig, c.DaemonTransport)
	}
	switch c.DaemonBootstrap {
	case "", "direct":
	case "ssh-forward":
		// Parsed but rejected by ensureRemote until forwarding lands.
	default:
		return fmt.Errorf("%w: unknown daemon bootstrap %q", ErrInvalidConfig, c.DaemonBootstrap)
	}
	if c.Transport == "daemon" && strings.TrimSpace(c.RemoteCatsyncd) == "" {
		return fmt.Errorf("%w: remote catsyncd name is empty", ErrInvalidConfig)
	}
	return nil
}

// applyDaemonOverrides applies only the daemon flags the user explicitly set
// on a resume; every other dimension keeps the recorded value.
func applyDaemonOverrides(run *state.Run, cfg Config) {
	if run.Config.Transport == "" {
		run.Config.Transport = "ssh"
	}
	if cfg.Transport != "" {
		run.Config.Transport = cfg.Transport
	}
	set := func(name string) bool { return cfg.DaemonExplicit[name] }
	if set("daemon-port") {
		run.Config.DaemonPort = cfg.DaemonPort
	}
	if set("daemon-transport") {
		run.Config.DaemonTransport = cfg.DaemonTransport
	}
	if set("daemon-bootstrap") {
		run.Config.DaemonBootstrap = cfg.DaemonBootstrap
	}
	if set("remote-catsyncd") {
		run.Config.RemoteCatsyncd = cfg.RemoteCatsyncd
	}
	if set("daemon-listen") {
		run.Config.DaemonListen = cfg.DaemonListen
	}
	if set("daemon-addr") {
		run.Config.DaemonAddr = cfg.DaemonAddr
	}
}

// daemonBootstrapTargets resolves the listen address, the client dial
// address, and (for ssh-forward) the local forwarding port.
func (e *Executor) daemonBootstrapTargets() (listen, daemonAddr string, forwardPort int, err error) {
	switch e.daemonBootstrap() {
	case "direct":
		return e.daemonListen(), e.daemonAddr(), 0, nil
	case "ssh-forward":
	default:
		return "", "", 0, fmt.Errorf("%w: unknown daemon bootstrap %q", ErrInvalidConfig, e.daemonBootstrap())
	}
	if e.daemonTransport() != "tcp" {
		return "", "", 0, fmt.Errorf("%w: ssh-forward is incompatible with daemon transport %q", ErrInvalidConfig, e.daemonTransport())
	}
	port := e.daemonPort()
	if port == 0 {
		return "", "", 0, fmt.Errorf("%w: ssh-forward needs a fixed --daemon-port (port 0 cannot be forwarded)", ErrInvalidConfig)
	}
	listen = fmt.Sprintf("127.0.0.1:%d", port)
	if explicit := e.cfg.DaemonListen; explicit != "" {
		host, _, herr := net.SplitHostPort(explicit)
		if herr != nil || host != "127.0.0.1" {
			return "", "", 0, fmt.Errorf("%w: ssh-forward requires --daemon-listen on loopback, got %q", ErrInvalidConfig, explicit)
		}
		listen = explicit
	}
	lp, lerr := freeTCPPort()
	if lerr != nil {
		return "", "", 0, fmt.Errorf("executor: choose forwarding port: %w", lerr)
	}
	return listen, fmt.Sprintf("127.0.0.1:%d", lp), lp, nil
}

// freeTCPPort reserves and releases a local loopback port for ssh -L. The
// ssh option ExitOnForwardFailure makes a lost race a clean bootstrap error.
func freeTCPPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		return 0, err
	}
	return port, nil
}

// ensureDaemonAgent starts the single per-run local rsh agent that bridges
// rsync's -e helper to the daemon exec channel. It never starts a second
// ssh process: the helper only talks to this local Unix socket.
func (e *Executor) ensureDaemonAgent(ctx context.Context) error {
	if e.effectiveTransport() != "daemon" {
		return nil
	}
	if e.rshAgent != nil {
		return nil
	}
	if e.daemonClient == nil {
		return errors.New("executor: daemon session is not ready for exec")
	}
	exe, err := catsyncExecutable()
	if err != nil {
		return fmt.Errorf("executor: locate catsync executable for rsh: %w", err)
	}
	endpoint := e.remoteEndpoint()
	agent, err := rsh.Start(ctx, endpoint.Host, endpoint.User, e.daemonClient)
	if err != nil {
		return fmt.Errorf("executor: start rsh agent: %w", err)
	}
	e.rshAgent = agent
	e.rshEnv = agent.Env()
	e.rshCommand = discovery.ShellQuote(exe) + " rsh"
	return nil
}

// catsyncExecutable is replaceable so tests can point the rsh helper at a
// real catsync binary built from the same tree.
var catsyncExecutable = os.Executable

// rsyncRsh is the daemon -e override (empty for ssh runs).
func (e *Executor) rsyncRsh() string {
	if e.effectiveTransport() == "daemon" {
		return e.rshCommand
	}
	return ""
}

// rsyncEnv returns the environment for an rsync process: the user's
// additions plus the local agent credentials in daemon mode.
func (e *Executor) rsyncEnv() []string {
	if e.effectiveTransport() != "daemon" || len(e.rshEnv) == 0 {
		return e.cfg.Env
	}
	out := make([]string, 0, len(e.cfg.Env)+len(e.rshEnv))
	out = append(out, e.cfg.Env...)
	return append(out, e.rshEnv...)
}
