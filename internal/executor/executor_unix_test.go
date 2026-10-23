//go:build unix

package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dotfrankruan/CatSync/internal/backend/multistream"
	"github.com/dotfrankruan/CatSync/internal/epochtime"
	"github.com/dotfrankruan/CatSync/internal/state"
	"github.com/dotfrankruan/CatSync/internal/target"
	"github.com/dotfrankruan/CatSync/internal/testutil/mockexec"
)

func TestMain(m *testing.M) {
	if mockexec.HelperActive() {
		os.Exit(mockexec.RunHelper())
	}
	os.Exit(m.Run())
}

// harness wires a source tree, a state directory, and the mock executables
// (ssh, rsync, mscp) in script mode. The mock ssh emulates a real remote
// end on the local filesystem for SFTP subsystem and controlled metadata
// commands: remoteDir is the "remote" root the native large-file backend
// really writes through the real OpenSSH sftp-server.
type harness struct {
	t         *testing.T
	src       string
	stateDir  string
	scriptDir string
	logPath   string
	progs     map[string]string
	remoteDir string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	binDir := t.TempDir()
	progs, err := mockexec.LinkPrograms(binDir, "ssh", "rsync", "mscp")
	if err != nil {
		t.Fatal(err)
	}
	return &harness{
		t:         t,
		src:       t.TempDir(),
		stateDir:  t.TempDir(),
		scriptDir: t.TempDir(),
		logPath:   filepath.Join(t.TempDir(), "invocations.jsonl"),
		progs:     progs,
		remoteDir: t.TempDir(),
	}
}

func (h *harness) config() Config {
	return Config{
		SourceDir:      h.src,
		Dest:           target.Destination{User: "u", Host: "h", Path: "/data"},
		Backend:        state.BackendAuto,
		Verify:         state.VerifySize,
		Jobs:           2,
		Streams:        2,
		LargeThreshold: 1024,
		StateDir:       h.stateDir,
		Programs: Programs{
			SSH:   h.progs["ssh"],
			Rsync: h.progs["rsync"],
		},
		Env: mockexec.EnvScript(h.scriptDir, h.logPath),
	}
}

// configNative is config() with the destination path pointed at the real
// local remoteDir: the native large-file backend transfers through the real
// sftp-server, so the destination must exist on disk.
func (h *harness) configNative() Config {
	cfg := h.config()
	cfg.Dest.Path = h.remoteDir
	cfg.Env = h.nativeEnv()
	return cfg
}

// nativeEnv returns the script env plus the record directory, which the
// mock ssh's SFTP emulation needs for its fail-nth counter; the directory
// is created so the counter persists.
func (h *harness) nativeEnv(extra ...string) []string {
	dir := filepath.Join(h.stateDir, "record")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		h.t.Fatal(err)
	}
	return append(append([]string{}, mockexec.EnvScript(h.scriptDir, h.logPath)...),
		append([]string{mockexec.EnvRecordDir + "=" + dir, mockexec.EnvSSHNative + "=1"}, extra...)...)
}

// sftpSessionInvocations counts mock-ssh invocations that requested the
// sftp subsystem (argv contains "-s" then "sftp").
func sftpSessionInvocations(invs []mockexec.Invocation) []mockexec.Invocation {
	var out []mockexec.Invocation
	for _, inv := range invs {
		if inv.Tool != "ssh" {
			continue
		}
		for i, a := range inv.Args {
			if a == "-s" && i+1 < len(inv.Args) {
				out = append(out, inv)
				break
			}
		}
	}
	return out
}

// writeScript installs a tool's script-style config.
func (h *harness) writeScript(tool, config string) {
	h.t.Helper()
	dir := filepath.Join(h.scriptDir, tool)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte(config), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

// writeSrc creates a source file with the given size.
func (h *harness) writeSrc(rel string, size int) {
	h.t.Helper()
	p := filepath.Join(h.src, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
		h.t.Fatal(err)
	}
}

// srcMeta returns the source file's size and mtime.
func (h *harness) srcMeta(rel string) (int64, time.Time) {
	h.t.Helper()
	fi, err := os.Stat(filepath.Join(h.src, filepath.FromSlash(rel)))
	if err != nil {
		h.t.Fatal(err)
	}
	return fi.Size(), fi.ModTime()
}

// manifestContent renders NUL-delimited manifest records.
func manifestContent(recs ...string) string {
	var b strings.Builder
	for _, r := range recs {
		b.WriteString(r)
	}
	return b.String()
}

func manifestRecord(path string, size int64, mt time.Time) string {
	return fmt.Sprintf("%s\x00%d\x00%s\x00", path, size, epochtime.Format(mt))
}

// newFileRecord renders the manifest record for a current source file.
func (h *harness) newFileRecord(rel string) string {
	size, mt := h.srcMeta(rel)
	return manifestRecord(rel, size, mt)
}

// writeManifest writes manifest records to a file and returns its path.
func (h *harness) writeManifest(name, content string) string {
	h.t.Helper()
	p := filepath.Join(h.t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		h.t.Fatal(err)
	}
	return p
}

// scriptSSHFreshDest scripts ssh for a sync against a missing destination:
// the first manifest call exits 42 (no such directory), later manifest
// calls serve postManifestFile, and mkdir succeeds.
func (h *harness) scriptSSHFreshDest(postManifestFile string) {
	h.writeScript("ssh", `match=if [ -d
nth=1
exit=42
---
match=if [ -d
stdout_file=`+postManifestFile+`
---
match=mkdir -p
exit=0
`)
}

func (h *harness) invocations() []mockexec.Invocation {
	h.t.Helper()
	invs, err := mockexec.ReadInvocations(h.logPath)
	if err != nil {
		h.t.Fatal(err)
	}
	return invs
}

// toolInvocations returns the invocations of one tool.
func toolInvocations(invs []mockexec.Invocation, tool string) []mockexec.Invocation {
	var out []mockexec.Invocation
	for _, inv := range invs {
		if inv.Tool == tool {
			out = append(out, inv)
		}
	}
	return out
}

// stdinPaths decodes a recorded NUL-delimited stdin payload.
func stdinPaths(t *testing.T, inv mockexec.Invocation) []string {
	t.Helper()
	data, err := inv.StdinBytes()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, p := range strings.Split(string(data), "\x00") {
		if p != "" {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}
func TestSyncAutoFreshDest(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt"), h.newFileRecord("big.bin")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")

	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Errorf("run status = %s, want completed (error %q)", run.Status, run.Error)
	}
	for _, task := range run.Tasks {
		if task.Status != state.StatusCompleted {
			t.Errorf("task %s = %s (%q), want completed", task.Path, task.Status, task.Error)
		}
	}
	// The native stage never writes the legacy external-fpsync artifacts.
	if run.FPSyncRunID != "" || run.FPSyncStateDir != "" || run.FPSyncOutput != "" {
		t.Errorf("legacy fpsync artifacts set on a native run: %q %q %q", run.FPSyncRunID, run.FPSyncStateDir, run.FPSyncOutput)
	}

	invs := h.invocations()
	if fps := toolInvocations(invs, "fpsync"); len(fps) != 0 {
		t.Errorf("external fpsync invoked %d times, want 0", len(fps))
	}
	if fp := toolInvocations(invs, "fpart"); len(fp) != 0 {
		t.Errorf("external fpart invoked %d times, want 0", len(fp))
	}
	// The external mscp is never executed: the large file went through the
	// native multistream backend.
	if ms := toolInvocations(invs, "mscp"); len(ms) != 0 {
		t.Errorf("external mscp invoked %d times, want 0", len(ms))
	}

	// The small file went through one native rsync batch.
	rs := toolInvocations(invs, "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1 (the small-file batch)", len(rs))
	}
	joined := strings.Join(rs[0].Args, "\x00")
	for _, want := range []string{"--no-implied-dirs", "--files-from=-", "--from0", "--partial-dir="} {
		if !strings.Contains(joined, want) {
			t.Errorf("rsync batch argv missing %q: %v", want, rs[0].Args)
		}
	}
	for _, bad := range []string{"--delete", "--whole-file", "--inplace"} {
		if strings.Contains(joined, bad) {
			t.Errorf("rsync batch argv contains forbidden flag %q: %v", bad, rs[0].Args)
		}
	}
	if got := stdinPaths(t, rs[0]); len(got) != 1 || got[0] != "small.txt" {
		t.Errorf("rsync batch stdin paths = %v, want [small.txt]", got)
	}

	// The large file was really transferred natively: exact content, mode,
	// and nanosecond mtime land at the destination; the temp is cleaned.
	final := filepath.Join(h.remoteDir, "big.bin")
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatalf("published final missing: %v", err)
	}
	want, err := os.ReadFile(filepath.Join(h.src, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Error("final content mismatch")
	}
	fiFinal, err := os.Lstat(final)
	if err != nil {
		t.Fatal(err)
	}
	fiSrc, err := os.Lstat(filepath.Join(h.src, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if fiFinal.Mode().Perm() != fiSrc.Mode().Perm() {
		t.Errorf("final mode = %o, want source mode %o", fiFinal.Mode().Perm(), fiSrc.Mode().Perm())
	}
	if !fiFinal.ModTime().Equal(fiSrc.ModTime()) {
		t.Errorf("final mtime = %v, want source mtime %v", fiFinal.ModTime(), fiSrc.ModTime())
	}
	// SFTP sessions happened: the hardlink probe plus the transfer pool.
	if got := len(sftpSessionInvocations(invs)); got < 2 {
		t.Errorf("sftp sessions = %d, want at least 2 (probe + transfer)", got)
	}
	// The remote temp was cleaned after completion.
	entries, err := os.ReadDir(h.remoteDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".catsync-tmp-") {
			t.Errorf("remote temp left behind: %s", e.Name())
		}
	}
	// The completed task's checkpoint is retired.
	for _, task := range run.Tasks {
		if task.Checkpoint != "" {
			t.Errorf("task %s retains checkpoint %s after completion", task.Path, task.Checkpoint)
		}
	}
	ckptDir := filepath.Join(h.stateDir, "checkpoints", run.ID)
	entries, _ = os.ReadDir(ckptDir)
	for _, e := range entries {
		t.Errorf("checkpoint left behind: %s", e.Name())
	}
}

func TestSyncAllThreeBackendsDisjoint(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small-new.txt", 10)
	h.writeSrc("big-new.bin", 2048)
	h.writeSrc("big-mod.bin", 4096)
	h.writeSrc("small-same.txt", 12)
	sameSize, sameMT := h.srcMeta("small-same.txt")
	pre := h.writeManifest("pre", manifestContent(
		manifestRecord("big-mod.bin", 999, time.Unix(1700000000, 0)),
		manifestRecord("small-same.txt", sameSize, sameMT),
	))
	post := h.writeManifest("post", manifestContent(
		h.newFileRecord("small-new.txt"),
		h.newFileRecord("big-new.bin"),
		h.newFileRecord("big-mod.bin"),
		h.newFileRecord("small-same.txt"),
	))
	h.writeScript("ssh", `match=if [ -d
nth=1
stdout_file=`+pre+`
---
match=if [ -d
stdout_file=`+post+`
`)
	h.writeScript("rsync", "exit=0")

	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s, want completed (error %q)", run.Status, run.Error)
	}
	byPath := map[string]state.Task{}
	for _, task := range run.Tasks {
		byPath[task.Path] = task
	}
	if got := byPath["small-same.txt"].Status; got != state.StatusSkipped {
		t.Errorf("unchanged file = %s, want skipped", got)
	}
	if got := byPath["small-new.txt"].Backend; got != state.BackendFPSync {
		t.Errorf("small new file backend = %s, want fpsync (native)", got)
	}
	if got := byPath["big-new.bin"].Backend; got != state.BackendMSCP {
		t.Errorf("large new file backend = %s, want mscp (native multistream)", got)
	}
	if got := byPath["big-mod.bin"].Backend; got != state.BackendRsync {
		t.Errorf("modified file backend = %s, want rsync", got)
	}

	invs := h.invocations()
	if ms := toolInvocations(invs, "mscp"); len(ms) != 0 {
		t.Errorf("external mscp invoked %d times, want 0", len(ms))
	}
	rs := toolInvocations(invs, "rsync")
	if len(rs) != 2 {
		t.Fatalf("rsync invocations = %d, want 2 (native small batch + conservative)", len(rs))
	}
	var native, conservative *mockexec.Invocation
	for i := range rs {
		joined := strings.Join(rs[i].Args, "\x00")
		if strings.Contains(joined, "--no-implied-dirs") {
			native = &rs[i]
		} else {
			conservative = &rs[i]
		}
	}
	if native == nil || conservative == nil {
		t.Fatalf("expected one native (--no-implied-dirs) and one conservative rsync run, got %d invocations", len(rs))
	}
	if got := stdinPaths(t, *native); len(got) != 1 || got[0] != "small-new.txt" {
		t.Errorf("native batch stdin = %v, want exactly [small-new.txt]", got)
	}
	if got := stdinPaths(t, *conservative); len(got) != 1 || got[0] != "big-mod.bin" {
		t.Errorf("conservative stdin = %v, want exactly [big-mod.bin] (single writer)", got)
	}
	for _, inv := range rs {
		if strings.Contains(strings.Join(inv.Args, "\x00"), "--delete") {
			t.Errorf("rsync argv contains --delete: %v", inv.Args)
		}
	}
	// The new large file really transferred natively; the existing one was
	// never touched by the native stage.
	final := filepath.Join(h.remoteDir, "big-new.bin")
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatalf("published final missing: %v", err)
	}
	want, err := os.ReadFile(filepath.Join(h.src, "big-new.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Error("native final content mismatch")
	}
	// The native stage never touched big-mod: only the mocked conservative
	// rsync owned it, and the mock writes nothing.
	if _, err := os.Lstat(filepath.Join(h.remoteDir, "big-mod.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("big-mod.bin appeared at the destination: native stage must not write it (%v)", err)
	}
	// No double writers: the destination path of the native file is only
	// in the native stage, and the conservative rsync only got big-mod.
	if got := len(sftpSessionInvocations(invs)); got < 2 {
		t.Errorf("sftp sessions = %d, want at least 2", got)
	}
}

func TestSyncUnknownManifestRoutesEverythingToRsync(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt"), h.newFileRecord("big.bin")))
	h.writeScript("ssh", `match=if [ -d
nth=1
exit=1
---
match=if [ -d
stdout_file=`+post+`
`)
	h.writeScript("rsync", "exit=0")

	run, err := Sync(context.Background(), h.config())
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (error %q)", run.Status, run.Error)
	}
	invs := h.invocations()
	if len(toolInvocations(invs, "fpsync")) != 0 || len(toolInvocations(invs, "mscp")) != 0 {
		t.Error("fpsync/mscp must not run when the destination state is unknown")
	}
	rs := toolInvocations(invs, "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1", len(rs))
	}
	if got := stdinPaths(t, rs[0]); len(got) != 2 {
		t.Errorf("rsync paths = %v, want both files", got)
	}
}

func TestSyncForcedMSCPRequiresManifest(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)
	h.writeScript("ssh", "match=if [ -d\nexit=1\n")
	cfg := h.config()
	cfg.Backend = state.BackendMSCP
	_, err := Sync(context.Background(), cfg)
	if err == nil {
		t.Fatal("Sync with forced mscp and unknown destination = nil, want error")
	}
	if len(toolInvocations(h.invocations(), "mscp")) != 0 {
		t.Error("mscp ran despite unknown destination state")
	}
}

func TestSyncForcedMSCPRejectsExistingFile(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)
	pre := h.writeManifest("pre", manifestContent(manifestRecord("big.bin", 2048, time.Unix(1700000000, 0))))
	h.writeScript("ssh", "match=if [ -d\nstdout_file="+pre+"\n")
	cfg := h.config()
	cfg.Backend = state.BackendMSCP
	_, err := Sync(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "refuses") {
		t.Fatalf("Sync = %v, want a forced-mscp refusal", err)
	}
	if len(toolInvocations(h.invocations(), "mscp")) != 0 {
		t.Error("mscp ran despite the existing destination file")
	}
}
func TestSyncWithoutFPSyncProgramsStillUsesNativeBackend(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (error %q)", run.Status, run.Error)
	}
	invs := h.invocations()
	if got := len(toolInvocations(invs, "fpsync")) + len(toolInvocations(invs, "fpart")); got != 0 {
		t.Errorf("fpsync/fpart invocations = %d, want 0 (native backend)", got)
	}
	rs := toolInvocations(invs, "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1 (native small-file batch)", len(rs))
	}
	if !strings.Contains(strings.Join(rs[0].Args, "\x00"), "--no-implied-dirs") {
		t.Errorf("small-file batch missing --no-implied-dirs: %v", rs[0].Args)
	}
	if got := stdinPaths(t, rs[0]); len(got) != 1 || got[0] != "small.txt" {
		t.Errorf("rsync paths = %v, want [small.txt]", got)
	}
}

// TestSyncForcedMSCPWithoutExternalTool: forcing --backend mscp selects the
// native multistream backend; a nonexistent mscp program path is irrelevant
// because the external tool is never executed.
func TestSyncForcedMSCPWithoutExternalTool(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)
	h.scriptSSHFreshDest("")
	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	cfg.Backend = state.BackendMSCP

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("forced mscp (native) without the external tool: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run = %s (%q), want completed", run.Status, run.Error)
	}
	if got := len(toolInvocations(h.invocations(), "mscp")); got != 0 {
		t.Errorf("external mscp invoked %d times, want 0", got)
	}
	got, err := os.ReadFile(filepath.Join(h.remoteDir, "big.bin"))
	if err != nil {
		t.Fatalf("published final missing: %v", err)
	}
	want, err := os.ReadFile(filepath.Join(h.src, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Error("final content mismatch")
	}

	// The native small-file backend has no extra tool dependency either:
	// forcing "fpsync" with no fpsync executable still works through rsync.
	h2 := newHarness(t)
	h2.writeSrc("small.txt", 10)
	post2 := h2.writeManifest("post", manifestContent(h2.newFileRecord("small.txt")))
	h2.scriptSSHFreshDest(post2)
	h2.writeScript("rsync", "exit=0")
	cfg2 := h2.config()
	cfg2.Backend = state.BackendFPSync
	run2, err := Sync(context.Background(), cfg2)
	if err != nil {
		t.Fatalf("forced fpsync (native) without fpsync/fpart executables: %v", err)
	}
	if run2.Status != state.StatusCompleted {
		t.Errorf("forced fpsync run = %s (%q), want completed", run2.Status, run2.Error)
	}
	if got := len(toolInvocations(h2.invocations(), "fpsync")); got != 0 {
		t.Errorf("external fpsync invoked %d times, want 0", got)
	}
}

func TestSyncDryRunTransfersNothing(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", manifestContent())
	h.scriptSSHFreshDest(post)
	cfg := h.config()
	cfg.DryRun = true

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted || !run.Config.DryRun {
		t.Errorf("run = %s dry_run=%v", run.Status, run.Config.DryRun)
	}
	for _, task := range run.Tasks {
		if task.Status != state.StatusPending {
			t.Errorf("task %s = %s, want pending (nothing transferred)", task.Path, task.Status)
		}
	}
	invs := h.invocations()
	for _, tool := range []string{"rsync", "fpsync", "mscp"} {
		if len(toolInvocations(invs, tool)) != 0 {
			t.Errorf("%s ran during a dry run", tool)
		}
	}
	if len(toolInvocations(invs, "ssh")) != 1 {
		t.Errorf("ssh invocations = %d, want 1 (plan manifest only)", len(toolInvocations(invs, "ssh")))
	}
	// A dry run opens no SFTP session and creates no checkpoint or temp.
	if got := len(sftpSessionInvocations(invs)); got != 0 {
		t.Errorf("sftp sessions = %d, want 0 during a dry run", got)
	}
	if _, err := os.Stat(filepath.Join(h.stateDir, "checkpoints")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("dry run created a checkpoint directory: %v", err)
	}
}

func TestSyncSubprocessFailurePropagates(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=23")
	cfg := h.config()
	cfg.Backend = state.BackendRsync

	run, err := Sync(context.Background(), cfg)
	if err == nil {
		t.Fatal("Sync with failing rsync = nil, want error")
	}
	if run.Status != state.StatusFailed {
		t.Errorf("run status = %s, want failed", run.Status)
	}
	task := run.Tasks[0]
	if task.Status != state.StatusFailed || !strings.Contains(task.Error, "status 23") {
		t.Errorf("task = %s (%q), want failed with exit 23", task.Status, task.Error)
	}
	// A failed task is never verified, so no second ssh call happens.
	if got := len(toolInvocations(h.invocations(), "ssh")); got != 1 {
		t.Errorf("ssh invocations = %d, want 1", got)
	}
}

func TestSyncInterruptionLeavesResumableState(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "nth=1\nsleep_ms=30000\n---\nexit=0\n")
	cfg := h.config()
	cfg.Backend = state.BackendRsync

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	run, err := Sync(ctx, cfg)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Sync = %v, want context.Canceled", err)
	}
	if run.Status != state.StatusFailed {
		t.Errorf("run status = %s, want failed (interrupted)", run.Status)
	}
	task := run.Tasks[0]
	if task.Status != state.StatusPending || !strings.Contains(task.Error, "interrupted") {
		t.Errorf("task = %s (%q), want pending with interruption", task.Status, task.Error)
	}

	// Resume finishes the pending task; the second rsync invocation exits 0.
	run2, err := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: cfg.Programs,
		Env:      cfg.Env,
	}, run.ID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if run2.Status != state.StatusCompleted {
		t.Errorf("resumed run = %s (%q), want completed", run2.Status, run2.Error)
	}
	if run2.Tasks[0].Status != state.StatusCompleted {
		t.Errorf("resumed task = %s (%q)", run2.Tasks[0].Status, run2.Tasks[0].Error)
	}
	if got := toolInvocations(h.invocations(), "rsync"); len(got) != 2 {
		t.Errorf("rsync invocations = %d, want 2 (interrupted + resumed)", len(got))
	}
}

func TestSyncSizeMismatchFails(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big-ok.bin", 2048)
	h.writeSrc("big-bad.bin", 4096)
	okSize, okMT := h.srcMeta("big-ok.bin")
	post := h.writeManifest("post", manifestContent(
		manifestRecord("big-ok.bin", okSize, okMT),
		manifestRecord("big-bad.bin", 1, okMT), // wrong size on purpose
	))
	h.scriptSSHFreshDest(post)

	cfg := h.configNative()
	run, err := Sync(context.Background(), cfg)
	if err == nil {
		t.Fatal("Sync with size mismatch = nil, want error")
	}
	if run.Status != state.StatusFailed {
		t.Errorf("run status = %s, want failed", run.Status)
	}
	byPath := map[string]state.Task{}
	for _, task := range run.Tasks {
		byPath[task.Path] = task
	}
	if got := byPath["big-ok.bin"].Status; got != state.StatusCompleted {
		t.Errorf("big-ok.bin = %s, want completed", got)
	}
	bad := byPath["big-bad.bin"]
	if bad.Status != state.StatusFailed || !strings.Contains(bad.Error, "size verification") {
		t.Errorf("big-bad.bin = %s (%q), want failed size verification", bad.Status, bad.Error)
	}
	// The failed task's checkpoint is retained for resume; the completed
	// task's checkpoint is retired.
	if bad.Checkpoint == "" {
		t.Error("failed task lost its checkpoint reference")
	} else if _, err := os.Stat(bad.Checkpoint); err != nil {
		t.Errorf("checkpoint of failed task must be retained: %v", err)
	}
	if byPath["big-ok.bin"].Checkpoint != "" {
		t.Error("completed task must not retain a checkpoint")
	}
	// The completed task's remote temp was removed; the failed task's is
	// retained for resume.
	entries, err := os.ReadDir(h.remoteDir)
	if err != nil {
		t.Fatal(err)
	}
	var temps []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".catsync-tmp-") {
			temps = append(temps, e.Name())
		}
	}
	if len(temps) != 1 {
		t.Errorf("remote temps = %v, want exactly the failed task's temp", temps)
	}
	if len(temps) == 1 && strings.Contains(temps[0], "big-ok") {
		t.Errorf("completed task's temp was retained: %s", temps[0])
	}
}

// TestResumeReusesCheckpoint: a mid-transfer session failure leaves a
// checkpoint with partial progress; the resume continues from it (same
// checkpoint path, same temp) and publishes a byte-exact final.
func TestResumeReusesCheckpoint(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 1<<20) // 1 MiB — several chunks at 256 KiB windows
	post := h.writeManifest("post", manifestContent(h.newFileRecord("big.bin")))
	h.scriptSSHFreshDest(post)

	// Session #1 is the hardlink probe; session #2 (first transfer
	// session) fails. The checkpoint exists with no completed chunk and
	// the temp may or may not exist yet.
	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	cfg.Env = h.nativeEnv("MOCKEXEC_SFTP_FAILNTH=2")
	run, err := Sync(context.Background(), cfg)
	if err == nil {
		t.Fatal("Sync with a failing SFTP session = nil, want error")
	}
	task := run.Tasks[0]
	if task.Status != state.StatusFailed || task.Checkpoint == "" {
		t.Fatalf("task = %s checkpoint %q", task.Status, task.Checkpoint)
	}
	if _, err := os.Stat(task.Checkpoint); err != nil {
		t.Fatalf("checkpoint must survive the failure: %v", err)
	}
	ckptPath := task.Checkpoint

	// Resume without the failure knob: the same checkpoint is reused, the
	// transfer completes, and the final is byte-exact.
	cfg2 := h.configNative()
	cfg2.Verify = state.VerifyNone
	cfg2.Env = h.nativeEnv()
	run2, err := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: cfg.Programs,
		Env:      cfg2.Env,
	}, run.ID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if run2.Status != state.StatusCompleted {
		t.Errorf("resumed run = %s (%q)", run2.Status, run2.Error)
	}
	got, err := os.ReadFile(filepath.Join(h.remoteDir, "big.bin"))
	if err != nil {
		t.Fatalf("published final missing after resume: %v", err)
	}
	want, err := os.ReadFile(filepath.Join(h.src, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Error("final content mismatch after resume")
	}
	// The completed resume retired the checkpoint and cleaned the temp.
	if _, err := os.Stat(ckptPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("checkpoint must be retired after completion: %v", err)
	}
	entries, err := os.ReadDir(h.remoteDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".catsync-tmp-") {
			t.Errorf("remote temp left behind: %s", e.Name())
		}
	}
	if ms := toolInvocations(h.invocations(), "mscp"); len(ms) != 0 {
		t.Errorf("external mscp invoked %d times, want 0", len(ms))
	}
}

// TestResumeWithoutCheckpointRestartsFresh: a run that failed before the
// checkpoint's temp was ever created resumes cleanly from the recorded
// checkpoint, creating the temp then.
func TestResumeWithoutCheckpointRestartsFresh(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 1<<20)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("big.bin")))
	h.scriptSSHFreshDest(post)

	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	cfg.Env = h.nativeEnv("MOCKEXEC_SFTP_FAILNTH=2")
	run, err := Sync(context.Background(), cfg)
	if err == nil {
		t.Fatal("Sync with a failing SFTP session = nil, want error")
	}
	ckpt := run.Tasks[0].Checkpoint
	if ckpt == "" {
		t.Fatal("no checkpoint recorded")
	}

	cfg2 := h.configNative()
	cfg2.Verify = state.VerifyNone
	cfg2.Env = h.nativeEnv()
	run2, err := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: cfg.Programs,
		Env:      cfg2.Env,
	}, run.ID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if run2.Status != state.StatusCompleted {
		t.Errorf("resumed run = %s (%q)", run2.Status, run2.Error)
	}
	got, err := os.ReadFile(filepath.Join(h.remoteDir, "big.bin"))
	if err != nil {
		t.Fatalf("published final missing: %v", err)
	}
	want, err := os.ReadFile(filepath.Join(h.src, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Error("final content mismatch after fresh restart")
	}
}

func TestResumeLegacyFPSyncStateNatively(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	// Resume has no plan-phase manifest; the only ssh call is verification.
	h.writeScript("ssh", "match=if [ -d\nstdout_file="+post+"\n")
	h.writeScript("rsync", "exit=0")

	// Persist a run exactly as the removed external-fpsync implementation
	// left it: failed run, failed small-file task, legacy artifacts set,
	// legacy state directory absent on disk.
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	size, mt := h.srcMeta("small.txt")
	id, err := state.NewID()
	if err != nil {
		t.Fatal(err)
	}
	run := &state.Run{
		ID:          id,
		Source:      state.Source{Path: h.src},
		Destination: state.Destination{User: "u", Host: "h", Path: "/data"},
		WorkDir:     h.src,
		Config: state.Config{
			Backend: state.BackendAuto, Verify: state.VerifySize,
			Jobs: 2, Streams: 2, LargeThreshold: 1024,
		},
		Status:         state.StatusFailed,
		FPSyncRunID:    "1792333896-74154",
		FPSyncStateDir: filepath.Join(h.stateDir, "legacy-fpsync"),
		FPSyncOutput:   filepath.Join(h.stateDir, "legacy-fpsync.out"),
		CreatedAt:      time.Now().UTC(),
		Tasks: []state.Task{{
			Path: "small.txt", Size: size, ModTime: mt, DestSize: -1,
			Backend: state.BackendFPSync, Status: state.StatusFailed,
			Attempts: 1, Error: "interrupted",
		}},
	}
	if err := store.Save(run); err != nil {
		t.Fatal(err)
	}

	cfg := h.config()
	run2, err := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: cfg.Programs,
		Env:      cfg.Env,
	}, run.ID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if run2.Status != state.StatusCompleted {
		t.Fatalf("resumed run = %s (%q), want completed", run2.Status, run2.Error)
	}
	if run2.Tasks[0].Status != state.StatusCompleted {
		t.Errorf("task = %s (%q), want completed", run2.Tasks[0].Status, run2.Tasks[0].Error)
	}

	// The legacy artifacts were retired atomically with completion.
	loaded, err := store.Load(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.FPSyncRunID != "" || loaded.FPSyncStateDir != "" || loaded.FPSyncOutput != "" {
		t.Errorf("legacy fpsync artifacts not retired: runID=%q stateDir=%q out=%q",
			loaded.FPSyncRunID, loaded.FPSyncStateDir, loaded.FPSyncOutput)
	}

	// No external fpsync/fpart ran; one native rsync batch did.
	invs := h.invocations()
	if got := len(toolInvocations(invs, "fpsync")) + len(toolInvocations(invs, "fpart")); got != 0 {
		t.Errorf("fpsync/fpart invocations = %d, want 0", got)
	}
	rs := toolInvocations(invs, "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1", len(rs))
	}
	if !strings.Contains(strings.Join(rs[0].Args, "\x00"), "--no-implied-dirs") {
		t.Errorf("resumed small-file batch missing --no-implied-dirs: %v", rs[0].Args)
	}
	if got := stdinPaths(t, rs[0]); len(got) != 1 || got[0] != "small.txt" {
		t.Errorf("resumed batch stdin = %v, want [small.txt]", got)
	}
}

func TestSyncSpecialFilenames(t *testing.T) {
	h := newHarness(t)
	names := []string{
		"with space.txt",
		"ünïcødé-文件.txt",
		"new\nline.txt",
		"quote'and\"double.txt",
		"-leading-dash.txt",
	}
	for _, n := range names {
		h.writeSrc(n, 10)
	}
	h.writeSrc("big dir/-large.bin", 2048)
	h.writeSrc("-big.bin", 4096)

	var postRecs []string
	for _, n := range names {
		postRecs = append(postRecs, h.newFileRecord(n))
	}
	postRecs = append(postRecs, h.newFileRecord("big dir/-large.bin"), h.newFileRecord("-big.bin"))
	post := h.writeManifest("post", manifestContent(postRecs...))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")

	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (error %q)", run.Status, run.Error)
	}
	invs := h.invocations()
	if ms := toolInvocations(invs, "mscp"); len(ms) != 0 {
		t.Errorf("external mscp invoked %d times, want 0", len(ms))
	}

	// The five small special files went through one native rsync batch,
	// NUL-delimited on stdin, never through argv.
	rs := toolInvocations(invs, "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1 (native small-file batch)", len(rs))
	}
	joined := strings.Join(rs[0].Args, "\x00")
	if !strings.Contains(joined, "--no-implied-dirs") {
		t.Errorf("native batch missing --no-implied-dirs: %v", rs[0].Args)
	}
	// The batch content is a set; discovery order defines the on-wire order.
	gotPaths := stdinPaths(t, rs[0])
	wantPaths := append([]string{}, names...)
	sort.Strings(wantPaths)
	if !reflect.DeepEqual(gotPaths, wantPaths) {
		t.Errorf("batch stdin paths = %q, want the five special names NUL-delimited", gotPaths)
	}
	for _, n := range names {
		if strings.Contains(joined, n) {
			t.Errorf("special filename %q leaked into argv: %v", n, rs[0].Args)
		}
	}

	// The large special files landed at the destination byte-exact, with
	// their names intact (dash-leading included).
	for _, rel := range []string{"big dir/-large.bin", "-big.bin"} {
		got, err := os.ReadFile(filepath.Join(h.remoteDir, filepath.FromSlash(rel)))
		if err != nil {
			t.Errorf("published final %q missing: %v", rel, err)
			continue
		}
		want, err := os.ReadFile(filepath.Join(h.src, filepath.FromSlash(rel)))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Errorf("final %q content mismatch", rel)
		}
	}
}

func TestSyncThresholdBoundaryWithNativeBackend(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	h.writeSrc("equal.bin", 1024)
	h.writeSrc("above.bin", 2048)
	post := h.writeManifest("post", manifestContent(
		h.newFileRecord("small.txt"),
		h.newFileRecord("equal.bin"),
		h.newFileRecord("above.bin"),
	))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")

	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (%q)", run.Status, run.Error)
	}
	byPath := map[string]state.Task{}
	for _, task := range run.Tasks {
		byPath[task.Path] = task
	}
	if got := byPath["equal.bin"].Backend; got != state.BackendMSCP {
		t.Errorf("equal-threshold file backend = %s, want mscp (native multistream)", got)
	}
	if got := byPath["small.txt"].Backend; got != state.BackendFPSync {
		t.Errorf("below-threshold file backend = %s, want fpsync (native)", got)
	}
	invs := h.invocations()
	if ms := toolInvocations(invs, "mscp"); len(ms) != 0 {
		t.Errorf("external mscp invoked %d times, want 0", len(ms))
	}
	rs := toolInvocations(invs, "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1", len(rs))
	}
	if got := stdinPaths(t, rs[0]); len(got) != 1 || got[0] != "small.txt" {
		t.Errorf("native batch stdin = %v, want exactly [small.txt]", got)
	}
	// Both at-or-above-threshold files really transferred natively.
	for _, rel := range []string{"equal.bin", "above.bin"} {
		got, err := os.ReadFile(filepath.Join(h.remoteDir, rel))
		if err != nil {
			t.Errorf("published final %s missing: %v", rel, err)
			continue
		}
		want, err := os.ReadFile(filepath.Join(h.src, rel))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Errorf("final %s content mismatch", rel)
		}
	}
}

// TestSyncTinyThresholdNative: with the native small-file backend there is
// no minimum threshold: threshold=1 keeps zero-byte files on the small
// batch and routes the 1-byte file (at the threshold) to the native
// multistream backend.
func TestSyncTinyThresholdNative(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("empty.bin", 0)
	h.writeSrc("one.bin", 1)
	post := h.writeManifest("post", manifestContent(
		h.newFileRecord("empty.bin"),
		h.newFileRecord("one.bin"),
	))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	cfg.LargeThreshold = 1

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (%q)", run.Status, run.Error)
	}
	invs := h.invocations()
	if got := len(toolInvocations(invs, "fpsync")) + len(toolInvocations(invs, "fpart")); got != 0 {
		t.Errorf("fpsync/fpart invocations = %d, want 0", got)
	}
	if got := len(toolInvocations(invs, "mscp")); got != 0 {
		t.Errorf("external mscp invocations = %d, want 0", got)
	}
	rs := toolInvocations(invs, "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1 (native batch for the zero-byte file)", len(rs))
	}
	if got := stdinPaths(t, rs[0]); len(got) != 1 || got[0] != "empty.bin" {
		t.Errorf("native batch stdin = %v, want [empty.bin]", got)
	}
	// The 1-byte file transferred natively.
	got, err := os.ReadFile(filepath.Join(h.remoteDir, "one.bin"))
	if err != nil {
		t.Fatalf("published final missing: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("final size = %d, want 1", len(got))
	}
}

// failSavesFrom replaces the executor's save function with one that fails
// from the given 1-based call onward, simulating a full disk or an
// unwritable state directory after the plan was persisted.
func failSavesFrom(e *Executor, call int) {
	count := 0
	store := e.store
	e.saveFn = func(r *state.Run) error {
		count++
		if count >= call {
			return errors.New("injected persistence failure")
		}
		return store.Save(r)
	}
}

// TestSaveFailureBeforeTransferBlocksLaunch proves a failed markRunning
// save prevents the subprocess from starting: the transfer would otherwise
// be invisible and unrecoverable.
func TestSaveFailureBeforeTransferBlocksLaunch(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()
	cfg.Backend = state.BackendRsync

	run, err := syncWith(context.Background(), cfg, func(e *Executor) { failSavesFrom(e, 2) })
	if err == nil {
		t.Fatal("Sync with failing markRunning save = nil, want error")
	}
	if !strings.Contains(err.Error(), "injected persistence failure") {
		t.Errorf("error = %v, want the injected persistence failure", err)
	}
	// No transfer process may run when its running state is not durable.
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 0 {
		t.Errorf("rsync invocations = %d, want 0 (transfer must not start)", got)
	}
	if got := len(toolInvocations(h.invocations(), "mscp")) + len(toolInvocations(h.invocations(), "fpsync")); got != 0 {
		t.Errorf("other transfer invocations = %d, want 0", got)
	}
	if run == nil {
		t.Fatal("Sync returned no run")
	}
	// The durable document is the pre-transfer one: running run, pending
	// task, recoverable by the next load/resume.
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Tasks[0].Status != state.StatusPending {
		t.Errorf("durable task status = %q, want pending", loaded.Tasks[0].Status)
	}
	if loaded.Status != state.StatusRunning {
		t.Errorf("durable run status = %q, want running (recoverable)", loaded.Status)
	}
}

// mscpOnlyHarness prepares a fresh-destination run with one large mscp file.
// nativeOnlyHarness builds a one-large-file harness whose native transfer
// runs against the real local sftp-server, no verification stage.
func mscpOnlyHarness(t *testing.T) (*harness, Config) {
	t.Helper()
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("big.bin")))
	h.scriptSSHFreshDest(post)
	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	cfg.Env = h.nativeEnv()
	return h, cfg
}

// TestCompletedPersistedBeforeCheckpointRemoval proves ordering: when the
// completion save fails, the checkpoint is NOT removed and the durable task
// state stays running (not completed-without-checkpoint).
func TestCompletedPersistedBeforeCheckpointRemoval(t *testing.T) {
	h, cfg := mscpOnlyHarness(t)
	// Save #1 is the initial run, #2 markRunning, #3 the completion save.
	run, err := syncWith(context.Background(), cfg, func(e *Executor) { failSavesFrom(e, 3) })
	if err == nil {
		t.Fatal("Sync with failing completion save = nil, want error")
	}
	if got := len(toolInvocations(h.invocations(), "mscp")); got != 0 {
		t.Fatalf("external mscp invocations = %d, want 0 (native backend)", got)
	}
	if got := len(sftpSessionInvocations(h.invocations())); got == 0 {
		t.Fatal("no SFTP session ran; the native transfer never started")
	}
	if len(run.Tasks) != 1 || run.Tasks[0].Checkpoint == "" {
		t.Fatalf("task checkpoint reference missing: %+v", run.Tasks)
	}
	cp := run.Tasks[0].Checkpoint
	if _, statErr := os.Stat(cp); statErr != nil {
		t.Fatalf("checkpoint was removed before completion was durable: %v", statErr)
	}
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Tasks[0].Status; got != state.StatusRunning {
		t.Errorf("durable task status = %q, want running (completion not persisted)", got)
	}
	if loaded.Tasks[0].Checkpoint != cp {
		t.Errorf("durable checkpoint ref = %q, want %q", loaded.Tasks[0].Checkpoint, cp)
	}

	// The published final is already live; a later resume must converge:
	// it recognizes the same-inode publish, persists completion, and only
	// then retires the checkpoint and the remote temp.
	final := filepath.Join(h.remoteDir, "big.bin")
	if _, err := os.Stat(final); err != nil {
		t.Fatalf("published final missing after the failed completion save: %v", err)
	}
	run2, err := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: h.config().Programs,
		Env:      h.nativeEnv(),
	}, run.ID)
	if err != nil {
		t.Fatalf("Resume after a published-but-unrecorded completion: %v", err)
	}
	if run2.Status != state.StatusCompleted {
		t.Errorf("resumed run = %s (%q), want completed", run2.Status, run2.Error)
	}
	if _, err := os.Stat(cp); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("checkpoint must be retired after convergence: %v", err)
	}
	entries, err := os.ReadDir(h.remoteDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".catsync-tmp-") {
			t.Errorf("remote temp left behind after convergence: %s", e.Name())
		}
	}
}

// TestCheckpointReferenceClearFailureRetainsCheckpoint covers the crash-safe
// window between "completed persisted" and "checkpoint reference cleared":
// a failed reference save must leave both the reference and the file, which
// is harmless because the task is already durably completed.
func TestCheckpointReferenceClearFailureRetainsCheckpoint(t *testing.T) {
	h, cfg := mscpOnlyHarness(t)
	// #1 initial, #2 markRunning, #3 completed, #4 clear reference.
	run, err := syncWith(context.Background(), cfg, func(e *Executor) { failSavesFrom(e, 4) })
	if err == nil {
		t.Fatal("Sync with failing checkpoint-reference save = nil, want error")
	}
	if len(run.Tasks) != 1 || run.Tasks[0].Checkpoint == "" {
		t.Fatalf("checkpoint reference missing: %+v", run.Tasks)
	}
	cp := run.Tasks[0].Checkpoint
	if _, statErr := os.Stat(cp); statErr != nil {
		t.Fatalf("checkpoint must be retained when its reference save failed: %v", statErr)
	}
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Tasks[0].Status; got != state.StatusCompleted {
		t.Errorf("durable task status = %q, want completed", got)
	}
	if loaded.Tasks[0].Checkpoint != cp {
		t.Errorf("durable checkpoint ref = %q, want %q retained", loaded.Tasks[0].Checkpoint, cp)
	}

	// Resume's recovery pass retires the leftover reference and file
	// without touching the completed status.
	e2, err := newExecutor(Config{StateDir: h.stateDir, Programs: h.config().Programs, Env: h.nativeEnv()})
	if err != nil {
		t.Fatal(err)
	}
	recovered, _, err := e2.store.LoadAndRecover(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	e2.run = recovered
	if err := e2.retireCompletedCheckpoints(); err != nil {
		t.Fatalf("retireCompletedCheckpoints: %v", err)
	}
	after, err := store.Load(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Tasks[0].Status != state.StatusCompleted {
		t.Errorf("recovery changed completed status to %q", after.Tasks[0].Status)
	}
	if after.Tasks[0].Checkpoint != "" {
		t.Errorf("recovery left checkpoint reference %q", after.Tasks[0].Checkpoint)
	}
	if _, statErr := os.Stat(cp); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("recovery left checkpoint file behind: %v", statErr)
	}
}

// TestSyncCancellationWithZeroExitBackendRemainsResumable is the
// state-layer regression for graceful cancellation: the mock backend
// handles the forwarded SIGTERM and exits 0. Sync must still report the
// cancellation, the run must never become completed, the task must be
// resumable (pending with the interruption note), the lock must be free,
// and no descendant writer may survive or write late.
func TestSyncCancellationWithZeroExitBackendRemainsResumable(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	ready := filepath.Join(t.TempDir(), "ready")
	marker := filepath.Join(t.TempDir(), "late-write")
	childDir := t.TempDir()
	sigFile := filepath.Join(t.TempDir(), "signal")
	h.writeScript("rsync", fmt.Sprintf(
		"zero_exit_signal=TERM\nready_file=%s\nsignal_file=%s\nspawn_children=1\nchild_sleep_ms=1500\nchild_touch=%s\nchild_pid_dir=%s\nsleep_ms=30000\n",
		ready, sigFile, marker, childDir))

	cfg := h.config()
	cfg.Backend = state.BackendRsync
	cfg.Verify = state.VerifyNone

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	runCh := make(chan *state.Run, 1)
	go func() {
		run, err := Sync(ctx, cfg)
		runCh <- run
		done <- err
	}()

	// Wait until the mock backend is live (the ready marker is written
	// after the signal handler and grandchild are set up), then cancel.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mock rsync did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	err := <-done
	run := <-runCh
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Sync = %v, want an error wrapping context.Canceled", err)
	}
	// The graceful zero-exit path really ran: the backend caught TERM.
	sig, serr := os.ReadFile(sigFile)
	if serr != nil {
		t.Fatalf("mock backend did not handle the forwarded signal: %v", serr)
	}
	if got := strings.TrimSpace(string(sig)); got != "terminated" {
		t.Errorf("forwarded signal = %q, want terminated", got)
	}
	if run == nil {
		t.Fatal("Sync returned no run")
	}
	if run.Status == state.StatusCompleted {
		t.Fatal("cancelled run was recorded as completed")
	}
	if run.Status != state.StatusFailed {
		t.Errorf("run status = %s, want failed (interrupted)", run.Status)
	}
	if len(run.Tasks) != 1 || run.Tasks[0].Status != state.StatusPending {
		t.Fatalf("task after cancellation = %+v, want pending", run.Tasks)
	}
	if !strings.Contains(run.Tasks[0].Error, "interrupted") {
		t.Errorf("task error = %q, want the interruption note", run.Tasks[0].Error)
	}
	if run.Tasks[0].Attempts != 1 {
		t.Errorf("attempts = %d, want 1 (started once)", run.Tasks[0].Attempts)
	}

	// The durable state agrees and is resumable.
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != state.StatusFailed || loaded.Tasks[0].Status != state.StatusPending {
		t.Errorf("durable state = %s/%s, want failed/pending", loaded.Status, loaded.Tasks[0].Status)
	}
	if !loaded.Resumable() {
		t.Error("cancelled run is not resumable")
	}
	// The destination lock was released.
	lock, err := store.LockDestination(state.Destination{User: "u", Host: "h", Path: "/data"})
	if err != nil {
		t.Fatalf("destination lock still held after cancellation: %v", err)
	}
	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	// Descendants are gone before Sync returned, and no late write appears.
	pids, perr := mockexec.ReadChildPids(childDir)
	if perr != nil {
		t.Fatal(perr)
	}
	if len(pids) != 1 {
		t.Fatalf("recorded %d descendant pids, want 1", len(pids))
	}
	for _, pid := range pids {
		if err := mockexec.ProcessAlive(pid); err == nil {
			t.Errorf("descendant writer %d survived the cancelled sync", pid)
		}
	}
	time.Sleep(1600 * time.Millisecond)
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("descendant wrote %s after cancellation: %v", marker, statErr)
	}
}

// TestResumeRejectsUnexpressibleSSHOverrideBeforeExecution is the regression
// for the resume validation gap: a resumable run with the current
// invocation's invalid CATSYNC_SSH must fail as a configuration error before
// any backend or ssh invocation, without touching the persisted state or
// attempt counts.
func TestResumeRejectsUnexpressibleSSHOverrideBeforeExecution(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=23")

	cfg := h.config()
	cfg.Backend = state.BackendRsync
	cfg.Verify = state.VerifyNone
	run, err := Sync(context.Background(), cfg)
	if err == nil {
		t.Fatal("setup sync with failing rsync = nil, want a failed run")
	}
	if run.Status != state.StatusFailed || !run.Resumable() {
		t.Fatalf("setup run = %s resumable=%v, want failed/resumable", run.Status, run.Resumable())
	}
	attemptsBefore := run.Tasks[0].Attempts
	invocationsBefore := len(h.invocations())

	badCfg := Config{
		StateDir: h.stateDir,
		Programs: Programs{SSH: "/opt/with space/ssh"},
		Env:      mockexec.EnvScript(h.scriptDir, h.logPath),
	}
	_, rerr := Resume(context.Background(), badCfg, run.ID)
	if !errors.Is(rerr, ErrInvalidConfig) {
		t.Fatalf("Resume with unexpressible ssh override = %v, want ErrInvalidConfig", rerr)
	}
	if !strings.Contains(rerr.Error(), "whitespace") {
		t.Errorf("error = %v, want the whitespace explanation", rerr)
	}
	// No ssh/rsync/fpsync/mscp invocation may have happened.
	if got := len(h.invocations()); got != invocationsBefore {
		t.Errorf("invocations = %d after rejected resume, want %d", got, invocationsBefore)
	}
	// The persisted run and its attempt count are untouched.
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Status != state.StatusFailed {
		t.Errorf("persisted run status = %q, want failed", loaded.Status)
	}
	if len(loaded.Tasks) != 1 || loaded.Tasks[0].Attempts != attemptsBefore {
		t.Errorf("persisted attempts = %+v, want %d", loaded.Tasks, attemptsBefore)
	}
	if loaded.Tasks[0].Status != state.StatusFailed {
		t.Errorf("persisted task status = %q, want failed", loaded.Tasks[0].Status)
	}
}

// maxObservedConcurrency computes the maximum number of simultaneously live
// mock processes from a track log of "start <pid>"/"end <pid>" lines.
func maxObservedConcurrency(t *testing.T, logPath string) int {
	t.Helper()
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read track log: %v", err)
	}
	live, max := 0, 0
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		switch {
		case strings.HasPrefix(line, "start "):
			live++
			if live > max {
				max = live
			}
		case strings.HasPrefix(line, "end "):
			live--
		}
	}
	return max
}

// batchPathSet returns the sorted stdin path list of a mock invocation.
func batchPathSet(t *testing.T, inv mockexec.Invocation) []string {
	t.Helper()
	return stdinPaths(t, inv)
}

// TestNativeParallelRsyncRespectsJobs proves the native stage runs at most
// Config.Jobs concurrent rsync processes, that batches are disjoint and
// cover every planned small file exactly once, and that the configured
// rsync/ssh overrides reach the batch commands.
func TestNativeParallelRsyncRespectsJobs(t *testing.T) {
	h := newHarness(t)
	// 600-byte files with a 1024-byte threshold: one file per batch.
	var names []string
	for i := 0; i < 6; i++ {
		names = append(names, fmt.Sprintf("f%d.txt", i))
		h.writeSrc(names[i], 600)
	}
	var recs []string
	for _, n := range names {
		recs = append(recs, h.newFileRecord(n))
	}
	post := h.writeManifest("post", manifestContent(recs...))
	h.scriptSSHFreshDest(post)
	trackLog := filepath.Join(t.TempDir(), "track.log")
	h.writeScript("rsync", "track_log="+trackLog+"\nsleep_ms=400\n")
	cfg := h.config()
	cfg.Jobs = 2

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (%q)", run.Status, run.Error)
	}

	invs := h.invocations()
	rs := toolInvocations(invs, "rsync")
	if len(rs) != 6 {
		t.Fatalf("rsync invocations = %d, want 6 (one file per batch)", len(rs))
	}
	if got := maxObservedConcurrency(t, trackLog); got != 2 {
		t.Errorf("max observed concurrency = %d, want exactly 2 (jobs=2)", got)
	}
	if got := len(toolInvocations(invs, "fpsync")) + len(toolInvocations(invs, "fpart")); got != 0 {
		t.Errorf("fpsync/fpart invocations = %d, want 0", got)
	}
	// Disjoint union: every planned small file appears in exactly one batch.
	var all []string
	for _, inv := range rs {
		all = append(all, batchPathSet(t, inv)...)
	}
	sort.Strings(all)
	want := append([]string{}, names...)
	sort.Strings(want)
	if !reflect.DeepEqual(all, want) {
		t.Errorf("union of batch paths = %v, want %v", all, want)
	}
	// Every batch uses --no-implied-dirs and the configured ssh transport.
	for i, inv := range rs {
		joined := strings.Join(inv.Args, "\x00")
		if !strings.Contains(joined, "--no-implied-dirs") {
			t.Errorf("batch %d missing --no-implied-dirs: %v", i, inv.Args)
		}
		wantTransport := h.progs["ssh"]
		found := false
		for j, a := range inv.Args {
			if a == "-e" && j+1 < len(inv.Args) {
				if inv.Args[j+1] != wantTransport {
					t.Errorf("batch %d transport = %q, want CATSYNC_SSH override %q", i, inv.Args[j+1], wantTransport)
				}
				found = true
			}
		}
		if !found {
			t.Errorf("batch %d has no -e transport: %v", i, inv.Args)
		}
	}
}

// TestNativeStageIPv6Transport proves --ipv6 reaches the native stage:
// ssh calls get -6 and the rsync -e transport becomes "<ssh> -6".
func TestNativeStageIPv6Transport(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()
	cfg.IPv6 = true

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (%q)", run.Status, run.Error)
	}
	rs := toolInvocations(h.invocations(), "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1", len(rs))
	}
	want := h.progs["ssh"] + " -6"
	found := false
	for i, a := range rs[0].Args {
		if a == "-e" && i+1 < len(rs[0].Args) {
			found = true
			if rs[0].Args[i+1] != want {
				t.Errorf("transport = %q, want %q", rs[0].Args[i+1], want)
			}
		}
	}
	if !found {
		t.Errorf("no -e in rsync argv: %v", rs[0].Args)
	}
	for _, inv := range toolInvocations(h.invocations(), "ssh") {
		if len(inv.Args) < 2 || inv.Args[0] != "-6" {
			t.Errorf("ssh argv missing -6: %v", inv.Args)
		}
	}
}

// TestNativeStageNeverLaunchesUnstartedBatches: with jobs=1, a failing
// first batch stops the stage before the remaining batches ever launch a
// process or mark their tasks.
func TestNativeStageNeverLaunchesUnstartedBatches(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("a.txt", 600)
	h.writeSrc("b.txt", 600)
	h.writeSrc("c.txt", 600)
	post := h.writeManifest("post", manifestContent(
		h.newFileRecord("a.txt"), h.newFileRecord("b.txt"), h.newFileRecord("c.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "nth=1\nexit=1\n---\nexit=0\n")
	cfg := h.config()
	cfg.Jobs = 1

	run, err := Sync(context.Background(), cfg)
	if err == nil {
		t.Fatal("Sync with failing first batch = nil, want error")
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 1 {
		t.Errorf("rsync invocations = %d, want exactly 1 (later batches never launched)", got)
	}
	failed, pending := 0, 0
	for _, task := range run.Tasks {
		switch task.Status {
		case state.StatusFailed:
			failed++
			if task.Attempts != 1 {
				t.Errorf("failed task attempts = %d, want 1", task.Attempts)
			}
		case state.StatusPending:
			pending++
			if task.Attempts != 0 {
				t.Errorf("never-launched task %s attempts = %d, want 0", task.Path, task.Attempts)
			}
		}
	}
	if failed != 1 || pending != 2 {
		t.Errorf("failed=%d pending=%d, want 1 failed + 2 pending", failed, pending)
	}
	if run.Status != state.StatusFailed {
		t.Errorf("run status = %s, want failed", run.Status)
	}
}

// TestNativeStageDetectsSourceChangeWhileQueued: a file that changes while
// its batch waits for a slot is caught by the per-batch pre-check and never
// enters rsync.
func TestNativeStageDetectsSourceChangeWhileQueued(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("first.txt", 600)
	h.writeSrc("second.txt", 600)
	post := h.writeManifest("post", manifestContent(
		h.newFileRecord("first.txt"), h.newFileRecord("second.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "nth=1\nsleep_ms=800\nexit=0\n---\nexit=0\n")
	cfg := h.config()
	cfg.Jobs = 1

	done := make(chan error, 1)
	go func() {
		run, err := Sync(context.Background(), cfg)
		_ = run
		done <- err
	}()

	// Wait for the first batch to start, learn which file it carries, and
	// mutate the OTHER file while its batch is still queued.
	deadline := time.Now().Add(10 * time.Second)
	var firstBatch []mockexec.Invocation
	for {
		firstBatch = toolInvocations(h.invocations(), "rsync")
		if len(firstBatch) >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first rsync batch never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	running := batchPathSet(t, firstBatch[0])
	queued := "second.txt"
	if len(running) == 1 && running[0] == "second.txt" {
		queued = "first.txt"
	}
	if err := os.WriteFile(filepath.Join(h.src, queued), []byte("mutated while queued, different size"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("Sync = nil, want error (one task failed its queued revalidation)")
	}

	if got := len(toolInvocations(h.invocations(), "rsync")); got != 1 {
		t.Errorf("rsync invocations = %d, want 1 (the mutated file never launched)", got)
	}
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Latest()
	if err != nil {
		t.Fatal(err)
	}
	byPath := map[string]state.Task{}
	for _, task := range loaded.Tasks {
		byPath[task.Path] = task
	}
	if got := byPath[queued].Status; got != state.StatusFailed {
		t.Errorf("queued-and-mutated file status = %s, want failed", got)
	}
	if !strings.Contains(byPath[queued].Error, "source changed since planning") {
		t.Errorf("queued file error = %q", byPath[queued].Error)
	}
	if got := byPath[running[0]].Status; got != state.StatusCompleted {
		t.Errorf("first batch file status = %s, want completed", got)
	}
}

// TestNativeStageVerifyNone: with --verify none the small-file batch
// completes tasks right after the post-check and no verification manifest
// is collected.
func TestNativeStageVerifyNone(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()
	cfg.Verify = state.VerifyNone

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Tasks[0].Status != state.StatusCompleted {
		t.Errorf("task = %s (%q), want completed", run.Tasks[0].Status, run.Tasks[0].Error)
	}
	if got := len(toolInvocations(h.invocations(), "ssh")); got != 1 {
		t.Errorf("ssh invocations = %d, want 1 (plan manifest only)", got)
	}
}

// TestNativeStageInterruptionThenResume: cancelling mid-batch reverts the
// running task to pending and resume finishes the work with fresh native
// batches, never touching fpsync.
func TestNativeStageInterruptionThenResume(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("a.txt", 600)
	h.writeSrc("b.txt", 600)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("a.txt"), h.newFileRecord("b.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "nth=1\nsleep_ms=30000\nexit=0\n---\nexit=0\n")
	cfg := h.config()
	cfg.Jobs = 1

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	runCh := make(chan *state.Run, 1)
	go func() {
		run, err := Sync(ctx, cfg)
		runCh <- run
		done <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for len(toolInvocations(h.invocations(), "rsync")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("first batch never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	err := <-done
	run := <-runCh
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Sync = %v, want context.Canceled", err)
	}
	if run.Status != state.StatusFailed {
		t.Errorf("run status = %s, want failed (interrupted)", run.Status)
	}
	for _, task := range run.Tasks {
		if task.Status != state.StatusPending {
			t.Errorf("task %s = %s after cancellation, want pending", task.Path, task.Status)
		}
	}

	run2, err := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: cfg.Programs,
		Env:      cfg.Env,
	}, run.ID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if run2.Status != state.StatusCompleted {
		t.Fatalf("resumed run = %s (%q), want completed", run2.Status, run2.Error)
	}
	for _, task := range run2.Tasks {
		if task.Status != state.StatusCompleted {
			t.Errorf("resumed task %s = %s (%q)", task.Path, task.Status, task.Error)
		}
	}
	invs := h.invocations()
	if got := len(toolInvocations(invs, "fpsync")) + len(toolInvocations(invs, "fpart")); got != 0 {
		t.Errorf("fpsync/fpart invocations = %d, want 0", got)
	}
	if got := len(toolInvocations(invs, "rsync")); got != 3 {
		t.Errorf("rsync invocations = %d, want 3 (cancelled + two resumed batches)", got)
	}
}

// TestNativeStageZeroExitCancellation: the backend catches TERM and exits 0
// on cancellation; the stage must still report the cancellation and leave
// the task resumable, never completed.
func TestNativeStageZeroExitCancellation(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	ready := filepath.Join(t.TempDir(), "ready")
	sigFile := filepath.Join(t.TempDir(), "signal")
	h.writeScript("rsync", "zero_exit_signal=TERM\nready_file="+ready+"\nsignal_file="+sigFile+"\nsleep_ms=30000\n")
	cfg := h.config()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	runCh := make(chan *state.Run, 1)
	go func() {
		run, err := Sync(ctx, cfg)
		runCh <- run
		done <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mock rsync never became ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	err := <-done
	run := <-runCh
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Sync = %v, want context.Canceled (zero-exit backend must not be a success)", err)
	}
	if run.Status == state.StatusCompleted {
		t.Fatal("cancelled zero-exit run was marked completed")
	}
	if run.Tasks[0].Status != state.StatusPending {
		t.Errorf("task = %s, want pending (resumable)", run.Tasks[0].Status)
	}
	sig, serr := os.ReadFile(sigFile)
	if serr != nil {
		t.Fatalf("mock did not record the forwarded signal: %v", serr)
	}
	if got := strings.TrimSpace(string(sig)); got != "terminated" {
		t.Errorf("forwarded signal = %q, want terminated", got)
	}
}

// TestNativeStageSaveFailureBlocksLaunch: a persistence failure while
// marking the batch running prevents the rsync subprocess from starting.
func TestNativeStageSaveFailureBlocksLaunch(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()

	run, err := syncWith(context.Background(), cfg, func(e *Executor) { failSavesFrom(e, 2) })
	if err == nil {
		t.Fatal("Sync with failing markRunning save = nil, want error")
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 0 {
		t.Errorf("rsync invocations = %d, want 0 (batch must not launch)", got)
	}
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Tasks[0].Status != state.StatusPending {
		t.Errorf("durable task status = %s, want pending", loaded.Tasks[0].Status)
	}
}

// TestNativeStageBatchedParentMkdir: all remote parents for the small-file
// batches are created by one mkdir call before any rsync batch launches.
func TestNativeStageBatchedParentMkdir(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("dir a/f1.txt", 10)
	h.writeSrc("dir b/nested/f2.txt", 10)
	post := h.writeManifest("post", manifestContent(
		h.newFileRecord("dir a/f1.txt"), h.newFileRecord("dir b/nested/f2.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")

	run, err := Sync(context.Background(), h.config())
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (%q)", run.Status, run.Error)
	}
	invs := h.invocations()
	var mkdirs []int
	for i, inv := range invs {
		if inv.Tool == "ssh" && strings.Contains(strings.Join(inv.Args, "\x00"), "mkdir -p") {
			mkdirs = append(mkdirs, i)
		}
	}
	if len(mkdirs) != 1 {
		t.Fatalf("mkdir ssh calls = %d, want exactly 1", len(mkdirs))
	}
	script := invs[mkdirs[0]].Args[len(invs[mkdirs[0]].Args)-1]
	if !strings.Contains(script, "'/data/dir a'") || !strings.Contains(script, "'/data/dir b/nested'") {
		t.Errorf("mkdir script missing quoted parents: %q", script)
	}
	// mkdir happens before the first rsync batch.
	firstRsync := -1
	for i, inv := range invs {
		if inv.Tool == "rsync" {
			firstRsync = i
			break
		}
	}
	if firstRsync < 0 || mkdirs[0] > firstRsync {
		t.Errorf("mkdir at invocation %d, first rsync at %d: mkdir must come first", mkdirs[0], firstRsync)
	}
}

// TestNativeBatchSaveFailureIsAtomic: three files in one batch; the
// batch-level running-intent save is injected to fail. No task may be left
// running (the old per-task loop left the first tasks committed as running
// while the later save failed), rsync must not start, and the reloaded
// state must contain no partial commit.
func TestNativeBatchSaveFailureIsAtomic(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("a.txt", 10)
	h.writeSrc("b.txt", 10)
	h.writeSrc("c.txt", 10)
	post := h.writeManifest("post", manifestContent(
		h.newFileRecord("a.txt"), h.newFileRecord("b.txt"), h.newFileRecord("c.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()

	// Save #1 is the initial run document; save #2 is the batch's running
	// intent — make it fail. TestNativeBatchRunningIntentSavedOnce proves
	// this is the ONLY save that can carry the running intent, so the old
	// partial-commit window (first task saved running, second task's save
	// fails, nothing rolled back) no longer exists.
	run, err := syncWith(context.Background(), cfg, func(e *Executor) { failSavesFrom(e, 2) })
	if err == nil {
		t.Fatal("Sync with failing batch save = nil, want error")
	}
	if !strings.Contains(err.Error(), "injected persistence failure") {
		t.Errorf("error = %v, want the injected failure", err)
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 0 {
		t.Errorf("rsync invocations = %d, want 0 (the batch never started)", got)
	}
	if run == nil {
		t.Fatal("Sync returned no run")
	}
	for _, task := range run.Tasks {
		if task.Status == state.StatusRunning {
			t.Errorf("task %s stuck at running after a failed batch save", task.Path)
		}
		if task.Status != state.StatusPending {
			t.Errorf("task %s = %s, want pending (resumable)", task.Path, task.Status)
		}
		if task.Attempts != 0 {
			t.Errorf("task %s attempts = %d, want 0 (never launched)", task.Path, task.Attempts)
		}
	}

	// The durable document has no half-commit either: recovery finds the
	// pre-transfer run and every task pending.
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, recovered, err := store.LoadAndRecover(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !recovered {
		t.Error("expected recovery to fix the run status recorded before the failed batch save")
	}
	for _, task := range loaded.Tasks {
		if task.Status != state.StatusPending {
			t.Errorf("durable task %s = %s, want pending", task.Path, task.Status)
		}
	}
	if !loaded.Resumable() {
		t.Error("run after failed batch save is not resumable")
	}
}

// TestNativeBatchRunningIntentSavedOnce: the whole batch's running intent
// costs exactly ONE run save, independent of batch size; the initial save,
// per-task completion saves, and any verification saves are distinct.
func TestNativeBatchRunningIntentSavedOnce(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("a.txt", 10)
	h.writeSrc("b.txt", 10)
	h.writeSrc("c.txt", 10)
	post := h.writeManifest("post", manifestContent(
		h.newFileRecord("a.txt"), h.newFileRecord("b.txt"), h.newFileRecord("c.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()
	cfg.Verify = state.VerifyNone

	var mu sync.Mutex
	var snapshots [][]state.Status
	run, err := syncWith(context.Background(), cfg, func(e *Executor) {
		orig := e.saveFn
		e.saveFn = func(r *state.Run) error {
			snap := make([]state.Status, len(r.Tasks))
			for i := range r.Tasks {
				snap[i] = r.Tasks[i].Status
			}
			mu.Lock()
			snapshots = append(snapshots, snap)
			mu.Unlock()
			return orig(r)
		}
	})
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (%q)", run.Status, run.Error)
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 1 {
		t.Fatalf("rsync invocations = %d, want 1 (one batch)", got)
	}

	// Count saves that newly introduce a running task relative to the
	// previous save — exactly one, however many files the batch holds.
	runningIntents := 0
	for i, snap := range snapshots {
		if i == 0 {
			continue
		}
		prev := snapshots[i-1]
		for j := range snap {
			if snap[j] == state.StatusRunning && prev[j] != state.StatusRunning {
				runningIntents++
				break
			}
		}
	}
	if runningIntents != 1 {
		t.Errorf("running-intent saves = %d, want exactly 1 (snapshots: %v)", runningIntents, snapshots)
	}
}

// TestNativeStageCancellationDuringBatchPreparation: a 100-file batch is
// cancelled exactly when its running intent is persisted. Regression: the
// old per-task loop would have kept saving each file's state and only then
// hit rsync with a dead context — the fix must do a constant number of
// saves (initial + batch running + batch revert + interruption record) and
// start no process.
func TestNativeStageCancellationDuringBatchPreparation(t *testing.T) {
	h := newHarness(t)
	var names []string
	var recs []string
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("f%03d.txt", i)
		h.writeSrc(name, 10)
		names = append(names, name)
		recs = append(recs, h.newFileRecord(name))
	}
	post := h.writeManifest("post", manifestContent(recs...))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	saves := 0
	var once sync.Once
	run, err := syncWith(ctx, cfg, func(e *Executor) {
		orig := e.saveFn
		e.saveFn = func(r *state.Run) error {
			running := false
			for i := range r.Tasks {
				if r.Tasks[i].Status == state.StatusRunning {
					running = true
				}
			}
			mu.Lock()
			saves++
			mu.Unlock()
			if running {
				// The batch's running intent just became durable: cancel
				// now, between the save and the rsync launch.
				once.Do(cancel)
			}
			return orig(r)
		}
	})
	if run == nil {
		t.Fatal("Sync returned no run")
	}
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Sync = %v, want an error wrapping context.Canceled", err)
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 0 {
		t.Errorf("rsync invocations = %d, want 0 (cancelled before launch)", got)
	}
	mu.Lock()
	n := saves
	mu.Unlock()
	// Exactly: initial save, batch running intent, batch revert to pending,
	// interruption record. Independent of the 100-file batch size.
	if n != 4 {
		t.Errorf("saves = %d, want exactly 4 (constant, not file-count-dependent)", n)
	}
	for _, task := range run.Tasks {
		if task.Status == state.StatusRunning {
			t.Errorf("task %s stuck at running after cancellation", task.Path)
		}
		if task.Status != state.StatusPending {
			t.Errorf("task %s = %s, want pending", task.Path, task.Status)
		}
	}

	// The interrupted run stays resumable.
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, _, err := store.LoadAndRecover(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Resumable() {
		t.Error("cancelled-during-preparation run is not resumable")
	}
}

// TestNativeStageCancelsSiblingsAndSkipsUnstarted: with jobs=2 and four
// one-file batches, one in-flight batch fails deterministically (gated on
// both in-flight batches being live) while two batches have not started.
// The sibling is cancelled back to pending; the unstarted batches never
// mark anything or launch anything.
//
// Determinism: failme.txt is the only root-level file, so discovery emits
// it before descending into sub/, making it batch 0 without relying on
// directory iteration order.
func TestNativeStageCancelsSiblingsAndSkipsUnstarted(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("failme.txt", 600)
	h.writeSrc("sub/good1.txt", 600)
	h.writeSrc("sub/good2.txt", 600)
	h.writeSrc("sub/good3.txt", 600)
	// 600-byte files under a 1024-byte threshold: one file per batch.
	post := h.writeManifest("post", manifestContent(
		h.newFileRecord("failme.txt"), h.newFileRecord("sub/good1.txt"),
		h.newFileRecord("sub/good2.txt"), h.newFileRecord("sub/good3.txt")))
	h.scriptSSHFreshDest(post)
	// The failing batch waits for the gate so both in-flight batches are
	// live when it fails — no sleep timing anywhere.
	gate := filepath.Join(t.TempDir(), "gate")
	h.writeScript("rsync", "match_stdin=failme.txt\nwait_for="+gate+"\nexit=1\n---\nsleep_ms=30000\nexit=0\n")
	cfg := h.config()
	cfg.Jobs = 2

	start := time.Now()
	done := make(chan error, 1)
	runCh := make(chan *state.Run, 1)
	go func() {
		run, err := Sync(context.Background(), cfg)
		runCh <- run
		done <- err
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if got := len(toolInvocations(h.invocations(), "rsync")); got >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the two in-flight batches never started")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.WriteFile(gate, []byte("go\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := <-done
	run := <-runCh
	if err == nil {
		t.Fatal("Sync with a failing batch = nil, want error")
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Errorf("Sync took %s: in-flight sibling was not cancelled promptly", elapsed)
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 2 {
		t.Errorf("rsync invocations = %d, want exactly 2 (unstarted batches never launched)", got)
	}
	if run.Status != state.StatusFailed {
		t.Errorf("run status = %s, want failed", run.Status)
	}
	byPath := map[string]state.Task{}
	for _, task := range run.Tasks {
		byPath[task.Path] = task
	}
	if got := byPath["failme.txt"]; got.Status != state.StatusFailed || got.Attempts != 1 ||
		!strings.Contains(got.Error, "rsync batch failed") {
		t.Errorf("failing batch task = %+v", got)
	}
	var cancelled, untouched int
	for _, task := range run.Tasks {
		if task.Path == "failme.txt" {
			continue
		}
		switch {
		case task.Status == state.StatusPending && task.Attempts == 1 &&
			strings.Contains(task.Error, "cancelled: another small-file batch failed"):
			cancelled++
		case task.Status == state.StatusPending && task.Attempts == 0 && task.Error == "":
			untouched++
		default:
			t.Errorf("unexpected task state: %+v", task)
		}
	}
	if cancelled != 1 || untouched != 2 {
		t.Errorf("cancelled=%d untouched=%d, want 1 cancelled + 2 untouched", cancelled, untouched)
	}
	for _, inv := range toolInvocations(h.invocations(), "rsync") {
		if err := mockexec.ProcessAlive(inv.PID); err == nil {
			t.Errorf("mock rsync pid %d still alive after cancellation", inv.PID)
		}
	}
}

// TestNativeStageCancellationRevertSaveFailure: cancellation arrives between
// the batch's running intent and the rsync launch, and the revert save
// itself fails. The revert error aborts the batch without ever launching
// rsync, and because the revert is atomic the in-memory tasks roll back to
// running (interruption recovery then returns them to pending), leaving the
// durable state recoverable and resumable.
//
// Determinism: the save wrapper cancels the context inside the batch's
// running-intent save (#2) and fails exactly the next save (#3), which is
// the revert; the interruption record (#4) succeeds.
func TestNativeStageCancellationRevertSaveFailure(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("a.txt", 10)
	h.writeSrc("b.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("a.txt"), h.newFileRecord("b.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	saves := 0
	var once sync.Once
	run, err := syncWith(ctx, cfg, func(e *Executor) {
		orig := e.saveFn
		e.saveFn = func(r *state.Run) error {
			running := false
			for i := range r.Tasks {
				if r.Tasks[i].Status == state.StatusRunning {
					running = true
				}
			}
			mu.Lock()
			saves++
			n := saves
			mu.Unlock()
			if running {
				// The batch's running intent just became durable: cancel
				// now, between the save and the rsync launch.
				once.Do(cancel)
				return orig(r)
			}
			if n == 3 {
				// Save #3 is the cancelled batch's revert to pending:
				// inject the persistence failure exactly there.
				return errors.New("injected persistence failure")
			}
			return orig(r)
		}
	})
	if run == nil {
		t.Fatal("Sync returned no run")
	}
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Sync = %v, want an error wrapping context.Canceled", err)
	}
	// The revert aborted the batch before any subprocess.
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 0 {
		t.Errorf("rsync invocations = %d, want 0 (cancelled before launch)", got)
	}
	mu.Lock()
	n := saves
	mu.Unlock()
	// Exactly: initial save, batch running intent, failed batch revert,
	// interruption record.
	if n != 4 {
		t.Errorf("saves = %d, want exactly 4", n)
	}
	// After the rollback and the interruption recovery, no task is stuck at
	// running and every task is resumable pending with its one recorded
	// (rolled-back, then recovered) attempt.
	for _, task := range run.Tasks {
		if task.Status == state.StatusRunning {
			t.Errorf("task %s stuck at running after the failed revert", task.Path)
		}
		if task.Status != state.StatusPending {
			t.Errorf("task %s = %s, want pending", task.Path, task.Status)
		}
		if task.Attempts != 1 {
			t.Errorf("task %s attempts = %d, want 1 (running intent was recorded once)", task.Path, task.Attempts)
		}
	}

	// The durable state agrees and stays recoverable.
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, _, err := store.LoadAndRecover(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range loaded.Tasks {
		if task.Status != state.StatusPending {
			t.Errorf("durable task %s = %s, want pending", task.Path, task.Status)
		}
	}
	if !loaded.Resumable() {
		t.Error("run after failed cancellation revert is not resumable")
	}

	// Resume finishes the work: exactly one rsync launch overall.
	run2, err := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: cfg.Programs,
		Env:      cfg.Env,
	}, run.ID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if run2.Status != state.StatusCompleted {
		t.Fatalf("resumed run = %s (%q), want completed", run2.Status, run2.Error)
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 1 {
		t.Errorf("rsync invocations = %d, want exactly 1 (only the resumed batch)", got)
	}
}

// TestNativeLegacyCheckpointDeterministic: plant the legacy checkpoint at
// the exact path the executor will compute (per-run checkpoint dir derived
// from a fixed run layout is not known ahead), so instead plant it via the
// task's persisted Checkpoint field on a crafted interrupted run: the
// resume path must fail closed.
func TestNativeLegacyCheckpointDeterministic(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)

	// Craft an interrupted run whose large task points at a legacy
	// .mscpckpt checkpoint path.
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	size, mt := h.srcMeta("big.bin")
	id, err := state.NewID()
	if err != nil {
		t.Fatal(err)
	}
	legacyCkpt := filepath.Join(h.stateDir, "checkpoints", id, "legacy.mscpckpt")
	if err := os.MkdirAll(filepath.Dir(legacyCkpt), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacyCkpt, []byte("binary legacy mscp state"), 0o600); err != nil {
		t.Fatal(err)
	}
	run := &state.Run{
		ID:        id,
		Status:    state.StatusFailed,
		CreatedAt: time.Now().UTC(),
		Config: state.Config{
			Backend:        state.BackendAuto,
			Verify:         state.VerifyNone,
			Jobs:           1,
			Streams:        2,
			LargeThreshold: 1024,
		},
		Source: state.Source{Path: h.src},
		Destination: state.Destination{
			User: "u", Host: "h", Path: h.remoteDir,
		},
		Tasks: []state.Task{{
			Path:       "big.bin",
			Size:       size,
			ModTime:    mt,
			Backend:    state.BackendMSCP,
			Status:     state.StatusPending,
			Checkpoint: legacyCkpt,
		}},
		WorkDir: h.src,
	}
	if err := store.Save(run); err != nil {
		t.Fatal(err)
	}

	_, err = Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: h.config().Programs,
		Env:      h.nativeEnv(),
	}, run.ID)
	if err == nil {
		t.Fatal("Resume with a legacy checkpoint = nil, want a fail-closed error")
	}
	loaded, err := store.Load(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := loaded.Tasks[0].Status; got != state.StatusFailed {
		t.Errorf("task = %s, want failed (fail closed)", got)
	}
	if !strings.Contains(loaded.Tasks[0].Error, "legacy external-mscp") {
		t.Errorf("task error = %q, want the legacy checkpoint message", loaded.Tasks[0].Error)
	}
	// The destination was never touched: no final, no temp.
	entries, derr := os.ReadDir(h.remoteDir)
	if derr != nil {
		t.Fatal(derr)
	}
	for _, e := range entries {
		t.Errorf("destination touched despite legacy fail-closed: %s", e.Name())
	}
	// The legacy checkpoint itself is preserved.
	if _, err := os.Stat(legacyCkpt); err != nil {
		t.Errorf("legacy checkpoint removed: %v", err)
	}
}

// TestNativeHardlinkUnsupportedAutoFallback: a server without the
// hardlink@openssh.com extension makes the native stage re-route auto-mode
// large files to the conservative rsync path; no temp is created and no
// final is published natively.
func TestNativeHardlinkUnsupportedAutoFallback(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("big.bin")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")

	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	cfg.Env = h.nativeEnv(mockexec.EnvSFTPNoExt + "=1")
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run = %s (%q), want completed via rsync fallback", run.Status, run.Error)
	}
	task := run.Tasks[0]
	if task.Backend != state.BackendRsync {
		t.Errorf("task backend = %s, want rerouted to rsync", task.Backend)
	}
	// rsync owned the file; the native stage wrote nothing.
	rs := toolInvocations(h.invocations(), "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1", len(rs))
	}
	if got := stdinPaths(t, rs[0]); len(got) != 1 || got[0] != "big.bin" {
		t.Errorf("rsync stdin = %v, want [big.bin]", got)
	}
	// Only the probe session ran; no transfer sessions.
	sessions := sftpSessionInvocations(h.invocations())
	if len(sessions) > 1 {
		t.Errorf("sftp sessions = %d, want at most the probe", len(sessions))
	}
	entries, derr := os.ReadDir(h.remoteDir)
	if derr != nil {
		t.Fatal(derr)
	}
	for _, e := range entries {
		t.Errorf("native stage created remote state despite fallback: %s", e.Name())
	}
}

// TestNativeHardlinkUnsupportedForcedFails: forced --backend mscp with a
// server lacking hardlink support fails with a clear error, preserving the
// checkpoint for a future resume.
func TestNativeHardlinkUnsupportedForcedFails(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("big.bin")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")

	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	cfg.Backend = state.BackendMSCP
	cfg.Env = h.nativeEnv(mockexec.EnvSFTPNoExt + "=1")
	run, err := Sync(context.Background(), cfg)
	if err == nil {
		t.Fatal("Sync = nil, want the hardlink-unsupported error")
	}
	task := run.Tasks[0]
	if task.Status != state.StatusFailed || !strings.Contains(task.Error, "hardlink@openssh.com") {
		t.Errorf("task = %s (%q), want failed with the hardlink error", task.Status, task.Error)
	}
	// No rsync fallback for a forced run.
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 0 {
		t.Errorf("rsync invocations = %d, want 0 (forced native)", got)
	}
}

// TestRemoteTempCleanupFailureKeepsFinal: a failing remote temp removal is
// a warning only — the published final stays intact and the run is already
// complete.
func TestRemoteTempCleanupFailureKeepsFinal(t *testing.T) {
	h, cfg := mscpOnlyHarness(t)
	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	final := filepath.Join(h.remoteDir, "big.bin")
	want, err := os.ReadFile(final)
	if err != nil {
		t.Fatalf("published final missing: %v", err)
	}

	var stderr bytes.Buffer
	e, err := newExecutor(Config{
		StateDir: h.stateDir,
		Programs: cfg.Programs,
		Env:      h.nativeEnv(),
		Stderr:   &stderr,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.run = run
	// A path that cannot be removed: the helper reports failure, the
	// cleanup logs and returns, and the final is untouched.
	e.removeRemoteTemp(context.Background(), filepath.Join(h.remoteDir, "no-such-dir", "no-such-temp"))
	if !strings.Contains(stderr.String(), "remove remote temp") {
		t.Errorf("stderr = %q, want a cleanup warning", stderr.String())
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatalf("final disappeared after a cleanup failure: %v", err)
	}
	if string(got) != string(want) {
		t.Error("final content changed after a cleanup failure")
	}
}

// craftNativeRun persists an interrupted run with one large pending task
// bound to the given checkpoint path.
func craftNativeRun(t *testing.T, h *harness, ckpt string) *state.Run {
	t.Helper()
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	size, mt := h.srcMeta("big.bin")
	id, err := state.NewID()
	if err != nil {
		t.Fatal(err)
	}
	run := &state.Run{
		ID:        id,
		Status:    state.StatusFailed,
		CreatedAt: time.Now().UTC(),
		Config: state.Config{
			Backend:        state.BackendAuto,
			Verify:         state.VerifyNone,
			Jobs:           1,
			Streams:        2,
			LargeThreshold: 1024,
		},
		Source:      state.Source{Path: h.src},
		Destination: state.Destination{User: "u", Host: "h", Path: h.remoteDir},
		WorkDir:     h.src,
		Tasks: []state.Task{{
			Path:       "big.bin",
			Size:       size,
			ModTime:    mt,
			Backend:    state.BackendMSCP,
			Status:     state.StatusPending,
			Checkpoint: ckpt,
		}},
	}
	if err := store.Save(run); err != nil {
		t.Fatal(err)
	}
	return run
}

// TestNativeResumeWithCheckpointFailsClosedWithoutHardlink: a task with an
// existing native checkpoint must never fall back to rsync when the server
// lacks hardlink support — rsync would bypass the no-clobber publish.
func TestNativeResumeWithCheckpointFailsClosedWithoutHardlink(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)
	remoteFinal := filepath.Join(h.remoteDir, "big.bin")
	tempPath := filepath.Join(h.remoteDir, ".catsync-tmp-big.bin")
	// A partial temp object and a real native checkpoint, as an interrupted
	// transfer leaves them.
	if err := os.WriteFile(tempPath, make([]byte, 2048), 0o600); err != nil {
		t.Fatal(err)
	}
	ckptPath := filepath.Join(h.stateDir, "checkpoints", "crafted", "abc.nativeckpt")
	if err := os.MkdirAll(filepath.Dir(ckptPath), 0o700); err != nil {
		t.Fatal(err)
	}
	ckpt, err := multistream.NewCheckpoint("big.bin", 2048, time.Unix(1_700_000_000, 0),
		"u@h:"+remoteFinal, tempPath, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err := ckpt.Save(ckptPath); err != nil {
		t.Fatal(err)
	}
	run := craftNativeRun(t, h, ckptPath)

	cfg := h.nativeEnv(mockexec.EnvSFTPNoExt + "=1")
	_, rerr := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: h.config().Programs,
		Env:      cfg,
	}, run.ID)
	if rerr == nil {
		t.Fatal("Resume = nil, want a fail-closed error")
	}
	// No rsync may run, and the backend must not change.
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 0 {
		t.Errorf("rsync invocations = %d, want 0 (fail closed, no fallback)", got)
	}
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	task := loaded.Tasks[0]
	if task.Backend != state.BackendMSCP {
		t.Errorf("backend = %s, want mscp unchanged (no fallback)", task.Backend)
	}
	if task.Status != state.StatusFailed || !strings.Contains(task.Error, "already started") {
		t.Errorf("task = %s (%q), want failed with the already-started message", task.Status, task.Error)
	}
	// Checkpoint, temp, and final are untouched.
	if _, err := os.Stat(ckptPath); err != nil {
		t.Errorf("native checkpoint removed: %v", err)
	}
	if _, err := os.Stat(tempPath); err != nil {
		t.Errorf("remote temp removed: %v", err)
	}
	if _, err := os.Lstat(remoteFinal); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("final path was touched: %v", err)
	}
}

// TestNativeResumePublishedCrashWindowProbeFailure: when the hardlink probe
// itself errors (not "unsupported"), a published crash-window transfer must
// not fall back and must not overwrite the live final.
func TestNativeResumePublishedCrashWindowProbeFailure(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("big.bin")))
	h.scriptSSHFreshDest(post)

	// A completed, published native transfer: final + temp + full checkpoint.
	cfg := h.configNative()
	cfg.Verify = state.VerifyNone
	if _, err := Sync(context.Background(), cfg); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	final := filepath.Join(h.remoteDir, "big.bin")
	want, err := os.ReadFile(final)
	if err != nil {
		t.Fatalf("published final missing: %v", err)
	}
	// Find the checkpoint the run recorded.
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Latest()
	if err != nil {
		t.Fatal(err)
	}
	crafted := craftNativeRun(t, h, first.Tasks[0].Checkpoint)
	_ = first.Tasks[0].Checkpoint

	// The probe session now fails: not "unsupported", a real error.
	env := h.nativeEnv("MOCKEXEC_SFTP_FAILNTH=1")
	_, rerr := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: h.config().Programs,
		Env:      env,
	}, crafted.ID)
	if rerr == nil {
		t.Fatal("Resume = nil, want the probe failure")
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 0 {
		t.Errorf("rsync invocations = %d, want 0 (a probe error never falls back)", got)
	}
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatalf("published final disappeared: %v", err)
	}
	if string(got) != string(want) {
		t.Error("published final was modified by the failed resume")
	}
}

// TestNativeResumeLegacyCheckpointWithoutHardlink: a legacy checkpoint with
// no hardlink support still reports the actionable legacy message and never
// falls back.
func TestNativeResumeLegacyCheckpointWithoutHardlink(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)
	legacy := filepath.Join(h.stateDir, "checkpoints", "crafted", "legacy.mscpckpt")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacy, []byte("binary legacy state"), 0o600); err != nil {
		t.Fatal(err)
	}
	run := craftNativeRun(t, h, "")
	// The derived legacy path is what the stage looks for; place the file
	// where the executor derives it.
	ckptDir, err := filepath.Abs(filepath.Join(h.stateDir, "checkpoints", run.ID))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(ckptDir, 0o700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("big.bin"))
	derived := filepath.Join(ckptDir, hex.EncodeToString(sum[:])+".mscpckpt")
	if err := os.WriteFile(derived, []byte("binary legacy state"), 0o600); err != nil {
		t.Fatal(err)
	}

	env := h.nativeEnv(mockexec.EnvSFTPNoExt + "=1")
	_, rerr := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: h.config().Programs,
		Env:      env,
	}, run.ID)
	if rerr == nil {
		t.Fatal("Resume = nil, want the legacy fail-closed error")
	}
	if got := len(toolInvocations(h.invocations(), "rsync")); got != 0 {
		t.Errorf("rsync invocations = %d, want 0 (legacy checkpoint must fail closed)", got)
	}
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(loaded.Tasks[0].Error, "legacy external-mscp") {
		t.Errorf("task error = %q, want the legacy checkpoint message", loaded.Tasks[0].Error)
	}
	if _, err := os.Stat(derived); err != nil {
		t.Errorf("legacy checkpoint removed: %v", err)
	}
}
