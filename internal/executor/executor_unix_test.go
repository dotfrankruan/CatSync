//go:build unix

package executor

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/frankruan/catsync/internal/state"
	"github.com/frankruan/catsync/internal/target"
	"github.com/frankruan/catsync/internal/testutil/mockexec"
)

func TestMain(m *testing.M) {
	if mockexec.HelperActive() {
		os.Exit(mockexec.RunHelper())
	}
	os.Exit(m.Run())
}

// harness wires a source tree, a state directory, and the mock executables
// (ssh, rsync, fpsync, fpart, mscp) in script mode.
type harness struct {
	t         *testing.T
	src       string
	stateDir  string
	scriptDir string
	logPath   string
	progs     map[string]string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	binDir := t.TempDir()
	progs, err := mockexec.LinkPrograms(binDir, "ssh", "rsync", "fpsync", "fpart", "mscp")
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
			SSH:    h.progs["ssh"],
			Rsync:  h.progs["rsync"],
			FPSync: h.progs["fpsync"],
			FPart:  h.progs["fpart"],
			MSCP:   h.progs["mscp"],
		},
		Env: mockexec.EnvScript(h.scriptDir, h.logPath),
	}
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
	return fmt.Sprintf("%s\x00%d\x00%d.%09d\x00", path, size, mt.Unix(), int64(mt.Nanosecond()))
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

// fpsyncOutputFile writes a verbose fpsync output carrying a run ID and the
// given S records (skipped large files).
func (h *harness) fpsyncOutputFile(name string, skipped ...string) string {
	var b strings.Builder
	b.WriteString("1792333896 Info: Fpsync started (pid: 74154)\n")
	b.WriteString("1792333896 Info: Run ID: 1792333896-74154\n")
	for _, s := range skipped {
		b.WriteString(s + "\n")
	}
	b.WriteString("1792333897 Info: Fpsync stopped (with success)\n")
	return h.writeManifest(name, b.String())
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
	h.writeScript("fpsync", "stdout_file="+h.fpsyncOutputFile("fpsync.out", "S\t2048\t./big.bin"))
	h.writeScript("mscp", "touch_flag=-W")

	run, err := Sync(context.Background(), h.config())
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
	if run.FPSyncRunID != "1792333896-74154" {
		t.Errorf("fpsync run id = %q", run.FPSyncRunID)
	}

	invs := h.invocations()
	fps := toolInvocations(invs, "fpsync")
	if len(fps) != 1 {
		t.Fatalf("fpsync invocations = %d, want 1", len(fps))
	}
	joined := strings.Join(fps[0].Args, "\x00")
	for _, want := range []string{"-n\x002", "-f", "-s\x001024", "-O\x00-S", "/data/"} {
		if !strings.Contains(joined, want) {
			t.Errorf("fpsync argv missing %q: %v", want, fps[0].Args)
		}
	}
	// The last two argv elements are the source and destination operands.
	if n := len(fps[0].Args); fps[0].Args[n-1] != "u@h:/data/" || !strings.HasSuffix(fps[0].Args[n-2], "/") {
		t.Errorf("fpsync operands = %v", fps[0].Args[n-2:])
	}

	ms := toolInvocations(invs, "mscp")
	if len(ms) != 1 {
		t.Fatalf("mscp invocations = %d, want 1", len(ms))
	}
	mj := strings.Join(ms[0].Args, "\x00")
	if !strings.Contains(mj, "-W\x00") || !strings.Contains(mj, "--\x00big.bin\x00u@h:/data/big.bin") {
		t.Errorf("mscp argv = %v", ms[0].Args)
	}
	if rs := toolInvocations(invs, "rsync"); len(rs) != 0 {
		t.Errorf("rsync invocations = %d, want 0 (no rsync-owned files)", len(rs))
	}
	if ssh := toolInvocations(invs, "ssh"); len(ssh) != 2 {
		t.Errorf("ssh invocations = %d, want 2 (plan manifest + verify manifest)", len(ssh))
	}
	// The completed mscp task's checkpoint is retired by the gate.
	for _, task := range run.Tasks {
		if task.Checkpoint != "" {
			t.Errorf("task %s retains checkpoint %s after completion", task.Path, task.Checkpoint)
		}
	}
	ckptDir := filepath.Join(h.stateDir, "checkpoints", run.ID)
	entries, _ := os.ReadDir(ckptDir)
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
---
match=mkdir -p
exit=0
`)
	h.writeScript("fpsync", "stdout_file="+h.fpsyncOutputFile("fpsync.out", "S\t2048\t./big-new.bin", "S\t4096\t./big-mod.bin"))
	h.writeScript("mscp", "touch_flag=-W")
	h.writeScript("rsync", "exit=0")

	run, err := Sync(context.Background(), h.config())
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
		t.Errorf("small new file backend = %s, want fpsync", got)
	}
	if got := byPath["big-new.bin"].Backend; got != state.BackendMSCP {
		t.Errorf("large new file backend = %s, want mscp", got)
	}
	if got := byPath["big-mod.bin"].Backend; got != state.BackendRsync {
		t.Errorf("modified file backend = %s, want rsync", got)
	}

	invs := h.invocations()
	rs := toolInvocations(invs, "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1", len(rs))
	}
	if got := stdinPaths(t, rs[0]); len(got) != 1 || got[0] != "big-mod.bin" {
		t.Errorf("rsync stdin paths = %v, want exactly [big-mod.bin] (single writer)", got)
	}
	if strings.Contains(strings.Join(rs[0].Args, "\x00"), "--delete") {
		t.Errorf("rsync argv contains --delete: %v", rs[0].Args)
	}
	if ms := toolInvocations(invs, "mscp"); len(ms) != 1 {
		t.Errorf("mscp invocations = %d, want 1", len(ms))
	}
	// ssh: pre manifest, mkdir (big files live at root? no subdirs here, so
	// no mkdir), post manifest.
	if ssh := toolInvocations(invs, "ssh"); len(ssh) != 2 {
		t.Errorf("ssh invocations = %d, want 2", len(ssh))
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

func TestSyncMissingFPSyncFallsBackToRsync(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()
	cfg.Programs.FPSync = filepath.Join(t.TempDir(), "no-fpsync")
	cfg.Programs.FPart = cfg.Programs.FPSync

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (error %q)", run.Status, run.Error)
	}
	invs := h.invocations()
	if len(toolInvocations(invs, "fpsync")) != 0 {
		t.Error("fpsync ran despite being unavailable")
	}
	rs := toolInvocations(invs, "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1", len(rs))
	}
	if got := stdinPaths(t, rs[0]); len(got) != 1 || got[0] != "small.txt" {
		t.Errorf("rsync paths = %v, want [small.txt]", got)
	}
}

func TestSyncForcedUnavailableBackendFails(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	cfg := h.config()
	cfg.Backend = state.BackendFPSync
	cfg.Programs.FPSync = filepath.Join(t.TempDir(), "no-fpsync")

	if _, err := Sync(context.Background(), cfg); err == nil {
		t.Fatal("forced fpsync without fpsync = nil, want error")
	}
	if len(toolInvocations(h.invocations(), "fpsync")) != 0 {
		t.Error("fpsync ran despite being unavailable")
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
	h.writeScript("mscp", "touch_flag=-W")

	run, err := Sync(context.Background(), h.config())
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
}

func TestResumeReusesCheckpointWithDashR(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("big.bin")))
	h.scriptSSHFreshDest(post)
	h.writeScript("mscp", "nth=1\ntouch_flag=-W\nexit=1\n---\nexit=0\n")

	run, err := Sync(context.Background(), h.config())
	if err == nil {
		t.Fatal("Sync with failing mscp = nil, want error")
	}
	task := run.Tasks[0]
	if task.Status != state.StatusFailed || task.Checkpoint == "" {
		t.Fatalf("task = %s checkpoint %q", task.Status, task.Checkpoint)
	}
	if _, err := os.Stat(task.Checkpoint); err != nil {
		t.Fatalf("checkpoint must survive the failure: %v", err)
	}

	run2, err := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: h.config().Programs,
		Env:      mockexec.EnvScript(h.scriptDir, h.logPath),
	}, run.ID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if run2.Status != state.StatusCompleted {
		t.Errorf("resumed run = %s (%q)", run2.Status, run2.Error)
	}
	ms := toolInvocations(h.invocations(), "mscp")
	if len(ms) != 2 {
		t.Fatalf("mscp invocations = %d, want 2", len(ms))
	}
	resumeArgs := ms[1].Args
	if len(resumeArgs) != 2 || resumeArgs[0] != "-R" || resumeArgs[1] != task.Checkpoint {
		t.Errorf("resume argv = %v, want exactly [-R %s]", resumeArgs, task.Checkpoint)
	}
	// Resume runs from the original working directory.
	wantDir, err := filepath.EvalSymlinks(h.src)
	if err != nil {
		t.Fatal(err)
	}
	if ms[1].Dir != wantDir {
		t.Errorf("resume cwd = %q, want %q", ms[1].Dir, wantDir)
	}
	// The completed resume retires the checkpoint.
	if _, err := os.Stat(task.Checkpoint); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("checkpoint must be retired after completion: %v", err)
	}
}

func TestResumeWithoutCheckpointRestartsWithDashW(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("big.bin")))
	h.scriptSSHFreshDest(post)
	// The failing first run does not even create its checkpoint.
	h.writeScript("mscp", "nth=1\nexit=1\n---\nexit=0\n")

	run, err := Sync(context.Background(), h.config())
	if err == nil {
		t.Fatal("Sync with failing mscp = nil, want error")
	}
	ckpt := run.Tasks[0].Checkpoint
	if ckpt == "" {
		t.Fatal("no checkpoint recorded")
	}
	if _, err := os.Stat(ckpt); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("checkpoint should not exist: %v", err)
	}
	if _, err := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: h.config().Programs,
		Env:      mockexec.EnvScript(h.scriptDir, h.logPath),
	}, run.ID); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	ms := toolInvocations(h.invocations(), "mscp")
	if len(ms) != 2 {
		t.Fatalf("mscp invocations = %d, want 2", len(ms))
	}
	joined := strings.Join(ms[1].Args, "\x00")
	if !strings.Contains(joined, "-W\x00") || strings.Contains(joined, "-R\x00") {
		t.Errorf("resume without checkpoint must restart with -W: %v", ms[1].Args)
	}
}

func TestFPSyncResumeByRunID(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	resumed := h.writeManifest("resumed.out", "1792333911 Info: Run ID: 1792333896-74154 (resumed)\n1792333912 Info: Fpsync stopped (with success)\n")
	h.writeScript("fpsync", "nth=1\nexit=1\nstdout_file="+h.fpsyncOutputFile("fpsync.out")+"\n---\nexit=0\nstdout_file="+resumed+"\n")

	run, err := Sync(context.Background(), h.config())
	if err == nil {
		t.Fatal("Sync with failing fpsync = nil, want error")
	}
	if run.FPSyncRunID != "1792333896-74154" {
		t.Fatalf("fpsync run id = %q", run.FPSyncRunID)
	}
	if run.Tasks[0].Status != state.StatusFailed {
		t.Fatalf("task = %s (%q)", run.Tasks[0].Status, run.Tasks[0].Error)
	}

	run2, err := Resume(context.Background(), Config{
		StateDir: h.stateDir,
		Programs: h.config().Programs,
		Env:      mockexec.EnvScript(h.scriptDir, h.logPath),
	}, run.ID)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if run2.Status != state.StatusCompleted {
		t.Errorf("resumed run = %s (%q)", run2.Status, run2.Error)
	}
	fps := toolInvocations(h.invocations(), "fpsync")
	if len(fps) != 2 {
		t.Fatalf("fpsync invocations = %d, want 2", len(fps))
	}
	joined := strings.Join(fps[1].Args, "\x00")
	if !strings.Contains(joined, "-r\x001792333896-74154") {
		t.Errorf("resume argv missing -r runid: %v", fps[1].Args)
	}
	for _, a := range fps[1].Args {
		if strings.Contains(a, "u@h") || a == "-O" {
			t.Errorf("resume argv carries initial-run token %q: %v", a, fps[1].Args)
		}
	}
}

func TestFPSyncOrphanSkipFails(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	// fpsync reports skipping a file the plan never saw: no backend owns
	// it, so the run must fail loudly instead of silently dropping data.
	h.writeScript("fpsync", "stdout_file="+h.fpsyncOutputFile("fpsync.out", "S\t9999\t./surprise.bin"))

	run, err := Sync(context.Background(), h.config())
	if err == nil {
		t.Fatal("Sync with orphan skip = nil, want error")
	}
	if run.Status != state.StatusFailed {
		t.Errorf("run status = %s, want failed", run.Status)
	}
	if !strings.Contains(run.Error, "no owning backend") {
		t.Errorf("run error = %q, want the orphan-skip explanation", run.Error)
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
	h.writeScript("fpsync", "stdout_file="+h.fpsyncOutputFile("fpsync.out",
		"S\t2048\t./big dir/-large.bin", "S\t4096\t./-big.bin"))
	h.writeScript("mscp", "touch_flag=-W")

	run, err := Sync(context.Background(), h.config())
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (error %q)", run.Status, run.Error)
	}
	invs := h.invocations()
	ms := toolInvocations(invs, "mscp")
	if len(ms) != 2 {
		t.Fatalf("mscp invocations = %d, want 2", len(ms))
	}
	operands := map[string]bool{}
	for _, inv := range ms {
		operands[inv.Args[len(inv.Args)-2]] = true
	}
	if !operands["./-big.bin"] {
		t.Errorf("dash-leading large file operand missing './' prefix: %v", operands)
	}
	if !operands["big dir/-large.bin"] {
		t.Errorf("space-containing large file operand corrupted: %v", operands)
	}
	// The remote parent directory of "big dir/-large.bin" was created with
	// one batched, quoted mkdir.
	var mkdirs []mockexec.Invocation
	for _, inv := range toolInvocations(invs, "ssh") {
		if strings.Contains(strings.Join(inv.Args, "\x00"), "mkdir -p") {
			mkdirs = append(mkdirs, inv)
		}
	}
	if len(mkdirs) != 1 {
		t.Fatalf("mkdir ssh calls = %d, want 1", len(mkdirs))
	}
	if !strings.Contains(mkdirs[0].Args[len(mkdirs[0].Args)-1], "'/data/big dir'") {
		t.Errorf("mkdir script does not quote the space directory: %q", mkdirs[0].Args[len(mkdirs[0].Args)-1])
	}
}

func TestSyncSymlinkAndSpecialAreExplicitlyReported(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("regular.txt", 10)
	if err := os.Symlink("regular.txt", filepath.Join(h.src, "link.txt")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(h.src, "fifo"), 0o644); err != nil {
		t.Fatal(err)
	}
	post := h.writeManifest("post", manifestContent(h.newFileRecord("regular.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()
	cfg.Backend = state.BackendRsync

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (error %q)", run.Status, run.Error)
	}
	reported := map[string]string{}
	for _, task := range run.Tasks {
		reported[task.Path] = task.Error
	}
	if !strings.Contains(reported["link.txt"], "symlink") {
		t.Errorf("symlink task error = %q, want an explicit symlink note", reported["link.txt"])
	}
	if !strings.Contains(reported["fifo"], "special") {
		t.Errorf("fifo task error = %q, want an explicit special-file note", reported["fifo"])
	}
	// Only the regular file reaches rsync.
	rs := toolInvocations(h.invocations(), "rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1", len(rs))
	}
	if got := stdinPaths(t, rs[0]); len(got) != 1 || got[0] != "regular.txt" {
		t.Errorf("rsync paths = %v, want [regular.txt]", got)
	}
}

func TestSyncDestinationLockContention(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)

	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	lock, err := store.LockDestination(state.Destination{User: "u", Host: "h", Path: "/data"})
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()

	_, err = Sync(context.Background(), h.config())
	if !errors.Is(err, state.ErrLocked) {
		t.Fatalf("Sync with held lock = %v, want ErrLocked", err)
	}
	// Nothing but the lock attempt happened.
	if got := len(h.invocations()); got != 0 {
		t.Errorf("invocations = %d, want 0", got)
	}
}

// TestSyncVerifyNoneSkipsRemoteCheck: with --verify none, tasks complete
// straight after their stage and no verification manifest is collected.
func TestSyncVerifyNoneSkipsRemoteCheck(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", manifestContent(h.newFileRecord("small.txt")))
	h.scriptSSHFreshDest(post)
	h.writeScript("rsync", "exit=0")
	cfg := h.config()
	cfg.Backend = state.BackendRsync
	cfg.Verify = state.VerifyNone

	run, err := Sync(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if run.Status != state.StatusCompleted {
		t.Fatalf("run status = %s (error %q)", run.Status, run.Error)
	}
	if run.Tasks[0].Status != state.StatusCompleted {
		t.Errorf("task = %s, want completed", run.Tasks[0].Status)
	}
	if got := len(toolInvocations(h.invocations(), "ssh")); got != 1 {
		t.Errorf("ssh invocations = %d, want 1 (plan manifest only; no verification call)", got)
	}
}

// TestSyncSHA256Refused: the sha256 mode fails fast and honestly instead of
// silently degrading to a size check.
func TestSyncSHA256Refused(t *testing.T) {
	h := newHarness(t)
	h.writeSrc("small.txt", 10)
	h.scriptSSHFreshDest(h.writeManifest("post", ""))
	cfg := h.config()
	cfg.Verify = state.VerifySHA256
	_, err := Sync(context.Background(), cfg)
	if !errors.Is(err, ErrVerifyNotImplemented) {
		t.Fatalf("Sync sha256 = %v, want ErrVerifyNotImplemented", err)
	}
	if got := len(h.invocations()); got != 0 {
		t.Errorf("invocations = %d, want 0 (fail fast before any transfer)", got)
	}
}
