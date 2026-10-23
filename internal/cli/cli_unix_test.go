//go:build unix

package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/frankruan/catsync/internal/state"
	"github.com/frankruan/catsync/internal/testutil/mockexec"
)

func TestMain(m *testing.M) {
	if mockexec.HelperActive() {
		os.Exit(mockexec.RunHelper())
	}
	os.Exit(m.Run())
}

// cliHarness wires the mock executables into the CATSYNC_* environment the
// CLI reads, with a script directory and invocation log. remoteDir is the
// real local directory acting as the remote destination for native
// transfers through the real sftp-server.
type cliHarness struct {
	t         *testing.T
	src       string
	stateDir  string
	scriptDir string
	logPath   string
	progs     map[string]string
	remoteDir string
}

func newCLIHarness(t *testing.T) *cliHarness {
	t.Helper()
	binDir := t.TempDir()
	progs, err := mockexec.LinkPrograms(binDir, "ssh", "rsync", "mscp")
	if err != nil {
		t.Fatal(err)
	}
	h := &cliHarness{
		t:         t,
		src:       t.TempDir(),
		stateDir:  t.TempDir(),
		scriptDir: t.TempDir(),
		logPath:   filepath.Join(t.TempDir(), "invocations.jsonl"),
		progs:     progs,
		remoteDir: t.TempDir(),
	}
	for tool, path := range progs {
		t.Setenv("CATSYNC_"+strings.ToUpper(tool), path)
	}
	// The CLI passes no Env additions, so children inherit this process'
	// environment: arm the mock through it.
	t.Setenv(mockexec.EnvMarker, "1")
	t.Setenv(mockexec.EnvScriptDir, h.scriptDir)
	t.Setenv(mockexec.EnvInvocationLog, h.logPath)
	return h
}

// armNative enables the mock ssh's real-remote emulation (SFTP subsystem,
// controlled metadata commands) against h.remoteDir.
func (h *cliHarness) armNative() {
	h.t.Helper()
	recDir := filepath.Join(h.stateDir, "record")
	if err := os.MkdirAll(recDir, 0o755); err != nil {
		h.t.Fatal(err)
	}
	h.t.Setenv(mockexec.EnvSSHNative, "1")
	h.t.Setenv(mockexec.EnvRecordDir, recDir)
}

func (h *cliHarness) writeScript(tool, config string) {
	h.t.Helper()
	dir := filepath.Join(h.scriptDir, tool)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "config"), []byte(config), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func (h *cliHarness) writeSrc(rel string, size int) {
	h.t.Helper()
	p := filepath.Join(h.src, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
		h.t.Fatal(err)
	}
}

func (h *cliHarness) writeManifest(name, content string) string {
	h.t.Helper()
	p := filepath.Join(h.t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		h.t.Fatal(err)
	}
	return p
}

func (h *cliHarness) record(rel string) string {
	h.t.Helper()
	fi, err := os.Stat(filepath.Join(h.src, filepath.FromSlash(rel)))
	if err != nil {
		h.t.Fatal(err)
	}
	return fmt.Sprintf("%s\x00%d\x00%d.%09d\x00", rel, fi.Size(), fi.ModTime().Unix(), int64(fi.ModTime().Nanosecond()))
}

func (h *cliHarness) invocations(tool string) []mockexec.Invocation {
	h.t.Helper()
	invs, err := mockexec.ReadInvocations(h.logPath)
	if err != nil {
		h.t.Fatal(err)
	}
	var out []mockexec.Invocation
	for _, inv := range invs {
		if inv.Tool == tool {
			out = append(out, inv)
		}
	}
	return out
}

// scriptFreshDestSSH scripts a missing destination (first manifest exits
// 42), then the post-transfer manifest, and a successful mkdir.
func (h *cliHarness) scriptFreshDestSSH(postFile string) {
	h.writeScript("ssh", `match=if [ -d
nth=1
exit=42
---
match=if [ -d
stdout_file=`+postFile+`
---
match=mkdir -p
exit=0
`)
}

// TestCLISyncEndToEnd drives the whole CLI with mock tools: sync completes
// and status then reports the persisted run.
func TestCLISyncEndToEnd(t *testing.T) {
	h := newCLIHarness(t)
	h.armNative()
	h.writeSrc("small.txt", 10)
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", h.record("small.txt")+h.record("big.bin"))
	h.scriptFreshDestSSH(post)
	h.writeScript("rsync", "exit=0")

	code, stdout, stderr := run(t, "sync", "--state-dir", h.stateDir,
		"--large-threshold", "1KiB", h.src, "user@host:"+h.remoteDir)
	if code != ExitOK {
		t.Fatalf("sync exit = %d, stderr:\n%s", code, stderr)
	}
	if !strings.Contains(stdout, "run ") || !strings.Contains(stdout, "completed") {
		t.Errorf("sync stdout missing the run summary:\n%s", stdout)
	}
	if len(h.invocations("fpsync")) != 0 || len(h.invocations("fpart")) != 0 {
		t.Errorf("external fpsync/fpart invoked: fpsync=%d fpart=%d, want 0",
			len(h.invocations("fpsync")), len(h.invocations("fpart")))
	}
	if len(h.invocations("rsync")) != 1 {
		t.Errorf("invocations: rsync=%d, want 1", len(h.invocations("rsync")))
	}
	// No external mscp; the large file went through the native backend and
	// really landed at the destination.
	if got := len(h.invocations("mscp")); got != 0 {
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

	code, stdout, stderr = run(t, "status", "--state-dir", h.stateDir)
	if code != ExitOK {
		t.Fatalf("status exit = %d, stderr:\n%s", code, stderr)
	}
	for _, want := range []string{"completed", "native-small 1, native-large (mscp) 1, rsync 0", "user@host:" + h.remoteDir} {
		if !strings.Contains(stdout, want) {
			t.Errorf("status output missing %q:\n%s", want, stdout)
		}
	}
}

// TestCLIPlanPrintsSummary checks the plan command output shape and that
// plan transfers nothing.
func TestCLIPlanPrintsSummary(t *testing.T) {
	h := newCLIHarness(t)
	h.writeSrc("small.txt", 10)
	h.writeSrc("big.bin", 2048)
	pre := h.writeManifest("pre", h.record("small.txt")) // small.txt unchanged? sizes must match
	h.writeScript("ssh", "match=if [ -d\nstdout_file="+pre+"\n")

	code, stdout, stderr := run(t, "plan", "--state-dir", h.stateDir,
		"--large-threshold", "1KiB", h.src, "user@host:/data")
	if code != ExitOK {
		t.Fatalf("plan exit = %d, stderr:\n%s", code, stderr)
	}
	for _, want := range []string{
		"destination metadata: authoritative",
		"mscp (large new, native multistream): 1",
		"logical source bytes",
		"no data was transferred",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("plan output missing %q:\n%s", want, stdout)
		}
	}
	if got := len(h.invocations("rsync")) + len(h.invocations("fpsync")) + len(h.invocations("mscp")) + len(h.invocations("fpart")); got != 0 {
		t.Errorf("plan invoked %d transfer processes, want 0", got)
	}
}

// TestCLIPlanForcedBackendRefusal: a forced-fpsync plan over a large file
// is unsafe and must fail nonzero.
func TestCLIPlanForcedBackendRefusal(t *testing.T) {
	h := newCLIHarness(t)
	h.writeSrc("big.bin", 2048)
	pre := h.writeManifest("pre", "")
	h.writeScript("ssh", "match=if [ -d\nstdout_file="+pre+"\n")

	code, _, stderr := run(t, "plan", "--state-dir", h.stateDir, "--backend", "fpsync",
		"--large-threshold", "1KiB", h.src, "user@host:/data")
	if code == ExitOK {
		t.Fatalf("plan with unsafe forced backend exit = 0, want nonzero")
	}
	if !strings.Contains(stderr, "refuses") {
		t.Errorf("stderr should explain the refusal:\n%s", stderr)
	}
}

// TestCLISyncFailureExitCode: a failing transfer exits 1 and records the
// failed run for status/resume.
func TestCLISyncFailureExitCode(t *testing.T) {
	h := newCLIHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", h.record("small.txt"))
	h.scriptFreshDestSSH(post)
	h.writeScript("rsync", "exit=23")

	code, _, stderr := run(t, "sync", "--state-dir", h.stateDir, "--backend", "rsync",
		h.src, "user@host:/data")
	if code != ExitError {
		t.Fatalf("sync exit = %d, want %d", code, ExitError)
	}
	if !strings.Contains(stderr, "status 23") {
		t.Errorf("stderr should carry the rsync exit status:\n%s", stderr)
	}
	code, stdout, _ := run(t, "status", "--state-dir", h.stateDir)
	if code != ExitOK {
		t.Fatalf("status exit = %d", code)
	}
	if !strings.Contains(stdout, "failed") {
		t.Errorf("status should show the failed run:\n%s", stdout)
	}

	// Resume retries the failed task; the mock rsync still fails, so
	// resume exits nonzero and the run stays failed.
	code, _, _ = run(t, "resume", "--state-dir", h.stateDir)
	if code != ExitError {
		t.Errorf("resume with still-failing rsync exit = %d, want %d", code, ExitError)
	}
}

// TestCLIIPv6CommandShapes checks that --ipv6 reaches every tool: ssh -6,
// the rsync transport "ssh -6", and mscp -6.
func TestCLIIPv6CommandShapes(t *testing.T) {
	h := newCLIHarness(t)
	h.armNative()
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", h.record("big.bin"))
	h.scriptFreshDestSSH(post)

	code, _, stderr := run(t, "sync", "--state-dir", h.stateDir, "--ipv6", "--backend", "mscp",
		"--large-threshold", "1KiB", h.src, "user@[2001:db8::1]:"+h.remoteDir)
	if code != ExitOK {
		t.Fatalf("sync exit = %d, stderr:\n%s", code, stderr)
	}
	// Every ssh invocation — manifest, SFTP sessions, metadata commands —
	// carries the standalone -6 flag; no external mscp runs.
	if got := len(h.invocations("mscp")); got != 0 {
		t.Errorf("external mscp invoked %d times, want 0", got)
	}
	ssh := h.invocations("ssh")
	if len(ssh) == 0 {
		t.Fatal("no ssh invocations")
	}
	for _, inv := range ssh {
		if len(inv.Args) < 1 || inv.Args[0] != "-6" {
			t.Errorf("ssh argv missing -6: %v", inv.Args)
		}
	}
	// The SFTP session argv is exactly the native shape: ssh -6 -T -s
	// user@[2001:db8::1] sftp, the remote path never an operand.
	var sftpArgv []string
	for _, inv := range ssh {
		joined := strings.Join(inv.Args, "\x00")
		if strings.Contains(joined, "-s\x00user@2001:db8::1\x00sftp") {
			sftpArgv = inv.Args
			break
		}
	}
	if sftpArgv == nil {
		t.Fatalf("no native SFTP session invocation found: %v", ssh)
	}
	for _, a := range sftpArgv {
		if strings.Contains(a, h.remoteDir) {
			t.Errorf("SFTP argv carries the remote path: %v", sftpArgv)
		}
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
}

// TestCLIResumeAfterInterrupt covers the resume command path end to end: a
// run interrupted mid-transfer is continued and completed by resume.
func TestCLIResumeAfterInterrupt(t *testing.T) {
	h := newCLIHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", h.record("small.txt"))
	h.scriptFreshDestSSH(post)
	h.writeScript("rsync", "nth=1\nsleep_ms=30000\n---\nexit=0\n")

	done := make(chan int, 1)
	go func() {
		code, _, _ := run(t, "sync", "--state-dir", h.stateDir, "--backend", "rsync", h.src, "user@host:/data")
		done <- code
	}()
	// Wait until the mock rsync has actually started (its invocation is
	// recorded before it sleeps), then interrupt: the CLI's signal context
	// cancels the transfer.
	deadline := time.Now().Add(10 * time.Second)
	for len(h.invocations("rsync")) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("mock rsync never started")
		}
		time.Sleep(20 * time.Millisecond)
	}
	self, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := self.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	code := <-done
	if code != ExitError {
		t.Fatalf("interrupted sync exit = %d, want %d", code, ExitError)
	}

	// The second rsync invocation succeeds; resume completes the run.
	code, stdout, stderr := run(t, "resume", "--state-dir", h.stateDir)
	if code != ExitOK {
		t.Fatalf("resume exit = %d, stderr:\n%s", code, stderr)
	}
	if !strings.Contains(stdout, "completed") {
		t.Errorf("resume stdout missing completion:\n%s", stdout)
	}
	if got := len(h.invocations("rsync")); got != 2 {
		t.Errorf("rsync invocations = %d, want 2", got)
	}
}

// TestCLIUsageAndRuntimeExitCodes pins the exit-code contract.
func TestCLIUsageAndRuntimeExitCodes(t *testing.T) {
	h := newCLIHarness(t)
	h.writeSrc("small.txt", 10)

	// Usage: unknown flag.
	if code, _, _ := run(t, "sync", "--bogus", h.src, "u@h:/d"); code != ExitUsage {
		t.Errorf("unknown flag exit = %d, want %d", code, ExitUsage)
	}
	// Usage: jobs x streams over budget.
	if code, _, _ := run(t, "sync", "--jobs", "64", "--streams", "64", h.src, "u@h:/d"); code != ExitUsage {
		t.Errorf("over-budget exit = %d, want %d", code, ExitUsage)
	}
	// Runtime: corrupt state file.
	if err := os.WriteFile(filepath.Join(h.stateDir, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := run(t, "status", "--state-dir", h.stateDir, "broken"); code != ExitError {
		t.Errorf("corrupt state exit = %d, want %d", code, ExitError)
	}
}

// TestCLIReportsUnsupportedEntries covers symlinks, FIFOs, and per-entry
// scan errors: each is recorded with the distinct unsupported status, the
// count and paths are reported without --verbose in both sync and status,
// and nothing is silently folded into skipped work.
func TestCLIReportsUnsupportedEntries(t *testing.T) {
	h := newCLIHarness(t)
	h.writeSrc("regular.txt", 10)
	if err := os.Symlink("regular.txt", filepath.Join(h.src, "link.txt")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(h.src, "fifo"), 0o644); err != nil {
		t.Fatal(err)
	}
	locked := filepath.Join(h.src, "locked")
	if err := os.Mkdir(locked, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(locked, "hidden.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(locked, 0o700) }() // let TempDir cleanup work

	post := h.writeManifest("post", h.record("regular.txt"))
	h.scriptFreshDestSSH(post)
	h.writeScript("rsync", "exit=0")

	code, stdout, stderr := run(t, "sync", "--state-dir", h.stateDir, "--backend", "rsync",
		h.src, "user@host:/data")
	if code != ExitOK {
		t.Fatalf("sync exit = %d, stderr:\n%s", code, stderr)
	}
	if !strings.Contains(stdout, "unsupported 3") {
		t.Errorf("sync stdout must report the unsupported count:\n%s", stdout)
	}
	for _, want := range []string{"unsupported 3", "NOT transferred"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("sync stdout missing %q:\n%s", want, stdout)
		}
	}
	for _, want := range []string{"warning: 3 unsupported", "link.txt", "fifo", "locked"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("sync stderr warning missing %q:\n%s", want, stderr)
		}
	}

	code, stdout, stderr = run(t, "status", "--state-dir", h.stateDir)
	if code != ExitOK {
		t.Fatalf("status exit = %d, stderr:\n%s", code, stderr)
	}
	for _, want := range []string{"unsupported 3", "NOT transferred"} {
		if !strings.Contains(stdout, want) {
			t.Errorf("status stdout missing %q:\n%s", want, stdout)
		}
	}

	// The persisted run distinguishes unsupported from skipped; nothing was
	// recorded as completed for the symlink, FIFO, or scan-failed directory.
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	runRec, err := store.Latest()
	if err != nil {
		t.Fatal(err)
	}
	byPath := map[string]state.Task{}
	for _, task := range runRec.Tasks {
		byPath[task.Path] = task
	}
	for _, p := range []string{"link.txt", "fifo", "locked"} {
		task, ok := byPath[p]
		if !ok {
			t.Fatalf("run has no task for %q", p)
		}
		if task.Status != state.StatusUnsupported {
			t.Errorf("%s status = %q, want unsupported", p, task.Status)
		}
		if task.Status == state.StatusSkipped {
			t.Errorf("%s was folded into skipped", p)
		}
	}
	if got := byPath["regular.txt"].Status; got != state.StatusCompleted {
		t.Errorf("regular.txt = %q, want completed", got)
	}
}

// TestCLIInterspersedArgsEndToEnd proves options accepted after, before, or
// between operands drive real commands end to end.
func TestCLIInterspersedArgsEndToEnd(t *testing.T) {
	h := newCLIHarness(t)
	h.armNative()
	h.writeSrc("small.txt", 10)
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", h.record("small.txt")+h.record("big.bin"))
	h.scriptFreshDestSSH(post)
	h.writeScript("rsync", "exit=0")

	// Every option trails the operands.
	code, stdout, stderr := run(t, "sync", h.src, "user@host:"+h.remoteDir,
		"--state-dir", h.stateDir, "--large-threshold", "1KiB",
		"--jobs", "2", "--streams", "2")
	if code != ExitOK {
		t.Fatalf("sync with trailing options exit = %d, stderr:\n%s", code, stderr)
	}
	if !strings.Contains(stdout, "run ") || !strings.Contains(stdout, "completed") {
		t.Errorf("sync stdout missing the run summary:\n%s", stdout)
	}

	// Options interleaved between operands for plan.
	code, stdout, stderr = run(t, "plan", "--state-dir", h.stateDir, h.src,
		"--large-threshold", "1KiB", "user@host:/data")
	if code != ExitOK {
		t.Fatalf("plan with interleaved options exit = %d, stderr:\n%s", code, stderr)
	}
	if !strings.Contains(stdout, "plan:") || !strings.Contains(stdout, "no data was transferred") {
		t.Errorf("plan stdout unexpected:\n%s", stdout)
	}

	// Run identifier first, options after (status), and options after the
	// identifier for resume.
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	runRec, err := store.Latest()
	if err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr = run(t, "status", runRec.ID, "--state-dir", h.stateDir, "--verbose")
	if code != ExitOK {
		t.Fatalf("status with trailing options exit = %d, stderr:\n%s", code, stderr)
	}
	if !strings.Contains(stdout, runRec.ID) {
		t.Errorf("status stdout missing the run id:\n%s", stdout)
	}
	code, _, stderr = run(t, "resume", runRec.ID, "--state-dir", h.stateDir)
	if code != ExitError || !strings.Contains(stderr, "not resumable") {
		t.Errorf("resume of completed run with trailing options = %d (%q), want not-resumable", code, stderr)
	}
}

// TestCLISSHOverrideReachesRsyncTransport pins the documented CATSYNC_SSH
// scope: the override is used for the manifest/mkdir ssh calls and also
// reaches rsync's -e transport, so diagnostics and tests control the whole
// ssh path CatSync itself starts.
func TestCLISSHOverrideReachesRsyncTransport(t *testing.T) {
	h := newCLIHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", h.record("small.txt"))
	h.scriptFreshDestSSH(post)
	h.writeScript("rsync", "exit=0")

	code, _, stderr := run(t, "sync", "--state-dir", h.stateDir, "--backend", "rsync",
		h.src, "user@host:/data")
	if code != ExitOK {
		t.Fatalf("sync exit = %d, stderr:\n%s", code, stderr)
	}
	rs := h.invocations("rsync")
	if len(rs) != 1 {
		t.Fatalf("rsync invocations = %d, want 1", len(rs))
	}
	wantTransport := h.progs["ssh"]
	found := false
	for i, a := range rs[0].Args {
		if a == "-e" {
			found = true
			if i+1 >= len(rs[0].Args) || rs[0].Args[i+1] != wantTransport {
				t.Errorf("rsync -e transport = %q, want the CATSYNC_SSH override %q", rs[0].Args[i+1], wantTransport)
			}
		}
	}
	if !found {
		t.Errorf("rsync argv has no -e transport: %v", rs[0].Args)
	}
	// The manifest and mkdir ssh calls were made through the same override
	// (the mock records the tool basename, which is "ssh" for the override
	// path as well).
	if got := len(h.invocations("ssh")); got < 1 {
		t.Errorf("ssh manifest invocations = %d, want at least 1", got)
	}
}

// allInvocations returns every mock invocation recorded so far.
func (h *cliHarness) allInvocations() []mockexec.Invocation {
	h.t.Helper()
	invs, err := mockexec.ReadInvocations(h.logPath)
	if err != nil {
		h.t.Fatal(err)
	}
	return invs
}

// TestResumeInvalidSSHOverrideIsUsageError covers the CLI layer: an invalid
// CATSYNC_SSH on resume exits 2 with the transport explanation, before any
// backend invocation, leaving the run untouched.
func TestResumeInvalidSSHOverrideIsUsageError(t *testing.T) {
	h := newCLIHarness(t)
	h.writeSrc("small.txt", 10)
	post := h.writeManifest("post", h.record("small.txt"))
	h.scriptFreshDestSSH(post)
	h.writeScript("rsync", "exit=23")

	// Create a genuinely resumable run: the transfer fails.
	code, _, stderr := run(t, "sync", "--state-dir", h.stateDir, "--backend", "rsync",
		"--verify", "none", h.src, "user@host:/data")
	if code != ExitError {
		t.Fatalf("setup sync exit = %d, stderr:\n%s", code, stderr)
	}
	store, err := state.NewStore(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	runRec, err := store.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if !runRec.Resumable() {
		t.Fatalf("setup run %s is not resumable", runRec.ID)
	}
	attemptsBefore := runRec.Tasks[0].Attempts
	invocationsBefore := len(h.allInvocations())

	t.Setenv("CATSYNC_SSH", "/opt/with space/ssh")
	code, _, stderr = run(t, "resume", "--state-dir", h.stateDir)
	if code != ExitUsage {
		t.Fatalf("resume with invalid CATSYNC_SSH exit = %d, want %d (stderr:\n%s)", code, ExitUsage, stderr)
	}
	if !strings.Contains(stderr, "whitespace") || !strings.Contains(stderr, "-e") {
		t.Errorf("stderr should name the transport reason:\n%s", stderr)
	}
	if got := len(h.allInvocations()); got != invocationsBefore {
		t.Errorf("backend invocations = %d after rejected resume, want %d", got, invocationsBefore)
	}
	after, err := store.Load(runRec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != state.StatusFailed || after.Tasks[0].Status != state.StatusFailed {
		t.Errorf("persisted state changed: run %s task %s", after.Status, after.Tasks[0].Status)
	}
	if after.Tasks[0].Attempts != attemptsBefore {
		t.Errorf("attempts changed to %d, want %d", after.Tasks[0].Attempts, attemptsBefore)
	}
}

// TestCATSYNCMSCPIgnored: the legacy CATSYNC_MSCP override no longer
// affects execution — a large file still transfers natively.
func TestCATSYNCMSCPIgnored(t *testing.T) {
	h := newCLIHarness(t)
	h.armNative()
	h.writeSrc("big.bin", 2048)
	post := h.writeManifest("post", h.record("big.bin"))
	h.scriptFreshDestSSH(post)
	t.Setenv("CATSYNC_MSCP", "/nonexistent/mscp-should-never-run")

	code, _, stderr := run(t, "sync", "--state-dir", h.stateDir,
		"--large-threshold", "1KiB", h.src, "user@host:"+h.remoteDir)
	if code != ExitOK {
		t.Fatalf("sync exit = %d, stderr:\n%s", code, stderr)
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
}
