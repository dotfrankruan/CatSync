//go:build unix

package fpsync

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/frankruan/catsync/internal/process"
	"github.com/frankruan/catsync/internal/testutil/mockexec"
)

func TestMain(m *testing.M) {
	if mockexec.HelperActive() {
		os.Exit(mockexec.RunHelper())
	}
	os.Exit(m.Run())
}

// mockConfig returns a Config executing the test binary as a mock fpsync
// that records into dir, configured by mockCfg. The state dir defaults to a
// fresh temp dir.
func mockConfig(t *testing.T, dir string, mockCfg map[string]string) Config {
	t.Helper()
	prog, err := mockexec.Program()
	if err != nil {
		t.Fatal(err)
	}
	c := validConfig()
	c.Program = prog
	c.StateDir = t.TempDir()
	c.Env = mockexec.Env(dir, mockCfg)
	return c
}

// fpsyncOutput is representative verbose fpsync output, including the
// run-id announcement and two fpart skip records.
const fpsyncOutput = `1792333896 Info: Fpsync started (syncing /srv/source/ => alice@example.com:/data/, pid: 74154)
1792333896 Info: Run ID: 1792333896-74154
1792333896 ===> Analyzing filesystem...
S	4096	./big.bin
S	2048	./dir with space/ünïcødé.bin
1792333897 <=== Fpart crawling finished
1792333897 <=== Fpsync completed without error in 1s.
1792333897 Info: Fpsync stopped (with success)
`

func TestRunCapturesRunIDSkippedAndOutput(t *testing.T) {
	dir := t.TempDir()
	stdoutFile := filepath.Join(dir, "fpsync-stdout")
	if err := os.WriteFile(stdoutFile, []byte(fpsyncOutput), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := mockConfig(t, dir, map[string]string{mockexec.EnvStdoutFile: stdoutFile})

	res, err := cfg.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.RunID != "1792333896-74154" {
		t.Errorf("run id = %q, want 1792333896-74154", res.RunID)
	}
	wantSkipped := []Skipped{
		{Path: "big.bin", Size: 4096},
		{Path: "dir with space/ünïcødé.bin", Size: 2048},
	}
	if !reflect.DeepEqual(res.Skipped, wantSkipped) {
		t.Errorf("skipped = %#v, want %#v", res.Skipped, wantSkipped)
	}
	// The preserved output lives in the state dir and holds everything.
	if filepath.Dir(res.OutputPath) != cfg.StateDir {
		t.Errorf("output %q not in state dir %q", res.OutputPath, cfg.StateDir)
	}
	data, err := os.ReadFile(res.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != fpsyncOutput {
		t.Errorf("preserved output = %q, want the fpsync stdout", data)
	}

	// The mock saw exactly the built argv and ran detached from any stdin.
	rec, err := mockexec.ReadRecord(dir)
	if err != nil {
		t.Fatal(err)
	}
	argv, err := cfg.Command()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rec.Args, argv[1:]) {
		t.Errorf("argv mismatch:\n got %#v\nwant %#v", rec.Args, argv[1:])
	}
}

func TestRunFailureStillExposesRunID(t *testing.T) {
	dir := t.TempDir()
	stdoutFile := filepath.Join(dir, "fpsync-stdout")
	if err := os.WriteFile(stdoutFile, []byte(fpsyncOutput), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := mockConfig(t, dir, map[string]string{
		mockexec.EnvStdoutFile: stdoutFile,
		mockexec.EnvExit:       "1",
	})
	res, err := cfg.Run(context.Background())
	if err == nil {
		t.Fatal("Run with exit 1 = nil, want error")
	}
	var exitErr *process.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("error %T = %v, want *process.ExitError", err, err)
	}
	// The run ID survives the failure so the run can be resumed.
	if res == nil || res.RunID != "1792333896-74154" {
		t.Errorf("result after failure = %+v, want run id preserved", res)
	}
	if res != nil && len(res.Skipped) != 2 {
		t.Errorf("skipped after failure = %#v, want 2 records", res.Skipped)
	}
}

func TestResumeCommandExecution(t *testing.T) {
	dir := t.TempDir()
	const resumeOutput = "1792333911 Info: Run ID: 1792333896-74154 (resumed)\n1792333912 Info: Fpsync stopped (with success)\n"
	stdoutFile := filepath.Join(dir, "fpsync-stdout")
	if err := os.WriteFile(stdoutFile, []byte(resumeOutput), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := mockConfig(t, dir, map[string]string{mockexec.EnvStdoutFile: stdoutFile})
	res, err := cfg.Resume(context.Background(), "1792333896-74154")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.RunID != "1792333896-74154" {
		t.Errorf("resumed run id = %q", res.RunID)
	}
	rec, err := mockexec.ReadRecord(dir)
	if err != nil {
		t.Fatal(err)
	}
	argv, err := cfg.ResumeCommand("1792333896-74154")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rec.Args, argv[1:]) {
		t.Errorf("resume argv mismatch:\n got %#v\nwant %#v", rec.Args, argv[1:])
	}
	// Resume carries no source/destination operands.
	for _, a := range rec.Args {
		if strings.Contains(a, "example.com") || strings.HasPrefix(a, "/srv") {
			t.Errorf("resume argv carries an operand %q: %#v", a, rec.Args)
		}
	}
}

func TestRunCancellation(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, map[string]string{mockexec.EnvSleepMS: "30000"})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	res, err := cfg.Run(ctx)
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Run with cancelled context = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("cancellation took %s, want prompt termination", elapsed)
	}
	// The result and its capture file still exist for later resume.
	if res == nil || res.OutputPath == "" {
		t.Fatal("cancelled run returned no result")
	}
	if _, err := os.Stat(res.OutputPath); err != nil {
		t.Errorf("output capture missing after cancellation: %v", err)
	}
	rec, rerr := mockexec.ReadRecord(dir)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if err := mockexec.ProcessAlive(rec.PID); err == nil {
		t.Errorf("process %d still exists (orphaned or zombie)", rec.PID)
	}
}

func TestRunMissingRunIDFails(t *testing.T) {
	dir := t.TempDir()
	stdoutFile := filepath.Join(dir, "fpsync-stdout")
	if err := os.WriteFile(stdoutFile, []byte("no run id here\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := mockConfig(t, dir, map[string]string{mockexec.EnvStdoutFile: stdoutFile})
	res, err := cfg.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "run id") {
		t.Fatalf("Run without run id = %v, want run-id error", err)
	}
	if res == nil || res.OutputPath == "" {
		t.Error("result missing on run-id failure")
	}
}

func TestRunValidationErrorDoesNotExecute(t *testing.T) {
	dir := t.TempDir()
	cfg := mockConfig(t, dir, nil)
	cfg.Jobs = 0
	if _, err := cfg.Run(context.Background()); err == nil {
		t.Fatal("Run with jobs=0 = nil, want error")
	}
	if _, err := mockexec.ReadRecord(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("mock executed despite invalid config (record err = %v)", err)
	}
}

// TestLocalRealFPSyncMixedTree runs the exact command the backend builds
// with real fpsync/fpart/rsync binaries against a local destination (no
// SSH): a mixed tree must come out split — small files copied, the file
// above the size limit skipped and reported as an S record.
func TestLocalRealFPSyncMixedTree(t *testing.T) {
	for _, tool := range []string{"fpsync", "fpart", "rsync"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not installed; skipping real local fpsync test", tool)
		}
	}
	prog, _ := exec.LookPath("fpsync")
	src := t.TempDir()
	dst := t.TempDir()
	writeFile := func(rel string, size int) {
		t.Helper()
		p := filepath.Join(src, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, bytes.Repeat([]byte("x"), size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeFile("small-a.txt", 6)
	writeFile("sub dir/small-b.txt", 7)
	writeFile("large.bin", 2048)

	cfg := validConfig()
	cfg.Program = prog
	cfg.SourceDir = src
	cfg.StateDir = t.TempDir()
	cfg.Jobs = 1
	cfg.FilesPerPartition = 100
	cfg.Threshold = 1024

	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	// Swap the remote destination operand for a local directory; the rest
	// of the command line stays exactly as built.
	argv[len(argv)-1] = dst + "/"

	capture := filepath.Join(t.TempDir(), "fpsync.out")
	out, err := os.Create(capture)
	if err != nil {
		t.Fatal(err)
	}
	runErr := process.Run(context.Background(), process.Spec{
		Name:   argv[0],
		Args:   argv[1:],
		Stdout: out,
	})
	if cerr := out.Close(); cerr != nil && runErr == nil {
		runErr = cerr
	}
	if runErr != nil {
		data, _ := os.ReadFile(capture)
		t.Fatalf("local fpsync: %v\noutput:\n%s", runErr, data)
	}
	data, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	runID, skipped := ParseOutput(string(data))
	if runID == "" {
		t.Errorf("no run id in fpsync output:\n%s", data)
	}
	wantSkipped := []Skipped{{Path: "large.bin", Size: 2048}}
	if !reflect.DeepEqual(skipped, wantSkipped) {
		t.Errorf("skipped = %#v, want %#v\noutput:\n%s", skipped, wantSkipped, data)
	}
	for _, rel := range []string{"small-a.txt", "sub dir/small-b.txt"} {
		if _, err := os.Stat(filepath.Join(dst, filepath.FromSlash(rel))); err != nil {
			t.Errorf("small file %q not copied: %v", rel, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dst, "large.bin")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("large file must not be copied, stat err = %v", err)
	}
}

// runLocalFPSync executes the exact command the backend builds against a
// local destination directory (no SSH) with the given fpart -s limit and
// returns the destination directory, preserved output, and parsed result.
func runLocalFPSync(t *testing.T, src string, limit uint64) (string, string, *Result) {
	t.Helper()
	if _, err := exec.LookPath("fpsync"); err != nil {
		t.Skip("fpsync not installed; skipping real local fpsync test")
	}
	dst := t.TempDir()
	cfg := validConfig()
	cfg.Program = "fpsync"
	cfg.SourceDir = src
	cfg.StateDir = t.TempDir()
	cfg.Jobs = 1
	cfg.FilesPerPartition = 100
	cfg.Threshold = limit

	argv, err := cfg.Command()
	if err != nil {
		t.Fatalf("Command: %v", err)
	}
	argv[len(argv)-1] = dst + "/"
	capture := filepath.Join(t.TempDir(), "fpsync.out")
	out, err := os.Create(capture)
	if err != nil {
		t.Fatal(err)
	}
	runErr := process.Run(context.Background(), process.Spec{Name: argv[0], Args: argv[1:], Stdout: out})
	if cerr := out.Close(); cerr != nil && runErr == nil {
		runErr = cerr
	}
	if runErr != nil {
		data, _ := os.ReadFile(capture)
		t.Fatalf("local fpsync (limit %d): %v\noutput:\n%s", limit, runErr, data)
	}
	data, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	_, skipped := ParseOutput(string(data))
	return dst, string(data), &Result{Skipped: skipped}
}

// TestLocalRealFPartThresholdBoundary pins the real fpart 1.7.1 skip-big
// semantics and the CatSync mapping against it: with a raw -s limit, a file
// whose size equals the limit is copied (fpart skips only strictly larger
// files), while LimitForThreshold's limit-1 makes the equal file a skipped
// S record. Only the below-threshold file is copied in the mapped run.
func TestLocalRealFPartThresholdBoundary(t *testing.T) {
	for _, tool := range []string{"fpsync", "fpart", "rsync"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not installed; skipping real fpart boundary test", tool)
		}
	}
	const largeThreshold = 1024
	src := t.TempDir()
	for name, size := range map[string]int{
		"below.bin": largeThreshold - 1,
		"equal.bin": largeThreshold,
		"above.bin": largeThreshold + 1,
	} {
		if err := os.WriteFile(filepath.Join(src, name), bytes.Repeat([]byte("x"), size), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Raw -s limit: fpart copies the file that equals the limit, which is
	// exactly the ownership hazard the mapped limit avoids.
	dstRaw, _, rawRes := runLocalFPSync(t, src, largeThreshold)
	for _, name := range []string{"below.bin", "equal.bin"} {
		if _, err := os.Stat(filepath.Join(dstRaw, name)); err != nil {
			t.Errorf("raw limit %d: %s should be copied: %v", largeThreshold, name, err)
		}
	}
	if len(rawRes.Skipped) != 1 || rawRes.Skipped[0].Path != "above.bin" {
		t.Errorf("raw limit %d: skipped = %#v, want only above.bin", largeThreshold, rawRes.Skipped)
	}

	// Mapped limit (threshold-1): the equal-size file is skipped, so the
	// small-file stage cannot write a path that mscp/rsync own.
	limit, err := LimitForThreshold(largeThreshold)
	if err != nil {
		t.Fatal(err)
	}
	if limit != largeThreshold-1 {
		t.Fatalf("LimitForThreshold(%d) = %d, want %d", largeThreshold, limit, largeThreshold-1)
	}
	dstMapped, _, mappedRes := runLocalFPSync(t, src, limit)
	if _, err := os.Stat(filepath.Join(dstMapped, "below.bin")); err != nil {
		t.Errorf("mapped limit %d: below-threshold file should be copied: %v", limit, err)
	}
	for _, name := range []string{"equal.bin", "above.bin"} {
		if _, err := os.Stat(filepath.Join(dstMapped, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("mapped limit %d: %s must not be copied by fpsync: %v", limit, name, err)
		}
	}
	wantSkipped := []Skipped{{Path: "equal.bin", Size: largeThreshold}, {Path: "above.bin", Size: largeThreshold + 1}}
	if !reflect.DeepEqual(mappedRes.Skipped, wantSkipped) {
		t.Errorf("mapped limit %d: skipped = %#v, want %#v", limit, mappedRes.Skipped, wantSkipped)
	}
}
