// Package mockexec implements a mock external command for tests of the
// process runner and the transfer backends.
//
// The mock is the test binary itself, re-executed in helper mode: a test
// package's TestMain calls HelperActive and, when set, RunHelper before
// running any test:
//
//	func TestMain(m *testing.M) {
//		if mockexec.HelperActive() {
//			os.Exit(mockexec.RunHelper())
//		}
//		os.Exit(m.Run())
//	}
//
// A test then uses Program as the backend's executable and Env to configure
// behavior. In helper mode the process records its argument vector, working
// directory, process ID, and standard input into the record directory,
// optionally touches a file (simulating checkpoint creation), sleeps,
// replays configured stdout/stderr content, and exits with a configured
// status. Recording happens before sleeping, so cancelled invocations still
// leave their record behind.
//
// Two configuration styles exist:
//
//   - Environment style (Env): one behavior for every invocation, set with
//     the EnvXxx variables. Used by the single-invocation package tests.
//   - Script style (EnvScript): every invocation appends one JSON record
//     to a log file, and behavior comes from a per-tool config file
//     selected by the invoked program's basename (argv[0]). A test copies
//     or links the test binary under tool names (ssh, rsync, fpsync,
//     mscp) and each tool's config file can dispatch on the arguments, so
//     one mock ssh can answer manifest, mkdir, and verification calls
//     differently. Used by the executor integration tests.
package mockexec

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Environment variables configuring a mock invocation. Env sets them.
const (
	// EnvMarker marks the child as a mock helper; any value enables it.
	EnvMarker = "MOCKEXEC_HELPER"
	// EnvRecordDir is the directory the helper writes its record files to
	// (environment style; each invocation overwrites the record).
	EnvRecordDir = "MOCKEXEC_RECORD_DIR"
	// EnvExit is the decimal exit status (default 0).
	EnvExit = "MOCKEXEC_EXIT"
	// EnvStdoutFile is a file whose contents the helper copies to stdout.
	EnvStdoutFile = "MOCKEXEC_STDOUT_FILE"
	// EnvStderrFile is a file whose contents the helper copies to stderr.
	EnvStderrFile = "MOCKEXEC_STDERR_FILE"
	// EnvSleepMS makes the helper sleep this many milliseconds after
	// recording, before producing output and exiting.
	EnvSleepMS = "MOCKEXEC_SLEEP_MS"
	// EnvTouch is a file the helper creates, simulating a tool that
	// writes a checkpoint.
	EnvTouch = "MOCKEXEC_TOUCH"
	// EnvEchoEnv names an environment variable whose value the helper
	// prints to stdout, proving environment propagation.
	EnvEchoEnv = "MOCKEXEC_ECHO_ENV"
	// EnvSignal names a signal (TERM, KILL, INT, HUP, QUIT, USR1, USR2)
	// the helper sends to itself after recording and output, simulating
	// death by signal. Unix only.
	EnvSignal = "MOCKEXEC_SIGNAL"

	// EnvScriptDir selects script style: the directory holding one
	// subdirectory per mocked tool, named after the tool's basename, each
	// containing a config file (see loadScript).
	EnvScriptDir = "MOCKEXEC_SCRIPT_DIR"
	// EnvInvocationLog names a file every invocation appends one JSON
	// record to (script style).
	EnvInvocationLog = "MOCKEXEC_INVOCATION_LOG"

	// EnvSpawnChildren makes the helper spawn N grandchild processes (the
	// test binary in grandchild role) before its own configured sleep, so
	// cancellation tests can prove the whole process tree dies.
	EnvSpawnChildren = "MOCKEXEC_SPAWN_CHILDREN"
	// EnvChildSleepMS is how long each grandchild sleeps (default
	// 30000). Grandchildren never write invocation records.
	EnvChildSleepMS = "MOCKEXEC_CHILD_SLEEP_MS"
	// EnvChildTouch is a file each grandchild writes after its sleep,
	// simulating a writer that must not survive cancellation (or a
	// post-cleanup write).
	EnvChildTouch = "MOCKEXEC_CHILD_TOUCH"
	// EnvChildPidDir is a directory each grandchild writes its PID to as
	// child-<index>.pid, so tests can verify it is gone.
	EnvChildPidDir = "MOCKEXEC_CHILD_PID_DIR"
	// EnvChildIgnoreTerm makes grandchildren ignore SIGTERM, forcing the
	// SIGKILL escalation path.
	EnvChildIgnoreTerm = "MOCKEXEC_CHILD_IGNORE_TERM"
	// EnvIgnoreTerm makes the helper itself ignore SIGTERM.
	EnvIgnoreTerm = "MOCKEXEC_IGNORE_TERM"
	// EnvCatchSignal makes the helper catch SIGINT/SIGTERM, record the
	// signal name to <record dir>/signal, and exit 42.
	EnvCatchSignal = "MOCKEXEC_CATCH_SIGNAL"
	// EnvRole is set for spawned grandchildren; the value "grandchild"
	// selects the grandchild behavior instead of normal helper behavior.
	EnvRole = "MOCKEXEC_ROLE"
	// EnvChildIndex is the grandchild's zero-based index.
	EnvChildIndex = "MOCKEXEC_CHILD_INDEX"
)

// RoleGrandchild is the EnvRole value selecting grandchild behavior.
const RoleGrandchild = "grandchild"

// Record file names within the record directory.
const (
	fileArgv  = "argv"  // NUL-separated argument vector
	fileCwd   = "cwd"   // working directory
	fileStdin = "stdin" // raw standard input bytes
	filePid   = "pid"   // decimal process ID
)

// HelperActive reports whether this process runs as a mock helper.
func HelperActive() bool { return os.Getenv(EnvMarker) != "" }

// behavior is one configured invocation outcome.
type behavior struct {
	exit       int
	stdoutFile string
	stderrFile string
	sleepMS    int
	touch      string
	echoEnv    string
	signal     string

	spawnChildren   int
	childSleepMS    int
	childTouch      string
	childPidDir     string
	childIgnoreTerm bool
	ignoreTerm      bool
	catchSignal     bool
}

// RunHelper executes the configured mock behavior and returns the process
// exit status. It is meant to be passed to os.Exit from TestMain.
func RunHelper() int {
	if os.Getenv(EnvRole) == RoleGrandchild {
		return runGrandchild()
	}
	tool := filepath.Base(os.Args[0])
	if dir := os.Getenv(EnvRecordDir); dir != "" {
		record(dir)
	}
	if log := os.Getenv(EnvInvocationLog); log != "" {
		appendInvocation(log, tool)
	}
	bh, err := configuredBehavior(tool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: %v\n", err)
		return 2
	}
	if bh.ignoreTerm {
		ignoreTerm()
	}
	if bh.catchSignal {
		installCatchSignal(os.Getenv(EnvRecordDir))
	}
	if bh.spawnChildren > 0 {
		spawnChildren(bh)
	}
	if bh.touch != "" {
		if err := os.WriteFile(bh.touch, []byte("mock checkpoint\n"), 0600); err != nil {
			fmt.Fprintf(os.Stderr, "mockexec: touch %s: %v\n", bh.touch, err)
			return 2
		}
	}
	if bh.sleepMS > 0 {
		time.Sleep(time.Duration(bh.sleepMS) * time.Millisecond)
	}
	if bh.stdoutFile != "" {
		data, err := os.ReadFile(bh.stdoutFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mockexec: read stdout content: %v\n", err)
			return 2
		}
		_, _ = os.Stdout.Write(data)
	}
	if bh.echoEnv != "" {
		fmt.Fprintln(os.Stdout, os.Getenv(bh.echoEnv))
	}
	if bh.stderrFile != "" {
		data, err := os.ReadFile(bh.stderrFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "mockexec: read stderr content: %v\n", err)
			return 2
		}
		_, _ = os.Stderr.Write(data)
	}
	if bh.signal != "" {
		if err := raiseSignal(bh.signal); err != nil {
			fmt.Fprintf(os.Stderr, "mockexec: %v\n", err)
			return 2
		}
		// The signal may be caught or ignored; fall through to exit.
	}
	return bh.exit
}

// runGrandchild implements the grandchild role: it records its PID, then
// sleeps and optionally writes a file, ignoring SIGTERM when configured.
func runGrandchild() int {
	idx := os.Getenv(EnvChildIndex)
	if dir := os.Getenv(EnvChildPidDir); dir != "" {
		_ = os.WriteFile(filepath.Join(dir, "child-"+idx+".pid"),
			[]byte(strconv.Itoa(os.Getpid())), 0600)
	}
	if os.Getenv(EnvChildIgnoreTerm) != "" {
		ignoreTerm()
	}
	if ms := os.Getenv(EnvChildSleepMS); ms != "" {
		if n, err := strconv.Atoi(ms); err == nil {
			time.Sleep(time.Duration(n) * time.Millisecond)
		}
	}
	if f := os.Getenv(EnvChildTouch); f != "" {
		_ = os.WriteFile(f, []byte("late write from grandchild\n"), 0600)
	}
	return 0
}

// spawnChildren starts the configured number of grandchild processes. They
// inherit stdout/stderr on purpose, modelling real tools whose children
// inherit the parent's pipes, and are never waited for: cleanup is the
// process runner's job.
func spawnChildren(bh behavior) {
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: locate executable for spawn: %v\n", err)
		return
	}
	sleep := bh.childSleepMS
	if sleep <= 0 {
		sleep = 30000
	}
	for i := 0; i < bh.spawnChildren; i++ {
		cmd := exec.Command(exe)
		env := append(os.Environ(),
			EnvRole+"="+RoleGrandchild,
			EnvChildIndex+"="+strconv.Itoa(i),
			EnvChildSleepMS+"="+strconv.Itoa(sleep),
		)
		if bh.childPidDir != "" {
			env = append(env, EnvChildPidDir+"="+bh.childPidDir)
		}
		if bh.childTouch != "" {
			env = append(env, EnvChildTouch+"="+bh.childTouch)
		}
		if bh.childIgnoreTerm {
			env = append(env, EnvChildIgnoreTerm+"=1")
		}
		cmd.Env = env
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "mockexec: spawn grandchild %d: %v\n", i, err)
		}
	}
}

// configuredBehavior resolves the invocation's behavior: script style when
// EnvScriptDir is set, environment style otherwise.
func configuredBehavior(tool string) (behavior, error) {
	if dir := os.Getenv(EnvScriptDir); dir != "" {
		toolDir := filepath.Join(dir, tool)
		nth := bumpCounter(toolDir)
		return loadScript(filepath.Join(toolDir, "config"), tool, nth)
	}
	return envBehavior()
}

// bumpCounter increments and returns the per-tool invocation counter kept
// in <toolDir>/.count, so script sections can gate on "nth=N". The tool
// directory is created when missing. Counter updates are not atomic across
// concurrent invocations of the same tool; script-style tests invoke
// counter-gated tools (ssh) sequentially.
func bumpCounter(toolDir string) int {
	if err := os.MkdirAll(toolDir, 0700); err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: create tool dir: %v\n", err)
		return 0
	}
	path := filepath.Join(toolDir, ".count")
	n := 0
	if data, err := os.ReadFile(path); err == nil {
		n, _ = strconv.Atoi(strings.TrimSpace(string(data)))
	}
	n++
	if err := os.WriteFile(path, []byte(strconv.Itoa(n)), 0600); err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: write counter: %v\n", err)
	}
	return n
}

// envBehavior implements the original environment-variable configuration.
func envBehavior() (behavior, error) {
	var bh behavior
	if e := os.Getenv(EnvExit); e != "" {
		n, err := strconv.Atoi(e)
		if err != nil {
			return bh, fmt.Errorf("invalid %s %q", EnvExit, e)
		}
		bh.exit = n
	}
	bh.stdoutFile = os.Getenv(EnvStdoutFile)
	bh.stderrFile = os.Getenv(EnvStderrFile)
	bh.touch = os.Getenv(EnvTouch)
	bh.echoEnv = os.Getenv(EnvEchoEnv)
	bh.signal = os.Getenv(EnvSignal)
	bh.ignoreTerm = os.Getenv(EnvIgnoreTerm) != ""
	bh.catchSignal = os.Getenv(EnvCatchSignal) != ""
	bh.childPidDir = os.Getenv(EnvChildPidDir)
	bh.childTouch = os.Getenv(EnvChildTouch)
	bh.childIgnoreTerm = os.Getenv(EnvChildIgnoreTerm) != ""
	if n := os.Getenv(EnvSpawnChildren); n != "" {
		v, err := strconv.Atoi(n)
		if err != nil {
			return bh, fmt.Errorf("invalid %s %q", EnvSpawnChildren, n)
		}
		bh.spawnChildren = v
	}
	if ms := os.Getenv(EnvChildSleepMS); ms != "" {
		v, err := strconv.Atoi(ms)
		if err != nil {
			return bh, fmt.Errorf("invalid %s %q", EnvChildSleepMS, ms)
		}
		bh.childSleepMS = v
	}
	if ms := os.Getenv(EnvSleepMS); ms != "" {
		n, err := strconv.Atoi(ms)
		if err != nil {
			return bh, fmt.Errorf("invalid %s %q", EnvSleepMS, ms)
		}
		bh.sleepMS = n
	}
	return bh, nil
}

// loadScript reads a script-style config file. The file holds one or more
// sections separated by a line containing only "---". Each section lists
// "key=value" lines:
//
//	match=TEXT       section applies only when the NUL-joined argument
//	                 vector contains TEXT; first matching section wins
//	nth=N            section applies only to the Nth invocation of this
//	                 tool (1-based); combines with match
//	exit=N           decimal exit status (default 0)
//	stdout_file=P    file copied to stdout
//	stderr_file=P    file copied to stderr
//	sleep_ms=N       delay after recording
//	touch=P          file created (checkpoint simulation)
//	touch_flag=F     create the file named by flag F's value in argv
//	                 (e.g. touch_flag=-W creates the mscp checkpoint)
//	signal=NAME      signal the helper sends itself after output
//	                 (TERM, KILL, ...; Unix only)
//	ignore_term=1    the helper ignores SIGTERM (forces SIGKILL escalation)
//	catch_signal=1   the helper records SIGINT/SIGTERM to <record dir>/signal
//	spawn_children=N start N grandchild processes before sleeping
//	child_sleep_ms=N grandchild sleep (default 30000)
//	child_touch=P    grandchild writes P after its sleep
//	child_pid_dir=D  grandchild writes child-<n>.pid into D
//	child_ignore_term=1 grandchildren ignore SIGTERM
//
// A section without match and nth keys always applies, so it works as the
// default or as the only section. Blank lines and lines starting with '#'
// are ignored. No matching section is an error, so misconfigurations are
// loud.
func loadScript(path, tool string, nth int) (behavior, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return behavior{}, fmt.Errorf("tool %q: read script config: %w", tool, err)
	}
	joined := strings.Join(os.Args[1:], "\x00")
	var matched *map[string]string
	for _, section := range strings.Split(string(data), "\n---\n") {
		kv := map[string]string{}
		ok := true
		for _, line := range strings.Split(section, "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			k, v, found := strings.Cut(line, "=")
			if !found {
				return behavior{}, fmt.Errorf("tool %q: malformed config line %q", tool, line)
			}
			kv[k] = v
			switch k {
			case "match":
				if !strings.Contains(joined, v) {
					ok = false
				}
			case "nth":
				want, perr := strconv.Atoi(v)
				if perr != nil {
					return behavior{}, fmt.Errorf("tool %q: invalid nth %q", tool, v)
				}
				if want != nth {
					ok = false
				}
			}
		}
		if ok {
			matched = &kv
			break
		}
	}
	if matched == nil {
		return behavior{}, fmt.Errorf("tool %q: no config section matches invocation %d", tool, nth)
	}
	kv := *matched
	var bh behavior
	if v := kv["exit"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return bh, fmt.Errorf("tool %q: invalid exit %q", tool, v)
		}
		bh.exit = n
	}
	bh.stdoutFile = kv["stdout_file"]
	bh.stderrFile = kv["stderr_file"]
	bh.touch = kv["touch"]
	if flag := kv["touch_flag"]; flag != "" {
		for i, a := range os.Args[1:] {
			if a == flag && i+2 <= len(os.Args[1:]) {
				bh.touch = os.Args[1:][i+1]
			}
		}
		if bh.touch == "" {
			return bh, fmt.Errorf("tool %q: touch_flag %s not found in argv", tool, flag)
		}
	}
	bh.signal = kv["signal"]
	bh.ignoreTerm = kv["ignore_term"] != ""
	bh.catchSignal = kv["catch_signal"] != ""
	bh.childPidDir = kv["child_pid_dir"]
	bh.childTouch = kv["child_touch"]
	bh.childIgnoreTerm = kv["child_ignore_term"] != ""
	if v := kv["spawn_children"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return bh, fmt.Errorf("tool %q: invalid spawn_children %q", tool, v)
		}
		bh.spawnChildren = n
	}
	if v := kv["child_sleep_ms"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return bh, fmt.Errorf("tool %q: invalid child_sleep_ms %q", tool, v)
		}
		bh.childSleepMS = n
	}
	if v := kv["sleep_ms"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return bh, fmt.Errorf("tool %q: invalid sleep_ms %q", tool, v)
		}
		bh.sleepMS = n
	}
	return bh, nil
}

// Invocation is one script-style invocation record appended to the log.
type Invocation struct {
	// Tool is the basename of the invoked program (argv[0]).
	Tool string `json:"tool"`
	// Args is the received argument vector after the program name.
	Args []string `json:"args"`
	// Dir is the working directory the mock ran in.
	Dir string `json:"dir"`
	// Stdin is the exact standard input, base64-encoded.
	Stdin string `json:"stdin_b64"`
	// PID is the mock process ID.
	PID int `json:"pid"`
}

// appendInvocation records one invocation as a single JSON line. The file
// is opened O_APPEND and written with one Write call, so concurrent
// invocations (parallel mscp tasks) never interleave a record.
func appendInvocation(log, tool string) {
	stdin, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: read stdin: %v\n", err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: getwd: %v\n", err)
	}
	inv := Invocation{
		Tool:  tool,
		Args:  append([]string{}, os.Args[1:]...),
		Dir:   cwd,
		Stdin: base64.StdEncoding.EncodeToString(stdin),
		PID:   os.Getpid(),
	}
	data, err := json.Marshal(inv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: encode invocation: %v\n", err)
		return
	}
	f, err := os.OpenFile(log, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: open invocation log: %v\n", err)
		return
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: write invocation log: %v\n", err)
	}
	if err := f.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: close invocation log: %v\n", err)
	}
}

// ReadInvocations loads every record appended to a script-style invocation
// log, in append order. A missing log yields an empty slice.
func ReadInvocations(log string) ([]Invocation, error) {
	data, err := os.ReadFile(log)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("mockexec: read invocation log: %w", err)
	}
	var out []Invocation
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var inv Invocation
		if err := json.Unmarshal([]byte(line), &inv); err != nil {
			return nil, fmt.Errorf("mockexec: decode invocation %q: %w", line, err)
		}
		out = append(out, inv)
	}
	return out, nil
}

// StdinBytes decodes an invocation's recorded standard input.
func (i Invocation) StdinBytes() ([]byte, error) {
	return base64.StdEncoding.DecodeString(i.Stdin)
}

// record captures the invocation into dir (environment style). Failures
// are reported on stderr but do not stop the helper, so the configured
// behavior still runs.
func record(dir string) {
	write := func(name string, data []byte) {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			fmt.Fprintf(os.Stderr, "mockexec: record %s: %v\n", name, err)
		}
	}
	write(fileArgv, []byte(strings.Join(os.Args[1:], "\x00")))
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: getwd: %v\n", err)
	}
	write(fileCwd, []byte(cwd))
	stdin, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mockexec: read stdin: %v\n", err)
	}
	write(fileStdin, stdin)
	write(filePid, []byte(strconv.Itoa(os.Getpid())))
}

// Program returns the running test binary's path, for use as the mock
// executable (a backend's program override or a process.Spec name).
func Program() (string, error) {
	p, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("mockexec: locate test binary: %w", err)
	}
	return p, nil
}

// LinkPrograms creates hard links (or copies, when linking fails) to the
// running test binary under each given tool name inside dir, and returns
// the tool-name → path map. Script-style tests use the map to point a
// system's program overrides at the mock: the helper dispatches behavior on
// the basename of argv[0].
func LinkPrograms(dir string, tools ...string) (map[string]string, error) {
	prog, err := Program()
	if err != nil {
		return nil, err
	}
	paths := make(map[string]string, len(tools))
	for _, tool := range tools {
		p := filepath.Join(dir, tool)
		if err := os.Link(prog, p); err != nil {
			data, rerr := os.ReadFile(prog)
			if rerr != nil {
				return nil, fmt.Errorf("mockexec: link %s: %v (copy failed too: %v)", tool, err, rerr)
			}
			if werr := os.WriteFile(p, data, 0700); werr != nil {
				return nil, fmt.Errorf("mockexec: copy %s: %w", tool, werr)
			}
		}
		paths[tool] = p
	}
	return paths, nil
}

// Env returns child-process environment additions (applied on top of the
// inherited environment by process.Run): the mock marker, the record
// directory, and cfg (keyed with the EnvXxx constants). Tests may add
// further variables by appending.
func Env(recordDir string, cfg map[string]string) []string {
	env := []string{EnvMarker + "=1"}
	if recordDir != "" {
		env = append(env, EnvRecordDir+"="+recordDir)
	}
	for k, v := range cfg {
		env = append(env, k+"="+v)
	}
	return env
}

// EnvScript returns child-process environment additions selecting script
// style: the marker, the script directory, and the invocation log.
func EnvScript(scriptDir, invocationLog string) []string {
	return []string{
		EnvMarker + "=1",
		EnvScriptDir + "=" + scriptDir,
		EnvInvocationLog + "=" + invocationLog,
	}
}

// Record is what one mock invocation captured (environment style).
type Record struct {
	// Args is the received argument vector (empty when none was passed).
	Args []string
	// Dir is the working directory the mock ran in.
	Dir string
	// Stdin is the exact standard input byte stream.
	Stdin []byte
	// PID is the mock process ID.
	PID int
}

// ReadRecord loads the record a mock invocation wrote into dir.
func ReadRecord(dir string) (Record, error) {
	var rec Record
	argv, err := os.ReadFile(filepath.Join(dir, fileArgv))
	if err != nil {
		return rec, fmt.Errorf("mockexec: read argv record: %w", err)
	}
	if len(argv) > 0 {
		rec.Args = strings.Split(string(argv), "\x00")
	}
	cwd, err := os.ReadFile(filepath.Join(dir, fileCwd))
	if err != nil {
		return rec, fmt.Errorf("mockexec: read cwd record: %w", err)
	}
	rec.Dir = string(cwd)
	rec.Stdin, err = os.ReadFile(filepath.Join(dir, fileStdin))
	if err != nil {
		return rec, fmt.Errorf("mockexec: read stdin record: %w", err)
	}
	pid, err := os.ReadFile(filepath.Join(dir, filePid))
	if err != nil {
		return rec, fmt.Errorf("mockexec: read pid record: %w", err)
	}
	rec.PID, err = strconv.Atoi(strings.TrimSpace(string(pid)))
	if err != nil {
		return rec, fmt.Errorf("mockexec: parse pid record %q: %w", pid, err)
	}
	return rec, nil
}

// ReadChildPids loads the PIDs written by spawned grandchildren into dir,
// in filename order.
func ReadChildPids(dir string) ([]int, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "child-*.pid"))
	if err != nil {
		return nil, fmt.Errorf("mockexec: glob child pids: %w", err)
	}
	var pids []int
	for _, m := range matches {
		data, err := os.ReadFile(m)
		if err != nil {
			return nil, fmt.Errorf("mockexec: read child pid %s: %w", m, err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil {
			return nil, fmt.Errorf("mockexec: parse child pid %s: %w", m, err)
		}
		pids = append(pids, pid)
	}
	return pids, nil
}
