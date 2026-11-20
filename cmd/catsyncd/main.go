// Command catsyncd is the one-shot remote agent for CatSync's daemon
// transport. It is started per run by catsync over a single ssh session,
// serves one bootstrap session, and exits when the transfer ends.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dotfrankruan/CatSync/internal/daemon"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

const usage = `Usage: catsyncd <command> [options]

Commands:
  serve --once   run one bootstrap session and exit
  probe          print this host's daemon capabilities as JSON
  version        print the catsyncd version

catsyncd is normally started by catsync over ssh; it is not a persistent
service. Install it yourself on the remote host; catsync never installs it.
`

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	switch args[0] {
	case "version", "-version", "--version":
		fmt.Printf("catsyncd %s\n", version)
		return 0
	case "help", "-h", "--help":
		fmt.Print(usage)
		return 0
	case "probe":
		return runProbe(args[1:])
	case "serve":
		return runServe(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "catsyncd: unknown command %q\n\n%s", args[0], usage)
		return 2
	}
}

func runProbe(args []string) int {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	jsonOut := fs.Bool("json", true, "print the report as JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "catsyncd: probe takes no operands")
		return 2
	}
	report := daemon.Probe(version)
	if !*jsonOut {
		fmt.Printf("catsyncd %s (%s), protocol %d, port %d\n", report.Version, report.Platform, report.Protocol, report.DefaultPort)
		fmt.Printf("  sftp: %v  exec: %v  manifest: %v  metadata: %v  hash: %v  stat: %v  samefile: %v\n",
			report.Features.SFTP, report.Features.Exec, report.Features.Manifest,
			report.Features.Metadata, report.Features.Hash, report.Features.Stat, report.Features.SameFile)
		fmt.Printf("  sftp-server: %q\n  rsync: %q\n  notes: %s\n", report.Details.SFTPServer, report.Details.Rsync, report.Details.Notes)
		return 0
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		fmt.Fprintf(os.Stderr, "catsyncd: encode probe report: %v\n", err)
		return 1
	}
	return 0
}

func runServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	once := fs.Bool("once", false, "serve exactly one bootstrap session and exit (required)")
	listen := fs.String("listen", ":43411", "TCP listen address; port 0 selects an ephemeral port")
	sftpServer := fs.String("sftp-server", "", "path to the OpenSSH sftp-server executable")
	maxSFTP := fs.Int("max-sftp-conns", 64, "maximum concurrent SFTP connections")
	maxExec := fs.Int("max-exec-conns", 64, "maximum concurrent exec connections (unused in M1)")
	maxControl := fs.Int("max-control-conns", 16, "maximum concurrent control RPC connections")
	maxPending := fs.Int("max-pending-handshakes", 64, "maximum concurrent TLS handshakes")
	maxConns := fs.Int("max-conns", 256, "global connection budget")
	idle := fs.Duration("idle-timeout", 15*time.Minute, "close data connections silent for this long")
	drain := fs.Duration("drain-timeout", 30*time.Second, "bounded DRAINING deadline")
	controlTimeout := fs.Duration("control-timeout", 30*time.Second, "terminate when the control channel is silent for this long")
	certTTL := fs.Duration("cert-ttl", daemon.DefaultCertTTL, "ephemeral certificate lifetime")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "catsyncd: serve takes no operands")
		return 2
	}
	if !*once {
		fmt.Fprintln(os.Stderr, "catsyncd: serve requires --once; catsyncd is not a persistent service")
		return 2
	}
	requestedPort := portFromListen(*listen)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	code := daemon.Serve(ctx, daemon.ServerConfig{
		Listen:           *listen,
		RequestedPort:    requestedPort,
		SFTPPath:         daemon.ResolveSFTPPath(*sftpServer),
		Limits:           protocolLimits(*maxSFTP, *maxExec, *maxControl, *maxPending, *maxConns),
		CertTTL:          *certTTL,
		IdleTimeout:      *idle,
		DrainTimeout:     *drain,
		ControlTimeout:   *controlTimeout,
		HandshakeTimeout: 10 * time.Second,
		Version:          version,
		Stdin:            os.Stdin,
		Stdout:           os.Stdout,
		Stderr:           os.Stderr,
	})
	return code
}

func portFromListen(listen string) int {
	_, portStr, err := netSplitHostPort(listen)
	if err != nil {
		return 0
	}
	p, err := strconv.Atoi(portStr)
	if err != nil || p < 0 || p > 65535 {
		return 0
	}
	return p
}
