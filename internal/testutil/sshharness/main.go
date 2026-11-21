// Command sshharness is a test-only stand-in for the ssh client. It runs the
// remote command locally and implements OpenSSH-style -L port forwarding, so
// CatSync's real binary can be exercised end-to-end without sshd.
package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func main() {
	args := os.Args[1:]
	var forwards []string
	var positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch a {
		case "-p", "-o", "-L", "-i", "-l", "-F", "-J", "-c":
			if i+1 < len(args) {
				if a == "-L" {
					forwards = append(forwards, args[i+1])
				}
				i++
			}
		default:
			if strings.HasPrefix(a, "-") {
				continue
			}
			positional = append(positional, a)
		}
	}
	if logPath := os.Getenv("CATSYNC_SSHH_LOG"); logPath != "" {
		if f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err == nil {
			fmt.Fprintf(f, "invoke %d\n", len(positional))
			_ = f.Close()
		}
	}
	if len(positional) < 2 {
		fmt.Fprintln(os.Stderr, "sshharness: missing host or command")
		os.Exit(255)
	}
	command := positional[len(positional)-1]
	if os.Getenv("CATSYNC_SSHH_FAIL_FORWARD") != "" && len(forwards) > 0 {
		fmt.Fprintln(os.Stderr, "sshharness: simulated forwarding failure")
		os.Exit(255)
	}
	var closers []io.Closer
	for _, spec := range forwards {
		ln, err := startForward(spec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sshharness: forward %s: %v\n", spec, err)
			os.Exit(255)
		}
		closers = append(closers, ln)
	}
	defer func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}()
	cmd := exec.Command("/bin/sh", "-c", command)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			os.Exit(ee.ExitCode())
		}
		os.Exit(255)
	}
}

func startForward(spec string) (net.Listener, error) {
	parts := strings.Split(spec, ":")
	if len(parts) != 4 {
		return nil, fmt.Errorf("unsupported forwarding spec %q", spec)
	}
	// parts[0] is the bind host (127.0.0.1), parts[1] local port,
	// parts[2] target host, parts[3] target port.
	port, err := strconv.Atoi(parts[1])
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid local port in %q", spec)
	}
	target := net.JoinHostPort(parts[2], parts[3])
	ln, err := net.Listen("tcp", net.JoinHostPort(parts[0], strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				up, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer up.Close()
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(up, c); done <- struct{}{} }()
				go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
				<-done
			}(conn)
		}
	}()
	return ln, nil
}
