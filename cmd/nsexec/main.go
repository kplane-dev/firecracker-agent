// nsexec enters a network namespace and sends a JSON command to a TCP endpoint.
// Purpose-built helper to avoid Go runtime corruption from in-process setns
// and overhead from shell+nc chains.
//
// Usage: nsexec <nsPath> <host:port> <json>
// Example: nsexec /proc/123/ns/net 10.0.0.2:1024 '{"cmd":["/bin/sh","-c","node -v"]}'
package main

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintf(os.Stderr, "usage: nsexec <nsPath> <host:port> <json>\n")
		os.Exit(2)
	}

	nsPath := os.Args[1]
	addr := os.Args[2]
	payload := os.Args[3]

	// Lock to current OS thread — setns applies to the thread
	runtime.LockOSThread()

	// Enter the target network namespace
	nsFile, err := os.Open(nsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open ns: %v\n", err)
		os.Exit(1)
	}
	_, _, errno := syscall.RawSyscall(308, nsFile.Fd(), 0x40000000, 0) // SYS_SETNS, CLONE_NEWNET
	nsFile.Close()
	if errno != 0 {
		fmt.Fprintf(os.Stderr, "setns: %v\n", errno)
		os.Exit(1)
	}

	// TCP connect to guest
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// Send JSON + newline
	conn.Write([]byte(payload))
	conn.Write([]byte("\n"))

	// Read response to stdout
	buf := make([]byte, 65536)
	n, _ := conn.Read(buf)
	os.Stdout.Write(buf[:n])
}
