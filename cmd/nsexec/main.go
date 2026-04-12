// nsexec enters a network namespace and sends a JSON command to a TCP endpoint.
// With -timing flag, outputs per-step nanosecond breakdown to stderr.
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
	timing := false
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "-timing" {
		timing = true
		args = args[1:]
	}

	if len(args) != 3 {
		fmt.Fprintf(os.Stderr, "usage: nsexec [-timing] <nsPath> <host:port> <json>\n")
		os.Exit(2)
	}

	nsPath := args[0]
	addr := args[1]
	payload := args[2]

	t0 := time.Now()

	runtime.LockOSThread()
	t1 := time.Now()

	// Open netns fd
	nsFile, err := os.Open(nsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open ns: %v\n", err)
		os.Exit(1)
	}
	t2 := time.Now()

	// setns
	_, _, errno := syscall.RawSyscall(308, nsFile.Fd(), 0x40000000, 0)
	nsFile.Close()
	if errno != 0 {
		fmt.Fprintf(os.Stderr, "setns: %v\n", errno)
		os.Exit(1)
	}
	t3 := time.Now()

	// TCP connect
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	t4 := time.Now()

	conn.SetDeadline(time.Now().Add(30 * time.Second))

	// Write
	conn.Write([]byte(payload))
	conn.Write([]byte("\n"))
	t5 := time.Now()

	// Read response
	buf := make([]byte, 65536)
	total := 0
	for {
		n, err := conn.Read(buf[total:])
		if n > 0 {
			total += n
		}
		if err != nil {
			break
		}
	}
	t6 := time.Now()

	conn.Close()
	os.Stdout.Write(buf[:total])

	if timing {
		fmt.Fprintf(os.Stderr, "nsexec timing:\n")
		fmt.Fprintf(os.Stderr, "  lock_thread: %v\n", t1.Sub(t0))
		fmt.Fprintf(os.Stderr, "  open_ns:     %v\n", t2.Sub(t1))
		fmt.Fprintf(os.Stderr, "  setns:       %v\n", t3.Sub(t2))
		fmt.Fprintf(os.Stderr, "  tcp_connect: %v\n", t4.Sub(t3))
		fmt.Fprintf(os.Stderr, "  write:       %v\n", t5.Sub(t4))
		fmt.Fprintf(os.Stderr, "  read:        %v\n", t6.Sub(t5))
		fmt.Fprintf(os.Stderr, "  total:       %v\n", t6.Sub(t0))
	}
}
