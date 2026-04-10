//go:build linux

// guest-agent runs inside Firecracker VMs and accepts exec commands via vsock.
package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"syscall"
	"unsafe"
)

const (
	agentPort    = 1024
	afVsock      = 40
	vmaddrCIDAny = 0xFFFFFFFF
)

type ExecRequest struct {
	Cmd []string `json:"cmd"`
}

type ExecResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

func main() {
	ln, err := listenVsock(agentPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "guest-agent: listen vsock port %d: %v\n", agentPort, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "guest-agent: listening on vsock port %d\n", agentPort)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "guest-agent: accept: %v\n", err)
			continue
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	var req ExecRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		json.NewEncoder(conn).Encode(ExecResponse{Stderr: err.Error(), ExitCode: 1})
		return
	}
	if len(req.Cmd) == 0 {
		json.NewEncoder(conn).Encode(ExecResponse{Stderr: "empty command", ExitCode: 1})
		return
	}

	cmd := exec.Command(req.Cmd[0], req.Cmd[1:]...)
	out, err := cmd.CombinedOutput()

	resp := ExecResponse{Stdout: string(out)}
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			resp.ExitCode = exitErr.ExitCode()
		} else {
			resp.ExitCode = 1
			resp.Stderr = err.Error()
		}
	}
	json.NewEncoder(conn).Encode(resp)
}

// sockaddrVM mirrors struct sockaddr_vm from linux/vm_sockets.h.
type sockaddrVM struct {
	family    uint16
	reserved1 uint16
	port      uint32
	cid       uint32
	flags     uint8
	zero      [3]uint8
}

func listenVsock(port uint32) (net.Listener, error) {
	fd, err := syscall.Socket(afVsock, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, fmt.Errorf("socket: %w", err)
	}
	sa := sockaddrVM{
		family: afVsock,
		port:   port,
		cid:    vmaddrCIDAny,
	}
	// Bind using RawSockaddr
	_, _, errno := syscall.Syscall(syscall.SYS_BIND, uintptr(fd),
		uintptr(unsafe.Pointer(&sa)), unsafe.Sizeof(sa))
	if errno != 0 {
		syscall.Close(fd)
		return nil, fmt.Errorf("bind: %w", errno)
	}
	if err := syscall.Listen(fd, 5); err != nil {
		syscall.Close(fd)
		return nil, fmt.Errorf("listen: %w", err)
	}
	f := os.NewFile(uintptr(fd), fmt.Sprintf("vsock:%d", port))
	ln, err := net.FileListener(f)
	f.Close()
	if err != nil {
		return nil, fmt.Errorf("file listener: %w", err)
	}
	return ln, nil
}
