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
	listenFd, err := vsockListen(agentPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "guest-agent: listen vsock port %d: %v\n", agentPort, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "guest-agent: listening on vsock port %d\n", agentPort)

	for {
		connFd, _, err := syscall.Accept(listenFd)
		if err != nil {
			fmt.Fprintf(os.Stderr, "guest-agent: accept: %v\n", err)
			continue
		}
		f := os.NewFile(uintptr(connFd), "vsock-conn")
		conn, err := net.FileConn(f)
		f.Close()
		if err != nil {
			syscall.Close(connFd)
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

func vsockListen(port uint32) (int, error) {
	fd, err := syscall.Socket(afVsock, syscall.SOCK_STREAM, 0)
	if err != nil {
		return -1, fmt.Errorf("socket: %w", err)
	}
	sa := sockaddrVM{
		family: afVsock,
		port:   port,
		cid:    vmaddrCIDAny,
	}
	_, _, errno := syscall.Syscall(syscall.SYS_BIND, uintptr(fd),
		uintptr(unsafe.Pointer(&sa)), unsafe.Sizeof(sa))
	if errno != 0 {
		syscall.Close(fd)
		return -1, fmt.Errorf("bind: %w", errno)
	}
	if err := syscall.Listen(fd, 5); err != nil {
		syscall.Close(fd)
		return -1, fmt.Errorf("listen: %w", err)
	}
	return fd, nil
}
