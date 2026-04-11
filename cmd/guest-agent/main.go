//go:build linux

// guest-agent runs inside Firecracker VMs and accepts exec commands via vsock.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
	"unsafe"
)

const (
	agentPort    = 1024
	afVsock      = 40
	vmaddrCIDAny = 0xFFFFFFFF
	maxReqSize   = 1 << 20 // 1MB max request
	readTimeout  = 30       // seconds
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
		connFd, _, errno := syscall.Syscall(syscall.SYS_ACCEPT, uintptr(listenFd), 0, 0)
		if errno != 0 {
			fmt.Fprintf(os.Stderr, "guest-agent: accept: %v\n", errno)
			time.Sleep(100 * time.Millisecond) // backoff on repeated errors
			continue
		}
		syscall.SetNonblock(int(connFd), false)
		go handleConn(int(connFd))
	}
}

func handleConn(fd int) {
	defer syscall.Close(fd)

	// Set read timeout to prevent goroutine leak from idle connections
	tv := syscall.Timeval{Sec: readTimeout}
	syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &tv)

	// Read request — loop to handle partial reads
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for len(buf) < maxReqSize {
		n, err := syscall.Read(fd, tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			// Check if we have a complete JSON object (ends with } or }\n)
			if len(buf) > 0 && (buf[len(buf)-1] == '}' || (len(buf) > 1 && buf[len(buf)-2] == '}' && buf[len(buf)-1] == '\n')) {
				break
			}
		}
		if err != nil {
			if len(buf) > 0 {
				break // try to parse what we have
			}
			writeJSON(fd, ExecResponse{Stderr: fmt.Sprintf("read: %v", err), ExitCode: 1})
			return
		}
	}

	var req ExecRequest
	if err := json.Unmarshal(buf, &req); err != nil {
		writeJSON(fd, ExecResponse{Stderr: fmt.Sprintf("decode: %v", err), ExitCode: 1})
		return
	}
	if len(req.Cmd) == 0 {
		writeJSON(fd, ExecResponse{Stderr: "empty command", ExitCode: 1})
		return
	}

	cmd := exec.Command(req.Cmd[0], req.Cmd[1:]...)
	cmd.Env = append(os.Environ(), "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
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
	writeJSON(fd, resp)
}

func writeJSON(fd int, v interface{}) {
	data, _ := json.Marshal(v)
	data = append(data, '\n')
	syscall.Write(fd, data)
}

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
	sa := sockaddrVM{family: afVsock, port: port, cid: vmaddrCIDAny}
	_, _, errno := syscall.Syscall(syscall.SYS_BIND, uintptr(fd),
		uintptr(unsafe.Pointer(&sa)), unsafe.Sizeof(sa))
	if errno != 0 {
		syscall.Close(fd)
		return -1, fmt.Errorf("bind: %w", errno)
	}
	if err := syscall.Listen(fd, 16); err != nil {
		syscall.Close(fd)
		return -1, fmt.Errorf("listen: %w", err)
	}
	return fd, nil
}
