//go:build linux

// guest-agent runs inside Firecracker VMs and accepts exec commands via TCP and vsock.
package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"
	"unsafe"
)

const (
	tcpPort      = 1024
	vsockPort    = 1024
	afVsock      = 40
	vmaddrCIDAny = 0xFFFFFFFF
	maxReqSize   = 1 << 20
	readTimeout  = 30
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
	// TCP listener — survives snapshot restore with no reset
	go func() {
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", tcpPort))
		if err != nil {
			fmt.Fprintf(os.Stderr, "guest-agent: tcp listen: %v\n", err)
			return
		}
		fmt.Fprintf(os.Stderr, "guest-agent: listening on tcp port %d\n", tcpPort)
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go handleTCPConn(conn)
		}
	}()

	// Vsock listener — fallback for VMs without network
	listenFd, err := vsockListen(vsockPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "guest-agent: vsock listen: %v (tcp-only mode)\n", err)
		// Block forever if vsock fails — TCP is the primary
		select {}
	}
	fmt.Fprintf(os.Stderr, "guest-agent: listening on vsock port %d\n", vsockPort)

	for {
		connFd, _, errno := syscall.Syscall(syscall.SYS_ACCEPT, uintptr(listenFd), 0, 0)
		if errno != 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		syscall.SetNonblock(int(connFd), false)
		go handleVsockConn(int(connFd))
	}
}

func handleTCPConn(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(30 * time.Second))

	var req ExecRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		json.NewEncoder(conn).Encode(ExecResponse{Stderr: err.Error(), ExitCode: 1})
		return
	}
	resp := execCmd(req)
	json.NewEncoder(conn).Encode(resp)
}

func handleVsockConn(fd int) {
	defer syscall.Close(fd)

	tv := syscall.Timeval{Sec: readTimeout}
	syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &tv)

	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for len(buf) < maxReqSize {
		n, err := syscall.Read(fd, tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			if len(buf) > 0 && (buf[len(buf)-1] == '}' || (len(buf) > 1 && buf[len(buf)-2] == '}' && buf[len(buf)-1] == '\n')) {
				break
			}
		}
		if err != nil {
			if len(buf) > 0 {
				break
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
	resp := execCmd(req)
	writeJSON(fd, resp)
}

func execCmd(req ExecRequest) ExecResponse {
	if len(req.Cmd) == 0 {
		return ExecResponse{Stderr: "empty command", ExitCode: 1}
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
	return resp
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
