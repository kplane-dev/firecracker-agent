//go:build linux

// guest-agent runs inside Firecracker VMs and accepts exec commands via TCP and vsock.
package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
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
	Cmd  []string `json:"cmd"`            // shell command (fallback)
	Eval string   `json:"eval,omitempty"` // code to evaluate in the runtime worker
}

type ExecResponse struct {
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	ExitCode int    `json:"exit_code"`
}

var runtimeWorker *worker

func main() {
	// Worker started lazily on first eval request (survives snapshot restore)

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

// evalViaServer sends code to the eval HTTP server on localhost:1025.
// Uses raw syscalls (not Go's net/http) to work correctly after snapshot restore.
func evalViaServer(code string) (ExecResponse, error) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return ExecResponse{}, fmt.Errorf("socket: %w", err)
	}
	defer syscall.Close(fd)

	sa := &syscall.SockaddrInet4{Port: 1025, Addr: [4]byte{127, 0, 0, 1}}
	if err := syscall.Connect(fd, sa); err != nil {
		return ExecResponse{}, fmt.Errorf("connect: %w", err)
	}

	// Send HTTP POST
	header := fmt.Sprintf("POST / HTTP/1.0\r\nContent-Length: %d\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n", len(code))
	syscall.Write(fd, []byte(header))
	syscall.Write(fd, []byte(code))

	// Read response
	var resp []byte
	buf := make([]byte, 4096)
	for {
		n, err := syscall.Read(fd, buf)
		if n > 0 {
			resp = append(resp, buf[:n]...)
		}
		if err != nil || n == 0 {
			break
		}
	}

	// Find HTTP body after \r\n\r\n
	body := resp
	for i := 0; i+3 < len(resp); i++ {
		if resp[i] == '\r' && resp[i+1] == '\n' && resp[i+2] == '\r' && resp[i+3] == '\n' {
			body = resp[i+4:]
			break
		}
	}

	var result ExecResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return ExecResponse{}, fmt.Errorf("decode: %w (body=%q)", err, string(body))
	}
	return result, nil
}

var workerMu sync.Mutex

func ensureWorker() {
	workerMu.Lock()
	defer workerMu.Unlock()
	if runtimeWorker != nil {
		return
	}
	// Ensure PATH is set for LookPath
	os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")

	// Lazy start — handles both first boot and post-restore
	if _, err := exec.LookPath("node"); err == nil {
		w, err := startNodeWorker()
		if err == nil {
			runtimeWorker = w
			fmt.Fprintf(os.Stderr, "guest-agent: node worker started (lazy)\n")
		}
	} else if _, err := exec.LookPath("python3"); err == nil {
		w, err := startPythonWorker()
		if err == nil {
			runtimeWorker = w
			fmt.Fprintf(os.Stderr, "guest-agent: python worker started (lazy)\n")
		}
	}
}

func execCmd(req ExecRequest) ExecResponse {
	// Eval path: try the eval server (port 1025, pre-started in snapshot) first,
	// fall back to the worker REPL
	if req.Eval != "" {
		if resp, err := evalViaServer(req.Eval); err == nil {
			return resp
		}
		// Fall back to worker
		ensureWorker()
		if runtimeWorker != nil {
			return runtimeWorker.eval(req.Eval)
		}
	}

	// Shell path: spawn a process
	if len(req.Cmd) == 0 && req.Eval == "" {
		return ExecResponse{Stderr: "empty command", ExitCode: 1}
	}

	// If only eval was set but no worker, fall back to shell
	args := req.Cmd
	if len(args) == 0 && req.Eval != "" {
		// No worker — try to run via the runtime binary directly
		if _, err := exec.LookPath("node"); err == nil {
			args = []string{"node", "-e", req.Eval}
		} else if _, err := exec.LookPath("python3"); err == nil {
			args = []string{"python3", "-c", req.Eval}
		} else {
			return ExecResponse{Stderr: "no runtime available for eval", ExitCode: 1}
		}
	}

	cmd := exec.Command(args[0], args[1:]...)
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
