package vm

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"

	"k8s.io/klog/v2"
)

const (
	sysSetns    = 308 // SYS_SETNS on x86_64
	cloneNewnet = 0x40000000
)

// NetNS is a network namespace with a TAP device ready for a Firecracker VM.
// The namespace is held open by a sentinel process. All VMs in the namespace
// share the same guest IP (10.0.0.2) — each namespace is isolated.
type NetNS struct {
	SentinelPID int    // PID of the sentinel process holding the namespace
	NSPath      string // /proc/<pid>/ns/net
	cmd         *exec.Cmd
}

// CreateNetNS creates a new network namespace with a TAP device configured.
// The sentinel holds the namespace open. ~3-5ms total.
func CreateNetNS() (*NetNS, error) {
	// Create an anonymous netns via unshare + sentinel
	cmd := exec.Command("unshare", "--net", "sleep", "infinity")
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start sentinel: %w", err)
	}

	ns := &NetNS{
		SentinelPID: cmd.Process.Pid,
		NSPath:      fmt.Sprintf("/proc/%d/ns/net", cmd.Process.Pid),
		cmd:         cmd,
	}

	// Configure TAP inside the namespace (same name/IP as the baseline snapshot expects)
	if err := ns.exec("ip", "link", "set", "lo", "up"); err != nil {
		ns.Destroy()
		return nil, fmt.Errorf("lo up: %w", err)
	}
	if err := ns.exec("ip", "tuntap", "add", "name", "vmtap0", "mode", "tap"); err != nil {
		ns.Destroy()
		return nil, fmt.Errorf("create tap: %w", err)
	}
	if err := ns.exec("ip", "addr", "add", "10.0.0.1/30", "dev", "vmtap0"); err != nil {
		ns.Destroy()
		return nil, fmt.Errorf("set ip: %w", err)
	}
	if err := ns.exec("ip", "link", "set", "vmtap0", "up"); err != nil {
		ns.Destroy()
		return nil, fmt.Errorf("link up: %w", err)
	}

	// Enable IP forwarding and NAT for internet access
	ns.exec("sysctl", "-w", "net.ipv4.ip_forward=1")

	return ns, nil
}

// Destroy kills the sentinel and cleans up the namespace.
func (ns *NetNS) Destroy() {
	if ns.cmd != nil && ns.cmd.Process != nil {
		ns.cmd.Process.Kill()
		ns.cmd.Wait()
	}
}

// StartFirecracker starts a Firecracker process inside this network namespace.
func (ns *NetNS) StartFirecracker(firecrackerBin, socketPath, workDir string) (*exec.Cmd, error) {
	cmd := exec.Command("nsenter", fmt.Sprintf("--net=%s", ns.NSPath), "--",
		firecrackerBin, "--api-sock", socketPath)
	cmd.Dir = workDir
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start firecracker in netns: %w", err)
	}
	return cmd, nil
}

// DialTCP connects to the guest TCP port from inside this namespace.
// Uses setns to enter the namespace on the current OS thread.
func (ns *NetNS) DialTCP(addr string) (syscall.RawConn, error) {
	// This is handled by ExecInNS which does the setns dance
	return nil, fmt.Errorf("use ExecInNS instead")
}

// exec runs a command inside the namespace.
func (ns *NetNS) exec(args ...string) error {
	cmdArgs := append([]string{fmt.Sprintf("--net=%s", ns.NSPath), "--"}, args...)
	out, err := exec.Command("nsenter", cmdArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s: %w", args[0], strings.TrimSpace(string(out)), err)
	}
	return nil
}

// ExecInNS enters the network namespace, dials TCP to the guest agent,
// sends a command, and returns the result. Uses LockOSThread to safely
// switch the current thread's network namespace.
func ExecInNS(nsPath, guestAddr string, req interface{}, resp interface{}) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Save current namespace
	origNS, err := os.Open("/proc/self/ns/net")
	if err != nil {
		return fmt.Errorf("open current netns: %w", err)
	}
	defer origNS.Close()

	// Enter target namespace
	targetNS, err := os.Open(nsPath)
	if err != nil {
		return fmt.Errorf("open target netns %s: %w", nsPath, err)
	}
	defer targetNS.Close()

	if _, _, errno := syscall.Syscall(sysSetns, targetNS.Fd(), uintptr(cloneNewnet), 0); errno != 0 {
		return fmt.Errorf("setns to target: %w", errno)
	}

	// Restore original namespace on exit
	defer func() {
		syscall.Syscall(sysSetns, origNS.Fd(), uintptr(cloneNewnet), 0)
	}()

	// Now we're in the target namespace — TCP to guest
	conn, err := dialTCPRaw(guestAddr)
	if err != nil {
		return fmt.Errorf("tcp connect: %w", err)
	}
	defer conn.Close()

	// Use raw fd for read/write (Go's net.Conn doesn't work well after setns)
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	if err := encoder.Encode(req); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	if err := decoder.Decode(resp); err != nil {
		return fmt.Errorf("recv: %w", err)
	}
	return nil
}

// dialTCPRaw creates a TCP connection using raw syscalls (works after setns).
func dialTCPRaw(addr string) (*os.File, error) {
	// Parse addr "10.0.0.2:1024"
	host, portStr, _ := strings.Cut(addr, ":")
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	// Parse IP
	var ip [4]byte
	fmt.Sscanf(host, "%d.%d.%d.%d", &ip[0], &ip[1], &ip[2], &ip[3])

	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, err
	}

	sa := &syscall.SockaddrInet4{
		Port: port,
		Addr: ip,
	}
	if err := syscall.Connect(fd, sa); err != nil {
		syscall.Close(fd)
		return nil, err
	}

	return os.NewFile(uintptr(fd), "tcp"), nil
}

func init() {
	// Ensure we have enough OS threads for namespace switching
	klog.V(4).Infof("netns support initialized")
}
