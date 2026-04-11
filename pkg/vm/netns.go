package vm

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"k8s.io/klog/v2"
)

// NetNS is a network namespace with a TAP device ready for a Firecracker VM.
// Held open by a sentinel process. All VMs share guest IP 10.0.0.2.
type NetNS struct {
	SentinelPID int
	NSPath      string // /proc/<pid>/ns/net
	cmd         *exec.Cmd
}

// CreateNetNS creates a new network namespace with a TAP device configured.
func CreateNetNS() (*NetNS, error) {
	cmd := exec.Command("unshare", "--net", "sleep", "infinity")
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start sentinel: %w", err)
	}

	ns := &NetNS{
		SentinelPID: cmd.Process.Pid,
		NSPath:      fmt.Sprintf("/proc/%d/ns/net", cmd.Process.Pid),
		cmd:         cmd,
	}

	if err := ns.hostExec("ip", "link", "set", "lo", "up"); err != nil {
		ns.Destroy()
		return nil, fmt.Errorf("lo up: %w", err)
	}
	if err := ns.hostExec("ip", "tuntap", "add", "name", "vmtap0", "mode", "tap"); err != nil {
		ns.Destroy()
		return nil, fmt.Errorf("create tap: %w", err)
	}
	if err := ns.hostExec("ip", "addr", "add", "10.0.0.1/30", "dev", "vmtap0"); err != nil {
		ns.Destroy()
		return nil, fmt.Errorf("set ip: %w", err)
	}
	if err := ns.hostExec("ip", "link", "set", "vmtap0", "up"); err != nil {
		ns.Destroy()
		return nil, fmt.Errorf("link up: %w", err)
	}

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

// hostExec runs a command inside the namespace using the host's binaries.
func (ns *NetNS) hostExec(args ...string) error {
	cmdArgs := []string{"--target", "1", "--mount", fmt.Sprintf("--net=%s", ns.NSPath), "--"}
	cmdArgs = append(cmdArgs, args...)
	out, err := exec.Command("nsenter", cmdArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s: %w", args[0], strings.TrimSpace(string(out)), err)
	}
	return nil
}

// ExecInNS runs a command in the guest via the network namespace.
// Uses nsenter as a subprocess to avoid corrupting Go runtime's network state.
func ExecInNS(nsPath, guestAddr string, req interface{}, resp interface{}) error {
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	// nc sends the JSON and reads the response via the namespace's TCP.
	host, port, _ := strings.Cut(guestAddr, ":")
	script := fmt.Sprintf(
		`printf '%%s\n' '%s' | nc -w 5 %s %s`,
		strings.ReplaceAll(string(reqJSON), "'", "'\\''"),
		host, port,
	)

	cmd := exec.Command("nsenter", fmt.Sprintf("--net=%s", nsPath), "--", "sh", "-c", script)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("nsenter exec: %w", err)
	}
	if len(out) == 0 {
		return fmt.Errorf("empty response from guest")
	}
	return json.Unmarshal(out, resp)
}

// EvalInNS sends code to the eval server (port 1025) via the namespace.
func EvalInNS(nsPath, addr, code string, resp interface{}) error {
	host, port, _ := strings.Cut(addr, ":")

	script := fmt.Sprintf(
		`wget -qO- --post-data='%s' http://%s:%s/ 2>/dev/null`,
		strings.ReplaceAll(code, "'", "'\\''"),
		host, port,
	)

	cmd := exec.Command("nsenter", fmt.Sprintf("--net=%s", nsPath), "--", "sh", "-c", script)
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("eval: %w", err)
	}
	if len(out) == 0 {
		return fmt.Errorf("empty eval response")
	}
	return json.Unmarshal(out, resp)
}

func init() {
	klog.V(4).Infof("netns support initialized")
}
