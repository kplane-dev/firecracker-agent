package vm

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"k8s.io/klog/v2"
)

// NetNS is a network namespace with a TAP device ready for a Firecracker VM.
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

// ExecInNS sends a JSON command to the guest agent via the network namespace.
// Uses the nsexec helper binary — one fork, no shell chain, no Go runtime corruption.
func ExecInNS(nsPath, guestAddr string, req interface{}, resp interface{}) error {
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	cmd := exec.Command("nsexec", "-timing", nsPath, guestAddr, string(reqJSON))
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if stderr.Len() > 0 {
		klog.V(4).Infof("nsexec: %s", stderr.String())
	}
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("nsexec: %s", string(exitErr.Stderr))
		}
		return fmt.Errorf("nsexec: %w", err)
	}
	if len(out) == 0 {
		return fmt.Errorf("empty response")
	}
	return json.Unmarshal(out, resp)
}

// EvalInNS sends code to the eval HTTP server via the namespace.
// Uses nsexec to POST to the eval server on port 1025.
func EvalInNS(nsPath, addr, code string, resp interface{}) error {
	// Build a minimal HTTP POST request as the JSON payload
	// The eval server accepts raw code as POST body and returns JSON
	httpReq := fmt.Sprintf("POST / HTTP/1.0\r\nContent-Length: %d\r\n\r\n%s", len(code), code)

	out, err := exec.Command("nsexec", nsPath, addr, httpReq).Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("nsexec eval: %s", string(exitErr.Stderr))
		}
		return fmt.Errorf("nsexec eval: %w", err)
	}

	// Parse HTTP response — find body after \r\n\r\n
	body := out
	for i := 0; i+3 < len(out); i++ {
		if out[i] == '\r' && out[i+1] == '\n' && out[i+2] == '\r' && out[i+3] == '\n' {
			body = out[i+4:]
			break
		}
	}
	if len(body) == 0 {
		return fmt.Errorf("empty eval response")
	}
	return json.Unmarshal(body, resp)
}

func init() {
	klog.V(4).Infof("netns support initialized")
}
