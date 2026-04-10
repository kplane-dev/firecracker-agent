// Package vm manages Firecracker microVM lifecycle via the Firecracker API socket.
package vm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// Config holds the paths to Firecracker binaries and images.
type Config struct {
	// FirecrackerBin is the path to the firecracker binary.
	FirecrackerBin string
	// KernelPath is the path to the guest kernel (vmlinux).
	KernelPath string
	// ImageDir is the directory containing rootfs images (<name>.initrd).
	ImageDir string
	// WorkDir is the directory for VM sockets and state.
	WorkDir string
}

// ImagePath returns the full path to a rootfs image.
func (c Config) ImagePath(image string) string {
	return filepath.Join(c.ImageDir, image+".initrd")
}

// VM represents a running Firecracker microVM.
type VM struct {
	ID         string
	Image      string
	SocketPath string
	VsockPath  string
	IP         string
	PID        int
	StartedAt  time.Time
	Config     Config

	cmd    *exec.Cmd
	client *http.Client
}

// Boot starts a new Firecracker VM and returns it.
func Boot(cfg Config, id string, image string, vcpus int, memMiB int) (*VM, error) {
	socketDir := filepath.Join(cfg.WorkDir, id)
	if err := os.MkdirAll(socketDir, 0o755); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}
	socketPath := filepath.Join(socketDir, "fc.sock")
	vsockPath := filepath.Join(socketDir, "vsock.sock")

	// Remove stale sockets
	os.Remove(socketPath)
	os.Remove(vsockPath)

	cmd := exec.Command(cfg.FirecrackerBin, "--api-sock", socketPath)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start firecracker: %w", err)
	}

	vm := &VM{
		ID:         id,
		Image:      image,
		SocketPath: socketPath,
		VsockPath:  vsockPath,
		Config:     cfg,
		cmd:        cmd,
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", socketPath)
				},
			},
			Timeout: 5 * time.Second,
		},
	}

	// Wait for the socket to appear
	for i := 0; i < 50; i++ {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Configure boot source
	if err := vm.apiPut("/boot-source", map[string]interface{}{
		"kernel_image_path": cfg.KernelPath,
		"initrd_path":       cfg.ImagePath(image),
		"boot_args":         "console=ttyS0 reboot=k panic=1 pci=off quiet",
	}); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("set boot source: %w", err)
	}

	// Configure machine with dirty page tracking for diff snapshots
	if err := vm.apiPut("/machine-config", map[string]interface{}{
		"vcpu_count":        vcpus,
		"mem_size_mib":      memMiB,
		"track_dirty_pages": true,
	}); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("set machine config: %w", err)
	}

	// Start the VM
	start := time.Now()
	if err := vm.apiPut("/actions", map[string]interface{}{
		"action_type": "InstanceStart",
	}); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("start instance: %w", err)
	}
	vm.StartedAt = start
	vm.PID = cmd.Process.Pid

	klog.Infof("vm %s booted in %v (pid=%d)", id, time.Since(start), vm.PID)
	return vm, nil
}

// Destroy kills the Firecracker process and cleans up.
func (vm *VM) Destroy() error {
	if vm.cmd != nil && vm.cmd.Process != nil {
		vm.cmd.Process.Kill()
		vm.cmd.Wait()
	}
	os.RemoveAll(filepath.Dir(vm.SocketPath))
	klog.Infof("vm %s destroyed", vm.ID)
	return nil
}

// Snapshot pauses the VM and saves its state.
// If diff is true, only dirty pages since last snapshot are saved.
// If diff is false, a full snapshot is taken.
func (vm *VM) Snapshot(snapshotDir string, diff bool) error {
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}

	if err := vm.apiPatch("/vm", map[string]interface{}{
		"state": "Paused",
	}); err != nil {
		return fmt.Errorf("pause: %w", err)
	}

	snapshotType := "Full"
	if diff {
		snapshotType = "Diff"
	}

	if err := vm.apiPut("/snapshot/create", map[string]interface{}{
		"snapshot_type": snapshotType,
		"snapshot_path": filepath.Join(snapshotDir, "vmstate"),
		"mem_file_path": filepath.Join(snapshotDir, "mem"),
	}); err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}

	klog.Infof("vm %s %s snapshot saved to %s", vm.ID, snapshotType, snapshotDir)
	return nil
}

// Resume unpauses the VM after a snapshot.
func (vm *VM) Resume() error {
	return vm.apiPatch("/vm", map[string]interface{}{
		"state": "Resumed",
	})
}

// ResetDirtyPages takes a throwaway diff snapshot to clear the dirty page bitmap.
// This makes subsequent diff snapshots much smaller and faster.
func (vm *VM) ResetDirtyPages() error {
	tmpDir := filepath.Join(vm.Config.WorkDir, ".warmup-"+vm.ID)
	defer os.RemoveAll(tmpDir)

	if err := vm.Snapshot(tmpDir, true); err != nil {
		return err
	}
	return vm.Resume()
}

// Exec runs a command inside the VM via the vsock guest agent.
func (vm *VM) Exec(cmd []string, timeout time.Duration) (stdout, stderr string, exitCode int, err error) {
	conn, err := vsockConnect(vm.VsockPath, 1024)
	if err != nil {
		return "", "", -1, fmt.Errorf("vsock connect: %w", err)
	}
	if timeout > 0 {
		conn.SetDeadline(time.Now().Add(timeout))
	}
	defer conn.Close()

	req := struct {
		Cmd []string `json:"cmd"`
	}{Cmd: cmd}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return "", "", -1, fmt.Errorf("send: %w", err)
	}

	var resp struct {
		Stdout   string `json:"stdout"`
		Stderr   string `json:"stderr"`
		ExitCode int    `json:"exit_code"`
	}
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return "", "", -1, fmt.Errorf("recv: %w", err)
	}
	return resp.Stdout, resp.Stderr, resp.ExitCode, nil
}

// vsockConnect connects to a guest vsock port via Firecracker's UDS.
func vsockConnect(udsPath string, port uint32) (net.Conn, error) {
	conn, err := net.Dial("unix", udsPath)
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", port); err != nil {
		conn.Close()
		return nil, err
	}
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read response: %w", err)
	}
	if !strings.HasPrefix(line, "OK ") {
		conn.Close()
		return nil, fmt.Errorf("vsock CONNECT failed: %s", strings.TrimSpace(line))
	}
	return conn, nil
}

// Restore boots a new VM from a snapshot.
func Restore(cfg Config, id string, image string, snapshotDir string) (*VM, error) {
	socketDir := filepath.Join(cfg.WorkDir, id)
	if err := os.MkdirAll(socketDir, 0o755); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}
	socketPath := filepath.Join(socketDir, "fc.sock")
	vsockPath := filepath.Join(socketDir, "vsock.sock")
	os.Remove(socketPath)
	os.Remove(vsockPath)

	cmd := exec.Command(cfg.FirecrackerBin, "--api-sock", socketPath)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start firecracker: %w", err)
	}

	vm := &VM{
		ID:         id,
		Image:      image,
		SocketPath: socketPath,
		VsockPath:  vsockPath,
		Config:     cfg,
		cmd:        cmd,
		client: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return net.Dial("unix", socketPath)
				},
			},
			Timeout: 5 * time.Second,
		},
	}

	for i := 0; i < 50; i++ {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	start := time.Now()
	if err := vm.apiPut("/snapshot/load", map[string]interface{}{
		"snapshot_path": filepath.Join(snapshotDir, "vmstate"),
		"mem_backend": map[string]interface{}{
			"backend_type": "File",
			"backend_path": filepath.Join(snapshotDir, "mem"),
		},
		"enable_diff_snapshots": false,
		"resume_vm":             true,
	}); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("load snapshot: %w", err)
	}

	vm.StartedAt = start
	vm.PID = cmd.Process.Pid

	klog.Infof("vm %s restored from %s in %v", id, snapshotDir, time.Since(start))
	return vm, nil
}

func (vm *VM) apiRequest(method, path string, body interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(method, "http://localhost"+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := vm.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API %s %s returned %d: %s", method, path, resp.StatusCode, string(respBody))
	}
	return nil
}

func (vm *VM) apiPut(path string, body interface{}) error {
	return vm.apiRequest(http.MethodPut, path, body)
}

func (vm *VM) apiPatch(path string, body interface{}) error {
	return vm.apiRequest(http.MethodPatch, path, body)
}

// ReflinkCopy copies a file using reflink (CoW) if supported, falling back to regular copy.
func ReflinkCopy(src, dst string) error {
	cmd := exec.Command("cp", "--reflink=auto", src, dst)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("reflink copy %s -> %s: %w", src, dst, err)
	}
	return nil
}

// Branch takes a diff snapshot of the source VM and restores N copies in parallel.
// The source VM is resumed immediately after the snapshot.
// Uses reflink copies when the filesystem supports it (btrfs/XFS).
func Branch(cfg Config, source *VM, snapshotDir string, count int) ([]*VM, error) {
	start := time.Now()
	if err := source.Snapshot(snapshotDir, true); err != nil {
		return nil, fmt.Errorf("snapshot source: %w", err)
	}
	snapDuration := time.Since(start)

	if err := source.Resume(); err != nil {
		klog.Warningf("branch: failed to resume source VM %s: %v", source.ID, err)
	}
	klog.Infof("branch: diff snapshot took %v, source resumed", snapDuration)

	// Restore all branches in parallel
	type result struct {
		idx int
		vm  *VM
		err error
	}
	ch := make(chan result, count)

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			branchID := fmt.Sprintf("%s-branch-%d", source.ID, i)
			branchSnapDir := filepath.Join(filepath.Dir(snapshotDir), branchID+"-snap")
			if err := os.MkdirAll(branchSnapDir, 0o755); err != nil {
				ch <- result{idx: i, err: fmt.Errorf("mkdir: %w", err)}
				return
			}

			copyStart := time.Now()
			if err := ReflinkCopy(filepath.Join(snapshotDir, "vmstate"), filepath.Join(branchSnapDir, "vmstate")); err != nil {
				ch <- result{idx: i, err: err}
				return
			}
			if err := ReflinkCopy(filepath.Join(snapshotDir, "mem"), filepath.Join(branchSnapDir, "mem")); err != nil {
				ch <- result{idx: i, err: err}
				return
			}
			copyDuration := time.Since(copyStart)

			restoreStart := time.Now()
			vm, err := Restore(cfg, branchID, source.Image, branchSnapDir)
			if err != nil {
				ch <- result{idx: i, err: fmt.Errorf("restore branch %d: %w", i, err)}
				return
			}
			restoreDuration := time.Since(restoreStart)

			klog.Infof("branch %d: copy=%v restore=%v total=%v",
				i, copyDuration, restoreDuration, time.Since(copyStart))
			ch <- result{idx: i, vm: vm}
		}(i)
	}

	go func() { wg.Wait(); close(ch) }()

	branches := make([]*VM, count)
	var firstErr error
	for r := range ch {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
		}
		if r.vm != nil {
			branches[r.idx] = r.vm
		}
	}

	if firstErr != nil {
		for _, b := range branches {
			if b != nil {
				b.Destroy()
			}
		}
		return nil, firstErr
	}
	return branches, nil
}
