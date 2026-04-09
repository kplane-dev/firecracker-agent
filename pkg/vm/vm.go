// Package vm manages Firecracker microVM lifecycle via the Firecracker API socket.
package vm

import (
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
	"time"

	"k8s.io/klog/v2"
)

// Config holds the paths to Firecracker binaries and images.
type Config struct {
	// FirecrackerBin is the path to the firecracker binary.
	FirecrackerBin string
	// KernelPath is the path to the guest kernel (vmlinux).
	KernelPath string
	// RootfsPath is the path to the root filesystem (initrd or image).
	RootfsPath string
	// WorkDir is the directory for VM sockets and state.
	WorkDir string
}

// VM represents a running Firecracker microVM.
type VM struct {
	ID         string
	SocketPath string
	IP         string
	PID        int
	StartedAt  time.Time
	Config     Config

	cmd    *exec.Cmd
	client *http.Client
}

// Boot starts a new Firecracker VM and returns it.
func Boot(cfg Config, id string, vcpus int, memMiB int) (*VM, error) {
	socketDir := filepath.Join(cfg.WorkDir, id)
	if err := os.MkdirAll(socketDir, 0o755); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}
	socketPath := filepath.Join(socketDir, "fc.sock")

	// Remove stale socket
	os.Remove(socketPath)

	cmd := exec.Command(cfg.FirecrackerBin, "--api-sock", socketPath)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start firecracker: %w", err)
	}

	vm := &VM{
		ID:         id,
		SocketPath: socketPath,
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
		"initrd_path":       cfg.RootfsPath,
		"boot_args":         "console=ttyS0 reboot=k panic=1 pci=off quiet",
	}); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("set boot source: %w", err)
	}

	// Configure machine
	if err := vm.apiPut("/machine-config", map[string]interface{}{
		"vcpu_count":  vcpus,
		"mem_size_mib": memMiB,
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
func (vm *VM) Snapshot(snapshotDir string) error {
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}

	// Pause the VM (PATCH /vm, not PUT /actions)
	if err := vm.apiPatch("/vm", map[string]interface{}{
		"state": "Paused",
	}); err != nil {
		return fmt.Errorf("pause: %w", err)
	}

	// Create snapshot
	if err := vm.apiPut("/snapshot/create", map[string]interface{}{
		"snapshot_type": "Full",
		"snapshot_path": filepath.Join(snapshotDir, "vmstate"),
		"mem_file_path": filepath.Join(snapshotDir, "mem"),
	}); err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}

	klog.Infof("vm %s snapshot saved to %s", vm.ID, snapshotDir)
	return nil
}

// Restore boots a new VM from a snapshot.
func Restore(cfg Config, id string, snapshotDir string) (*VM, error) {
	socketDir := filepath.Join(cfg.WorkDir, id)
	if err := os.MkdirAll(socketDir, 0o755); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}
	socketPath := filepath.Join(socketDir, "fc.sock")
	os.Remove(socketPath)

	cmd := exec.Command(cfg.FirecrackerBin, "--api-sock", socketPath)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start firecracker: %w", err)
	}

	vm := &VM{
		ID:         id,
		SocketPath: socketPath,
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

	// Wait for socket
	for i := 0; i < 50; i++ {
		if _, err := os.Stat(socketPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Load snapshot
	start := time.Now()
	if err := vm.apiPut("/snapshot/load", map[string]interface{}{
		"snapshot_path":    filepath.Join(snapshotDir, "vmstate"),
		"mem_backend": map[string]interface{}{
			"backend_type":  "File",
			"backend_path":  filepath.Join(snapshotDir, "mem"),
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
	// Try reflink first (btrfs/XFS)
	cmd := exec.Command("cp", "--reflink=auto", src, dst)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("reflink copy %s -> %s: %w", src, dst, err)
	}
	return nil
}

// Branch snapshots the source VM and restores N copies from the snapshot.
// Uses reflink copies when the filesystem supports it (btrfs/XFS).
func Branch(cfg Config, source *VM, snapshotDir string, count int) ([]*VM, error) {
	// Snapshot the source
	start := time.Now()
	if err := source.Snapshot(snapshotDir); err != nil {
		return nil, fmt.Errorf("snapshot source: %w", err)
	}
	snapDuration := time.Since(start)
	klog.Infof("branch: snapshot took %v", snapDuration)

	// Restore N copies
	var branches []*VM
	for i := 0; i < count; i++ {
		branchID := fmt.Sprintf("%s-branch-%d", source.ID, i)
		branchSnapDir := filepath.Join(filepath.Dir(snapshotDir), branchID+"-snap")
		if err := os.MkdirAll(branchSnapDir, 0o755); err != nil {
			return branches, fmt.Errorf("create branch snap dir: %w", err)
		}

		// Reflink copy the snapshot files
		copyStart := time.Now()
		if err := ReflinkCopy(filepath.Join(snapshotDir, "vmstate"), filepath.Join(branchSnapDir, "vmstate")); err != nil {
			return branches, err
		}
		if err := ReflinkCopy(filepath.Join(snapshotDir, "mem"), filepath.Join(branchSnapDir, "mem")); err != nil {
			return branches, err
		}
		copyDuration := time.Since(copyStart)

		// Restore
		restoreStart := time.Now()
		vm, err := Restore(cfg, branchID, branchSnapDir)
		if err != nil {
			return branches, fmt.Errorf("restore branch %d: %w", i, err)
		}
		restoreDuration := time.Since(restoreStart)

		klog.Infof("branch %d: copy=%v restore=%v total=%v",
			i, copyDuration, restoreDuration, time.Since(copyStart))
		branches = append(branches, vm)
	}

	return branches, nil
}
