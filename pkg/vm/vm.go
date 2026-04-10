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
	"strings"
	"sync"
	"syscall"
	"time"

	"k8s.io/klog/v2"
)

// Config holds the paths to Firecracker binaries and images.
type Config struct {
	FirecrackerBin string
	KernelPath     string
	ImageDir       string
	WorkDir        string
}

func (c Config) ImagePath(image string) string {
	return filepath.Join(c.ImageDir, image+".initrd")
}

// VM represents a running Firecracker microVM.
type VM struct {
	ID          string
	Image       string
	SocketPath  string
	VsockPath   string
	BaselineDir string // baseline snapshot dir (for diff-based branching)
	PID         int
	StartedAt   time.Time
	Config      Config

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
	os.Remove(socketPath)
	os.Remove(filepath.Join(socketDir, "vsock.sock"))

	cmd := exec.Command(cfg.FirecrackerBin, "--api-sock", socketPath)
	cmd.Dir = socketDir
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start firecracker: %w", err)
	}

	vm := &VM{
		ID:         id,
		Image:      image,
		SocketPath: socketPath,
		VsockPath:  filepath.Join(socketDir, "vsock.sock"),
		Config:     cfg,
		cmd:        cmd,
		client:     newAPIClient(socketPath),
	}

	waitForSocket(socketPath)

	if err := vm.apiPut("/boot-source", map[string]interface{}{
		"kernel_image_path": cfg.KernelPath,
		"initrd_path":       cfg.ImagePath(image),
		"boot_args":         "console=ttyS0 reboot=k panic=1 pci=off quiet",
	}); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("set boot source: %w", err)
	}

	if err := vm.apiPut("/machine-config", map[string]interface{}{
		"vcpu_count":        vcpus,
		"mem_size_mib":      memMiB,
		"track_dirty_pages": true,
	}); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("set machine config: %w", err)
	}

	if err := vm.apiPut("/vsock", map[string]interface{}{
		"guest_cid": 3,
		"uds_path":  "vsock.sock",
	}); err != nil {
		cmd.Process.Kill()
		return nil, fmt.Errorf("set vsock: %w", err)
	}

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

// WaitReady polls the vsock guest agent until it responds or the timeout expires.
func (vm *VM) WaitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, _, err := vm.Exec([]string{"/bin/true"}, 2*time.Second)
		if err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("guest agent not ready after %v", timeout)
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

// Restore boots a new VM from a snapshot.
func Restore(cfg Config, id string, image string, vmstatePath string, memPath string) (*VM, error) {
	vm, err := PrepareRestore(cfg, id, image)
	if err != nil {
		return nil, err
	}
	if err := vm.LoadSnapshot(vmstatePath, memPath); err != nil {
		return nil, err
	}
	return vm, nil
}

// PrepareRestore starts a Firecracker process and waits for its API socket.
// Call LoadSnapshot to actually load VM state. This split allows overlapping
// process startup with other work (e.g., merge).
func PrepareRestore(cfg Config, id string, image string) (*VM, error) {
	socketDir := filepath.Join(cfg.WorkDir, id)
	if err := os.MkdirAll(socketDir, 0o755); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}
	socketPath := filepath.Join(socketDir, "fc.sock")
	os.Remove(socketPath)

	cmd := exec.Command(cfg.FirecrackerBin, "--api-sock", socketPath)
	cmd.Dir = socketDir
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start firecracker: %w", err)
	}

	vm := &VM{
		ID:         id,
		Image:      image,
		SocketPath: socketPath,
		VsockPath:  filepath.Join(socketDir, "vsock.sock"),
		Config:     cfg,
		cmd:        cmd,
		client:     newAPIClient(socketPath),
	}

	waitForSocket(socketPath)
	return vm, nil
}

// LoadSnapshot loads a snapshot into a prepared Firecracker process.
func (vm *VM) LoadSnapshot(vmstatePath string, memPath string) error {
	start := time.Now()
	if err := vm.apiPut("/snapshot/load", map[string]interface{}{
		"snapshot_path": vmstatePath,
		"mem_backend": map[string]interface{}{
			"backend_type": "File",
			"backend_path": memPath,
		},
		"enable_diff_snapshots": false,
		"resume_vm":             true,
	}); err != nil {
		vm.cmd.Process.Kill()
		return fmt.Errorf("load snapshot: %w", err)
	}

	vm.StartedAt = start
	vm.PID = vm.cmd.Process.Pid
	klog.Infof("vm %s restored in %v", vm.ID, time.Since(start))
	return nil
}

// Branch takes a diff snapshot, merges it with the baseline, and restores N copies in parallel.
// The merged mem file is shared across all branches (MAP_PRIVATE/CoW).
func Branch(cfg Config, source *VM, count int) ([]*VM, error) {
	if source.BaselineDir == "" {
		return branchFull(cfg, source, count)
	}

	// Write diff snapshot to tmpfs (snap-ram) for RAM-speed writes.
	snapBase := filepath.Join(cfg.WorkDir, "snap-ram")
	if _, err := os.Stat(snapBase); err != nil {
		snapBase = filepath.Join(cfg.WorkDir, "snapshots")
	}
	snapshotDir := filepath.Join(snapBase, source.ID)

	// Diff snapshot — only pages dirtied since baseline
	start := time.Now()
	if err := source.Snapshot(snapshotDir, true); err != nil {
		return nil, fmt.Errorf("diff snapshot: %w", err)
	}
	snapDuration := time.Since(start)

	if err := source.Resume(); err != nil {
		klog.Warningf("branch: failed to resume source: %v", err)
	}
	klog.Infof("branch: diff snapshot took %v, source resumed", snapDuration)

	srcVmstate := filepath.Join(snapshotDir, "vmstate")

	// Pre-start Firecracker processes + copy vmstate while merge runs.
	// This overlaps process startup (~5ms each) with the merge step.
	type prepared struct {
		vm        *VM
		vmstate   string
		err       error
	}
	preps := make([]prepared, count)
	var prepWg sync.WaitGroup

	for i := 0; i < count; i++ {
		prepWg.Add(1)
		go func(i int) {
			defer prepWg.Done()
			branchID := fmt.Sprintf("%s-branch-%d", source.ID, i)
			branchDir := filepath.Join(cfg.WorkDir, branchID)
			if err := os.MkdirAll(branchDir, 0o755); err != nil {
				preps[i] = prepared{err: err}
				return
			}
			branchVmstate := filepath.Join(branchDir, "vmstate")
			if err := ReflinkCopy(srcVmstate, branchVmstate); err != nil {
				preps[i] = prepared{err: err}
				return
			}
			vm, err := PrepareRestore(cfg, branchID, source.Image)
			if err != nil {
				preps[i] = prepared{err: err}
				return
			}
			preps[i] = prepared{vm: vm, vmstate: branchVmstate}
		}(i)
	}

	// Merge runs concurrently with process preparation above
	mergeDir := filepath.Join(cfg.WorkDir, "snapshots", source.ID)
	os.MkdirAll(mergeDir, 0o755)
	mergedMem := filepath.Join(mergeDir, "mem-merged")
	mergeStart := time.Now()
	if err := mergeSnapshot(
		filepath.Join(source.BaselineDir, "mem"),
		filepath.Join(snapshotDir, "mem"),
		mergedMem,
	); err != nil {
		return nil, fmt.Errorf("merge snapshot: %w", err)
	}
	klog.Infof("branch: merge took %v", time.Since(mergeStart))

	prepWg.Wait()

	// Load snapshots in parallel — processes are already running
	type result struct {
		idx int
		vm  *VM
		err error
	}
	ch := make(chan result, count)
	var loadWg sync.WaitGroup

	for i := 0; i < count; i++ {
		if preps[i].err != nil {
			ch <- result{idx: i, err: preps[i].err}
			continue
		}
		loadWg.Add(1)
		go func(i int) {
			defer loadWg.Done()
			loadStart := time.Now()
			if err := preps[i].vm.LoadSnapshot(preps[i].vmstate, mergedMem); err != nil {
				ch <- result{idx: i, err: err}
				preps[i].vm.Destroy()
				return
			}
			klog.Infof("branch %d: load=%v", i, time.Since(loadStart))
			ch <- result{idx: i, vm: preps[i].vm}
		}(i)
	}

	go func() { loadWg.Wait(); close(ch) }()

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

// branchFull is the fallback when no baseline exists (full snapshot).
func branchFull(cfg Config, source *VM, count int) ([]*VM, error) {
	snapBase := filepath.Join(cfg.WorkDir, "snap-ram")
	if _, err := os.Stat(snapBase); err != nil {
		snapBase = filepath.Join(cfg.WorkDir, "snapshots")
	}
	snapshotDir := filepath.Join(snapBase, source.ID)

	start := time.Now()
	if err := source.Snapshot(snapshotDir, false); err != nil {
		return nil, fmt.Errorf("snapshot source: %w", err)
	}
	klog.Infof("branch: full snapshot took %v", time.Since(start))

	if err := source.Resume(); err != nil {
		klog.Warningf("branch: failed to resume source: %v", err)
	}

	sharedMem := filepath.Join(snapshotDir, "mem")
	srcVmstate := filepath.Join(snapshotDir, "vmstate")

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
			branchDir := filepath.Join(cfg.WorkDir, branchID)
			os.MkdirAll(branchDir, 0o755)

			branchVmstate := filepath.Join(branchDir, "vmstate")
			ReflinkCopy(srcVmstate, branchVmstate)

			restoreStart := time.Now()
			vm, err := Restore(cfg, branchID, source.Image, branchVmstate, sharedMem)
			if err != nil {
				ch <- result{idx: i, err: err}
				return
			}
			klog.Infof("branch %d: restore=%v", i, time.Since(restoreStart))
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

// mergeSnapshot creates a complete memory file by reflinking the base and
// overlaying only the dirty pages from the diff using SEEK_DATA/SEEK_HOLE.
func mergeSnapshot(baseMem, diffMem, outputMem string) error {
	if err := ReflinkCopy(baseMem, outputMem); err != nil {
		return fmt.Errorf("reflink base: %w", err)
	}

	diff, err := os.Open(diffMem)
	if err != nil {
		return fmt.Errorf("open diff: %w", err)
	}
	defer diff.Close()

	out, err := os.OpenFile(outputMem, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}
	defer out.Close()

	diffFd := int(diff.Fd())
	fi, err := diff.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()

	const (
		seekData = 3 // SEEK_DATA
		seekHole = 4 // SEEK_HOLE
	)

	buf := make([]byte, 256*1024) // 256KB copy buffer
	offset := int64(0)
	copied := int64(0)

	for offset < fileSize {
		dataStart, err := syscall.Seek(diffFd, offset, seekData)
		if err != nil {
			break
		}
		holeStart, err := syscall.Seek(diffFd, dataStart, seekHole)
		if err != nil {
			holeStart = fileSize
		}

		for pos := dataStart; pos < holeStart; {
			n := int64(len(buf))
			if pos+n > holeStart {
				n = holeStart - pos
			}
			nr, err := diff.ReadAt(buf[:n], pos)
			if nr > 0 {
				if _, werr := out.WriteAt(buf[:nr], pos); werr != nil {
					return fmt.Errorf("write at %d: %w", pos, werr)
				}
				copied += int64(nr)
			}
			if err != nil {
				break
			}
			pos += int64(nr)
		}

		offset = holeStart
	}

	klog.Infof("merge: overlaid %d bytes of dirty pages onto reflinked base", copied)
	return nil
}

// ReflinkCopy copies a file using reflink (CoW) if supported.
func ReflinkCopy(src, dst string) error {
	cmd := exec.Command("cp", "--reflink=auto", src, dst)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("reflink copy %s -> %s: %w", src, dst, err)
	}
	return nil
}

func vsockConnect(udsPath string, port uint32) (net.Conn, error) {
	conn, err := net.Dial("unix", udsPath)
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", port); err != nil {
		conn.Close()
		return nil, err
	}
	var resp []byte
	buf := make([]byte, 1)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("read response: %w", err)
		}
		if n > 0 {
			resp = append(resp, buf[0])
			if buf[0] == '\n' {
				break
			}
		}
	}
	if !strings.HasPrefix(string(resp), "OK ") {
		conn.Close()
		return nil, fmt.Errorf("vsock CONNECT failed: %s", strings.TrimSpace(string(resp)))
	}
	return conn, nil
}

func newAPIClient(socketPath string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", socketPath)
			},
		},
		Timeout: 5 * time.Second,
	}
}

func waitForSocket(path string) {
	for i := 0; i < 50; i++ {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
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
