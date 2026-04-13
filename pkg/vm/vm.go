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
	return filepath.Join(c.ImageDir, image+".ext4")
}

// VM represents a running Firecracker microVM.
type VM struct {
	ID          string
	Image       string
	SocketPath  string
	VsockPath   string
	BaselineDir string   // baseline snapshot dir (for diff-based branching)
	Net         Network  // network configuration (legacy, per-VM TAP)
	NS          *NetNS   // network namespace (preferred — zero post-restore config)
	CPUMillis   int      // CPU limit in millicores (0 = no limit)
	MemoryMiB   int      // booted memory size
	BalloonMiB  int      // current balloon inflation (memory reclaimed from guest)
	PID         int
	StartedAt   time.Time
	Config      Config

	cmd         *exec.Cmd
	uffdHandler *exec.Cmd    // uffd page fault handler process
	client      *http.Client
	serialIn    io.Writer       // Firecracker stdin → guest serial
	serialOut   *bufio.Scanner // Firecracker stdout → guest serial output
	serialMu    sync.Mutex     // serialize serial commands
}

// Boot starts a new Firecracker VM and returns it.
// If net is non-empty, a TAP device is created and a network interface is configured.
func Boot(cfg Config, id string, image string, vcpus int, memMiB int, net ...Network) (*VM, error) {
	socketDir := filepath.Join(cfg.WorkDir, id)
	if err := os.MkdirAll(socketDir, 0o755); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}
	socketPath := filepath.Join(socketDir, "fc.sock")
	os.Remove(socketPath)
	os.Remove(filepath.Join(socketDir, "vsock.sock"))

	var vmNet Network
	if len(net) > 0 {
		vmNet = net[0]
	}

	var cmd *exec.Cmd
	// If the VM has a namespace assigned, start Firecracker inside it
	if vmNet.nsPath != "" {
		cmd = exec.Command("nsenter", fmt.Sprintf("--net=%s", vmNet.nsPath), "--",
			cfg.FirecrackerBin, "--api-sock", socketPath)
	} else {
		cmd = exec.Command(cfg.FirecrackerBin, "--api-sock", socketPath)
	}
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
		Net:        vmNet,
		Config:     cfg,
		cmd:        cmd,
		client:     newAPIClient(socketPath),
	}

	waitForSocket(socketPath)

	// cleanup destroys the VM on error (kills process, removes TAP, cleans socket dir)
	cleanup := func(err error) (*VM, error) {
		vm.Destroy()
		return nil, err
	}

	if err := vm.apiPut("/boot-source", map[string]interface{}{
		"kernel_image_path": cfg.KernelPath,
		"boot_args":         "console=ttyS0 reboot=k panic=1 pci=off quiet init=/init root=/dev/vda ro",
	}); err != nil {
		return cleanup(fmt.Errorf("set boot source: %w", err))
	}

	// Block device rootfs — demand-paged, no initrd unpack overhead
	if err := vm.apiPut("/drives/rootfs", map[string]interface{}{
		"drive_id":       "rootfs",
		"path_on_host":   cfg.ImagePath(image),
		"is_root_device": true,
		"is_read_only":   true,
	}); err != nil {
		return cleanup(fmt.Errorf("set drive: %w", err))
	}

	if err := vm.apiPut("/machine-config", map[string]interface{}{
		"vcpu_count":        vcpus,
		"mem_size_mib":      memMiB,
		"track_dirty_pages": true,
	}); err != nil {
		return cleanup(fmt.Errorf("set machine config: %w", err))
	}

	if err := vm.apiPut("/vsock", map[string]interface{}{
		"guest_cid": 3,
		"uds_path":  "vsock.sock",
	}); err != nil {
		return cleanup(fmt.Errorf("set vsock: %w", err))
	}

	// Balloon device for dynamic memory adjustment after boot
	if err := vm.apiPut("/balloon", map[string]interface{}{
		"amount_mib":               0,
		"deflate_on_oom":           true,
		"stats_polling_interval_s": 0,
	}); err != nil {
		klog.Warningf("vm %s: balloon config failed (continuing without): %v", id, err)
	}

	vm.MemoryMiB = memMiB

	// Network interface (TAP must be created on host before configuring FC)
	if vm.Net.TAPName != "" {
		if err := vm.apiPut("/network-interfaces/eth0", map[string]interface{}{
			"iface_id":      "eth0",
			"guest_mac":     vm.Net.GuestMAC,
			"host_dev_name": vm.Net.TAPName,
		}); err != nil {
			return cleanup(fmt.Errorf("set network interface: %w", err))
		}
	}

	start := time.Now()
	if err := vm.apiPut("/actions", map[string]interface{}{
		"action_type": "InstanceStart",
	}); err != nil {
		return cleanup(fmt.Errorf("start instance: %w", err))
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
	if vm.uffdHandler != nil && vm.uffdHandler.Process != nil {
		vm.uffdHandler.Process.Kill()
		vm.uffdHandler.Wait()
	}
	if vm.NS != nil {
		vm.NS.Destroy()
	} else if vm.Net.TAPName != "" {
		vm.Net.TeardownTAP()
	}
	os.RemoveAll(filepath.Dir(vm.SocketPath))
	klog.Infof("vm %s destroyed", vm.ID)
	return nil
}

// SetBalloon inflates or deflates the memory balloon.
// amountMiB is how much memory to RECLAIM from the guest.
// Guest available memory = MemoryMiB - amountMiB.
func (vm *VM) SetBalloon(amountMiB int) error {
	if err := vm.apiPatch("/balloon", map[string]interface{}{
		"amount_mib": amountMiB,
	}); err != nil {
		return fmt.Errorf("set balloon to %dMiB: %w", amountMiB, err)
	}
	vm.BalloonMiB = amountMiB
	klog.Infof("vm %s: balloon=%dMiB (guest has %dMiB)", vm.ID, amountMiB, vm.MemoryMiB-amountMiB)
	return nil
}

// SetCPULimit applies a cgroup v2 CPU quota to the Firecracker process.
// milliCPU uses the Kubernetes convention: 1000 = 1 full core, 500 = half core.
// Uses the host cgroup hierarchy (/host/sys/fs/cgroup) since the container's
// cgroup doesn't allow moving processes to sub-cgroups.
func (vm *VM) SetCPULimit(milliCPU int) error {
	if milliCPU <= 0 {
		return nil
	}

	// Create a cgroup for this VM in the host hierarchy.
	// /host is the hostPath mount of /.
	hostCgroupBase := "/host/sys/fs/cgroup/firecracker.slice"
	vmCgroup := filepath.Join(hostCgroupBase, vm.ID)
	if err := os.MkdirAll(vmCgroup, 0o755); err != nil {
		return fmt.Errorf("create cgroup %s: %w", vmCgroup, err)
	}

	// Enable cpu controller
	os.WriteFile(filepath.Join(hostCgroupBase, "cgroup.subtree_control"), []byte("+cpu\n"), 0o644)

	// Set CPU quota: milliCPU 500 → 50000µs per 100000µs period
	quota := milliCPU * 100
	period := 100000
	if err := os.WriteFile(filepath.Join(vmCgroup, "cpu.max"),
		[]byte(fmt.Sprintf("%d %d\n", quota, period)), 0o644); err != nil {
		return fmt.Errorf("set cpu.max: %w", err)
	}

	// Move Firecracker process into the cgroup (use host PID since hostPID=true)
	if err := os.WriteFile(filepath.Join(vmCgroup, "cgroup.procs"),
		[]byte(fmt.Sprintf("%d\n", vm.PID)), 0o644); err != nil {
		return fmt.Errorf("move pid %d to cgroup: %w", vm.PID, err)
	}

	vm.CPUMillis = milliCPU
	klog.Infof("vm %s: cpu=%dm (pid %d → %s)", vm.ID, milliCPU, vm.PID, vmCgroup)
	return nil
}

// Network holds the network configuration for a VM.
type Network struct {
	TAPName  string // host TAP device name
	HostIP   string // host-side IP (e.g., 10.0.1.1)
	GuestIP  string // guest-side IP (e.g., 10.0.1.2)
	GuestMAC string // guest MAC address
	nsPath   string // network namespace path (if running in a netns)
}

// NetworkInNS returns a network config for a VM running in a network namespace.
func NetworkInNS(ns *NetNS) Network {
	return Network{
		TAPName:  "vmtap0",
		HostIP:   "10.0.0.1",
		GuestIP:  "10.0.0.2",
		GuestMAC: "AA:FC:00:00:00:01",
		nsPath:   ns.NSPath,
	}
}

// AllocateNetwork derives network params from an index.
func AllocateNetwork(vmID string, index int) Network {
	return Network{
		TAPName:  fmt.Sprintf("vmtap-%s", vmID),
		HostIP:   fmt.Sprintf("10.0.%d.1", index),
		GuestIP:  fmt.Sprintf("10.0.%d.2", index),
		GuestMAC: fmt.Sprintf("AA:FC:00:00:%02X:%02X", index/256, index%256),
	}
}

// hostExec runs a command in the host's mount+net namespace via nsenter.
func hostExec(args ...string) error {
	cmdArgs := append([]string{"--target", "1", "--mount", "--net", "--"}, args...)
	out, err := exec.Command("nsenter", cmdArgs...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s: %w", args[0], strings.TrimSpace(string(out)), err)
	}
	return nil
}

// SetupTAP creates a TAP device on the host and configures it.
func (n Network) SetupTAP() error {
	if err := hostExec("ip", "tuntap", "add", "dev", n.TAPName, "mode", "tap"); err != nil {
		return fmt.Errorf("create tap: %w", err)
	}
	if err := hostExec("ip", "addr", "add", n.HostIP+"/30", "dev", n.TAPName); err != nil {
		return fmt.Errorf("set ip: %w", err)
	}
	if err := hostExec("ip", "link", "set", n.TAPName, "up"); err != nil {
		return fmt.Errorf("link up: %w", err)
	}
	return nil
}

// TeardownTAP removes the TAP device from the host.
func (n Network) TeardownTAP() {
	hostExec("ip", "tuntap", "del", "dev", n.TAPName, "mode", "tap")
}

// SetupNAT enables IP forwarding and masquerade for VM traffic (called once at startup).
func SetupNAT() error {
	if err := hostExec("sysctl", "-w", "net.ipv4.ip_forward=1"); err != nil {
		return err
	}
	// Masquerade all VM traffic (10.0.0.0/16)
	hostExec("iptables", "-t", "nat", "-D", "POSTROUTING", "-s", "10.0.0.0/16", "-j", "MASQUERADE")
	return hostExec("iptables", "-t", "nat", "-A", "POSTROUTING", "-s", "10.0.0.0/16", "-j", "MASQUERADE")
}

// ConfigureGuestNetwork sets up the network inside the VM.
// Uses vsock (not TCP) because TCP requires the network to be configured first.
func (vm *VM) ConfigureGuestNetwork(net Network) error {
	cmd := fmt.Sprintf(
		"ip addr flush dev eth0 2>/dev/null; ip addr add %s/30 dev eth0 && ip link set eth0 up && ip route add default via %s && echo 'nameserver 8.8.8.8' > /etc/resolv.conf",
		net.GuestIP, net.HostIP,
	)
	_, stderr, exitCode, err := vm.execVsock([]string{"/bin/sh", "-c", cmd}, 5*time.Second)
	if err != nil {
		return fmt.Errorf("configure guest network: %w", err)
	}
	if exitCode != 0 {
		return fmt.Errorf("configure guest network: exit %d: %s", exitCode, stderr)
	}
	klog.Infof("vm %s: network configured (guest=%s, host=%s, tap=%s)", vm.ID, net.GuestIP, net.HostIP, net.TAPName)
	return nil
}

// EnableSSH generates a host key, injects the user's public key, and starts dropbear.
// The public key is written via a heredoc to prevent shell injection.
func (vm *VM) EnableSSH(authorizedKey string) error {
	// Validate the key looks like an SSH public key (no shell metacharacters)
	if strings.ContainsAny(authorizedKey, "'`$\\;&|") {
		return fmt.Errorf("invalid characters in SSH public key")
	}

	cmd := fmt.Sprintf(`dropbearkey -t ed25519 -f /etc/dropbear/dropbear_ed25519_host_key 2>/dev/null
cat > /root/.ssh/authorized_keys << 'SSHEOF'
%s
SSHEOF
chmod 600 /root/.ssh/authorized_keys
dropbear -R -s -g -p 22`, authorizedKey)
	_, stderr, exitCode, err := vm.Exec([]string{"/bin/sh", "-c", cmd}, 5*time.Second)
	if err != nil {
		return fmt.Errorf("enable ssh: %w", err)
	}
	if exitCode != 0 {
		return fmt.Errorf("enable ssh: exit %d: %s", exitCode, stderr)
	}
	klog.Infof("vm %s: SSH enabled (dropbear listening on :22)", vm.ID)
	return nil
}

// DisableSSH stops the dropbear daemon.
func (vm *VM) DisableSSH() error {
	_, _, _, err := vm.Exec([]string{"/bin/sh", "-c", "killall dropbear 2>/dev/null; rm -f /root/.ssh/authorized_keys"}, 5*time.Second)
	if err != nil {
		return fmt.Errorf("disable ssh: %w", err)
	}
	klog.Infof("vm %s: SSH disabled", vm.ID)
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
// Pause stops the vCPU. The VM state is preserved in memory.
func (vm *VM) Pause() error {
	return vm.apiPatch("/vm", map[string]interface{}{
		"state": "Paused",
	})
}

func (vm *VM) Resume() error {
	return vm.apiPatch("/vm", map[string]interface{}{
		"state": "Resumed",
	})
}

// WaitReady polls the guest agent until it responds or the timeout expires.
// Uses vsock (works before network is configured).
func (vm *VM) WaitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, _, err := vm.execVsock([]string{"/bin/true"}, 2*time.Second)
		if err == nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("guest agent not ready after %v", timeout)
}

// execVsock runs a command via the vsock guest agent (for network config before TCP is available).
func (vm *VM) execVsock(cmd []string, timeout time.Duration) (stdout, stderr string, exitCode int, err error) {
	var conn net.Conn
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		conn, err = vsockConnect(vm.VsockPath, 1024)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return "", "", -1, fmt.Errorf("vsock connect: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
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

// ExecEval sends code to the eval server inside the VM via the guest-agent.
// The guest-agent on port 1024 routes eval requests to the eval server on
// localhost:1025 (pre-started in snapshot, zero startup cost).
func (vm *VM) ExecEval(code string, timeout time.Duration) (stdout, stderr string, exitCode int, err error) {
	if vm.NS != nil {
		req := struct {
			Eval string `json:"eval"`
		}{Eval: code}
		var resp struct {
			Stdout   string `json:"stdout"`
			Stderr   string `json:"stderr"`
			ExitCode int    `json:"exit_code"`
		}

		deadline := time.Now().Add(500 * time.Millisecond)
		for {
			err = ExecInNS(vm.NS.NSPath, "10.0.0.2:1024", req, &resp)
			if err == nil {
				return resp.Stdout, resp.Stderr, resp.ExitCode, nil
			}
			if time.Now().After(deadline) {
				return "", "", -1, fmt.Errorf("eval: %w", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Vsock fallback
	return vm.execVsock([]string{"/bin/sh", "-c", fmt.Sprintf("node -e '%s'", code)}, timeout)
}

// Exec runs a command inside the VM.
// Preferred: serial console (zero overhead after restore).
// Fallback: TCP via namespace, then vsock.
func (vm *VM) Exec(cmd []string, timeout time.Duration) (stdout, stderr string, exitCode int, err error) {
	// Extract the shell command string
	cmdStr := strings.Join(cmd, " ")
	if len(cmd) >= 3 && cmd[0] == "/bin/sh" && cmd[1] == "-c" {
		cmdStr = strings.Join(cmd[2:], " ")
	}

	// Serial path — direct pipe to guest, no TCP, no fork
	if vm.serialIn != nil {
		return vm.execSerial(cmdStr, timeout)
	}

	// Namespace TCP fallback
	if vm.NS != nil {
		req := struct {
			Cmd []string `json:"cmd"`
		}{Cmd: cmd}
		var resp struct {
			Stdout   string `json:"stdout"`
			Stderr   string `json:"stderr"`
			ExitCode int    `json:"exit_code"`
		}

		deadline := time.Now().Add(500 * time.Millisecond)
		for {
			err = ExecInNS(vm.NS.NSPath, "10.0.0.2:1024", req, &resp)
			if err == nil {
				return resp.Stdout, resp.Stderr, resp.ExitCode, nil
			}
			if time.Now().After(deadline) {
				return "", "", -1, fmt.Errorf("ns exec: %w", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Vsock fallback
	return vm.execVsock(cmd, timeout)
}

// SerialKick sends a warmup command via serial to exercise the guest kernel.
// Runs synchronously — absorbs the ~65ms cold penalty so the user's exec is fast.
// The claim takes longer but the exec is faster. Net effect: ~30ms improvement
// because the warmup overlaps with the HTTP round-trip back to the client.
func (vm *VM) SerialKick() {
	if vm.serialIn != nil {
		vm.execSerial("/bin/true", 2*time.Second)
	}
}

// execSerial sends a command via the Firecracker serial console pipe.
// The C serial-agent on the guest reads from ttyS0, runs the command,
// and writes a JSON response. Zero runtime overhead after snapshot restore.
func (vm *VM) execSerial(command string, timeout time.Duration) (string, string, int, error) {
	vm.serialMu.Lock()
	defer vm.serialMu.Unlock()

	// Write command to serial stdin
	_, err := fmt.Fprintf(vm.serialIn, "%s\n", command)
	if err != nil {
		return "", "", -1, fmt.Errorf("serial write: %w", err)
	}

	// Read lines from serial stdout until we get a JSON response
	// (skip kernel messages, FC logs, and empty lines)
	for vm.serialOut.Scan() {
		line := vm.serialOut.Text()
		if len(line) == 0 || line[0] != '{' {
			continue // skip non-JSON lines (kernel messages, prompts)
		}

		var resp struct {
			Stdout   string `json:"stdout"`
			Stderr   string `json:"stderr"`
			ExitCode int    `json:"exit_code"`
		}
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			continue // not valid JSON, skip
		}
		return resp.Stdout, resp.Stderr, resp.ExitCode, nil
	}

	return "", "", -1, fmt.Errorf("serial: no response (scanner error: %v)", vm.serialOut.Err())
}

// Restore boots a new VM from a snapshot.
func Restore(cfg Config, id string, image string, vmstatePath string, memPath string, net ...Network) (*VM, error) {
	var n Network
	if len(net) > 0 {
		n = net[0]
	}
	vm, err := PrepareRestore(cfg, id, image, n)
	if err != nil {
		return nil, err
	}
	if err := vm.LoadSnapshot(vmstatePath, memPath); err != nil {
		return nil, err
	}
	return vm, nil
}

// RestoreInNS boots a VM from snapshot inside a network namespace.
// The namespace already has the TAP configured — no post-restore network config needed.
// If useUffd is true, a UFFD handler pre-populates pages eagerly.
func RestoreInNS(cfg Config, id string, image string, vmstatePath string, memPath string, ns *NetNS, useUffd ...bool) (*VM, error) {
	socketDir := filepath.Join(cfg.WorkDir, id)
	if err := os.MkdirAll(socketDir, 0o755); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}
	socketPath := filepath.Join(socketDir, "fc.sock")
	os.Remove(socketPath)
	os.Remove(filepath.Join(socketDir, "vsock.sock")) // prevent "Address in use"

	cmd, serialIn, serialOutReader, err := ns.StartFirecracker(cfg.FirecrackerBin, socketPath, socketDir)
	if err != nil {
		return nil, err
	}

	vm := &VM{
		ID:         id,
		Image:      image,
		SocketPath: socketPath,
		VsockPath:  filepath.Join(socketDir, "vsock.sock"),
		NS:         ns,
		Config:     cfg,
		cmd:        cmd,
		client:     newAPIClient(socketPath),
		serialIn:   serialIn,
		serialOut:  bufio.NewScanner(serialOutReader),
	}

	waitForSocket(socketPath)

	if len(useUffd) > 0 && useUffd[0] {
		if err := vm.LoadSnapshotUffd(vmstatePath, memPath); err != nil {
			return nil, err
		}
	} else {
		if err := vm.LoadSnapshot(vmstatePath, memPath); err != nil {
			return nil, err
		}
	}
	return vm, nil
}

// PrepareRestore starts a Firecracker process and waits for its API socket.
// Call LoadSnapshot to actually load VM state. This split allows overlapping
// process startup with other work (e.g., merge).
func PrepareRestore(cfg Config, id string, image string, net ...Network) (*VM, error) {
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
	if len(net) > 0 {
		vm.Net = net[0]
	}

	waitForSocket(socketPath)
	return vm, nil
}

// LoadSnapshot loads a snapshot into a prepared Firecracker process.
// If the VM has a Network with a TAP name, network_overrides is used to
// remap the snapshot's network device to the new TAP.
func (vm *VM) LoadSnapshot(vmstatePath string, memPath string) error {
	return vm.loadSnapshot(vmstatePath, memPath, false)
}

// LoadSnapshotUffd loads a snapshot using a UFFD page fault handler.
// The handler pre-populates all pages eagerly, eliminating demand-paging latency.
func (vm *VM) LoadSnapshotUffd(vmstatePath string, memPath string) error {
	return vm.loadSnapshot(vmstatePath, memPath, true)
}

func (vm *VM) loadSnapshot(vmstatePath string, memPath string, useUffd bool) error {
	start := time.Now()

	var loadReq map[string]interface{}

	if useUffd {
		// Start the UFFD handler process before loading the snapshot.
		// Firecracker will connect to the UDS and send the UFFD fd.
		uffdSock := filepath.Join(filepath.Dir(vm.SocketPath), "uffd.sock")
		os.Remove(uffdSock)

		handler := exec.Command("uffd-handler", uffdSock, memPath)
		handler.Stderr = os.Stderr
		if err := handler.Start(); err != nil {
			return fmt.Errorf("start uffd-handler: %w", err)
		}
		vm.uffdHandler = handler

		// Wait for handler to be ready (it creates uffd.sock.ready)
		readyPath := uffdSock + ".ready"
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(readyPath); err == nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		os.Remove(readyPath)

		loadReq = map[string]interface{}{
			"snapshot_path": vmstatePath,
			"mem_backend": map[string]interface{}{
				"backend_type": "Uffd",
				"backend_path": uffdSock,
			},
			"enable_diff_snapshots": false,
			"resume_vm":             false, // don't resume yet — wait for prefetch
		}
	} else {
		loadReq = map[string]interface{}{
			"snapshot_path": vmstatePath,
			"mem_backend": map[string]interface{}{
				"backend_type": "File",
				"backend_path": memPath,
			},
			"enable_diff_snapshots": false,
			"resume_vm":             true,
		}
	}

	if vm.Net.TAPName != "" {
		loadReq["network_overrides"] = []map[string]interface{}{
			{"iface_id": "eth0", "host_dev_name": vm.Net.TAPName},
		}
	}
	if err := vm.apiPut("/snapshot/load", loadReq); err != nil {
		vm.Destroy()
		return fmt.Errorf("load snapshot: %w", err)
	}

	// If using UFFD, wait for prefetch to complete, then resume the VM.
	// All pages are pre-populated — the guest starts with zero page faults.
	if useUffd {
		uffdSock := filepath.Join(filepath.Dir(vm.SocketPath), "uffd.sock")
		donePath := uffdSock + ".done"
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(donePath); err == nil {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		os.Remove(donePath)

		if err := vm.apiPatch("/vm", map[string]interface{}{"state": "Resumed"}); err != nil {
			vm.Destroy()
			return fmt.Errorf("resume after prefetch: %w", err)
		}
		klog.Infof("vm %s: prefetch complete, resumed", vm.ID)
	}

	vm.StartedAt = start
	vm.PID = vm.cmd.Process.Pid
	klog.Infof("vm %s restored in %v (uffd=%v)", vm.ID, time.Since(start), useUffd)
	return nil
}

// NetAllocFunc allocates a network for a branch VM. If nil, branches have no network.
type NetAllocFunc func(vmID string) (Network, error)

// Branch takes a diff snapshot, merges it with the baseline, and restores N copies in parallel.
// The merged mem file is shared across all branches (MAP_PRIVATE/CoW).
func Branch(cfg Config, source *VM, count int, allocNet NetAllocFunc) ([]*VM, error) {
	if source.BaselineDir == "" {
		return branchFull(cfg, source, count, allocNet)
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
			// Allocate network for this branch
			var branchNet Network
			if allocNet != nil {
				n, err := allocNet(branchID)
				if err != nil {
					klog.Warningf("branch %d: network alloc failed: %v", i, err)
				} else {
					branchNet = n
				}
			}
			vm, err := PrepareRestore(cfg, branchID, source.Image, branchNet)
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
			// Configure guest network for this branch (new IP for new TAP)
			if preps[i].vm.Net.TAPName != "" {
				if err := preps[i].vm.ConfigureGuestNetwork(preps[i].vm.Net); err != nil {
					klog.Warningf("branch %d: guest network config failed: %v", i, err)
				}
			}
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
// Each branch gets its own network namespace (like the warm pool) to
// avoid TAP device conflicts.
func branchFull(cfg Config, source *VM, count int, allocNet NetAllocFunc) ([]*VM, error) {
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

			// Each branch gets its own netns to avoid TAP conflicts
			ns, err := CreateNetNS()
			if err != nil {
				ch <- result{idx: i, err: fmt.Errorf("branch %d netns: %w", i, err)}
				return
			}

			restoreStart := time.Now()
			vm, err := RestoreInNS(cfg, branchID, source.Image, branchVmstate, sharedMem, ns)
			if err != nil {
				ns.Destroy()
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

// PrefetchFile reads a file into the kernel page cache to avoid page faults during mmap.
func PrefetchFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	buf := make([]byte, 256*1024) // 256KB read buffer
	for {
		_, err := f.Read(buf)
		if err != nil {
			break
		}
	}
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
