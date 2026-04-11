// Package pool manages a snapshot pool of Firecracker VM baselines.
// Instead of keeping paused VMs in memory, we keep baseline snapshot files
// and restore on demand (~70ms per claim).
package pool

import (
	"fmt"
	"sync"
	"time"

	"github.com/kplane-dev/firecracker-agent/pkg/vm"
	"k8s.io/klog/v2"
)

// baseline holds the paths to a snapshot that can be restored.
type baseline struct {
	vmstateDir string // directory containing vmstate + mem files
	image      string
}

// Pool manages baselines and active VMs.
type Pool struct {
	cfg        vm.Config
	maxVMs     int
	warmVCPUs  int
	warmMemMiB int
	warmImages []string

	mu        sync.Mutex
	baselines map[string]*baseline // image -> baseline snapshot
	active    map[string]*vm.VM    // vmID -> VM
	nextID    int
	nextNet   int
}

// New creates a new pool.
func New(cfg vm.Config, maxVMs int, warmImages []string, minWarm, warmVCPUs, warmMemMiB int) *Pool {
	return &Pool{
		cfg:        cfg,
		maxVMs:     maxVMs,
		warmImages: warmImages,
		warmVCPUs:  warmVCPUs,
		warmMemMiB: warmMemMiB,
		baselines:  make(map[string]*baseline),
		active:     make(map[string]*vm.VM),
	}
}

// Claim restores a VM from the baseline snapshot for the requested image.
// Every claim is a snapshot restore (~70ms). No cold boot path.
func (p *Pool) Claim(image string, cpuMillis, memMiB int) (*vm.VM, bool, error) {
	p.mu.Lock()
	bl, ok := p.baselines[image]
	if !ok {
		p.mu.Unlock()
		return nil, false, fmt.Errorf("no baseline for image %q (pool not ready)", image)
	}
	if len(p.active) >= p.maxVMs {
		p.mu.Unlock()
		return nil, false, fmt.Errorf("max VMs reached (%d)", p.maxVMs)
	}
	id := p.nextVMID()
	p.mu.Unlock()

	start := time.Now()
	vmstatePath := fmt.Sprintf("%s/vmstate", bl.vmstateDir)
	memPath := fmt.Sprintf("%s/mem", bl.vmstateDir)

	// Create a network namespace for this VM (~3ms)
	ns, err := vm.CreateNetNS()
	if err != nil {
		return nil, false, fmt.Errorf("create netns: %w", err)
	}

	// Restore inside the namespace — guest network is pre-configured
	// in the baseline snapshot, no post-restore config needed.
	v, err := vm.RestoreInNS(p.cfg, id, image, vmstatePath, memPath, ns)
	if err != nil {
		ns.Destroy()
		return nil, false, fmt.Errorf("restore from baseline: %w", err)
	}

	p.mu.Lock()
	p.active[v.ID] = v
	p.mu.Unlock()

	// Pre-warm the runtime worker (starts Node.js/Python REPL on first eval)
	v.ExecEval("1", 5*time.Second)

	p.applyResources(v, cpuMillis, memMiB)

	restoreMs := time.Since(start).Milliseconds()
	klog.Infof("pool: claimed %s (image=%s, restore=%dms, cpu=%dm, mem=%dMiB)",
		v.ID, image, restoreMs, v.CPUMillis, v.MemoryMiB-v.BalloonMiB)
	return v, true, nil
}

// applyResources sets CPU cgroup limit and adjusts balloon for the requested memory.
func (p *Pool) applyResources(v *vm.VM, cpuMillis, memMiB int) {
	if cpuMillis > 0 {
		if err := v.SetCPULimit(cpuMillis); err != nil {
			klog.Warningf("pool: cpu limit failed for %s: %v", v.ID, err)
		}
	}
	if memMiB > 0 && memMiB < v.MemoryMiB {
		if err := v.SetBalloon(v.MemoryMiB - memMiB); err != nil {
			klog.Warningf("pool: balloon failed for %s: %v", v.ID, err)
		}
	} else if v.BalloonMiB > 0 {
		if err := v.SetBalloon(0); err != nil {
			klog.Warningf("pool: balloon deflate failed for %s: %v", v.ID, err)
		}
	}
}

// Release destroys a VM.
func (p *Pool) Release(vmID string) error {
	p.mu.Lock()
	v, ok := p.active[vmID]
	if !ok {
		p.mu.Unlock()
		return fmt.Errorf("VM %s not found", vmID)
	}
	delete(p.active, vmID)
	p.mu.Unlock()

	return v.Destroy()
}

// Fill creates one baseline snapshot per image.
// Boot a VM, wait for guest agent, inflate balloon, snapshot, destroy.
// The snapshot files are kept for restoring on Claim().
func (p *Pool) Fill() error {
	for _, image := range p.warmImages {
		p.mu.Lock()
		_, hasBaseline := p.baselines[image]
		p.mu.Unlock()
		if hasBaseline {
			continue
		}

		start := time.Now()

		p.mu.Lock()
		id := p.nextVMID()
		p.mu.Unlock()

		// Boot baseline in a namespace with pre-configured network
		ns, err := vm.CreateNetNS()
		if err != nil {
			klog.Errorf("pool: netns creation failed for %s: %v", id, err)
			continue
		}

		// Boot with the TAP in the namespace
		net := vm.NetworkInNS(ns)
		v, err := vm.Boot(p.cfg, id, image, p.warmVCPUs, p.warmMemMiB, net)
		if err != nil {
			ns.Destroy()
			klog.Errorf("pool: failed to boot %s for baseline: %v", image, err)
			continue
		}
		v.NS = ns

		if err := v.WaitReady(10 * time.Second); err != nil {
			klog.Warningf("pool: guest agent not ready for %s: %v", v.ID, err)
		}
		// Network is configured via vsock during baseline creation (one-time cost)
		if err := v.ConfigureGuestNetwork(net); err != nil {
			klog.Warningf("pool: guest network config failed for %s: %v", v.ID, err)
		}

		// Inflate balloon to minimize snapshot size
		minGuestMiB := 64
		if p.warmMemMiB > minGuestMiB {
			if err := v.SetBalloon(p.warmMemMiB - minGuestMiB); err != nil {
				klog.Warningf("pool: balloon inflate failed for %s: %v", v.ID, err)
			}
		}

		// Snapshot
		baselineDir := fmt.Sprintf("%s/baselines/%s", p.cfg.WorkDir, image)
		if err := v.Snapshot(baselineDir, false); err != nil {
			klog.Errorf("pool: baseline snapshot failed for %s: %v", v.ID, err)
			v.Destroy()
			continue
		}

		// Destroy the VM — we only keep the snapshot files
		v.Destroy()

		p.mu.Lock()
		p.baselines[image] = &baseline{vmstateDir: baselineDir, image: image}
		p.mu.Unlock()

		klog.Infof("pool: baseline for %s ready in %v (snapshot at %s)",
			image, time.Since(start), baselineDir)
	}
	return nil
}

// GetActive returns an active VM by ID, or nil if not found.
func (p *Pool) GetActive(vmID string) *vm.VM {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active[vmID]
}

// TrackActive adds a VM to the active set (used for branch VMs).
func (p *Pool) TrackActive(v *vm.VM) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.active[v.ID] = v
}

// Status returns pool stats.
func (p *Pool) Status() (warmByImage map[string]int, active, max int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	warmByImage = make(map[string]int)
	for image := range p.baselines {
		warmByImage[image] = 1 // baseline ready
	}
	return warmByImage, len(p.active), p.maxVMs
}

// AllocateNetwork assigns a unique network config and creates the TAP.
func (p *Pool) AllocateNetwork(vmID string) (vm.Network, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.allocateNetworkLocked(vmID)
}

func (p *Pool) allocateNetworkLocked(vmID string) (vm.Network, error) {
	p.nextNet++
	net := vm.AllocateNetwork(vmID, p.nextNet)
	if err := net.SetupTAP(); err != nil {
		return net, err
	}
	return net, nil
}

// Shutdown destroys all active VMs.
func (p *Pool) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, v := range p.active {
		v.Destroy()
	}
	p.active = nil
}

func (p *Pool) totalLocked() int {
	return len(p.active)
}

func (p *Pool) nextVMID() string {
	p.nextID++
	return fmt.Sprintf("fc-%d", p.nextID)
}
