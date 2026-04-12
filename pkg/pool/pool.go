// Package pool manages pre-warmed, paused Firecracker VMs.
// VMs are restored from baseline snapshots, warmed (node -v), and paused.
// Claim resumes a paused VM (~1ms). Release destroys it completely.
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
	vmstateDir string
	image      string
}

// Pool manages baselines, warm (paused) VMs, and active VMs.
type Pool struct {
	cfg        vm.Config
	maxVMs     int
	minWarm    int
	warmVCPUs  int
	warmMemMiB int
	warmImages []string

	mu        sync.Mutex
	baselines map[string]*baseline   // image -> baseline snapshot
	warm      map[string][]*vm.VM    // image -> paused warm VMs
	active    map[string]*vm.VM      // vmID -> running VM
	nextID    int
	nextNet   int
}

// New creates a new pool.
func New(cfg vm.Config, maxVMs int, warmImages []string, minWarm, warmVCPUs, warmMemMiB int) *Pool {
	return &Pool{
		cfg:        cfg,
		maxVMs:     maxVMs,
		minWarm:    minWarm,
		warmImages: warmImages,
		warmVCPUs:  warmVCPUs,
		warmMemMiB: warmMemMiB,
		baselines:  make(map[string]*baseline),
		warm:       make(map[string][]*vm.VM),
		active:     make(map[string]*vm.VM),
	}
}

// Claim grabs a pre-warmed paused VM and resumes it (~1ms).
// Falls back to restore-from-baseline if the warm pool is empty.
func (p *Pool) Claim(image string, cpuMillis, memMiB int) (*vm.VM, bool, error) {
	p.mu.Lock()

	if len(p.active) >= p.maxVMs {
		p.mu.Unlock()
		return nil, false, fmt.Errorf("max VMs reached (%d)", p.maxVMs)
	}

	// Try warm pool — pre-warmed, paused VMs
	if vms := p.warm[image]; len(vms) > 0 {
		v := vms[len(vms)-1]
		p.warm[image] = vms[:len(vms)-1]
		p.active[v.ID] = v
		p.mu.Unlock()

		remaining := len(p.warm[image])
		p.mu.Unlock()

		// Resume the paused VM (~1ms, no kernel warmup needed)
		start := time.Now()
		if err := v.Resume(); err != nil {
			klog.Errorf("pool: resume failed for %s: %v", v.ID, err)
			v.Destroy()
			return nil, false, fmt.Errorf("resume: %w", err)
		}

		p.applyResources(v, cpuMillis, memMiB)

		// Replenish in background immediately
		go p.replenishWarm(image)

		klog.Infof("pool: claimed warm %s (image=%s, resume=%dms, %d warm remaining)",
			v.ID, image, time.Since(start).Milliseconds(), remaining)
		return v, true, nil
	}

	// Fall back to cold restore from baseline
	bl, ok := p.baselines[image]
	if !ok {
		p.mu.Unlock()
		return nil, false, fmt.Errorf("no baseline for image %q", image)
	}
	id := p.nextVMID()
	p.mu.Unlock()

	start := time.Now()
	v, err := p.restoreAndWarm(id, image, bl)
	if err != nil {
		return nil, false, err
	}

	p.mu.Lock()
	p.active[v.ID] = v
	p.mu.Unlock()

	p.applyResources(v, cpuMillis, memMiB)

	klog.Infof("pool: claimed cold %s (image=%s, restore=%dms)",
		v.ID, image, time.Since(start).Milliseconds())
	return v, false, nil
}

// restoreAndWarm creates a VM from baseline: restore → warmup → ready.
func (p *Pool) restoreAndWarm(id, image string, bl *baseline) (*vm.VM, error) {
	ns, err := vm.CreateNetNS()
	if err != nil {
		return nil, fmt.Errorf("netns: %w", err)
	}

	vmstatePath := fmt.Sprintf("%s/vmstate", bl.vmstateDir)
	memPath := fmt.Sprintf("%s/mem", bl.vmstateDir)

	v, err := vm.RestoreInNS(p.cfg, id, image, vmstatePath, memPath, ns)
	if err != nil {
		ns.Destroy()
		return nil, fmt.Errorf("restore: %w", err)
	}

	// Warm the guest — exercises kernel scheduler + loads node pages.
	// This absorbs the ~65ms post-restore cold penalty.
	v.SerialKick()
	v.Exec([]string{"node", "-v"}, 5*time.Second)

	return v, nil
}

// applyResources sets CPU cgroup limit and adjusts balloon.
func (p *Pool) applyResources(v *vm.VM, cpuMillis, memMiB int) {
	if cpuMillis > 0 {
		if err := v.SetCPULimit(cpuMillis); err != nil {
			klog.Warningf("pool: cpu limit failed for %s: %v", v.ID, err)
		}
	}
}

// Release destroys a VM completely. Never reused — full isolation.
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

// Fill creates baselines and pre-warms VMs.
// Baselines are created once. Warm VMs are replenished continuously.
func (p *Pool) Fill() error {
	// Create baselines (one-time per image)
	for _, image := range p.warmImages {
		p.mu.Lock()
		_, hasBaseline := p.baselines[image]
		p.mu.Unlock()
		if hasBaseline {
			// Replenish warm pool
			p.replenishWarm(image)
			continue
		}

		start := time.Now()
		p.mu.Lock()
		id := p.nextVMID()
		p.mu.Unlock()

		ns, err := vm.CreateNetNS()
		if err != nil {
			klog.Errorf("pool: netns failed for baseline %s: %v", id, err)
			continue
		}

		net := vm.NetworkInNS(ns)
		v, err := vm.Boot(p.cfg, id, image, p.warmVCPUs, p.warmMemMiB, net)
		if err != nil {
			ns.Destroy()
			klog.Errorf("pool: boot failed for baseline %s: %v", image, err)
			continue
		}
		v.NS = ns

		if err := v.WaitReady(10 * time.Second); err != nil {
			klog.Warningf("pool: guest not ready for %s: %v", v.ID, err)
		}
		if err := v.ConfigureGuestNetwork(net); err != nil {
			klog.Warningf("pool: network config failed for %s: %v", v.ID, err)
		}

		baselineDir := fmt.Sprintf("%s/baselines/%s", p.cfg.WorkDir, image)
		if err := v.Snapshot(baselineDir, false); err != nil {
			klog.Errorf("pool: baseline snapshot failed for %s: %v", v.ID, err)
			v.Destroy()
			continue
		}

		v.Destroy()

		p.mu.Lock()
		p.baselines[image] = &baseline{vmstateDir: baselineDir, image: image}
		p.mu.Unlock()

		klog.Infof("pool: baseline for %s ready in %v", image, time.Since(start))
	}

	return nil
}

// replenishWarm fills the warm pool for an image up to minWarm.
func (p *Pool) replenishWarm(image string) {
	p.mu.Lock()
	current := len(p.warm[image])
	needed := p.minWarm - current
	total := len(p.active) + p.totalWarmLocked()
	capacity := p.maxVMs - total
	if needed > capacity {
		needed = capacity
	}
	bl := p.baselines[image]
	p.mu.Unlock()

	if needed <= 0 || bl == nil {
		return
	}

	// Create warm VMs in parallel
	var wg sync.WaitGroup
	for i := 0; i < needed; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			p.mu.Lock()
			id := p.nextVMID()
			p.mu.Unlock()

			start := time.Now()
			v, err := p.restoreAndWarm(id, image, bl)
			if err != nil {
				klog.Errorf("pool: warm VM failed for %s: %v", image, err)
				return
			}

			// Pause — VM is warm and ready, waiting for a claim
			if err := v.Pause(); err != nil {
				klog.Errorf("pool: pause failed for %s: %v", v.ID, err)
				v.Destroy()
				return
			}

			p.mu.Lock()
			p.warm[image] = append(p.warm[image], v)
			klog.Infof("pool: warm VM %s (image=%s) ready in %v (%d/%d warm)",
				v.ID, image, time.Since(start), len(p.warm[image]), p.minWarm)
			p.mu.Unlock()
		}()
	}
	wg.Wait()
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

// AllocateNetwork assigns a unique network config and creates the TAP.
func (p *Pool) AllocateNetwork(vmID string) (vm.Network, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.nextNet++
	net := vm.AllocateNetwork(vmID, p.nextNet)
	if err := net.SetupTAP(); err != nil {
		return net, err
	}
	return net, nil
}

// Status returns pool stats.
func (p *Pool) Status() (warmByImage map[string]int, active, max int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	warmByImage = make(map[string]int)
	for image, vms := range p.warm {
		warmByImage[image] = len(vms)
	}
	return warmByImage, len(p.active), p.maxVMs
}

// Shutdown destroys all VMs.
func (p *Pool) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, vms := range p.warm {
		for _, v := range vms {
			v.Destroy()
		}
	}
	for _, v := range p.active {
		v.Destroy()
	}
	p.warm = nil
	p.active = nil
}

func (p *Pool) totalWarmLocked() int {
	total := 0
	for _, vms := range p.warm {
		total += len(vms)
	}
	return total
}

func (p *Pool) nextVMID() string {
	p.nextID++
	return fmt.Sprintf("fc-%d", p.nextID)
}
