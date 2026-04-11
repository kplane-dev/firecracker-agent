// Package pool manages a warm pool of pre-booted Firecracker VMs.
package pool

import (
	"fmt"
	"sync"
	"time"

	"github.com/kplane-dev/firecracker-agent/pkg/vm"
	"k8s.io/klog/v2"
)

// Pool manages warm and active VMs, keyed by rootfs image.
// Warm VMs are paused at their baseline snapshot point.
// Claiming resumes them; branching uses diff snapshots against the baseline.
type Pool struct {
	cfg        vm.Config
	maxVMs     int
	minWarm    int
	warmVCPUs  int
	warmMemMiB int
	warmImages []string

	mu      sync.Mutex
	warm    map[string][]*vm.VM // image -> paused warm VMs
	active  map[string]*vm.VM   // vmID -> VM
	nextID  int
	nextNet int // network index counter for IP allocation
}

// New creates a new pool.
func New(cfg vm.Config, maxVMs int, warmImages []string, minWarm, warmVCPUs, warmMemMiB int) *Pool {
	return &Pool{
		cfg:        cfg,
		maxVMs:     maxVMs,
		warmImages: warmImages,
		minWarm:    minWarm,
		warmVCPUs:  warmVCPUs,
		warmMemMiB: warmMemMiB,
		warm:       make(map[string][]*vm.VM),
		active:     make(map[string]*vm.VM),
	}
}

// Claim takes a VM from the warm pool (resuming it) or cold boots one.
// cpuMillis is in Kubernetes-style millicores (500 = half core, 1000 = full core, 0 = no limit).
// memMiB is the desired guest memory (0 = use pool default).
func (p *Pool) Claim(image string, cpuMillis, memMiB int) (*vm.VM, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.active) >= p.maxVMs {
		return nil, false, fmt.Errorf("max VMs reached (%d)", p.maxVMs)
	}

	// Try warm pool — VMs are paused with balloon inflated, resume and adjust on claim
	if vms := p.warm[image]; len(vms) > 0 {
		v := vms[len(vms)-1]
		p.warm[image] = vms[:len(vms)-1]

		if err := v.Resume(); err != nil {
			klog.Errorf("pool: failed to resume warm VM %s: %v", v.ID, err)
			v.Destroy()
			// Fall through to cold boot
		} else {
			p.active[v.ID] = v

			// Adjust resources to requested levels
			p.applyResources(v, cpuMillis, memMiB)

			klog.Infof("pool: claimed warm VM %s (image=%s, cpu=%dm, mem=%dMiB, %d warm remaining)",
				v.ID, image, v.CPUMillis, v.MemoryMiB-v.BalloonMiB, len(p.warm[image]))
			return v, true, nil
		}
	}

	// Cold boot
	id := p.nextVMID()
	bootMem := p.warmMemMiB
	if memMiB > bootMem {
		bootMem = memMiB
	}
	v, err := vm.Boot(p.cfg, id, image, 1, bootMem)
	if err != nil {
		return nil, false, err
	}
	p.active[v.ID] = v
	p.applyResources(v, cpuMillis, memMiB)
	klog.Infof("pool: cold booted VM %s (image=%s, cpu=%dm, mem=%dMiB)", v.ID, image, v.CPUMillis, v.MemoryMiB-v.BalloonMiB)
	return v, false, nil
}

// applyResources sets CPU cgroup limit and adjusts balloon for the requested memory.
func (p *Pool) applyResources(v *vm.VM, cpuMillis, memMiB int) {
	if cpuMillis > 0 {
		if err := v.SetCPULimit(cpuMillis); err != nil {
			klog.Warningf("pool: cpu limit failed for %s: %v", v.ID, err)
		}
	}
	if memMiB > 0 && memMiB < v.MemoryMiB {
		balloonAmount := v.MemoryMiB - memMiB
		if err := v.SetBalloon(balloonAmount); err != nil {
			klog.Warningf("pool: balloon failed for %s: %v", v.ID, err)
		}
	} else if memMiB == 0 || memMiB >= v.MemoryMiB {
		// Give all memory — deflate balloon fully
		if v.BalloonMiB > 0 {
			if err := v.SetBalloon(0); err != nil {
				klog.Warningf("pool: balloon deflate failed for %s: %v", v.ID, err)
			}
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

// Fill pre-boots VMs, takes a baseline snapshot, and leaves them paused.
// The baseline enables fast diff-based branching later.
func (p *Pool) Fill() error {
	for _, image := range p.warmImages {
		p.mu.Lock()
		current := len(p.warm[image])
		needed := p.minWarm - current
		capacity := p.maxVMs - p.totalLocked()
		if needed > capacity {
			needed = capacity
		}
		p.mu.Unlock()

		for i := 0; i < needed; i++ {
			id := func() string {
				p.mu.Lock()
				defer p.mu.Unlock()
				return p.nextVMID()
			}()

			start := time.Now()

			// Allocate network (TAP + IPs) for this VM
			net, err := func() (vm.Network, error) {
				p.mu.Lock()
				defer p.mu.Unlock()
				return p.allocateNetworkLocked(id)
			}()
			if err != nil {
				klog.Errorf("pool: network setup failed for %s: %v", id, err)
				continue
			}

			v, err := vm.Boot(p.cfg, id, image, p.warmVCPUs, p.warmMemMiB, net)
			if err != nil {
				net.TeardownTAP()
				klog.Errorf("pool: failed to pre-boot %s VM: %v", image, err)
				continue
			}

			// Wait for guest agent, then configure guest network
			if err := v.WaitReady(5 * time.Second); err != nil {
				klog.Warningf("pool: guest agent not ready for %s: %v", v.ID, err)
			}
			if err := v.ConfigureGuestNetwork(net); err != nil {
				klog.Warningf("pool: guest network config failed for %s: %v", v.ID, err)
			}

			// Inflate balloon to reclaim most memory while paused in the warm pool.
			// Keep 64MB for the guest (kernel + init + guest-agent). The rest is
			// returned to the host. On claim, we deflate to the requested level.
			minGuestMiB := 64
			if p.warmMemMiB > minGuestMiB {
				if err := v.SetBalloon(p.warmMemMiB - minGuestMiB); err != nil {
					klog.Warningf("pool: balloon inflate failed for %s: %v", v.ID, err)
				}
			}

			// Take baseline snapshot (with balloon inflated) and leave VM paused.
			// Dirty page tracking resets here — future diffs capture only post-resume changes.
			baselineDir := fmt.Sprintf("%s/baselines/%s", p.cfg.WorkDir, v.ID)
			if err := v.Snapshot(baselineDir, false); err != nil {
				klog.Errorf("pool: baseline snapshot failed for %s: %v", v.ID, err)
				v.Destroy()
				continue
			}
			v.BaselineDir = baselineDir
			// VM stays paused — resumed on Claim

			p.mu.Lock()
			p.warm[image] = append(p.warm[image], v)
			klog.Infof("pool: warm VM %s (image=%s) ready in %v (paused at baseline, %d/%d warm)",
				v.ID, image, time.Since(start), len(p.warm[image]), p.minWarm)
			p.mu.Unlock()
		}
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

func (p *Pool) totalLocked() int {
	total := len(p.active)
	for _, vms := range p.warm {
		total += len(vms)
	}
	return total
}

func (p *Pool) nextVMID() string {
	p.nextID++
	return fmt.Sprintf("fc-%d", p.nextID)
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
