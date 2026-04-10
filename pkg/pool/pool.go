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
type Pool struct {
	cfg        vm.Config
	maxVMs     int
	minWarm    int // per-image minimum
	warmVCPUs  int
	warmMemMiB int
	warmImages []string

	mu     sync.Mutex
	warm   map[string][]*vm.VM // image -> warm VMs
	active map[string]*vm.VM   // vmID -> VM
	nextID int
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

// Claim takes a VM from the warm pool (matching image) or cold boots one.
func (p *Pool) Claim(image string, vcpus, memMiB int) (*vm.VM, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.active) >= p.maxVMs {
		return nil, false, fmt.Errorf("max VMs reached (%d)", p.maxVMs)
	}

	// Try warm pool for this image
	if vms := p.warm[image]; len(vms) > 0 {
		v := vms[len(vms)-1]
		p.warm[image] = vms[:len(vms)-1]
		p.active[v.ID] = v
		klog.Infof("pool: claimed warm VM %s (image=%s, %d warm remaining)", v.ID, image, len(p.warm[image]))
		return v, true, nil
	}

	// Cold boot
	id := p.nextVMID()
	v, err := vm.Boot(p.cfg, id, image, vcpus, memMiB)
	if err != nil {
		return nil, false, err
	}
	p.active[v.ID] = v
	klog.Infof("pool: cold booted VM %s (image=%s)", v.ID, image)
	return v, false, nil
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

// Fill pre-boots VMs to maintain the minimum warm count per image.
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
			v, err := vm.Boot(p.cfg, id, image, p.warmVCPUs, p.warmMemMiB)
			if err != nil {
				klog.Errorf("pool: failed to pre-boot %s VM: %v", image, err)
				continue
			}

			p.mu.Lock()
			p.warm[image] = append(p.warm[image], v)
			klog.Infof("pool: pre-booted warm VM %s (image=%s) in %v (%d/%d warm)",
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
