// Package pool manages a warm pool of pre-booted Firecracker VMs.
package pool

import (
	"fmt"
	"sync"
	"time"

	"github.com/kplane-dev/firecracker-agent/pkg/vm"
	"k8s.io/klog/v2"
)

// Pool manages warm and active VMs.
type Pool struct {
	cfg     vm.Config
	maxVMs  int
	minWarm int

	mu     sync.Mutex
	warm   []*vm.VM
	active map[string]*vm.VM
	nextID int
}

// New creates a new pool.
func New(cfg vm.Config, maxVMs, minWarm int) *Pool {
	return &Pool{
		cfg:    cfg,
		maxVMs: maxVMs,
		minWarm: minWarm,
		active:  make(map[string]*vm.VM),
	}
}

// Claim takes a VM from the warm pool or cold boots one.
func (p *Pool) Claim(vcpus, memMiB int) (*vm.VM, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.active) >= p.maxVMs {
		return nil, false, fmt.Errorf("max VMs reached (%d)", p.maxVMs)
	}

	// Try warm pool first
	if len(p.warm) > 0 {
		v := p.warm[len(p.warm)-1]
		p.warm = p.warm[:len(p.warm)-1]
		p.active[v.ID] = v
		klog.Infof("pool: claimed warm VM %s (%d warm remaining)", v.ID, len(p.warm))
		return v, true, nil
	}

	// Cold boot
	id := p.nextVMID()
	v, err := vm.Boot(p.cfg, id, vcpus, memMiB)
	if err != nil {
		return nil, false, err
	}
	p.active[v.ID] = v
	klog.Infof("pool: cold booted VM %s", v.ID)
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

// Fill pre-boots VMs to maintain the minimum warm count.
func (p *Pool) Fill(vcpus, memMiB int) error {
	p.mu.Lock()
	needed := p.minWarm - len(p.warm)
	total := len(p.warm) + len(p.active)
	capacity := p.maxVMs - total
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
		v, err := vm.Boot(p.cfg, id, vcpus, memMiB)
		if err != nil {
			klog.Errorf("pool: failed to pre-boot VM: %v", err)
			continue
		}

		p.mu.Lock()
		p.warm = append(p.warm, v)
		klog.Infof("pool: pre-booted warm VM %s in %v (%d/%d warm)",
			v.ID, time.Since(start), len(p.warm), p.minWarm)
		p.mu.Unlock()
	}
	return nil
}

// Status returns pool stats.
func (p *Pool) Status() (warm, active, max int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.warm), len(p.active), p.maxVMs
}

// Shutdown destroys all VMs.
func (p *Pool) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, v := range p.warm {
		v.Destroy()
	}
	for _, v := range p.active {
		v.Destroy()
	}
	p.warm = nil
	p.active = nil
}

func (p *Pool) nextVMID() string {
	p.nextID++
	return fmt.Sprintf("fc-%d", p.nextID)
}
