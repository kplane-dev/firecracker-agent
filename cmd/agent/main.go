package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"encoding/json"

	"github.com/kplane-dev/firecracker-agent/pkg/pool"
	"github.com/kplane-dev/firecracker-agent/pkg/vm"
)

func main() {
	var (
		listenAddr     string
		firecrackerBin string
		kernelPath     string
		rootfsPath     string
		workDir        string
		maxVMs         int
		minWarm        int
		warmVCPUs      int
		warmMemMiB     int
	)

	flag.StringVar(&listenAddr, "listen", ":8090", "HTTP API listen address")
	flag.StringVar(&firecrackerBin, "firecracker-bin", "/opt/kata/bin/firecracker", "Path to firecracker binary")
	flag.StringVar(&kernelPath, "kernel", "/opt/kata/share/kata-containers/vmlinux-6.18.15-186", "Path to guest kernel")
	flag.StringVar(&rootfsPath, "rootfs", "/opt/kata/share/kata-containers/kata-alpine-3.22.initrd", "Path to rootfs/initrd")
	flag.StringVar(&workDir, "work-dir", "/var/lib/firecracker-agent", "Working directory for VM sockets and state")
	flag.IntVar(&maxVMs, "max-vms", 10, "Maximum concurrent VMs")
	flag.IntVar(&minWarm, "min-warm", 3, "Minimum warm pool size")
	flag.IntVar(&warmVCPUs, "warm-vcpus", 1, "vCPUs for warm pool VMs")
	flag.IntVar(&warmMemMiB, "warm-memory", 128, "Memory (MiB) for warm pool VMs")
	flag.Parse()

	os.MkdirAll(workDir, 0o755)

	cfg := vm.Config{
		FirecrackerBin: firecrackerBin,
		KernelPath:     kernelPath,
		RootfsPath:     rootfsPath,
		WorkDir:        workDir,
	}

	p := pool.New(cfg, maxVMs, minWarm)

	// Fill warm pool
	log.Printf("filling warm pool (min=%d, vcpus=%d, mem=%dMiB)...", minWarm, warmVCPUs, warmMemMiB)
	if err := p.Fill(warmVCPUs, warmMemMiB); err != nil {
		log.Printf("warn: warm pool fill error: %v", err)
	}

	// Start HTTP API (simple JSON API for testing; will be gRPC later)
	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		warm, active, max := p.Status()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"warm":   warm,
			"active": active,
			"max":    max,
		})
	})

	mux.HandleFunc("/claim", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		v, fromWarm, err := p.Claim(warmVCPUs, warmMemMiB)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		elapsed := time.Since(start)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"vm_id":          v.ID,
			"pid":            v.PID,
			"from_warm_pool": fromWarm,
			"claim_time_ms":  elapsed.Milliseconds(),
		})
	})

	mux.HandleFunc("/release", func(w http.ResponseWriter, r *http.Request) {
		vmID := r.URL.Query().Get("vm_id")
		if vmID == "" {
			http.Error(w, "vm_id required", http.StatusBadRequest)
			return
		}
		if err := p.Release(vmID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"released": vmID})
	})

	mux.HandleFunc("/branch", func(w http.ResponseWriter, r *http.Request) {
		vmID := r.URL.Query().Get("vm_id")
		countStr := r.URL.Query().Get("count")
		if vmID == "" {
			http.Error(w, "vm_id required", http.StatusBadRequest)
			return
		}
		count := 3
		if countStr != "" {
			fmt.Sscanf(countStr, "%d", &count)
		}

		sourceVM := p.GetActive(vmID)
		if sourceVM == nil {
			http.Error(w, "VM not found", http.StatusNotFound)
			return
		}

		snapshotDir := fmt.Sprintf("%s/snapshots/%s", workDir, vmID)
		start := time.Now()
		branches, err := vm.Branch(cfg, sourceVM, snapshotDir, count)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		elapsed := time.Since(start)
		result := map[string]interface{}{
			"source_vm_id":    vmID,
			"branch_count":    len(branches),
			"total_time_ms":   elapsed.Milliseconds(),
			"branches":        []map[string]interface{}{},
		}
		branchList := make([]map[string]interface{}, len(branches))
		for i, b := range branches {
			p.TrackActive(b)
			branchList[i] = map[string]interface{}{
				"vm_id": b.ID,
				"pid":   b.PID,
			}
		}
		result["branches"] = branchList
		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("firecracker-agent listening on %s", listenAddr)

	server := &http.Server{Handler: mux}
	go server.Serve(ln)

	// Warm pool maintenance loop
	go func() {
		for {
			time.Sleep(5 * time.Second)
			p.Fill(warmVCPUs, warmMemMiB)
		}
	}()

	// Wait for shutdown signal
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	<-ctx.Done()

	log.Println("shutting down...")
	server.Shutdown(context.Background())
	p.Shutdown()
}
