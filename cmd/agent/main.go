package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kplane-dev/firecracker-agent/pkg/btrfs"
	"github.com/kplane-dev/firecracker-agent/pkg/pool"
	"github.com/kplane-dev/firecracker-agent/pkg/vm"
)

func main() {
	var (
		listenAddr     string
		firecrackerBin string
		kernelPath     string
		imageDir       string
		workDir        string
		btrfsSize      int
		maxVMs         int
		minWarm        int
		warmImages     string
		warmVCPUs      int
		warmMemMiB     int
	)

	flag.StringVar(&listenAddr, "listen", ":8090", "HTTP API listen address")
	flag.StringVar(&firecrackerBin, "firecracker-bin", "/opt/firecracker/bin/firecracker", "Path to firecracker binary")
	flag.StringVar(&kernelPath, "kernel", "/opt/firecracker/vmlinux", "Path to guest kernel")
	flag.StringVar(&imageDir, "image-dir", "/opt/firecracker/images", "Directory containing rootfs images (<name>.initrd)")
	flag.StringVar(&workDir, "work-dir", "/var/lib/firecracker-agent", "Working directory for VM sockets and state")
	flag.IntVar(&btrfsSize, "btrfs-size", 50, "Size in GB for btrfs loopback image (0 to disable)")
	flag.IntVar(&maxVMs, "max-vms", 10, "Maximum concurrent VMs")
	flag.IntVar(&minWarm, "min-warm", 2, "Minimum warm pool size per image")
	flag.StringVar(&warmImages, "warm-images", "python,node", "Comma-separated list of images to keep warm")
	flag.IntVar(&warmVCPUs, "warm-vcpus", 1, "vCPUs for warm pool VMs")
	flag.IntVar(&warmMemMiB, "warm-memory", 192, "Memory (MiB) for warm pool VMs")
	flag.Parse()

	if err := btrfs.EnsureMount(workDir, btrfsSize); err != nil {
		log.Printf("warn: btrfs setup failed (falling back to host fs): %v", err)
	}

	cfg := vm.Config{
		FirecrackerBin: firecrackerBin,
		KernelPath:     kernelPath,
		ImageDir:       imageDir,
		WorkDir:        workDir,
	}

	images := strings.Split(warmImages, ",")
	p := pool.New(cfg, maxVMs, images, minWarm, warmVCPUs, warmMemMiB)

	log.Printf("filling warm pool (images=%v, min=%d per image, vcpus=%d, mem=%dMiB)...",
		images, minWarm, warmVCPUs, warmMemMiB)
	if err := p.Fill(); err != nil {
		log.Printf("warn: warm pool fill error: %v", err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		warmByImage, active, max := p.Status()
		json.NewEncoder(w).Encode(map[string]interface{}{
			"warm":   warmByImage,
			"active": active,
			"max":    max,
		})
	})

	mux.HandleFunc("/claim", func(w http.ResponseWriter, r *http.Request) {
		image := r.URL.Query().Get("image")
		if image == "" {
			image = "python"
		}
		start := time.Now()
		v, fromWarm, err := p.Claim(image, warmVCPUs, warmMemMiB)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"vm_id":          v.ID,
			"image":          v.Image,
			"pid":            v.PID,
			"from_warm_pool": fromWarm,
			"claim_time_ms":  time.Since(start).Milliseconds(),
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

		start := time.Now()
		branches, err := vm.Branch(cfg, sourceVM, count)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		result := map[string]interface{}{
			"source_vm_id":  vmID,
			"branch_count":  len(branches),
			"total_time_ms": time.Since(start).Milliseconds(),
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

	mux.HandleFunc("/exec", func(w http.ResponseWriter, r *http.Request) {
		vmID := r.URL.Query().Get("vm_id")
		cmdStr := r.URL.Query().Get("cmd")
		if vmID == "" || cmdStr == "" {
			http.Error(w, "vm_id and cmd required", http.StatusBadRequest)
			return
		}

		v := p.GetActive(vmID)
		if v == nil {
			http.Error(w, "VM not found", http.StatusNotFound)
			return
		}

		stdout, stderr, exitCode, err := v.Exec([]string{"/bin/sh", "-c", cmdStr}, 30*time.Second)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"stdout":    stdout,
			"stderr":    stderr,
			"exit_code": exitCode,
		})
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

	// Warm pool maintenance
	go func() {
		for {
			time.Sleep(5 * time.Second)
			p.Fill()
		}
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	<-ctx.Done()

	log.Println("shutting down...")
	server.Shutdown(context.Background())
	p.Shutdown()
}
