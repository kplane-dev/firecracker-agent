// benchmark measures TTI: create sandbox → node -v → response.
// Uses persistent HTTP connections (like a real SDK would).
//
// Measures:
//   - claim_ms: server-side claim latency (from /claim response)
//   - exec1_ms: first exec RTT (cold — first command after resume)
//   - exec2_ms: second exec RTT on the same VM (warm — reveals page cache effects)
//   - total_ms: claim + exec1 (the user-visible TTI)
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type result struct {
	claimMs    int
	exec1Ms    int
	exec2Ms    int
	ttiMs      int // claim + exec1 — the user-visible TTI
	fromWarm   bool
}

func main() {
	var (
		agentURL string
		image    string
		cmd      string
		runs     int
	)
	flag.StringVar(&agentURL, "agent", "http://localhost:8090", "Agent URL")
	flag.StringVar(&image, "image", "node", "Image to claim")
	flag.StringVar(&cmd, "cmd", "node -v", "Command to execute")
	flag.IntVar(&runs, "runs", 100, "Number of iterations")
	flag.Parse()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	var results []result
	success, fail := 0, 0

	fmt.Printf("TTI breakdown: %d runs (%s → %s)\n\n", runs, image, cmd)

	for i := 0; i < runs; i++ {
		totalStart := time.Now()

		// Claim
		resp, err := client.Get(fmt.Sprintf("%s/claim?image=%s", agentURL, image))
		if err != nil {
			fail++
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var claim struct {
			VMID        string `json:"vm_id"`
			ClaimTimeMs int64  `json:"claim_time_ms"`
			FromWarm    bool   `json:"from_warm_pool"`
		}
		json.Unmarshal(body, &claim)
		if claim.VMID == "" {
			fail++
			continue
		}

		cmdEncoded := strings.ReplaceAll(cmd, " ", "%20")

		// First exec (cold — first command after resume)
		exec1Start := time.Now()
		resp, err = client.Get(fmt.Sprintf("%s/exec?vm_id=%s&cmd=%s", agentURL, claim.VMID, cmdEncoded))
		if err != nil {
			fail++
			client.Get(fmt.Sprintf("%s/release?vm_id=%s", agentURL, claim.VMID))
			continue
		}
		body, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		exec1Ms := int(time.Since(exec1Start).Milliseconds())
		ttiMs := int(time.Since(totalStart).Milliseconds()) // claim + exec1

		firstOutput := string(body)

		// Second exec (warm — same VM, immediately after)
		exec2Start := time.Now()
		resp, err = client.Get(fmt.Sprintf("%s/exec?vm_id=%s&cmd=%s", agentURL, claim.VMID, cmdEncoded))
		if err != nil {
			fail++
			client.Get(fmt.Sprintf("%s/release?vm_id=%s", agentURL, claim.VMID))
			continue
		}
		body, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		exec2Ms := int(time.Since(exec2Start).Milliseconds())

		// Release (not timed)
		client.Get(fmt.Sprintf("%s/release?vm_id=%s", agentURL, claim.VMID))

		if strings.Contains(firstOutput, "v22") || strings.Contains(firstOutput, "v20") || strings.Contains(firstOutput, "ok") {
			success++
			results = append(results, result{
				claimMs:  int(claim.ClaimTimeMs),
				exec1Ms:  exec1Ms,
				exec2Ms:  exec2Ms,
				ttiMs:    ttiMs,
				fromWarm: claim.FromWarm,
			})
		} else {
			fail++
		}

		if (i+1)%25 == 0 {
			fmt.Printf("  [%d/%d] success=%d fail=%d\n", i+1, runs, success, fail)
		}
	}

	if len(results) == 0 {
		fmt.Println("All runs failed")
		return
	}

	fmt.Printf("\nSuccess: %d/%d\n\n", success, success+fail)

	printStats := func(label string, vals []int) {
		sort.Ints(vals)
		total := 0
		for _, v := range vals {
			total += v
		}
		fmt.Printf("%-12s  avg=%3dms  p50=%3dms  p95=%3dms  p99=%3dms  min=%3dms  max=%3dms\n",
			label,
			total/len(vals),
			vals[len(vals)/2],
			vals[len(vals)*95/100],
			vals[len(vals)*99/100],
			vals[0],
			vals[len(vals)-1],
		)
	}

	var claimVals, exec1Vals, exec2Vals, ttiVals []int
	warmCount := 0
	for _, r := range results {
		claimVals = append(claimVals, r.claimMs)
		exec1Vals = append(exec1Vals, r.exec1Ms)
		exec2Vals = append(exec2Vals, r.exec2Ms)
		ttiVals = append(ttiVals, r.ttiMs)
		if r.fromWarm {
			warmCount++
		}
	}

	fmt.Printf("Warm pool hits: %d/%d (%.0f%%)\n\n", warmCount, len(results), float64(warmCount)/float64(len(results))*100)

	printStats("TTI:", ttiVals)
	fmt.Println()
	printStats("  claim:", claimVals)
	printStats("  exec1:", exec1Vals)
	printStats("  exec2:", exec2Vals)

	// Highlight the delta between exec1 and exec2
	sort.Ints(exec1Vals)
	sort.Ints(exec2Vals)
	delta := exec1Vals[len(exec1Vals)/2] - exec2Vals[len(exec2Vals)/2]
	fmt.Printf("\nexec1 p50 - exec2 p50 = %dms  (page cache / V8 effect)\n", delta)
	if delta <= 5 {
		fmt.Println("→ exec1 ≈ exec2: V8 startup is CPU-bound, not page cache")
	} else {
		fmt.Printf("→ exec1 > exec2 by %dms: first exec pays a warm-up cost\n", delta)
	}
}
