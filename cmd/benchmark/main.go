// benchmark measures TTI: create sandbox → node -v → response.
// Uses persistent HTTP connections (like a real SDK would).
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

	var results []int
	success, fail := 0, 0

	fmt.Printf("Sequential TTI: %d runs (%s → %s)\n\n", runs, image, cmd)

	for i := 0; i < runs; i++ {
		start := time.Now()

		// Claim
		resp, err := client.Get(fmt.Sprintf("%s/claim?image=%s", agentURL, image))
		if err != nil {
			fail++
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var claim struct {
			VMID string `json:"vm_id"`
		}
		json.Unmarshal(body, &claim)
		if claim.VMID == "" {
			fail++
			continue
		}

		// Exec
		resp, err = client.Get(fmt.Sprintf("%s/exec?vm_id=%s&cmd=%s", agentURL, claim.VMID, strings.ReplaceAll(cmd, " ", "%20")))
		if err != nil {
			fail++
			client.Get(fmt.Sprintf("%s/release?vm_id=%s", agentURL, claim.VMID))
			continue
		}
		body, _ = io.ReadAll(resp.Body)
		resp.Body.Close()

		elapsed := time.Since(start)

		// Check success
		if strings.Contains(string(body), "v22") || strings.Contains(string(body), "ok") {
			success++
			results = append(results, int(elapsed.Milliseconds()))
		} else {
			fail++
		}

		// Release (not timed)
		client.Get(fmt.Sprintf("%s/release?vm_id=%s", agentURL, claim.VMID))

		if (i+1)%25 == 0 {
			fmt.Printf("  [%d/%d] success=%d fail=%d\n", i+1, runs, success, fail)
		}
	}

	if len(results) == 0 {
		fmt.Println("All runs failed")
		return
	}

	sort.Ints(results)
	total := 0
	for _, r := range results {
		total += r
	}

	fmt.Printf("\nSuccess: %d/%d\n", success, success+fail)
	fmt.Printf("Average: %dms\n", total/len(results))
	fmt.Printf("Min:     %dms\n", results[0])
	fmt.Printf("Median:  %dms\n", results[len(results)/2])
	fmt.Printf("P95:     %dms\n", results[len(results)*95/100])
	fmt.Printf("P99:     %dms\n", results[len(results)*99/100])
	fmt.Printf("Max:     %dms\n", results[len(results)-1])
}
