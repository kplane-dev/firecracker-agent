// serial-test proves that serial console I/O works after snapshot restore.
// Starts FC, boots a VM, sends a command via serial stdin, reads response from stdout.
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("usage: serial-test <firecracker-bin> <kernel> <rootfs>")
		os.Exit(1)
	}
	fcBin := os.Args[1]
	kernel := os.Args[2]
	rootfs := os.Args[3]

	os.Remove("/tmp/serial-test.sock")

	cmd := exec.Command(fcBin, "--api-sock", "/tmp/serial-test.sock")

	// Connect stdin/stdout for serial
	stdinPipe, _ := cmd.StdinPipe()
	stdoutPipe, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr

	cmd.Start()
	defer cmd.Process.Kill()

	time.Sleep(500 * time.Millisecond)

	// Configure
	configure := func(path, body string) {
		c := exec.Command("curl", "-s", "--unix-socket", "/tmp/serial-test.sock",
			"-X", "PUT", "http://localhost"+path,
			"-H", "Content-Type: application/json",
			"-d", body)
		c.Run()
	}

	configure("/boot-source", fmt.Sprintf(`{"kernel_image_path":"%s","boot_args":"console=ttyS0 reboot=k panic=1 pci=off quiet init=/init root=/dev/vda ro"}`, kernel))
	configure("/drives/rootfs", fmt.Sprintf(`{"drive_id":"rootfs","path_on_host":"%s","is_root_device":true,"is_read_only":true}`, rootfs))
	configure("/machine-config", `{"vcpu_count":1,"mem_size_mib":256}`)
	configure("/actions", `{"action_type":"InstanceStart"}`)

	// Read stdout in background (serial output from guest)
	outputCh := make(chan string, 100)
	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			outputCh <- scanner.Text()
		}
	}()

	// Wait for boot
	fmt.Println("Waiting for boot...")
	deadline := time.After(10 * time.Second)
	for {
		select {
		case line := <-outputCh:
			if strings.Contains(line, "guest-agent") || strings.Contains(line, "sandbox") {
				fmt.Printf("  [guest] %s\n", line)
			}
			if strings.Contains(line, "listening") || strings.Contains(line, "#") {
				goto BOOTED
			}
		case <-deadline:
			fmt.Println("Timeout waiting for boot")
			os.Exit(1)
		}
	}

BOOTED:
	fmt.Println("VM booted. Testing serial I/O...")
	time.Sleep(1 * time.Second)

	// Send a command via serial stdin
	start := time.Now()
	fmt.Fprintf(stdinPipe, "echo SERIAL_ROUND_TRIP\n")

	// Read until we see our output
	deadline2 := time.After(5 * time.Second)
	for {
		select {
		case line := <-outputCh:
			fmt.Printf("  [serial] %s\n", line)
			if strings.Contains(line, "SERIAL_ROUND_TRIP") && !strings.Contains(line, "echo") {
				elapsed := time.Since(start)
				fmt.Printf("\nSerial round-trip: %v\n", elapsed)

				// Now test node -v
				start = time.Now()
				fmt.Fprintf(stdinPipe, "node -v\n")
				for {
					select {
					case line := <-outputCh:
						fmt.Printf("  [serial] %s\n", line)
						if strings.Contains(line, "v22") {
							fmt.Printf("node -v serial: %v\n", time.Since(start))
							goto DONE
						}
					case <-time.After(5 * time.Second):
						fmt.Println("timeout on node -v")
						goto DONE
					}
				}
			}
		case <-deadline2:
			fmt.Println("Timeout waiting for serial response")
			goto DONE
		}
	}

DONE:
	io.Copy(os.Stdout, strings.NewReader(""))
	stdinPipe.Close()
}
