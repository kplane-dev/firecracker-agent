//go:build linux

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// worker is a persistent language runtime process that evaluates code without
// the overhead of process startup on each exec.
type worker struct {
	mu     sync.Mutex
	stdin  io.WriteCloser
	stdout *bufio.Reader
	cmd    *exec.Cmd
}

// sentinelEnd is written after each eval to delimit the output.
// Must be unique enough to not appear in normal output.
const sentinelEnd = "__AGENT_EVAL_END__"

// startNodeWorker starts a persistent Node.js process with a JSON eval loop.
func startNodeWorker() (*worker, error) {
	cmd := exec.Command("node", "-e", nodeEvalLoop)
	cmd.Env = append(os.Environ(), "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start node worker: %w", err)
	}

	return &worker{
		stdin:  stdin,
		stdout: bufio.NewReader(stdout),
		cmd:    cmd,
	}, nil
}

// startPythonWorker starts a persistent Python process with a JSON eval loop.
func startPythonWorker() (*worker, error) {
	cmd := exec.Command("python3", "-u", "-c", pythonEvalLoop)
	cmd.Env = append(os.Environ(), "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start python worker: %w", err)
	}

	return &worker{
		stdin:  stdin,
		stdout: bufio.NewReader(stdout),
		cmd:    cmd,
	}, nil
}

// eval sends code to the worker and returns the output.
// Thread-safe — serializes access to the worker process.
func (w *worker) eval(code string) ExecResponse {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Send the code as a JSON line
	req, _ := json.Marshal(code)
	fmt.Fprintf(w.stdin, "%s\n", req)

	// Read output until sentinel
	var output strings.Builder
	for {
		line, err := w.stdout.ReadString('\n')
		if err != nil {
			return ExecResponse{Stderr: fmt.Sprintf("worker read: %v", err), ExitCode: 1}
		}
		line = strings.TrimRight(line, "\n")
		if line == sentinelEnd {
			break
		}
		if output.Len() > 0 {
			output.WriteByte('\n')
		}
		output.WriteString(line)
	}

	// Read the exit status line
	statusLine, _ := w.stdout.ReadString('\n')
	statusLine = strings.TrimSpace(statusLine)
	exitCode := 0
	if statusLine != "0" {
		exitCode = 1
	}

	return ExecResponse{Stdout: output.String() + "\n", ExitCode: exitCode}
}

func (w *worker) stop() {
	w.stdin.Close()
	w.cmd.Wait()
}

// The Node.js eval loop: reads JSON-encoded code from stdin, evaluates it,
// prints output, then prints the sentinel and exit status.
const nodeEvalLoop = `
const readline = require('readline');
const rl = readline.createInterface({ input: process.stdin });
const SENTINEL = '` + sentinelEnd + `';

rl.on('line', (line) => {
  let code;
  try { code = JSON.parse(line); } catch(e) { code = line; }
  let exitCode = 0;
  try {
    const result = eval(code);
    if (result !== undefined) console.log(result);
  } catch(e) {
    console.log(e.message);
    exitCode = 1;
  }
  console.log(SENTINEL);
  console.log(exitCode);
});
`

// The Python eval loop: reads JSON-encoded code from stdin, exec's it,
// prints output, then prints the sentinel and exit status.
const pythonEvalLoop = `
import sys, json

SENTINEL = '` + sentinelEnd + `'

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        code = json.loads(line)
    except:
        code = line
    exit_code = 0
    try:
        exec(code)
    except Exception as e:
        print(str(e))
        exit_code = 1
    print(SENTINEL)
    print(exit_code)
    sys.stdout.flush()
`
