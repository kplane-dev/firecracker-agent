// Minimal eval server — runs code sent via HTTP POST.
// Started at boot, captured in snapshot, survives restore.
const http = require('http');
const vm = require('vm');

const context = vm.createContext({ console, require, process, setTimeout, setInterval, Buffer });

const server = http.createServer((req, res) => {
  let body = '';
  req.on('data', c => body += c);
  req.on('end', () => {
    let stdout = '';
    const fakeConsole = {
      log: (...args) => { stdout += args.map(String).join(' ') + '\n'; },
      error: (...args) => { stdout += args.map(String).join(' ') + '\n'; },
    };
    context.console = fakeConsole;

    let exitCode = 0;
    let stderr = '';
    try {
      const result = vm.runInContext(body, context, { timeout: 30000 });
      if (result !== undefined && stdout === '') {
        stdout = String(result) + '\n';
      }
    } catch (e) {
      stderr = e.message;
      exitCode = 1;
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ stdout, stderr, exit_code: exitCode }));
  });
});

server.listen(1025, '0.0.0.0', () => {
  process.stderr.write('eval-server: listening on :1025\n');
});
