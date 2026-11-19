// Copies the jobrun CLI and its agent skill into the extension (vscode:prepublish), so the extension works
// without a separate install and can run `jobrun setup` for the terminal and the agents.
const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..', '..');
const out = path.join(__dirname, '..', 'bundle');
fs.rmSync(out, { recursive: true, force: true });
fs.mkdirSync(path.join(out, 'skills', 'jobrun'), { recursive: true });
fs.copyFileSync(path.join(root, 'jobrun.py'), path.join(out, 'jobrun.py'));
fs.copyFileSync(path.join(root, 'skills', 'jobrun', 'SKILL.md'), path.join(out, 'skills', 'jobrun', 'SKILL.md'));
fs.chmodSync(path.join(out, 'jobrun.py'), 0o755);
// `jobrun` for VS Code's terminals (the extension appends bundle/bin to their PATH)
fs.mkdirSync(path.join(out, 'bin'));
fs.writeFileSync(path.join(out, 'bin', 'jobrun'), '#!/bin/sh\nexec python3 "$(dirname "$0")/../jobrun.py" "$@"\n', { mode: 0o755 });
console.log(`bundled jobrun.py, bin/jobrun and skills/jobrun into ${out}`);
