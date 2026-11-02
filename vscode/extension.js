// Job Board: a tree view over `jobrun ls --json`. All job logic lives in the jobrun CLI;
// this file only polls it, renders rows and forwards stop/remove/clean.
const vscode = require('vscode');
const cp = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const ACTIVE = new Set(['starting', 'pending', 'running', 'unknown']);
const ICONS = {
  starting: ['loading~spin', undefined],
  pending: ['clock', 'charts.yellow'],
  running: ['sync~spin', 'charts.blue'],
  unknown: ['question', 'charts.yellow'],
  done: ['pass-filled', 'testing.iconPassed'],
  failed: ['error', 'testing.iconFailed'],
  killed: ['circle-slash', 'disabledForeground'],
  timeout: ['watch', 'testing.iconFailed'],
  oom: ['error', 'testing.iconFailed'],
  ended: ['circle-large-outline', undefined],
  lost: ['warning', 'charts.orange'],
};

function config() {
  return vscode.workspace.getConfiguration('jobboard');
}

function jobrun(args) {
  const configured = config().get('command');
  const local = path.join(os.homedir(), '.local', 'bin', 'jobrun');
  const command = configured || (fs.existsSync(local) ? local : 'jobrun');
  const env = { ...process.env };
  if (config().get('registry')) env.JOBRUN_HOME = config().get('registry');
  return new Promise((resolve, reject) => {
    cp.execFile(command, args, { env, timeout: 60000, maxBuffer: 64 * 1024 * 1024 }, (err, stdout, stderr) => {
      if (err) reject(new Error((stderr || err.message).trim()));
      else resolve(stdout);
    });
  });
}

function duration(seconds) {
  if (seconds == null) return '';
  const s = Math.floor(seconds);
  if (s >= 86400) return `${Math.floor(s / 86400)}d${String(Math.floor((s % 86400) / 3600)).padStart(2, '0')}h`;
  if (s >= 3600) return `${Math.floor(s / 3600)}h${String(Math.floor((s % 3600) / 60)).padStart(2, '0')}m`;
  return `${Math.floor(s / 60)}m${String(s % 60).padStart(2, '0')}s`;
}

function rate(perSecond) {
  if (!perSecond) return '';
  for (const [unit, scale] of [['s', 1], ['min', 60], ['h', 3600]]) {
    if (perSecond * scale >= 1) return `${+(perSecond * scale).toPrecision(3)}/${unit}`;
  }
  return `${+(perSecond * 86400).toPrecision(3)}/d`;
}

function clock(epoch) {
  if (!epoch) return '';
  const when = new Date(epoch * 1000);
  const time = when.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  if (when.toDateString() === new Date().toDateString()) return time;
  return `${when.toLocaleDateString([], { weekday: 'short' })} ${time}`;
}

function count(n) {
  if (n == null) return '';
  for (const [suffix, scale] of [['G', 1e9], ['M', 1e6], ['k', 1e3]]) {
    if (n >= (suffix === 'k' ? 10 * scale : scale)) return `${+(n / scale).toPrecision(3)}${suffix}`;
  }
  return String(n);
}

function bar(fraction, width = 10) {
  const filled = Math.round(Math.max(0, Math.min(1, fraction)) * width);
  return '█'.repeat(filled) + '░'.repeat(width - filled);
}

function describe(job) {
  const parts = [];
  if (job.jobs) parts.push(`${job.finished}/${job.jobs} jobs${job.failed ? ` (${job.failed} failed)` : ''}`);
  if (ACTIVE.has(job.status)) {
    if (job.fraction != null) parts.push(`${bar(job.fraction)} ${Math.round(job.fraction * 100)}%`, `${count(job.done)}/${count(job.total)}`);
    else if (job.done != null) parts.push(`${count(job.done)} done`);
    else parts.push(job.status);
    if (job.rate_per_s) parts.push(rate(job.rate_per_s));
    if (job.eta) parts.push(`ETA ${clock(job.eta)}`);
    parts.push(duration(job.elapsed_s));
  } else {
    parts.push(job.exit_code != null && job.status !== 'done' ? `${job.status} (${job.exit_code})` : job.status);
    if (job.done != null) parts.push(job.total ? `${count(job.done)}/${count(job.total)}` : `${count(job.done)} done`);
    parts.push(duration(job.elapsed_s));
    if (job.ended) parts.push(`ended ${clock(job.ended)}`);
  }
  return parts.join(' · ');
}

function tooltip(job) {
  const md = new vscode.MarkdownString();
  const where = [job.host, job.slurm_id && `slurm ${job.slurm_id}`, job.pid && `pid ${job.pid}`].filter(Boolean).join(', ');
  md.appendMarkdown(`**${job.name}** — ${job.status}\n\n`);
  const rows = [
    ['id', job.id], ['where', where], ['started', job.started && new Date(job.started * 1000).toLocaleString()],
    ['ended', job.ended && new Date(job.ended * 1000).toLocaleString()], ['elapsed', duration(job.elapsed_s)],
    ['progress', job.done != null ? `${job.done}${job.total ? ` / ${job.total}` : ''}` : ''],
    ['rate', rate(job.rate_per_s)], ['ETA', job.eta && new Date(job.eta * 1000).toLocaleString()],
    ['cwd', job.cwd], ['log', job.log],
  ];
  for (const [key, value] of rows) if (value) md.appendMarkdown(`${key}: \`${value}\`  \n`);
  if (job.cmd) md.appendCodeblock(job.cmd, 'shell');
  if (job.last_line) md.appendCodeblock(job.last_line, 'text');
  return md;
}

class JobsProvider {
  constructor() {
    this.emitter = new vscode.EventEmitter();
    this.onDidChangeTreeData = this.emitter.event;
    this.jobs = [];
    this.groups = [];
    this.error = null;
    this.loaded = false;
    this.busy = false;
    this.previous = new Map();
    this.statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 50);
    this.statusBar.command = 'jobboard.jobs.focus';
  }

  async refresh() {
    if (this.busy) return;
    this.busy = true;
    try {
      ({ jobs: this.jobs, groups: this.groups } = JSON.parse(
        await jobrun(['ls', '--json', '--days', String(config().get('finishedDays'))])));
      this.error = null;
      this.notify();
    } catch (err) {
      this.error = err.message;
    } finally {
      this.busy = false;
    }
    this.loaded = true;
    this.updateStatusBar();
    this.emitter.fire();
  }

  // One message per finished ungrouped job, per failed group member, and per finished group.
  notify() {
    const seen = this.previous;
    this.previous = new Map([...this.jobs.map(job => [job.id, job.status]),
      ...this.groups.map(g => [`group:${g.name}`, g.status])]);
    if (!seen.size || !config().get('notifyOnFinish')) return;
    const finished = (key, status) => seen.has(key) && ACTIVE.has(seen.get(key)) && !ACTIVE.has(status);
    for (const job of this.jobs) {
      if (!finished(job.id, job.status) || (job.group && job.status === 'done')) continue;
      const text = `Job "${job.name}"${job.group ? ` (${job.group})` : ''} ${job.status} after ${duration(job.elapsed_s)}`;
      const show = job.status === 'done' ? vscode.window.showInformationMessage : vscode.window.showWarningMessage;
      show(text, 'Open Log').then(choice => { if (choice) openLog(job); });
    }
    for (const g of this.groups) {
      if (!finished(`group:${g.name}`, g.status)) continue;
      const text = `Group "${g.name}" finished: ${g.jobs - g.failed}/${g.jobs} jobs done after ${duration(g.elapsed_s)}`;
      (g.failed ? vscode.window.showWarningMessage : vscode.window.showInformationMessage)(text);
    }
  }

  updateStatusBar() {
    const running = this.jobs.filter(job => ACTIVE.has(job.status));
    if (!running.length) return this.statusBar.hide();
    this.statusBar.text = `$(pulse) ${running.length} job${running.length > 1 ? 's' : ''}`;
    this.statusBar.tooltip = running.map(job => `${job.name}: ${describe(job)}`).join('\n');
    this.statusBar.show();
  }

  getTreeItem(element) {
    return element;
  }

  getChildren(element) {
    if (!element) {
      if (this.error) {
        const item = new vscode.TreeItem(`jobrun failed: ${this.error}`);
        item.iconPath = new vscode.ThemeIcon('error');
        item.tooltip = this.error;
        return [item];
      }
      if (!this.loaded) return [new vscode.TreeItem('Loading…')];
      const entries = [
        ...this.groups.map(g => ({ ...g, members: this.jobs.filter(job => job.group === g.name) })),
        ...this.jobs.filter(job => !job.group),
      ];
      const active = entries.filter(e => ACTIVE.has(e.status)).sort((a, b) => b.started - a.started);
      const finished = entries.filter(e => !ACTIVE.has(e.status))
        .sort((a, b) => (b.ended || b.started) - (a.ended || a.started));
      return [section('Running', active, true), section('Finished', finished, false)];
    }
    if (element.entries) return element.entries.map(e => (e.members ? groupItem(e) : jobItem(e)));
    if (element.group) return element.group.members.map(jobItem);
    if (element.job) return detailItems(element.job);
    return [];
  }
}

function section(label, entries, expanded) {
  const item = new vscode.TreeItem(`${label} (${entries.length})`,
    expanded ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.Collapsed);
  item.id = `section-${label}`;
  item.entries = entries;
  return item;
}

function groupItem(group) {
  const item = new vscode.TreeItem(group.name, vscode.TreeItemCollapsibleState.Collapsed);
  item.id = `group:${group.name}`;
  item.group = group;
  item.description = describe(group);
  const [, color] = ICONS[group.status] || [];
  item.iconPath = new vscode.ThemeIcon('layers', color && new vscode.ThemeColor(color));
  item.tooltip = `${group.name}: ${group.status}\n${describe(group)}`;
  item.contextValue = ACTIVE.has(group.status) ? 'group-active' : 'group-finished';
  return item;
}

function jobItem(job) {
  const item = new vscode.TreeItem(job.name, vscode.TreeItemCollapsibleState.Collapsed);
  item.id = job.id;
  item.job = job;
  item.description = describe(job);
  item.tooltip = tooltip(job);
  const [icon, color] = ICONS[job.status] || ['circle-outline', undefined];
  item.iconPath = new vscode.ThemeIcon(icon, color && new vscode.ThemeColor(color));
  item.contextValue = ACTIVE.has(job.status) ? 'job-active' : 'job-finished';
  return item;
}

function detailItems(job) {
  const where = [job.host, job.slurm_id && `slurm ${job.slurm_id}`, job.pid && `pid ${job.pid}`].filter(Boolean).join(', ');
  const details = [
    ['id', job.id, 'tag'], ['where', where, 'server'], ['started', job.started && new Date(job.started * 1000).toLocaleString(), 'calendar'],
    ['cmd', job.cmd, 'terminal'], ['cwd', job.cwd, 'folder'], ['log', job.log, 'output'], ['last', job.last_line, 'note'],
  ];
  return details.filter(([, value]) => value).map(([key, value, icon]) => {
    const item = new vscode.TreeItem(key);
    item.id = `${job.id}-${key}`;
    item.description = String(value);
    item.tooltip = String(value);
    item.iconPath = new vscode.ThemeIcon(icon);
    if (key === 'log') item.command = { command: 'jobboard.openLog', title: 'Open Log', arguments: [{ job }] };
    if (key === 'cwd') item.command = { command: 'jobboard.openFolder', title: 'Reveal', arguments: [{ job }] };
    return item;
  });
}

async function openLog(job) {
  if (!job.log || !fs.existsSync(job.log)) {
    vscode.window.showWarningMessage(`No log file for "${job.name}"${job.log ? `: ${job.log}` : ''}`);
    return;
  }
  const document = await vscode.workspace.openTextDocument(vscode.Uri.file(job.log));
  const editor = await vscode.window.showTextDocument(document, { preview: true });
  const end = document.lineAt(document.lineCount - 1).range.end;
  editor.selection = new vscode.Selection(end, end);
  editor.revealRange(new vscode.Range(end, end));
}

function activate(context) {
  const provider = new JobsProvider();
  const view = vscode.window.createTreeView('jobboard.jobs', { treeDataProvider: provider, showCollapseAll: true });
  const run = (args, message) => jobrun(args)
    .then(out => { if (message) vscode.window.setStatusBarMessage(message || out.trim(), 4000); })
    .catch(err => vscode.window.showErrorMessage(`jobrun ${args[0]}: ${err.message}`))
    .finally(() => provider.refresh());

  let timer;
  const schedule = () => {
    clearInterval(timer);
    timer = setInterval(() => provider.refresh(), Math.max(2, config().get('refreshSeconds')) * 1000);
  };
  schedule();
  provider.refresh();

  context.subscriptions.push(
    view, provider.statusBar, { dispose: () => clearInterval(timer) },
    vscode.workspace.onDidChangeConfiguration(event => {
      if (event.affectsConfiguration('jobboard')) { schedule(); provider.refresh(); }
    }),
    view.onDidChangeVisibility(event => { if (event.visible) provider.refresh(); }),
    vscode.commands.registerCommand('jobboard.refresh', () => provider.refresh()),
    vscode.commands.registerCommand('jobboard.openLog', item => openLog(item.job)),
    vscode.commands.registerCommand('jobboard.tailLog', item => {
      if (!item.job.log) return vscode.window.showWarningMessage(`No log file for "${item.job.name}"`);
      const terminal = vscode.window.createTerminal({ name: `log: ${item.job.name}` });
      terminal.sendText(`tail -n 200 -F '${item.job.log.replace(/'/g, `'\\''`)}'`);
      terminal.show();
    }),
    vscode.commands.registerCommand('jobboard.openFolder', item => {
      if (item.job.cwd) vscode.commands.executeCommand('revealInExplorer', vscode.Uri.file(item.job.cwd));
    }),
    vscode.commands.registerCommand('jobboard.copyCommand', item => vscode.env.clipboard.writeText(item.job.cmd || '')),
    vscode.commands.registerCommand('jobboard.stopGroup', async item => {
      const running = item.group.members.filter(job => ACTIVE.has(job.status)).length;
      const choice = await vscode.window.showWarningMessage(
        `Stop ${running} running job${running === 1 ? '' : 's'} of group "${item.group.name}"?`, { modal: true }, 'Stop', 'Kill now');
      if (choice) run(['stop', '--group', item.group.name, ...(choice === 'Kill now' ? ['--force'] : [])], `Stopped ${item.group.name}`);
    }),
    vscode.commands.registerCommand('jobboard.removeGroup', item => run(['rm', '--group', item.group.name])),
    vscode.commands.registerCommand('jobboard.stop', async item => {
      const choice = await vscode.window.showWarningMessage(
        `Stop "${item.job.name}" (${item.job.slurm_id ? `slurm ${item.job.slurm_id}` : `pid ${item.job.pid}`})?`,
        { modal: true }, 'Stop', 'Kill now');
      if (choice) run(['stop', item.job.id, ...(choice === 'Kill now' ? ['--force'] : [])], `Stopped ${item.job.name}`);
    }),
    vscode.commands.registerCommand('jobboard.remove', item => run(['rm', item.job.id])),
    vscode.commands.registerCommand('jobboard.clean', async () => {
      const choice = await vscode.window.showWarningMessage('Remove all finished jobs from the list? Logs written by jobrun are deleted; logs it only read are kept.',
        { modal: true }, 'Remove');
      if (choice) run(['clean', '--days', '0'], 'Cleared finished jobs');
    }),
  );
}

function deactivate() {}

module.exports = { activate, deactivate };
