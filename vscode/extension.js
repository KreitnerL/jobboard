// Job Board: a tree view over `jobrun ls --json`. All job logic lives in the jobrun CLI;
// this file only polls it, renders rows and forwards stop/remove/clean.
const vscode = require('vscode');
const cp = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const JSON_SCHEMA = 2;  // newest `jobrun ls --json` format this extension understands
const ACTIVE = new Set(['starting', 'waiting', 'running', 'unknown']);
// [codicon, theme color]; active states first, then outcomes
const ICONS = {
  waiting: ['clock', 'charts.yellow'],              // dependency or Slurm queue: not started yet
  starting: ['loading~spin', undefined],
  running: ['sync~spin', 'charts.blue'],
  unknown: ['question', 'charts.yellow'],
  done: ['pass-filled', 'testing.iconPassed'],
  error: ['error', 'testing.iconFailed'],           // non-zero exit, signal, time limit, out of memory
  stopped: ['debug-stop', 'charts.orange'],         // stopped while running
  cancelled: ['circle-slash', 'disabledForeground'],  // never started
  lost: ['warning', 'charts.orange'],               // supervisor vanished, outcome unknown
  ended: ['circle-large-outline', undefined],       // tracked process ended, exit code unknown
};
const LEGACY_STATUS = { failed: 'error', timeout: 'error', oom: 'error', killed: 'stopped', skipped: 'cancelled', pending: 'waiting' };

function normalize(item) {
  item.status = LEGACY_STATUS[item.status] || item.status;
  return item;
}

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

function progressText(item) {
  if (item.done == null) return '';
  return item.total ? `${count(item.done)}/${count(item.total)}` : `${count(item.done)} done`;
}

// Time since the last step next to the average step time, so a hanging job stands out; ⚠ once it is a
// few average steps overdue (`stalled`, decided by jobrun).
function lastStep(job) {
  const average = job.step_s ? ` (avg ${duration(job.step_s)})` : '';
  return `${job.stalled ? '⚠ ' : ''}last step ${duration(job.idle_s)} ago${average}`;
}

function describe(job) {
  const parts = [];
  if (job.jobs) {
    const other = Object.entries(job.counts || {}).filter(([status]) => status !== 'done' && !ACTIVE.has(status))
      .map(([status, n]) => `${n} ${status}`);
    parts.push(`${job.finished}/${job.jobs} jobs${other.length ? ` (${other.join(', ')})` : ''}`);
  }
  if (!job.jobs && job.status === 'waiting') return `waiting for ${(job.waiting_for || []).join(', ') || 'dependencies'}`;
  if (!job.jobs && job.status === 'cancelled') return `cancelled: ${job.reason || 'never started'}`;
  if (job.stage) parts.push(`${job.stage_no}/${job.stage_of || '?'} ${job.stage}`);
  if (ACTIVE.has(job.status)) {
    if (job.fraction != null) parts.push(`${bar(job.fraction)} ${Math.round(job.fraction * 100)}%`, `${count(job.done)}/${count(job.total)}`);
    else if (job.done != null) parts.push(`${count(job.done)} done`);
    else parts.push(job.status);
    if (job.rate_per_s && !job.finishing) parts.push(rate(job.rate_per_s));
    if (job.finishing) parts.push('finishing');
    else if (job.eta) parts.push(`ETA ${clock(job.eta)}`);
    if (job.idle_s != null) parts.push(lastStep(job));
    parts.push(duration(job.elapsed_s));
  } else {
    parts.push(job.reason ? `${job.status}: ${job.reason}` : job.status);
    if (job.done != null) parts.push(progressText(job));
    parts.push(duration(job.elapsed_s));
    if (job.ended) parts.push(`ended ${clock(job.ended)}`);
  }
  return parts.join(' · ');
}

function tooltip(job) {
  const md = new vscode.MarkdownString();
  const where = [job.host, job.slurm_id && `slurm ${job.slurm_id}`, job.pid && `pid ${job.pid}`].filter(Boolean).join(', ');
  md.appendMarkdown(`$(${(ICONS[job.status] || ['circle-outline'])[0].split('~')[0]}) **${job.name}** — ${job.status}\n\n`);
  md.supportThemeIcons = true;
  const rows = [
    ['id', job.id], ['where', where], ['started', job.started && new Date(job.started * 1000).toLocaleString()],
    ['ended', job.ended && new Date(job.ended * 1000).toLocaleString()], ['elapsed', duration(job.elapsed_s)],
    ['progress', job.done != null ? `${job.done}${job.total ? ` / ${job.total}` : ''}` : ''],
    ['rate', rate(job.rate_per_s)], ['ETA', job.eta && new Date(job.eta * 1000).toLocaleString()],
    ['last progress', job.progress_at && `${new Date(job.progress_at * 1000).toLocaleString()} (${duration(Date.now() / 1000 - job.progress_at)} ago)`],
    ['average step', job.step_s && duration(job.step_s)],
    ['cwd', job.cwd], ['log', job.log],
    ['after', (job.after_names || []).join(', ')], ['waiting for', (job.waiting_for || []).join(', ')], ['reason', job.reason],
  ];
  for (const [key, value] of rows) if (value) md.appendMarkdown(`${key}: \`${value}\`  \n`);
  if (job.stalled) md.appendMarkdown(`\n*No step for ${duration(job.idle_s)}, several times the average step (${duration(job.step_s)}): the job may hang.*\n\n`);
  if (job.finishing) md.appendMarkdown('\n*At or past its total but still running: a final phase the progress pattern does not count, or a too small total.*\n\n');
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
    this.stale = false;
    this.view = null;
    this.previous = new Map();
    this.statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 50);
    this.statusBar.command = 'jobboard.jobs.focus';
  }

  // Redraw only when someone can see it. Every tree event makes VS Code show the view's progress bar, and events
  // sent while the window is minimized queue up and replay as a burst of animations when it is restored.
  render(force = false) {
    const seen = this.view && this.view.visible && (vscode.window.state.focused || config().get('updateWhileUnfocused'));
    if (seen || force) {
      this.stale = false;
      this.emitter.fire();
    } else {
      this.stale = true;
    }
  }

  async refresh(force = false) {
    if (this.busy) return;
    this.busy = true;
    try {
      const data = JSON.parse(await jobrun(['ls', '--json', '--days', String(config().get('finishedDays'))]));
      if (Array.isArray(data)) [this.jobs, this.groups] = [data, []];  // jobrun < 0.1
      else if ((data.schema || 1) > JSON_SCHEMA) throw new Error('jobrun is newer than this extension; update the extension');
      else [this.jobs, this.groups] = [data.jobs || [], data.groups || []];
      this.jobs.forEach(normalize);
      this.groups.forEach(normalize);
      this.error = null;
      this.notify();
    } catch (err) {
      this.error = err.message;
    } finally {
      this.busy = false;
    }
    this.loaded = true;
    this.updateStatusBar();
    this.render(force);
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
      const outcome = job.reason ? `${job.status}: ${job.reason}` : job.status;
      const text = `Job "${job.name}"${job.group ? ` (${job.group})` : ''} ${outcome}${job.elapsed_s != null ? ` after ${duration(job.elapsed_s)}` : ''}`;
      const show = job.status === 'done' ? vscode.window.showInformationMessage : vscode.window.showWarningMessage;
      show(text, 'Open Log').then(choice => { if (choice) openLog(job); });
    }
    for (const g of this.groups) {
      if (!finished(`group:${g.name}`, g.status)) continue;
      const counts = Object.entries(g.counts || {}).map(([status, n]) => `${n} ${status}`).join(', ');
      const text = `Group "${g.name}" finished after ${duration(g.elapsed_s)}: ${counts}`;
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
    if (element.entries) {
      const groups = element.entries.filter(e => e.members);
      return [...groups.map(groupItem), ...dependencyOrder(element.entries.filter(e => !e.members)).map(jobItem)];
    }
    if (element.group) return dependencyOrder(element.group.members).map(jobItem);
    if (element.job) return [...stageItems(element.job), ...detailItems(element.job)];
    return [];
  }
}

// Flat list; a job that waits for others (--after) comes after them, so a chain reads top to bottom.
function dependencyOrder(jobs) {
  const byId = new Map(jobs.map(job => [job.id, job]));
  const placed = new Set();
  const ordered = [];
  const place = job => {
    if (placed.has(job.id)) return;
    placed.add(job.id);
    for (const dep of job.after || []) if (byId.has(dep)) place(byId.get(dep));
    ordered.push(job);
  };
  jobs.forEach(place);
  return ordered;
}

function stageItems(job) {
  const now = Date.now() / 1000;
  return (job.stages || []).map((stage, index) => {
    const item = new vscode.TreeItem(`${index + 1}. ${stage.name}`);
    item.id = `${job.id}-stage-${index}`;
    // a finished stage without an end time was read from an old log: its duration is unknown
    const time = stage.started && (stage.ended || stage.status === 'running') ? duration((stage.ended || now) - stage.started) : '';
    item.description = stage.status === 'pending' ? 'pending' : [progressText(stage), time].filter(Boolean).join(' · ');
    const [icon, color] = stage.status === 'pending' ? ['circle-outline', undefined] : ICONS[stage.status] || ['circle-outline'];
    item.iconPath = new vscode.ThemeIcon(icon, color && new vscode.ThemeColor(color));
    return item;
  });
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
  const [icon, color] = job.stalled ? ['sync~spin', 'charts.orange'] : ICONS[job.status] || ['circle-outline', undefined];
  item.iconPath = new vscode.ThemeIcon(icon, color && new vscode.ThemeColor(color));
  item.contextValue = ACTIVE.has(job.status) ? 'job-active' : 'job-finished';
  return item;
}

function detailItems(job) {
  const where = [job.host, job.slurm_id && `slurm ${job.slurm_id}`, job.pid && `pid ${job.pid}`].filter(Boolean).join(', ');
  const details = [
    ['id', job.id, 'tag'], ['after', (job.after_names || []).join(', '), 'debug-step-into'], ['where', where, 'server'], ['started', job.started && new Date(job.started * 1000).toLocaleString(), 'calendar'],
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
  provider.view = view;
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
    view.onDidChangeVisibility(event => { if (event.visible) provider.refresh(true); }),
    vscode.window.onDidChangeWindowState(state => { if (state.focused && provider.stale) provider.refresh(true); }),
    vscode.commands.registerCommand('jobboard.refresh', () => provider.refresh(true)),
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
