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
const DEGRADED_COLORS = { error: 'charts.red', warn: 'charts.yellow' };  // a running job that reported JOBRUN_EVENT
const LEGACY_STATUS = { failed: 'error', timeout: 'error', oom: 'error', killed: 'stopped', skipped: 'cancelled', pending: 'waiting' };

function normalize(item) {
  item.status = LEGACY_STATUS[item.status] || item.status;
  return item;
}

function config() {
  return vscode.workspace.getConfiguration('jobboard');
}

// A setting's value only when the user set it (user, remote or workspace settings), else undefined.
function explicitValue(key) {
  const inspected = config().inspect(key) || {};
  return [inspected.workspaceFolderValue, inspected.workspaceValue, inspected.globalValue].find(v => v !== undefined);
}

// Settings that jobrun itself uses, kept in the registry's config.json so they also hold for jobs started outside
// VS Code and on other machines: VS Code setting -> `jobrun config` key.
const SHARED_SETTINGS = {
  stallFactor: 'stall_factor', stallGapFactor: 'stall_gap_factor', stallMinSeconds: 'stall_min_s', maxLogMB: 'max_log_mb',
  logKeepHeadMB: 'log_keep_head_mb',
  autoCleanDays: 'auto_clean_days',
};

// Write the shared settings the user has set; a setting reset in VS Code (event) goes back to jobrun's default.
// Settings never touched in VS Code are left alone, so a value set with `jobrun config` survives.
async function syncSharedSettings(event) {
  const args = [];
  for (const [key, name] of Object.entries(SHARED_SETTINGS)) {
    const value = explicitValue(key);
    if (value !== undefined) args.push(`${name}=${value}`);
    else if (event && event.affectsConfiguration(`jobboard.${key}`)) args.push('--unset', name);
  }
  if (!args.length) return;
  try {
    await jobrun(['config', ...args]);
  } catch (err) {
    vscode.window.showWarningMessage(`Job Board could not store its settings with jobrun (${err.message}); update jobrun with \`jobrun setup\`.`);
  }
}

// Events that notify (jobboard.notifyOn); the old notifyOnFinish: false, if set and notifyOn is not, means none.
function notifyEvents() {
  if (explicitValue('notifyOn') === undefined && explicitValue('notifyOnFinish') === false) return new Set();
  return new Set(config().get('notifyOn') || []);
}

let jobrunSettings = {};  // the effective jobrun settings, from `jobrun ls --json`

function logBig(job) {
  return !job.log_owned && jobrunSettings.max_log_mb > 0 && job.log_bytes > jobrunSettings.max_log_mb * 2 ** 20;
}

function logNote(job) {
  if (job.log_dropped > 0) return `${bytes(job.log_dropped)} cut from the middle (log cap ${jobrunSettings.max_log_mb || '?'} MB)`;
  if (logBig(job)) return `${bytes(job.log_bytes)}: larger than the log cap (${jobrunSettings.max_log_mb} MB), which applies only to logs jobrun writes`;
  return '';
}

let extensionPath = '';

function onPath(name) {
  return (process.env.PATH || '').split(path.delimiter).some(dir => dir && fs.existsSync(path.join(dir, name)));
}

// The jobrun CLI: the setting, ~/.local/bin/jobrun (jobrun setup), jobrun on PATH (pipx), else the copy bundled
// with this extension, so the Job Board works right after installing it from a .vsix or the Marketplace.
function jobrunCommand() {
  const configured = config().get('command');
  if (configured) return [configured, []];
  const local = path.join(os.homedir(), '.local', 'bin', 'jobrun');
  if (fs.existsSync(local)) return [local, []];
  if (onPath('jobrun')) return ['jobrun', []];
  return ['python3', [path.join(extensionPath, 'bundle', 'jobrun.py')]];
}

function jobrun(args) {
  const [command, prefix] = jobrunCommand();
  const env = { ...process.env };
  if (config().get('registry')) env.JOBRUN_HOME = config().get('registry');
  return new Promise((resolve, reject) => {
    cp.execFile(command, [...prefix, ...args], { env, timeout: 60000, maxBuffer: 64 * 1024 * 1024 }, (err, stdout, stderr) => {
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

function eta(epoch) {
  const left = `in ${duration(Math.max(0, epoch - Date.now() / 1000))}`;
  const format = config().get('etaFormat');
  return format === 'remaining' ? `ETA ${left}` : format === 'both' ? `ETA ${clock(epoch)} (${left})` : `ETA ${clock(epoch)}`;
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

// Time since the last step next to the average step time, so a hanging job stands out; ⚠ once it is well
// overdue (`stalled`, decided by jobrun).
function lastStep(job) {
  const average = job.step_s ? ` (avg ${duration(job.step_s)})` : '';
  return `${job.stalled ? '⚠ ' : ''}last step ${duration(job.idle_s)} ago${average}`;
}

// The newest problem a job reported with a JOBRUN_EVENT line (a group: the most severe level among its jobs).
function eventText(job) {
  if (!job.degraded) return '';
  const newest = (job.events || []).slice(-1)[0];
  if (!newest) return `⚠ ${job.degraded}`;
  const more = job.event_count > 1 ? ` (+${job.event_count - 1} more)` : '';
  return `⚠ ${newest.level}: ${newest.message}${more}`;
}

// Slurm pending reasons that need a word of explanation
const REASON_HINTS = {
  PartitionConfig: "PartitionConfig: the partition does not admit this job's account or group, it will never start (try -A)",
};

// A job in the Slurm queue: when and where the scheduler expects to start it, and why it still waits.
function queueText(job) {
  const parts = [];
  if (job.expected_start && job.expected_start > Date.now() / 1000) {
    parts.push(`start ~${clock(job.expected_start)}${job.sched_nodes ? ` on ${job.sched_nodes}` : ''}`);
  }
  if (job.pending_reason) parts.push(`(${REASON_HINTS[job.pending_reason] || job.pending_reason})`);
  return parts.join(' ');
}

function waitingText(job) {
  return [`waiting for ${(job.waiting_for || []).join(', ') || 'dependencies'}`, queueText(job)].filter(Boolean).join(' · ');
}

// A pool's workers: how many run, on how many hosts, and how many failed.
// A pool's members: running workers (all while none runs), monitors, the machines the workers run on, failures.
function workersText(pool, jobs) {
  const members = jobs.filter(job => job.pool === pool.id && job.role !== 'monitor');
  const failed = members.filter(job => ['error', 'lost'].includes(job.status)).length;
  const n = pool.active_workers || members.length;
  const monitors = (pool.monitors || []).length;
  const hosts = pool.hosts || [];
  const where = hosts.length > 3 ? ` on ${hosts.length} hosts` : hosts.length ? ` on ${hosts.join(', ')}` : '';
  const monitor = monitors === 1 ? ' + monitor' : monitors ? ` + ${monitors} monitors` : '';
  return `${n} worker${n === 1 ? '' : 's'}${monitor}${where}${failed ? ` · ${failed} failed` : ''}`;
}

// A pool's worker: where it runs and what it does; the one progress bar is on the pool's row.
function describeWorker(job) {
  const parts = [];
  if (job.degraded) parts.push(eventText(job));
  if (job.status === 'waiting') return waitingText(job);
  parts.push(job.reason ? `${job.status}: ${job.reason}` : job.status);
  if (job.role === 'monitor') parts.push('monitor');
  parts.push([job.host, job.slurm_id && `slurm ${job.slurm_id}`].filter(Boolean).join(' '));
  if (job.item_label && ACTIVE.has(job.status)) parts.push(`last: ${job.item_label}`);
  parts.push(duration(job.elapsed_s));
  return parts.filter(Boolean).join(' · ');
}

// part: 'all' (one row), 'head' (what comes before the progress) or 'tail' (the progress and what follows it)
function describe(job, jobs = [], part = 'all') {
  if (job.pool) return part === 'tail' ? '' : describeWorker(job);
  const parts = [];
  if (job.degraded) parts.push(eventText(job));
  if (job.jobs) {
    const other = Object.entries(job.counts || {}).filter(([status]) => status !== 'done' && !ACTIVE.has(status))
      .map(([status, n]) => `${n} ${status}`);
    parts.push(`${job.finished}/${job.jobs} jobs${other.length ? ` (${other.join(', ')})` : ''}`);
  }
  if (job.kind === 'pool') parts.push(workersText(job, jobs));
  if (!job.jobs && job.status === 'waiting') return part === 'tail' ? '' : waitingText(job);
  if (!job.jobs && job.status === 'cancelled') return part === 'tail' ? '' : `cancelled: ${job.reason || 'never started'}`;
  if (job.stage) parts.push(`${job.stage_no}/${job.stage_of || '?'} ${job.stage}`);
  let cut = null;
  if (ACTIVE.has(job.status)) {
    cut = parts.length;
    const show = new Set(config().get('rowDetails') || ['rate', 'eta', 'lastStep', 'elapsed']);
    if (job.fraction != null) parts.push(`${bar(job.fraction)} ${Math.round(job.fraction * 100)}%`, `${count(job.done)}/${count(job.total)}`);
    else if (job.done != null) parts.push(`${count(job.done)} done`);
    else parts.push(job.status);
    if (job.rate_per_s && !job.finishing && show.has('rate')) parts.push(rate(job.rate_per_s));
    if (job.finishing) parts.push('finishing');
    else if (job.eta && show.has('eta')) parts.push(eta(job.eta));
    if (job.idle_s != null && (show.has('lastStep') || job.stalled)) parts.push(lastStep(job));
    if (show.has('elapsed')) parts.push(duration(job.elapsed_s));
  } else {
    parts.push(job.reason ? `${job.status}: ${job.reason}` : job.status);
    if (job.done != null) parts.push(progressText(job));
    parts.push(duration(job.elapsed_s));
    if (job.ended) parts.push(`ended ${clock(job.ended)}`);
  }
  if (part === 'head') return parts.slice(0, cut ?? parts.length).join(' · ');
  if (part === 'tail') return cut == null ? '' : parts.slice(cut).join(' · ');
  return parts.join(' · ');
}

// Rows the user has expanded. With jobboard.progressRow, an expanded running job or group shows its progress on a
// row of its own under the name (progressItem), so a narrow sidebar still shows it; collapsed rows keep one line.
const expandedRows = new Set();

function progressSplit(entry, jobs) {
  return config().get('progressRow') !== false && expandedRows.has(entry.id) && !entry.pool
    && ACTIVE.has(entry.status) && describe(entry, jobs, 'tail') !== '';
}

function progressItem(entry, jobs) {
  const item = new vscode.TreeItem('');
  item.id = `${entry.id}-progress`;
  item.description = describe(entry, jobs, 'tail');  // grey, as on the collapsed row
  item.tooltip = describe(entry, jobs);
  item.iconPath = new vscode.ThemeIcon('pulse');
  return item;
}

function tooltip(job) {
  const md = new vscode.MarkdownString();
  const where = job.kind === 'pool' ? (job.hosts || []).join(', ')
    : [job.host, job.slurm_id && `slurm ${job.slurm_id}`, job.pid && `pid ${job.pid}`].filter(Boolean).join(', ');
  md.appendMarkdown(`$(${(ICONS[job.status] || ['circle-outline'])[0].split('~')[0]}) **${job.name}** — ${job.status}\n\n`);
  md.supportThemeIcons = true;
  const rows = [
    ['id', job.id], ['where', where], ['started', job.started && new Date(job.started * 1000).toLocaleString()],
    ['ended', job.ended && new Date(job.ended * 1000).toLocaleString()], ['elapsed', duration(job.elapsed_s)],
    ['progress', job.done != null ? `${job.done}${job.total ? ` / ${job.total}` : ''}` : ''],
    ['rate', rate(job.rate_per_s)], ['ETA', job.eta && new Date(job.eta * 1000).toLocaleString()],
    ['last progress', job.progress_at && `${new Date(job.progress_at * 1000).toLocaleString()} (${duration(Date.now() / 1000 - job.progress_at)} ago)`],
    ['average step', job.step_s && duration(job.step_s)],
    ['cwd', job.cwd], ['log', job.log], ['log size', logNote(job)],
    ['after', (job.after_names || []).join(', ')], ['waiting for', (job.waiting_for || []).join(', ')], ['reason', job.reason],
    ['expected start', job.expected_start && `${new Date(job.expected_start * 1000).toLocaleString()} (Slurm's estimate; it moves as other jobs end or arrive)`],
    ['planned nodes', job.sched_nodes], ['why queued', job.pending_reason],
    ['workers', job.kind === 'pool' && `${job.active_workers} running of ${(job.workers || []).length}${(job.monitors || []).length ? `, ${job.monitors.length} monitor` : ''}`],
    ['last item', job.item_label],
  ];
  for (const [key, value] of rows) if (value) md.appendMarkdown(`${key}: \`${value}\`  \n`);
  if (job.stalled) md.appendMarkdown(`\n*No step for ${duration(job.idle_s)}, far longer than the average step (${duration(job.step_s)}) and the usual pauses between steps: the job may hang.*\n\n`);
  if ((job.events || []).length) {
    md.appendMarkdown(`\n**Reported problems** (JOBRUN_EVENT, ${job.event_count} in total${job.event_count > job.events.length ? `, newest ${job.events.length}` : ''}):\n\n`);
    for (const event of job.events) {
      md.appendMarkdown(`- ${event.level}${event.at ? ` at ${new Date(event.at * 1000).toLocaleTimeString()}` : ''}: `);
      md.appendText(event.message);
      md.appendMarkdown('\n');
    }
    md.appendMarkdown('\n');
  }
  if (job.incomplete) md.appendMarkdown(`\n*All workers ended, but the list is at ${job.done}/${job.total}. Check whether items were left unprocessed, or whether the count is stale.*\n\n`);
  if (job.pool) md.appendMarkdown('\n*Worker of a pool: the progress, rate and ETA of the whole list are on the pool\'s row.*\n\n');
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

  // A refresh asked for while one is running (e.g. right after `jobrun clean`) runs again when that one ends;
  // dropping it would show the pre-clean list until the next interval.
  async refresh(force = false) {
    if (this.busy) {
      this.again = this.again || { force: false };
      this.again.force = this.again.force || force;
      return;
    }
    this.busy = true;
    try {
      const data = JSON.parse(await jobrun(['ls', '--json', '--days', String(config().get('finishedDays'))]));
      if (Array.isArray(data)) [this.jobs, this.groups] = [data, []];  // jobrun < 0.1
      else if ((data.schema || 1) > JSON_SCHEMA) throw new Error('jobrun is newer than this extension; update the extension');
      else [this.jobs, this.groups] = [data.jobs || [], data.groups || []];
      jobrunSettings = data.settings || {};
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
    await this.checkDisks();
    this.updateStatusBar();
    this.render(force);
    if (this.again) {
      const again = this.again;
      this.again = null;
      return this.refresh(again.force);
    }
  }

  // Show the effect of rm / clean at once, before jobrun has finished and the list is read again.
  drop(predicate) {
    const gone = new Set(this.jobs.filter(predicate).map(job => job.id));
    if (!gone.size) return;
    this.jobs = this.jobs.filter(job => !gone.has(job.id));
    this.groups = this.groups.filter(g => this.jobs.some(job => job.group === g.name));
    this.render(true);
  }

  // Messages for the events in jobboard.notifyOn: a job ended (done / error / stopped / cancelled; a group member
  // that succeeded only through its group's message), started to stall, reported a problem (JOBRUN_EVENT), or had
  // its log cut.
  notify() {
    const seen = this.previous;
    this.previous = new Map([
      ...this.jobs.map(job => [job.id, { status: job.status, stalled: job.stalled, log: Boolean(logNote(job)), events: job.event_count || 0 }]),
      ...this.groups.map(g => [`group:${g.name}`, { status: g.status }])]);
    const events = notifyEvents();
    if (!seen.size || !events.size) return;
    const EVENT = { done: 'done', ended: 'done', error: 'error', lost: 'error', stopped: 'stopped', cancelled: 'cancelled' };
    const withLog = (show, text, job) => job.log ? show(text, 'Open Log').then(choice => { if (choice) openLog(job); }) : show(text);
    const pools = new Set(this.jobs.filter(job => job.kind === 'pool').map(job => job.id));
    for (const job of this.jobs) {
      const before = seen.get(job.id);
      if (!before) continue;
      const label = `${job.kind === 'pool' ? 'Pool' : 'Job'} "${job.name}"${job.group ? ` (${job.group})` : ''}`;
      if (pools.has(job.pool)) {  // its end, failure, stall and events reach the user through its pool's row
        if (logNote(job) && !before.log && events.has('logTrimmed')) withLog(vscode.window.showInformationMessage, `${label}: log ${logNote(job)}`, job);
        continue;
      }
      if (ACTIVE.has(before.status) && !ACTIVE.has(job.status) && events.has(EVENT[job.status])
          && !(job.group && job.status === 'done')) {
        const outcome = job.reason ? `${job.status}: ${job.reason}` : job.status;
        withLog(job.status === 'done' ? vscode.window.showInformationMessage : vscode.window.showWarningMessage,
          `${label} ${outcome}${job.elapsed_s != null ? ` after ${duration(job.elapsed_s)}` : ''}`, job);
      }
      if (job.stalled && !before.stalled && events.has('stalled')) {
        withLog(vscode.window.showWarningMessage, `${label} may hang: no step for ${duration(job.idle_s)} (average step ${duration(job.step_s)})`, job);
      }
      if ((job.event_count || 0) > (before.events || 0) && events.has('event')) {
        const newest = (job.events || []).slice(-1)[0];
        const more = job.event_count - (before.events || 0) > 1 ? ` (+${job.event_count - (before.events || 0) - 1} more)` : '';
        if (newest) withLog(newest.level === 'error' ? vscode.window.showErrorMessage : vscode.window.showWarningMessage,
          `${label} reported ${newest.level === 'error' ? 'an error' : 'a warning'}: ${newest.message}${more}`, job);
      }
      if (logNote(job) && !before.log && events.has('logTrimmed')) {
        withLog(vscode.window.showInformationMessage, `${label}: log ${logNote(job)}`, job);
      }
    }
    for (const g of this.groups) {
      const before = seen.get(`group:${g.name}`);
      if (!before || !ACTIVE.has(before.status) || ACTIVE.has(g.status) || !events.has(g.failed ? 'error' : 'done')) continue;
      const counts = Object.entries(g.counts || {}).map(([status, n]) => `${n} ${status}`).join(', ');
      const text = `Group "${g.name}" finished after ${duration(g.elapsed_s)}: ${counts}`;
      (g.failed ? vscode.window.showWarningMessage : vscode.window.showInformationMessage)(text);
    }
  }

  // Free space on the disks with the registry and with the logs of running jobs (jobboard.diskWarnGB); a message
  // the first time a disk runs low in this window.
  async checkDisks() {
    const limit = (config().get('diskWarnGB') || 0) * 2 ** 30;
    this.lowDisks = [];
    if (!limit || !fs.promises.statfs) return;
    const registry = config().get('registry') || process.env.JOBRUN_HOME || path.join(os.homedir(), '.jobs');
    const dirs = new Map([[registry, 'registry']]);
    for (const job of this.jobs) if (ACTIVE.has(job.status) && job.log) dirs.set(path.dirname(job.log), 'logs');
    const devices = new Map();
    for (const [dir, what] of dirs) {
      try {
        const device = fs.statSync(dir).dev;
        if (devices.has(device)) { devices.get(device).what.add(what); continue; }
        const info = await fs.promises.statfs(dir);
        devices.set(device, { dir, free: info.bavail * info.bsize, what: new Set([what]) });
      } catch (err) { /* not mounted here (e.g. a Slurm node's local disk) */ }
    }
    this.warned = this.warned || new Set();
    for (const [device, disk] of devices) {
      if (disk.free >= limit) { this.warned.delete(device); continue; }
      this.lowDisks.push(disk);
      if (!this.warned.has(device)) {
        this.warned.add(device);
        vscode.window.showWarningMessage(`Low disk space: ${bytes(disk.free)} free on the disk of ${disk.dir} (${[...disk.what].join(', ')}). Jobs may fail to write their output.`);
      }
    }
  }

  updateStatusBar() {
    const mode = config().get('statusBar');
    const running = this.jobs.filter(job => job.kind !== 'pool' && (mode === 'active' ? ACTIVE.has(job.status) : job.status === 'running'));
    if (mode === 'hidden' || !running.length) return this.statusBar.hide();
    this.statusBar.text = `$(pulse) ${running.length} job${running.length > 1 ? 's' : ''}`;
    this.statusBar.tooltip = running.map(job => `${job.name}: ${describe(job, this.jobs)}`).join('\n');
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
      if (!this.jobs.length) return [];  // shows the welcome content (Run Demo, Set Up) from package.json
      const warnings = (this.lowDisks || []).map(diskItem);
      // a pool's workers are listed under the pool
      const pools = new Set(this.jobs.filter(job => job.kind === 'pool').map(job => job.id));
      const top = this.jobs.filter(job => !pools.has(job.pool));
      const entries = (jobs, prefix = '') => [
        ...this.groups.map(g => ({ ...g, members: jobs.filter(job => job.group === g.name), prefix }))
          .filter(g => g.members.length),
        ...jobs.filter(job => !job.group),
      ];
      const active = list => list.filter(e => ACTIVE.has(e.status)).sort((a, b) => b.started - a.started);
      const finished = list => list.filter(e => !ACTIVE.has(e.status))
        .sort((a, b) => (b.ended || b.started) - (a.ended || a.started));
      const grouping = config().get('groupBy');
      if (grouping === 'none') {
        const all = entries(top);
        return [...warnings, ...entryItems([...active(all), ...finished(all)], this.jobs)];
      }
      if (grouping === 'host') {
        const hostOf = job => job.kind === 'pool' ? 'pools' : job.host || 'unknown';
        const hosts = [...new Set(top.map(hostOf))];
        const sections = hosts.map(host => {
          const list = entries(top.filter(job => hostOf(job) === host), `${host}/`);
          return { host, list: [...active(list), ...finished(list)], busy: active(list).length };
        }).sort((a, b) => (b.busy > 0) - (a.busy > 0) || a.host.localeCompare(b.host, undefined, { numeric: true }));
        return [...warnings, ...sections.map(sec => section(sec.host, sec.list, sec.busy > 0, true))];
      }
      const all = entries(top);
      return [...warnings, section('Running', active(all), true), section('Finished', finished(all), false)];
    }
    const item = job => jobItem(job, this.jobs);
    const entry = element.group || element.job;
    // a row VS Code shows expanded (e.g. restored after a reload) without an expand event: redraw it split
    if (entry && entry.id && !expandedRows.has(entry.id)) {
      expandedRows.add(entry.id);
      if (progressSplit(entry, this.jobs)) setTimeout(() => this.render(true), 0);
    }
    const progress = entry && progressSplit(entry, this.jobs) ? [progressItem(entry, this.jobs)] : [];
    if (element.entries) return element.ordered ? entryItems(element.entries, this.jobs) : [
      ...element.entries.filter(e => e.members).map(g => groupItem(g, this.jobs)),
      ...dependencyOrder(element.entries.filter(e => !e.members)).map(item)];
    if (element.group) return [...progress, ...dependencyOrder(element.group.members).map(item)];
    if (element.job && element.job.kind === 'pool') {
      const members = dependencyOrder(this.jobs.filter(job => job.pool === element.job.id));
      const monitor = job => job.role === 'monitor';
      return [...progress, ...[...members.filter(monitor), ...members.filter(job => !monitor(job))].map(item)];  // details: tooltip
    }
    if (element.job) return [...progress, ...stageItems(element.job), ...detailItems(element.job)];
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

// ordered: keep the entries' order (running first) instead of groups first, then jobs in dependency order
function section(label, entries, expanded, ordered = false) {
  const item = new vscode.TreeItem(`${label} (${entries.length})`,
    expanded ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.Collapsed);
  item.id = `section-${label}`;
  item.entries = entries;
  item.ordered = ordered;
  if (ordered) item.iconPath = new vscode.ThemeIcon('server');
  return item;
}

function entryItems(entries, jobs) {
  return entries.map(e => e.members ? groupItem(e, jobs) : jobItem(e, jobs));
}

function diskItem(disk) {
  const item = new vscode.TreeItem(`Low disk space: ${bytes(disk.free)} free`);
  item.id = `disk:${disk.dir}`;
  item.description = `${disk.dir} (${[...disk.what].join(', ')})`;
  item.tooltip = `The disk of ${disk.dir} has ${bytes(disk.free)} free, less than jobboard.diskWarnGB. Jobs may fail to write their output.`;
  item.iconPath = new vscode.ThemeIcon('warning', new vscode.ThemeColor('charts.orange'));
  return item;
}

function groupItem(group, jobs) {
  const item = new vscode.TreeItem(group.name, vscode.TreeItemCollapsibleState.Collapsed);
  item.id = `${group.prefix || ''}group:${group.name}`;
  item.group = group;
  group.id = item.id;
  item.description = describe(group, jobs, progressSplit(group, jobs) ? 'head' : 'all');
  let [, color] = ICONS[group.status] || [];
  if (group.degraded && ACTIVE.has(group.status)) color = DEGRADED_COLORS[group.degraded];
  item.iconPath = new vscode.ThemeIcon('layers', color && new vscode.ThemeColor(color));
  item.tooltip = `${group.name}: ${group.status}\n${describe(group, jobs)}`;
  item.contextValue = ACTIVE.has(group.status) ? 'group-active' : 'group-finished';
  return item;
}

function jobItem(job, jobs = []) {
  const item = new vscode.TreeItem(job.name, vscode.TreeItemCollapsibleState.Collapsed);
  item.id = job.id;
  item.job = job;
  item.description = describe(job, jobs, progressSplit(job, jobs) ? 'head' : 'all');
  item.tooltip = tooltip(job);
  let [icon, color] = job.stalled ? ['sync~spin', 'charts.orange'] : ICONS[job.status] || ['circle-outline', undefined];
  if (job.degraded && ACTIVE.has(job.status)) color = DEGRADED_COLORS[job.degraded];
  // a pool's members while they run: a gear for workers, an eye for monitors, so they read as parts of the pool
  if (job.pool && job.status === 'running') icon = job.role === 'monitor' ? 'eye' : 'gear~spin';
  item.iconPath = new vscode.ThemeIcon(icon, color && new vscode.ThemeColor(color));
  if (job.kind === 'pool' && job.incomplete) item.iconPath = new vscode.ThemeIcon('warning', new vscode.ThemeColor('charts.yellow'));
  // a pool no job has joined runs nothing: it can be removed at once
  const empty = job.kind === 'pool' && !(job.workers || []).length && !(job.monitors || []).length;
  item.contextValue = `${job.kind === 'pool' ? 'pool' : 'job'}-${ACTIVE.has(job.status) && !empty ? 'active' : 'finished'}`;
  return item;
}

function detailItems(job) {
  const where = job.kind === 'pool' ? (job.hosts || []).join(', ')
    : [job.host, job.slurm_id && `slurm ${job.slurm_id}`, job.pid && `pid ${job.pid}`].filter(Boolean).join(', ');
  const details = [
    ['id', job.id, 'tag'], ['after', (job.after_names || []).join(', '), 'debug-step-into'], ['where', where, 'server'], ['started', job.started && new Date(job.started * 1000).toLocaleString(), 'calendar'],
    ['cmd', job.cmd, 'terminal'], ['cwd', job.cwd, 'folder'], ['log', job.log, 'output'], ['log size', logNote(job), 'warning'],
    ['last', job.last_line, 'note'],
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

// ---------------------------------------------------------------- columns
// Tree rows use the proportional UI font, so rows that should read as a grid pad each cell to the widest cell of its
// column. Widths are estimates in em, averaged over the humanist UI fonts VS Code uses (Segoe UI, San Francisco,
// Ubuntu / Noto Sans), and the padding consists of figure spaces (the width of a digit) and punctuation spaces (the
// width of a period), which HTML does not collapse. Digits, spaces and the fixed words of a column cost no error;
// only cells with letters (names) are estimated, to within a few pixels. Emoji count the same in every row of a
// column, so their width cancels.

const CHAR_EM = {};
for (const [chars, em] of [
  ['0123456789#$?', 0.556], ['bdghnpqu', 0.59], ['a', 0.54], ['c', 0.47], ['e', 0.55], ['o', 0.58], ['s', 0.45],
  ['ijl\'', 0.25], ['f', 0.31], ['t', 0.34], ['r', 0.37], ['k', 0.51], ['vxy', 0.5], ['z', 0.46], ['m', 0.89],
  ['w', 0.77], [' ', 0.26], ['.,:;!|', 0.26], ['/', 0.33], ['-', 0.32], ['_', 0.45], ['()[]', 0.31],
  ['A', 0.65], ['B', 0.62], ['C', 0.63], ['D', 0.7], ['E', 0.53], ['F', 0.5], ['G', 0.7], ['H', 0.72], ['I', 0.28],
  ['J', 0.27], ['K', 0.6], ['L', 0.5], ['M', 0.87], ['N', 0.74], ['OQ', 0.74], ['P', 0.58], ['R', 0.61], ['S', 0.53],
  ['T', 0.55], ['U', 0.71], ['V', 0.62], ['W', 0.93], ['X', 0.6], ['Y', 0.57], ['Z', 0.56],
  ['+=<>~', 0.58], ['%', 0.82], ['*', 0.45], ['·', 0.3], ['…', 0.85], ['█░', 0.64],
]) for (const ch of chars) CHAR_EM[ch] = em;
CHAR_EM['\u2007'] = CHAR_EM['0'];
CHAR_EM['\u2008'] = CHAR_EM['.'];
const FIGURE_SPACE = ' ';  // as wide as a digit
const PUNCT_SPACE = ' ';  // as wide as a period

function textWidth(text) {
  let width = 0;
  for (const ch of String(text)) width += CHAR_EM[ch] ?? (ch.codePointAt(0) >= 0x2600 ? 1.25 : 0.556);
  return width;
}

function withoutPadding(text) {
  return String(text).replace(/[\u2007\u2008]+$/, '');
}

function spaces(em) {
  const figures = Math.max(0, Math.floor((em + 0.14) / CHAR_EM['0']));
  return FIGURE_SPACE.repeat(figures) + (em - figures * CHAR_EM['0'] >= 0.14 ? PUNCT_SPACE : '');
}

// rows: arrays of cells; a cell is a string or { text, right: true } (right-aligned, e.g. numbers). seps[i] goes
// before column i (default two spaces). Returns one string per row; the last column is not padded.
function grid(rows, seps = []) {
  const n = Math.max(0, ...rows.map(r => r.length));
  const text = cell => (cell == null ? '' : typeof cell === 'string' ? cell : cell.text);
  const widths = [...Array(n).keys()].map(i => Math.max(0, ...rows.map(r => textWidth(text(r[i])))));
  return rows.map(r => [...Array(n).keys()].map(i => {
    const value = text(r[i]);
    const gap = i === n - 1 && !(r[i] && r[i].right) ? '' : spaces(widths[i] - textWidth(value));
    return (i ? seps[i] ?? '  ' : '') + (r[i] && r[i].right ? gap + value : value + gap);
  }).join('').replace(/[  ]+$/, ''));
}

// Pad the labels of sibling rows to the same width, so their descriptions start in one column.
// Idempotent: rows that are drawn again (e.g. re-expanded) are not padded twice.
function alignLabels(items) {
  for (const item of items) item.label = withoutPadding(item.label);
  const width = Math.max(0, ...items.map(item => textWidth(item.label)));
  for (const item of items) item.label += spaces(width - textWidth(item.label));
  return items;
}

// ---------------------------------------------------------------- filter
// The filter of the Resources and Cluster views (button in the title bar): only rows whose text (label, description
// and tooltip: node, job, user, GPU model, partition, ...) contains the query, with the rows above them, expanded. A row
// that matches itself keeps all its rows below (collapsed). The ids carry the query, so VS Code expands the rows anew
// for each query instead of restoring what was expanded before.

function rowText(item) {
  const tooltip = item.tooltip && typeof item.tooltip === 'object' ? item.tooltip.value : item.tooltip || '';
  return `${item.label} ${item.description || ''} ${tooltip}`.replace(/[\u2007\u2008\s]+/g, ' ').toLowerCase();
}

function expandable(item) {
  return item.collapsibleState && item.collapsibleState !== vscode.TreeItemCollapsibleState.None;
}

function anyMatch(provider, item, query) {
  return rowText(item).includes(query) || (expandable(item) && provider.rows(item).some(child => anyMatch(provider, child, query)));
}

// provider: rows(element) gives the unfiltered rows, filter the lowercase query, data / error its state
function filteredRows(provider, element) {
  const rows = provider.rows(element);
  const query = provider.filter;
  if (!query || !provider.data || provider.error || (element && element.unfiltered)) return rows;
  const kept = [];
  for (const item of rows) {
    const own = rowText(item).includes(query);
    if (!own && !(expandable(item) && provider.rows(item).some(child => anyMatch(provider, child, query)))) continue;
    item.unfiltered = own;
    item.baseId = item.baseId || item.id;
    if (item.baseId) item.id = `${item.baseId}~filter:${query}`;
    if (!own) item.collapsibleState = vscode.TreeItemCollapsibleState.Expanded;
    kept.push(item);
  }
  if (!element && provider.view) provider.view.message = kept.length ? `Filter: "${query}"` : `Nothing matches "${query}"`;
  return kept;
}

// The filter's input box: filters while typing; the query stays when the box closes.
function editFilter(provider, contextKey, title) {
  const box = vscode.window.createInputBox();
  box.title = title;
  box.placeholder = 'node, job, user, GPU model, partition, …';
  box.value = provider.filter || '';
  box.onDidChangeValue(value => setFilter(provider, contextKey, value));
  box.onDidAccept(() => box.hide());
  box.onDidHide(() => box.dispose());
  box.show();
}

function setFilter(provider, contextKey, text) {
  provider.filter = text.trim().replace(/\s+/g, ' ').toLowerCase();
  vscode.commands.executeCommand('setContext', contextKey, Boolean(provider.filter));
  if (!provider.filter && provider.view) provider.view.message = undefined;
  provider.emitter.fire();
}

// Long job names would push every row's columns far right: keep the start and the end, which tell runs apart.
function shortName(name, max = 26) {
  return name.length <= max ? name : `${name.slice(0, Math.ceil(max / 2) - 1)}…${name.slice(-Math.floor(max / 2))}`;
}

const right = text => ({ text, right: true });

// What a node's children are in a view (jobboard.resourcesNodeDetails, jobboard.clusterNodeDetails): one row per
// resource with the jobs below, or the jobs directly.
function nodeDetails(view) {
  return config().get(`${view}NodeDetails`) === 'jobs' ? 'jobs' : 'resources';
}

// ---------------------------------------------------------------- resources view
// `jobrun res --json`: per node CPU, RAM, GPU, VRAM (means over the time since the last call), Slurm reservations
// and per-job usage. Polled only while the view is visible, every jobboard.resourceSeconds.

function bytes(n) {
  if (n == null) return '?';
  for (const [unit, scale] of [['T', 2 ** 40], ['G', 2 ** 30], ['M', 2 ** 20]]) if (n >= scale) return `${+(n / scale).toPrecision(3)}${unit}`;
  return n > 0 ? `${Math.round(n / 1024)}K` : '0';
}

function cores(n) {
  return n == null ? '?' : `${+n.toFixed(n >= 10 ? 0 : 1)}`;
}

const SOURCES = { slurm: 'Slurm allocation', budget: 'declared (JOBRUN_BUDGET line or --cpus/--mem/--gpus)', cgroup: 'cgroup limit (e.g. systemd-run MemoryMax)' };

function resourceItem(id, label, fraction, text, tooltip, icon, children) {
  const item = new vscode.TreeItem(label, children && children.length
    ? vscode.TreeItemCollapsibleState.Collapsed : vscode.TreeItemCollapsibleState.None);
  item.id = id;
  item.description = fraction != null ? `${bar(fraction)} ${text}` : text;
  item.tooltip = tooltip || text;
  item.iconPath = new vscode.ThemeIcon(icon);
  item.children = children || [];
  return item;
}

function jobTooltip(job) {
  const r = job.reserved;
  return `${job.name} (${job.id})${job.group ? `\ngroup: ${job.group}` : ''}\n` +
    (r ? `reserved: ${cores(r.cpus)} CPUs · ${bytes(r.mem)} RAM · ${cores(r.gpus)} GPU\nfrom: ${r.source.split('+').map(k => SOURCES[k] || k).join(', ')}`
      : 'No budget: not a Slurm job, no JOBRUN_BUDGET line or --cpus/--mem/--gpus, no cgroup limit.') +
    (job.kind === 'slurm' ? '\nUsage from sstat.' : '\nRAM is the resident memory of its processes (shared pages count once per process).');
}

function runningIcon(job) {
  const running = job.pool ? (job.role === 'monitor' ? 'eye' : 'gear~spin') : 'sync~spin';  // as in the Jobs view
  return new vscode.ThemeIcon(running, new vscode.ThemeColor('charts.blue'));
}

// One job's share of a resource: used / reserved, with a bar of used against reserved when it has a reservation.
function shareItem(id, job, used, reserved, fmt, extra) {
  const item = new vscode.TreeItem(shortName(job.name));
  item.id = id;
  const usedText = !job.measured ? 'no data' : used == null ? '?' : fmt(used);
  const fraction = job.measured && used != null && reserved ? used / reserved : null;
  // grid cells, aligned with the job's siblings in getChildren
  item.cells = [fraction != null ? bar(fraction, 6) : '', reserved != null ? `${usedText} / ${fmt(reserved)}` : `${usedText} · no budget`, extra || ''];
  item.tooltip = jobTooltip(job);
  item.iconPath = runningIcon(job);
  return item;
}

function plainItem(id, label, description, tooltip, icon, cells) {
  const item = new vscode.TreeItem(label);
  item.id = id;
  item.description = description;
  if (cells) item.cells = cells;
  item.tooltip = tooltip;
  item.iconPath = new vscode.ThemeIcon(icon);
  return item;
}

// used / reserved as two grid cells, e.g. "0.7" "/2"; "no data" for a job that could not be measured
function usedOf(job, used, reserved, fmt) {
  const text = !job.measured ? 'no data' : used == null ? '?' : fmt(used);
  return [right(text), reserved != null ? `/${fmt(reserved)}` : ''];
}

class ResourcesProvider {
  constructor() {
    this.emitter = new vscode.EventEmitter();
    this.onDidChangeTreeData = this.emitter.event;
    this.data = null;
    this.error = null;
    this.busy = false;
    this.view = null;
  }

  visible() {
    return this.view && this.view.visible && (vscode.window.state.focused || config().get('updateWhileUnfocused'));
  }

  async refresh(force = false) {
    if (this.busy || !(force || this.visible())) return;
    this.busy = true;
    try {
      this.data = JSON.parse(await jobrun(['res', '--json']));
      this.error = null;
    } catch (err) {
      this.error = err.message;
    } finally {
      this.busy = false;
    }
    this.emitter.fire();
  }

  getTreeItem(element) {
    return element;
  }

  getChildren(element) {
    return filteredRows(this, element);
  }

  rows(element) {
    if (!element) {
      if (this.error) {
        const item = new vscode.TreeItem(`jobrun res failed: ${this.error}`);
        item.iconPath = new vscode.ThemeIcon('error');
        item.tooltip = this.error;
        return [item];
      }
      if (!this.data) return [new vscode.TreeItem('Measuring…')];
      const nodes = this.data.nodes || [];
      const items = nodes.map(node => this.nodeItem(node));
      if (nodeDetails('resources') === 'jobs') {
        grid(nodes.map(node => this.nodeCells(node))).forEach((text, i) => { items[i].description = text; });
      }
      return alignLabels(items);
    }
    if (element.node) return nodeDetails('resources') === 'jobs' ? this.jobRows(element.node) : alignLabels(this.nodeChildren(element.node));
    const children = element.children || [];
    const cells = children.filter(item => item.cells);
    grid(cells.map(item => item.cells), ['', ' ', '  ']).forEach((text, i) => { cells[i].description = text; });
    return alignLabels(children);
  }

  nodeItem(node) {
    const item = new vscode.TreeItem(node.host, vscode.TreeItemCollapsibleState.Expanded);
    item.id = `node:${node.host}`;
    item.node = node;
    const where = node.local ? 'this machine' : 'via Slurm';
    item.description = `${where} · ${node.jobs.length} job${node.jobs.length === 1 ? '' : 's'}`;
    item.tooltip = `${node.host} (${where})\nUsed: measured. Reserved: Slurm allocations plus the budgets of your other jobs.\n` +
      (nodeDetails('resources') === 'jobs' ? 'Each row: a job\'s used / reserved.\n' : 'Expand CPU, RAM or GPU to see each job\'s share: used / reserved.\n') +
      `Means over the last ${duration(this.data.interval_s)}; RAM and VRAM are current.` +
      (node.coarse ? '\nCPU and RAM use come from slurmd (load average, free memory) and lag by minutes; ' +
        'when your jobs on the node measurably use more, their sum is shown instead.' : '');
    item.iconPath = new vscode.ThemeIcon(node.local ? 'vm' : 'server');
    return item;
  }

  // jobboard.resourcesNodeDetails = jobs: the node's row carries the resources, one column each: used / total, reserved.
  nodeCells(node) {
    const reserved = node.reserved || {};
    const gpus = node.gpus || [];
    const util = gpus.length ? gpus.reduce((sum, g) => sum + g.util, 0) / gpus.length : null;
    const vram = gpus.length ? [gpus.reduce((s, g) => s + g.vram, 0), gpus.reduce((s, g) => s + g.vram_total, 0)] : null;
    return [
      node.local ? 'this machine' : 'via Slurm',
      'CPU', right(cores(node.cpu)), `/${node.cpus || '?'}`, `res. ${cores(reserved.cpus)}`,
      'RAM', right(bytes(node.mem)), `/${bytes(node.mem_total)}`, `res. ${bytes(reserved.mem)}`,
      node.gpu_count ? 'GPU' : '', util != null ? right(`${Math.round(util)}%`) : '',
      vram ? `VRAM ${bytes(vram[0])}/${bytes(vram[1])}` : '', node.gpu_count ? `res. ${cores(reserved.gpus)}/${node.gpu_count}` : '',
      `${node.jobs.length} job${node.jobs.length === 1 ? '' : 's'}`,
    ];
  }

  // jobboard.resourcesNodeDetails = jobs: one row per job with its used / reserved CPU, RAM and GPU, then everything else.
  jobRows(node) {
    const id = `node:${node.host}`;
    const items = [], rows = [];
    for (const job of node.jobs) {
      const item = new vscode.TreeItem(shortName(job.name));
      item.id = `${id}:job:${job.id}`;
      item.tooltip = jobTooltip(job);
      item.iconPath = runningIcon(job);
      const r = job.reserved || {};
      items.push(item);
      rows.push(['CPU', ...usedOf(job, job.cpu, r.cpus, cores), 'RAM', ...usedOf(job, job.mem, r.mem, bytes),
        job.gpu_util != null || job.vram || r.gpus ? 'GPU' : '',
        job.gpu_util != null ? right(`${Math.round(job.gpu_util)}%`) : '', job.vram ? `VRAM ${bytes(job.vram)}` : '',
        r.gpus ? `res. ${cores(r.gpus)}` : '', job.reserved ? '' : 'no budget']);
    }
    if (node.other) {
      items.push(plainItem(`${id}:other`, 'other', '', 'Everything not started through jobrun: other users, Slurm jobs of others, the system.', 'person'));
      rows.push(['CPU', right(cores(node.other.cpu)), '', 'RAM', right(bytes(node.other.mem)), '',
        node.other.vram != null ? 'GPU' : '', '', node.other.vram != null ? `VRAM ${bytes(node.other.vram)}` : '', '', '']);
    }
    const others = ((node.slurm || {}).jobs || []).filter(job => !job.mine);
    if (others.length) {
      const sum = key => others.reduce((total, job) => total + job[key], 0);
      items.push(plainItem(`${id}:slurm-others`, `Slurm jobs of others (${others.length})`, '',
        [...new Set(others.map(job => job.user))].join(', '), 'organization'));
      rows.push(['CPU', right(''), `res. ${cores(sum('cpus'))}`, 'RAM', right(''), `res. ${bytes(sum('mem'))}`,
        sum('gpus') ? 'GPU' : '', '', '', sum('gpus') ? `res. ${cores(sum('gpus'))}` : '', '']);
    }
    grid(rows).forEach((text, i) => { items[i].description = text; });
    return alignLabels(items);
  }

  nodeChildren(node) {
    const id = `node:${node.host}`;
    const reserved = node.reserved || { cpus: 0, mem: 0, gpus: 0 };
    const others = ((node.slurm || {}).jobs || []).filter(job => !job.mine);
    const othersReserved = key => others.reduce((total, job) => total + job[key], 0);
    // per resource: the jobs that use it, then everything else
    const rest = (key, used, fmt) => {
      const items = [];
      if (node.other && used != null) items.push(plainItem(`${id}:${key}:other`, 'other', fmt(used),
        'Everything not started through jobrun: other users, Slurm jobs of others, the system.', 'person', ['', fmt(used), '']));
      if (others.length && othersReserved(key === 'vram' ? 'gpus' : key)) {
        const amount = fmt(othersReserved(key === 'vram' ? 'gpus' : key));
        items.push(plainItem(`${id}:${key}:slurm-others`, `Slurm jobs of others (${others.length})`, `${amount} reserved`,
          [...new Set(others.map(job => job.user))].join(', '), 'organization', ['', `${amount} reserved`, '']));
      }
      return items;
    };
    const jobs = node.jobs;
    const cpuItems = [...jobs.map(job => shareItem(`${id}:cpu:${job.id}`, job, job.cpu, job.reserved && job.reserved.cpus, cores)),
      ...rest('cpus', node.other && node.other.cpu, cores)];
    const ramItems = [...jobs.map(job => shareItem(`${id}:ram:${job.id}`, job, job.mem, job.reserved && job.reserved.mem, bytes)),
      ...rest('mem', node.other && node.other.mem, bytes)];
    const gpuJobs = jobs.filter(job => job.gpu_util != null || job.vram || (job.reserved && job.reserved.gpus));
    const gpuShare = job => {
      const item = shareItem(`${id}:gpu:${job.id}`, job, null, null, cores);
      const r = job.reserved && job.reserved.gpus;
      item.cells = [job.gpu_util != null ? bar(job.gpu_util / 100, 6) : '',
        [job.gpu_util != null ? `${Math.round(job.gpu_util)}%` : job.measured ? '' : 'no data', job.vram ? `VRAM ${bytes(job.vram)}` : '']
          .filter(Boolean).join(' · '), r ? `${cores(r)} GPU reserved` : 'no GPU reserved'];
      return item;
    };
    // A Slurm node: what is reserved comes first and fills the bar when it exceeds the use, since a reserved core is
    // not available to a new job whatever the (coarse) load says.
    const usedNote = (fromJobs, fallback) => fromJobs ? ' (your jobs)' : fallback;
    const share = (used, held, total) => total && (used != null || held) ? Math.max(used || 0, held || 0) / total : null;
    const items = node.coarse ? [
      resourceItem(`${id}:cpu`, 'CPU', share(node.cpu, reserved.cpus, node.cpus),
        `${cores(reserved.cpus)} / ${node.cpus || '?'} cores reserved · ${cores(node.cpu)} used${usedNote(node.cpu_from_jobs, ' (load)')}`,
        'Reserved: Slurm allocations of all users plus the budgets of your other jobs. Used: slurmd\'s load average, ' +
        'or the sum of your jobs on the node when that is higher.', 'pulse', cpuItems),
      resourceItem(`${id}:ram`, 'RAM', share(node.mem, reserved.mem, node.mem_total),
        `${bytes(reserved.mem)} / ${bytes(node.mem_total)} reserved · ${bytes(node.mem)} used${usedNote(node.mem_from_jobs, '')}`,
        null, 'database', ramItems),
    ] : [
      resourceItem(`${id}:cpu`, 'CPU', node.cpus && node.cpu != null ? node.cpu / node.cpus : null,
        `${cores(node.cpu)} / ${node.cpus || '?'} cores · ${cores(reserved.cpus)} reserved`, null, 'pulse', cpuItems),
      resourceItem(`${id}:ram`, 'RAM', node.mem_total && node.mem != null ? node.mem / node.mem_total : null,
        `${bytes(node.mem)} / ${bytes(node.mem_total)} · ${bytes(reserved.mem)} reserved`, null, 'database', ramItems),
    ];
    for (const gpu of node.gpus || []) {
      items.push(resourceItem(`${id}:gpu${gpu.index}`, `GPU ${gpu.index}`, gpu.util / 100,
        `${Math.round(gpu.util)}% · VRAM ${bytes(gpu.vram)} / ${bytes(gpu.vram_total)}`, gpu.name, 'circuit-board',
        [...gpuJobs.filter(job => !job.gpus || job.gpus.includes(gpu.index)).map(gpuShare),
          ...rest('vram', node.other && node.other.vram, bytes)]));
    }
    if (node.gpu_count) {
      const why = (node.gpus || []).length ? '' : node.local ? ' · not measured: no job holds a GPU' : ' · utilization not available';
      items.push(resourceItem(`${id}:gpus`, 'GPUs', reserved.gpus / node.gpu_count,
        `${cores(reserved.gpus)} / ${node.gpu_count} reserved${why}`,
        node.local && !(node.gpus || []).length ? 'The GPU is queried only while one of your jobs has it open, so an idle GPU is never woken up.' : null,
        'circuit-board', (node.gpus || []).length ? [] : [...gpuJobs.map(gpuShare), ...rest('gpus', null, cores)]));
    }
    return items;
  }
}

// ---------------------------------------------------------------- cluster view
// `jobrun cluster --json`: every Slurm node and this machine, what is allocated of its CPUs, RAM and GPUs, and the jobs
// on it. Collapsed by default; polled only while expanded and visible, every jobboard.clusterSeconds (0: by hand).

const LIGHTS = [[0.2, '🟢', 'green'], [0.8, '🟠', 'orange'], [Infinity, '🔴', 'red']];

// [emoji, color name] of a fraction allocated
function light(fraction) {
  return fraction == null ? ['⚪', 'grey'] : LIGHTS.find(([limit]) => fraction <= limit + 1e-9).slice(1);
}

// A traffic light as an SVG with a fixed color: a colored ThemeIcon turns white in a selected row.
function lightIcon(color, size) {
  const file = theme => vscode.Uri.file(path.join(extensionPath, 'media', 'lights', `${color}-${size}-${theme}.svg`));
  return { light: file('light'), dark: file('dark') };
}

// What a node holds of a resource: Slurm's allocation; on this machine the larger of that and what it uses, since
// work started there outside Slurm reserves nothing.
function held(node, key) {
  return Math.max(node[`${key}_alloc`] || 0, (node.used || {})[key] || 0);
}

function fractions(node) {
  const of = key => node[key] ? held(node, key) / node[key] : null;
  return { cpus: of('cpus'), mem: of('mem'), gpus: node.gpus ? of('gpus') : undefined };
}

// The fraction the node's icon shows (jobboard.clusterIcon); GPUs count only on nodes that have them.
function iconFraction(node, mode) {
  const f = fractions(node);
  const all = [f.cpus, f.mem, f.gpus].filter(v => v != null);
  if (!all.length) return null;
  if (mode === 'best') return Math.min(...all);
  if (mode === 'cpu') return f.cpus;
  if (mode === 'ram') return f.mem;
  if (mode === 'gpu' && f.gpus != null) return f.gpus;
  return Math.max(...(mode === 'gpu' ? [f.cpus, f.mem].filter(v => v != null) : all));
}

function limitText(seconds) {
  return seconds == null ? 'no limit' : duration(seconds);
}

function allocText(job) {
  return [`${cores(job.cpus)} CPU`, bytes(job.mem), job.gpus ? `${cores(job.gpus)} GPU` : ''].filter(Boolean).join(' · ');
}

const CLUSTER_GROUPINGS = {
  sharedPartitions: 'Partitions with several nodes; the other nodes together',
  partitions: 'Every partition',
  gpuType: 'GPU model',
  none: 'No groups',
};
const CLUSTER_ICONS = {
  worst: 'Fullest of CPU, RAM and GPU', best: 'Emptiest of CPU, RAM and GPU',
  gpu: 'GPU (nodes without GPUs: fuller of CPU and RAM)', cpu: 'CPU only', ram: 'RAM only',
};
const NODE_DETAILS = {
  resources: 'CPU, RAM and GPU rows, each with the jobs that hold it',
  jobs: 'The jobs directly; the node\'s row shows CPU, RAM and GPU',
};
// a node's resources: key in `jobrun cluster`, label, format, unit after the total
const CLUSTER_RESOURCES = [['cpus', 'CPU', cores, ' cores'], ['mem', 'RAM', bytes, ''], ['gpus', 'GPU', cores, '']];

class ClusterProvider {
  constructor() {
    this.emitter = new vscode.EventEmitter();
    this.onDidChangeTreeData = this.emitter.event;
    this.data = null;
    this.error = null;
    this.busy = false;
    this.view = null;
  }

  visible() {
    return this.view && this.view.visible && (vscode.window.state.focused || config().get('updateWhileUnfocused'));
  }

  auto() {
    return config().get('clusterSeconds') > 0;
  }

  async refresh(force = false) {
    if (this.busy || !(force || this.visible())) return;
    this.busy = true;
    try {
      this.data = JSON.parse(await jobrun(['cluster', '--json']));
      this.error = null;
    } catch (err) {
      this.error = err.message;
    } finally {
      this.busy = false;
    }
    if (this.view && this.data) {
      this.view.description = `${this.auto() ? '' : 'manual refresh · '}as of ${clock(this.data.time)}`;
    }
    this.emitter.fire();
  }

  getTreeItem(element) {
    return element;
  }

  getChildren(element) {
    return filteredRows(this, element);
  }

  rows(element) {
    if (!element) {
      if (this.error) {
        const item = new vscode.TreeItem(`jobrun cluster failed: ${this.error}`);
        item.iconPath = new vscode.ThemeIcon('error');
        item.tooltip = this.error;
        return [item];
      }
      if (!this.data) return [new vscode.TreeItem('Loading…')];
      const groups = this.groups();
      const notes = this.slurmNote();
      if (groups.length === 1 && config().get('clusterGroupBy') === 'none') return [...notes, ...this.nodeItems(groups[0].nodes)];
      return [...notes, ...groups.map(g => this.groupItem(g))];
    }
    if (element.slurmNote) return element.children;
    if (element.clusterGroup) return this.nodeItems(element.clusterGroup.nodes);
    if (element.clusterNode) {
      return nodeDetails('cluster') === 'jobs' ? this.jobItems(element.clusterNode) : this.resourceItems(element.clusterNode);
    }
    if (element.clusterJobs) return this.jobItems(element.owner, element.clusterJobs, element.resource);
    return [];
  }

  // Why Slurm shows nothing (no sinfo / squeue here, or they failed) or only part (nodes without jobs), and which
  // commands the view needs; the view then shows this machine only.
  slurmNote() {
    const status = this.data.slurm_status || (this.data.slurm ? 'ok' : 'missing');
    if (status === 'ok') return [];
    const message = this.data.slurm_message || '';
    const needed = this.data.slurm_needed || { sinfo: 'nodes, partitions and their allocations', squeue: 'the jobs on each node' };
    const partial = status === 'partial';
    const item = new vscode.TreeItem(
      { missing: 'No Slurm cluster found', error: 'Slurm controller not reachable', partial: 'Slurm answered in part' }[status] || 'Slurm unavailable',
      vscode.TreeItemCollapsibleState.Expanded);
    item.id = 'cluster:slurm-status';
    item.slurmNote = true;
    item.description = partial ? '' : 'showing this machine only';
    item.iconPath = new vscode.ThemeIcon(partial ? 'warning' : 'info', new vscode.ThemeColor(partial ? 'charts.yellow' : 'charts.blue'));
    item.tooltip = `${message}\n\nThe Cluster view runs these Slurm commands on this machine (any Slurm client that reaches the controller):\n` +
      Object.entries(needed).map(([command, what]) => `${command}: ${what}`).join('\n');
    item.children = [
      plainItem('cluster:slurm-status:why', message || 'no answer', '', message, partial ? 'warning' : 'error'),
      ...(partial ? [] : Object.entries(needed).map(([command, what]) => plainItem(`cluster:slurm-status:${command}`, command,
        `needed for ${what}`, `\`${command}\` must be on PATH and reach the Slurm controller.`, 'terminal'))),
    ];
    return [item];
  }

  // Non-empty sections in display order: this machine's first, then the default partition, then the larger ones.
  // Which nodes take your jobs: jobrun's verdict per partition, corrected by jobboard.clusterPartitionAccess; a node
  // is usable if one of its partitions is (this machine outside Slurm always is).
  applyAccess() {
    const overrides = config().get('clusterPartitionAccess') || {};
    const byName = new Map((this.data.partitions || []).map(p => [p.name, p]));
    for (const p of byName.values()) {
      p.usable = typeof overrides[p.name] === 'boolean' ? overrides[p.name] : p.access !== false;
      p.overridden = typeof overrides[p.name] === 'boolean';
    }
    for (const node of this.data.nodes || []) {
      const parts = node.partitions.map(name => byName.get(name)).filter(Boolean);
      node.usable = !parts.length || parts.some(p => p.usable);
      node.useAccounts = [...new Set(parts.filter(p => p.usable && p.account && !p.overridden).map(p => p.account))];
      node.accessParts = parts;
    }
  }

  groups() {
    this.applyAccess();
    const show = config().get('clusterShowNoAccess') !== false;
    const nodes = (this.data.nodes || []).filter(n => show || n.usable);
    const byName = new Map(nodes.map(n => [n.name, n]));
    const partitions = (this.data.partitions || []).map(p => ({
      key: `p:${p.name}`, label: p.name, partition: p, nodes: p.nodes.map(name => byName.get(name)).filter(Boolean)}));
    const mode = config().get('clusterGroupBy');
    let groups;
    if (mode === 'none') {
      groups = [{ key: 'all', label: 'All nodes', nodes }];
    } else if (mode === 'gpuType') {
      const model = n => n.gpus ? (n.gpu_types.length ? n.gpu_types.join(' + ') : 'GPU (model unknown)') : 'no GPU';
      groups = [...new Set(nodes.map(model))].sort((a, b) => (a === 'no GPU') - (b === 'no GPU') || a.localeCompare(b, undefined, { numeric: true }))
        .map(label => ({ key: `gpu:${label}`, label, nodes: nodes.filter(n => model(n) === label) }));
    } else if (mode === 'partitions') {
      groups = partitions;
      const alone = nodes.filter(n => !n.partitions.length);
      if (alone.length) groups = [{ key: 'local', label: 'This machine', nodes: alone }, ...groups];
    } else {  // sharedPartitions
      const shared = partitions.filter(p => p.nodes.length > 1);
      const inShared = new Set(shared.flatMap(p => p.nodes.map(n => n.name)));
      const local = nodes.filter(n => n.local && !inShared.has(n.name));
      const other = nodes.filter(n => !n.local && !inShared.has(n.name));
      groups = [{ key: 'local', label: 'This machine', nodes: local }, ...shared,
        { key: 'other', label: 'Other nodes', nodes: other, last: true }];
    }
    const rank = g => [g.nodes.some(n => n.local) ? 0 : 1, g.last ? 1 : 0, g.partition && g.partition.default ? 0 : 1,
      -g.nodes.length];
    const compare = (a, b) => {
      const [ra, rb] = [rank(a), rank(b)];
      for (let i = 0; i < ra.length; i++) if (ra[i] !== rb[i]) return ra[i] - rb[i];
      return 0;
    };
    const local = n => (n.local ? 0 : 1);
    return groups.filter(g => g.nodes.length).sort(compare)
      .map(g => ({ ...g, nodes: [...g.nodes].sort((a, b) => local(a) - local(b) || a.name.localeCompare(b.name, undefined, { numeric: true })) }));  // node2 before node10
  }

  groupItem(group) {
    const item = new vscode.TreeItem(group.label, vscode.TreeItemCollapsibleState.Expanded);
    item.id = `cluster:${group.key}`;
    item.clusterGroup = group;
    const up = group.nodes.filter(n => n.available && n.usable);
    const closed = group.nodes.filter(n => !n.usable).length;
    const free = key => up.reduce((sum, n) => sum + Math.max(0, (n[key] || 0) - held(n, key)), 0);
    const p = group.partition;
    item.description = [`${group.nodes.filter(n => n.available).length}/${group.nodes.length} nodes up`,
      closed ? `${closed} no access` : '',
      `free ${cores(free('cpus'))} cores · ${bytes(free('mem'))} · ${cores(free('gpus'))} GPU`,
      p && p.default ? 'default' : '', p ? (p.time_limit_s == null ? 'no time limit' : `limit ${duration(p.time_limit_s)}`) : '',
    ].filter(Boolean).join(' · ');
    item.tooltip = `${group.label}: ${group.nodes.length} node${group.nodes.length === 1 ? '' : 's'}, ${up.length} taking your jobs.\n` +
      'Free: what is not allocated on the nodes that take your jobs (this machine: not used either).' +
      (p ? `\nPartition time limit: ${p.time_limit}` : '');
    item.iconPath = new vscode.ThemeIcon(group.key === 'local' ? 'vm' : 'server-environment');
    return item;
  }

  // Columns line up across every node of the view, not only within a section, since all sections share the indent.
  nodeItems(nodes) {
    const all = this.data.nodes || [];
    const texts = new Map(grid(all.map(n => this.nodeCells(n))).map((text, i) => [all[i].name, text]));
    // the resource columns start in one column; a short summary simply follows the name
    const width = nodeDetails('cluster') === 'jobs' ? Math.max(0, ...all.map(n => textWidth(n.name))) : 0;
    return nodes.map(node => {
      const item = this.nodeItem(node);
      item.label = node.name + spaces(width - textWidth(node.name));
      item.description = texts.get(node.name);
      return item;
    });
  }

  // The node row: with resource rows below it (jobboard.clusterNodeDetails = resources), only the jobs and the state;
  // otherwise also each resource, allocated / total with its traffic light.
  nodeCells(node) {
    const counts = [(node.jobs || []).length ? `${node.jobs.length} job${node.jobs.length === 1 ? '' : 's'}` : '',
      (node.pending || []).length ? `${node.pending.length} queued` : ''].filter(Boolean).join(' · ');
    const notes = [node.local ? 'this machine' : '', node.usable ? '' : '🔒 no access', node.available ? '' : `⛔ ${node.state}`,
      node.usable && node.useAccounts.length ? `-A ${node.useAccounts.join(' / ')}` : ''].filter(Boolean).join(' · ');
    if (nodeDetails('cluster') !== 'jobs') return [[notes, counts].filter(Boolean).join(' · ')];
    const f = fractions(node);
    const cells = [];
    for (const [key, label, fmt] of CLUSTER_RESOURCES) {
      if (key === 'gpus' && !node.gpus) {
        cells.push('', '', '', '');
        continue;
      }
      cells.push(`${node.available && node.usable ? light(f[key])[0] : '⚪'} ${label}`, right(fmt(held(node, key))),
        `/${node[key] != null ? fmt(node[key]) : '?'}`, key === 'gpus' ? node.gpu_types.join('+') : '');
    }
    // the state first in the last column: in a narrow sidebar the end of a row is cut off
    return [...cells, [notes, counts].filter(Boolean).join(' · ')];
  }

  nodeItem(node) {
    const jobs = (node.jobs || []).length + (node.pending || []).length;
    const expandable = jobs || nodeDetails('cluster') !== 'jobs';
    const item = new vscode.TreeItem(node.name, expandable ? vscode.TreeItemCollapsibleState.Collapsed : vscode.TreeItemCollapsibleState.None);
    item.id = `cluster-node:${node.name}`;
    item.clusterNode = node;
    // large: the node's light stands above its resources' small ones
    item.iconPath = !node.usable ? new vscode.ThemeIcon('lock', new vscode.ThemeColor('disabledForeground'))
      : node.available ? lightIcon(light(iconFraction(node, config().get('clusterIcon')))[1], 'large')
        : new vscode.ThemeIcon('circle-slash', new vscode.ThemeColor('disabledForeground'));
    const f = fractions(node);
    const md = new vscode.MarkdownString();
    md.appendMarkdown(`**${node.name}**${node.local ? ' (this machine)' : ''} — ${node.state}${node.available ? '' : ', takes no new jobs'}\n\n`);
    if (node.partitions.length) md.appendMarkdown(`partitions: \`${node.partitions.join(', ')}\`  \n`);
    for (const p of node.accessParts || []) {
      const why = p.overridden ? `${p.usable ? 'usable' : 'no access'} (jobboard.clusterPartitionAccess)` : p.why || (p.usable ? 'open' : 'no access');
      md.appendMarkdown(`${p.usable ? '✓' : '🔒'} partition \`${p.name}\`: ${why}  \n`);
    }
    for (const [key, label, fmt] of CLUSTER_RESOURCES) {
      if (key === 'gpus' && !node.gpus) continue;
      const used = node.used && node.used[key] != null ? ` · used ${fmt(node.used[key])}` : '';
      const name = key === 'gpus' ? `GPUs (${node.gpu_types.join(', ') || 'model unknown'})` : label;
      md.appendMarkdown(`${node.available ? light(f[key])[0] : ''} ${name}: allocated \`${fmt(node[`${key}_alloc`] || 0)}\` of \`${node[key] != null ? fmt(node[key]) : '?'}\`${used}  \n`);
    }
    if (node.load != null) md.appendMarkdown(`load average (slurmd, minutes old): \`${node.load}\`  \n`);
    md.appendMarkdown('\n🟢 up to 20% allocated, 🟠 up to 80%, 🔴 above.' +
      (node.local ? ' On this machine the larger of allocated and used counts (work outside Slurm reserves nothing).' : ''));
    item.tooltip = md;
    return item;
  }

  // jobboard.clusterNodeDetails = resources: CPU, RAM, GPU (allocated / total, with the jobs that hold it), then the queue.
  resourceItems(node) {
    const id = `cluster-node:${node.name}`;
    const f = fractions(node);
    const items = [];
    for (const [key, label, fmt, unit] of CLUSTER_RESOURCES) {
      if (key === 'gpus' && !node.gpus) continue;
      const jobs = (node.jobs || []).filter(job => job[key] > 0);
      const item = new vscode.TreeItem(label, jobs.length || node.used ? vscode.TreeItemCollapsibleState.Collapsed
        : vscode.TreeItemCollapsibleState.None);
      item.id = `${id}:${key}`;
      item.clusterJobs = jobs;
      item.owner = node;
      item.resource = key;
      item.iconPath = lightIcon(node.available && node.usable ? light(f[key])[1] : 'grey', 'small');
      const alloc = fmt(node[`${key}_alloc`] || 0);
      const used = node.used && node.used[key] != null ? `used ${fmt(node.used[key])}` : '';
      item.tooltip = `${label}: ${alloc} of ${node[key] != null ? fmt(node[key]) : '?'}${unit} allocated by Slurm` +
        (used ? `, ${used} on this machine (the larger counts)` : '');
      // every bar is ten characters long, so the text after it starts in one column
      item.description = `${f[key] != null ? `${bar(f[key])} ` : ''}` + [`${fmt(held(node, key))}/${node[key] != null ? fmt(node[key]) : '?'}${unit}`,
        key === 'gpus' ? node.gpu_types.join('+') : '', used ? `allocated ${alloc}, ${used}` : ''].filter(Boolean).join(' · ');
      items.push(item);
    }
    if ((node.pending || []).length) {
      const item = new vscode.TreeItem('Queued', vscode.TreeItemCollapsibleState.Collapsed);
      item.id = `${id}:queued`;
      item.clusterJobs = node.pending;
      item.owner = node;
      item.resource = 'queued';
      item.tooltip = 'Queued jobs the scheduler plans to start on this node, with its estimated start.';
      item.iconPath = new vscode.ThemeIcon('clock', new vscode.ThemeColor('charts.yellow'));
      item.description = `${node.pending.length} job${node.pending.length === 1 ? '' : 's'}`;
      return [...alignLabels(items), item];  // the bars of CPU, RAM and GPU start in one column
    }
    return alignLabels(items);
  }

  // Jobs on a node, aligned in columns: running ones ending first, then queued ones starting first. resource: only
  // that resource's amount (a row of jobboard.clusterNodeDetails = resources), or 'queued'; none: all of it.
  jobItems(node, only = null, resource = null) {
    const id = `cluster-node:${node.name}`;
    const running = resource === 'queued' ? [] : [...(only || node.jobs || [])].sort((a, b) => (a.ends || Infinity) - (b.ends || Infinity));
    const queued = resource && resource !== 'queued' ? [] : [...(resource ? only : node.pending || [])]
      .sort((a, b) => (a.expected_start || Infinity) - (b.expected_start || Infinity));
    const share = job => job.nodes > 1 ? ` (its share on this node; ${job.nodes} nodes in total)` : '';
    // below a resource row (jobboard.clusterNodeDetails = resources) every cell is left-aligned text; the job list
    // of jobboard.clusterNodeDetails = jobs right-aligns the numbers
    const amount = job => {
      if (resource === 'cpus') return [`${cores(job.cpus)} cores`];
      if (resource === 'mem') return [bytes(job.mem)];
      if (resource === 'gpus') return [`${cores(job.gpus)} GPU`];
      if (resource === 'queued') return [allocText(job)];
      return [right(cores(job.cpus)), 'CPU', right(bytes(job.mem)), job.gpus ? `${cores(job.gpus)} GPU` : ''];
    };
    const items = [], rows = [];
    for (const job of running) {
      const item = new vscode.TreeItem(shortName(job.name || job.slurm_id));
      item.id = `${id}:${resource || 'all'}:${job.slurm_id}`;
      item.tooltip = `${job.name} (slurm ${job.slurm_id}) — ${job.state.toLowerCase()}\nuser: ${job.user}\npartition: ${job.partition}\n` +
        `allocated: ${allocText(job)}${share(job)}\ntime limit: ${job.time_limit}` +
        (job.started ? `\nstarted: ${new Date(job.started * 1000).toLocaleString()}` : '') +
        (job.ends ? `\nends at the latest: ${new Date(job.ends * 1000).toLocaleString()}` : '');
      item.iconPath = new vscode.ThemeIcon(job.mine ? 'account' : 'person', job.mine ? new vscode.ThemeColor('charts.blue') : undefined);
      items.push(item);
      rows.push([job.user, ...amount(job), `limit ${limitText(job.time_limit_s)}`, job.ends ? `ends ${clock(job.ends)}` : '']);
    }
    for (const job of queued) {
      const item = new vscode.TreeItem(shortName(job.name || job.slurm_id));
      item.id = `${id}:${resource || 'all'}:queued:${job.slurm_id}`;
      item.tooltip = `${job.name} (slurm ${job.slurm_id}) — queued, planned for this node\nuser: ${job.user}\npartition: ${job.partition}\n` +
        `requested: ${allocText(job)}${share(job)}\ntime limit: ${job.time_limit}` +
        (job.expected_start ? `\nexpected start: ${new Date(job.expected_start * 1000).toLocaleString()} (Slurm's estimate)` : '') +
        (job.reason ? `\nwhy queued: ${job.reason}` : '');
      item.iconPath = new vscode.ThemeIcon('clock', new vscode.ThemeColor(job.mine ? 'charts.blue' : 'charts.yellow'));
      items.push(item);
      rows.push([job.user, ...amount(job),
        `limit ${limitText(job.time_limit_s)}`, job.expected_start ? `start ~${clock(job.expected_start)}` : 'queued',
        job.reason ? `(${job.reason})` : '']);
    }
    if (node.used && resource && ['cpus', 'mem', 'gpus'].includes(resource)) {
      const fmt = resource === 'mem' ? bytes : cores;
      items.push(plainItem(`${id}:${resource}:used`, 'in use now', '',
        'Measured on this machine: all processes, also those outside Slurm.', 'pulse'));
      rows.push(['', `${fmt(node.used[resource])}${resource === 'cpus' ? ' cores' : resource === 'gpus' ? ' GPU' : ''}`, '', '']);
    }
    grid(rows, resource ? [] : ['', '  ', ' ', '  ']).forEach((text, i) => { items[i].description = text; });
    return alignLabels(items);
  }
}

async function pickSetting(key, options, title) {
  const current = config().get(key);
  const picked = await vscode.window.showQuickPick(Object.entries(options).map(([value, label]) => ({
    label, value, description: value === current ? '(current)' : ''})), { title });
  if (picked) await config().update(key, picked.value, vscode.ConfigurationTarget.Global);
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

// What the extension cannot do itself: put `jobrun` on PATH outside VS Code and install the skill for agents other
// than Copilot. Say once which command does that.
function offerSetup(context) {
  const [command, prefix] = jobrunCommand();
  if (!prefix.length || context.globalState.get('setupNoticeShown')) return;  // a jobrun CLI is installed already
  context.globalState.update('setupNoticeShown', true);
  vscode.window.showInformationMessage(
    'Job Board is ready, and `jobrun` works in VS Code terminals. To use it in other shells and to teach your coding ' +
    `agents (Claude Code, Codex, Cursor, ...), run: ${command} ${prefix.join(' ')} setup`,
    'Run Setup', 'Run Demo Jobs').then(choice => {
    if (choice === 'Run Setup') vscode.commands.executeCommand('jobboard.setup');
    if (choice === 'Run Demo Jobs') vscode.commands.executeCommand('jobboard.demo');
  });
}

function activate(context) {
  extensionPath = context.extensionPath;
  for (const file of [['bundle', 'bin', 'jobrun'], ['bundle', 'jobrun.py']]) {
    try { fs.chmodSync(path.join(extensionPath, ...file), 0o755); } catch (err) { /* not bundled (development) */ }
  }
  // the bundled `jobrun` in VS Code's terminals; appended, so an installed jobrun takes precedence
  context.environmentVariableCollection.description = 'Adds the jobrun command bundled with Job Board';
  context.environmentVariableCollection.append('PATH', path.delimiter + path.join(extensionPath, 'bundle', 'bin'));
  const provider = new JobsProvider();
  const view = vscode.window.createTreeView('jobboard.jobs', { treeDataProvider: provider, showCollapseAll: true });
  provider.view = view;
  const toggled = (element, open) => {
    const entry = element.group || element.job;
    if (!entry || !entry.id) return;
    if (open) expandedRows.add(entry.id); else expandedRows.delete(entry.id);
    if (config().get('progressRow') !== false) provider.render(true);
  };
  context.subscriptions.push(view.onDidExpandElement(e => toggled(e.element, true)),
    view.onDidCollapseElement(e => toggled(e.element, false)));
  const run = (args, message) => jobrun(args)
    .then(out => { if (message) vscode.window.setStatusBarMessage(message || out.trim(), 4000); })
    .catch(err => vscode.window.showErrorMessage(`jobrun ${args[0]}: ${err.message}`))
    .finally(() => provider.refresh(true));

  const resources = new ResourcesProvider();
  const resourceView = vscode.window.createTreeView('jobboard.resources', { treeDataProvider: resources, showCollapseAll: true });
  resources.view = resourceView;

  const cluster = new ClusterProvider();
  const clusterView = vscode.window.createTreeView('jobboard.cluster', { treeDataProvider: cluster, showCollapseAll: true });
  cluster.view = clusterView;
  // expanded with jobboard.clusterSeconds 0: load once, then only with the refresh button
  const showCluster = () => { if (cluster.auto() || !cluster.data) cluster.refresh(true); };

  let timer, resourceTimer, clusterTimer;
  const schedule = () => {
    clearInterval(timer);
    clearInterval(resourceTimer);
    clearInterval(clusterTimer);
    timer = setInterval(() => provider.refresh(), Math.max(2, config().get('refreshSeconds')) * 1000);
    resourceTimer = setInterval(() => resources.refresh(), Math.max(5, config().get('resourceSeconds')) * 1000);
    if (cluster.auto()) clusterTimer = setInterval(() => cluster.refresh(), Math.max(10, config().get('clusterSeconds')) * 1000);
  };
  schedule();
  syncSharedSettings().finally(() => provider.refresh());
  resources.refresh();
  offerSetup(context);

  context.subscriptions.push(
    view, resourceView, clusterView, provider.statusBar,
    { dispose: () => { clearInterval(timer); clearInterval(resourceTimer); clearInterval(clusterTimer); } },
    resourceView.onDidChangeVisibility(event => { if (event.visible) resources.refresh(true); }),
    vscode.commands.registerCommand('jobboard.refreshResources', () => resources.refresh(true)),
    clusterView.onDidChangeVisibility(event => { if (event.visible) showCluster(); }),
    vscode.commands.registerCommand('jobboard.refreshCluster', () => cluster.refresh(true)),
    ...['filterResources', 'refilterResources'].map(name => vscode.commands.registerCommand(`jobboard.${name}`,
      () => editFilter(resources, 'jobboard.resourcesFiltered', 'Filter Resources'))),
    ...['filterCluster', 'refilterCluster'].map(name => vscode.commands.registerCommand(`jobboard.${name}`,
      () => editFilter(cluster, 'jobboard.clusterFiltered', 'Filter Cluster'))),
    vscode.commands.registerCommand('jobboard.clearResourcesFilter', () => setFilter(resources, 'jobboard.resourcesFiltered', '')),
    vscode.commands.registerCommand('jobboard.clearClusterFilter', () => setFilter(cluster, 'jobboard.clusterFiltered', '')),
    vscode.commands.registerCommand('jobboard.clusterGroupBy', () => pickSetting('clusterGroupBy', CLUSTER_GROUPINGS, 'Group nodes by')),
    vscode.commands.registerCommand('jobboard.resourcesNodeDetails', () => pickSetting('resourcesNodeDetails', NODE_DETAILS, 'Resources: expanding a node shows')),
    vscode.commands.registerCommand('jobboard.clusterNodeDetails', () => pickSetting('clusterNodeDetails', NODE_DETAILS, 'Cluster: expanding a node shows')),
    vscode.commands.registerCommand('jobboard.clusterIcon', () => pickSetting('clusterIcon', CLUSTER_ICONS, "A node's icon color shows")),
    vscode.workspace.onDidChangeConfiguration(async event => {
      if (!event.affectsConfiguration('jobboard')) return;
      schedule();
      if (Object.keys(SHARED_SETTINGS).some(key => event.affectsConfiguration(`jobboard.${key}`))) await syncSharedSettings(event);
      provider.refresh(true);
      if (event.affectsConfiguration('jobboard.resourcesNodeDetails')) resources.emitter.fire();  // redraw, no new measurement
      else resources.refresh();
      if (['clusterGroupBy', 'clusterIcon', 'clusterNodeDetails', 'clusterShowNoAccess', 'clusterPartitionAccess'].some(key => event.affectsConfiguration(`jobboard.${key}`))) {
        cluster.emitter.fire();  // regroup and recolor what is shown, no new Slurm query
      } else if (cluster.visible() && cluster.auto()) cluster.refresh();
    }),
    view.onDidChangeVisibility(event => { if (event.visible) provider.refresh(true); }),
    vscode.window.onDidChangeWindowState(state => {
      if (state.focused && provider.stale) provider.refresh(true);
      if (state.focused) resources.refresh();
      if (state.focused && cluster.auto()) cluster.refresh();
    }),
    vscode.commands.registerCommand('jobboard.refresh', () => provider.refresh(true)),
    vscode.commands.registerCommand('jobboard.demo', () => run(['demo'], 'Started the demo jobs')),
    vscode.commands.registerCommand('jobboard.setup', () => {
      // the wizard asks questions, so it runs in a terminal; the bundled copy also installs the command itself
      const [command, prefix] = jobrunCommand();
      const quote = arg => `'${arg.replace(/'/g, `'\\''`)}'`;
      const terminal = vscode.window.createTerminal({ name: 'jobrun setup' });
      terminal.sendText([command, ...prefix, 'setup'].map(quote).join(' '));
      terminal.show();
    }),
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
    vscode.commands.registerCommand('jobboard.removeGroup', item => {
      provider.drop(job => job.group === item.group.name && !ACTIVE.has(job.status));
      return run(['rm', '--group', item.group.name]);
    }),
    vscode.commands.registerCommand('jobboard.stop', async item => {
      const running = provider.jobs.filter(job => job.pool === item.job.id && ACTIVE.has(job.status)).length;
      const what = item.job.kind === 'pool' ? `pool "${item.job.name}" with its ${running} running worker${running === 1 ? '' : 's'}`
        : `"${item.job.name}" (${item.job.slurm_id ? `slurm ${item.job.slurm_id}` : `pid ${item.job.pid}`})`;
      const choice = await vscode.window.showWarningMessage(`Stop ${what}?`, { modal: true }, 'Stop', 'Kill now');
      if (choice) run(['stop', item.job.id, ...(choice === 'Kill now' ? ['--force'] : [])], `Stopped ${item.job.name}`);
    }),
    vscode.commands.registerCommand('jobboard.remove', item => {
      provider.drop(job => job.id === item.job.id || (job.pool === item.job.id && !ACTIVE.has(job.status)));
      return run(['rm', item.job.id]);
    }),
    vscode.commands.registerCommand('jobboard.clean', async () => {
      const choice = await vscode.window.showWarningMessage('Remove all finished jobs from the list? Logs written by jobrun are deleted; logs it only read are kept.',
        { modal: true }, 'Remove');
      if (!choice) return;
      provider.drop(job => !ACTIVE.has(job.status));
      run(['clean', '--days', '0'], 'Cleared finished jobs');
    }),
  );
}

function deactivate() {}

module.exports = { activate, deactivate };
