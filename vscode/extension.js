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
  if (job.status === 'waiting') return `waiting for ${(job.waiting_for || []).join(', ') || 'dependencies'}`;
  parts.push(job.reason ? `${job.status}: ${job.reason}` : job.status);
  if (job.role === 'monitor') parts.push('monitor');
  parts.push([job.host, job.slurm_id && `slurm ${job.slurm_id}`].filter(Boolean).join(' '));
  if (job.item_label && ACTIVE.has(job.status)) parts.push(`last: ${job.item_label}`);
  parts.push(duration(job.elapsed_s));
  return parts.filter(Boolean).join(' · ');
}

function describe(job, jobs = []) {
  if (job.pool) return describeWorker(job);
  const parts = [];
  if (job.degraded) parts.push(eventText(job));
  if (job.jobs) {
    const other = Object.entries(job.counts || {}).filter(([status]) => status !== 'done' && !ACTIVE.has(status))
      .map(([status, n]) => `${n} ${status}`);
    parts.push(`${job.finished}/${job.jobs} jobs${other.length ? ` (${other.join(', ')})` : ''}`);
  }
  if (job.kind === 'pool') parts.push(workersText(job, jobs));
  if (!job.jobs && job.status === 'waiting') return `waiting for ${(job.waiting_for || []).join(', ') || 'dependencies'}`;
  if (!job.jobs && job.status === 'cancelled') return `cancelled: ${job.reason || 'never started'}`;
  if (job.stage) parts.push(`${job.stage_no}/${job.stage_of || '?'} ${job.stage}`);
  if (ACTIVE.has(job.status)) {
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
  return parts.join(' · ');
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
        }).sort((a, b) => (b.busy > 0) - (a.busy > 0) || a.host.localeCompare(b.host));
        return [...warnings, ...sections.map(sec => section(sec.host, sec.list, sec.busy > 0, true))];
      }
      const all = entries(top);
      return [...warnings, section('Running', active(all), true), section('Finished', finished(all), false)];
    }
    const item = job => jobItem(job, this.jobs);
    if (element.entries) return element.ordered ? entryItems(element.entries, this.jobs) : [
      ...element.entries.filter(e => e.members).map(g => groupItem(g, this.jobs)),
      ...dependencyOrder(element.entries.filter(e => !e.members)).map(item)];
    if (element.group) return dependencyOrder(element.group.members).map(item);
    if (element.job && element.job.kind === 'pool') {
      const members = dependencyOrder(this.jobs.filter(job => job.pool === element.job.id));
      const monitor = job => job.role === 'monitor';
      return [...members.filter(monitor), ...members.filter(job => !monitor(job))].map(item);  // details: tooltip
    }
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
  item.description = describe(group, jobs);
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
  item.description = describe(job, jobs);
  item.tooltip = tooltip(job);
  let [icon, color] = job.stalled ? ['sync~spin', 'charts.orange'] : ICONS[job.status] || ['circle-outline', undefined];
  if (job.degraded && ACTIVE.has(job.status)) color = DEGRADED_COLORS[job.degraded];
  // a pool's members while they run: a gear for workers, an eye for monitors, so they read as parts of the pool
  if (job.pool && job.status === 'running') icon = job.role === 'monitor' ? 'eye' : 'gear~spin';
  item.iconPath = new vscode.ThemeIcon(icon, color && new vscode.ThemeColor(color));
  if (job.kind === 'pool' && job.incomplete) item.iconPath = new vscode.ThemeIcon('warning', new vscode.ThemeColor('charts.yellow'));
  item.contextValue = `${job.kind === 'pool' ? 'pool' : 'job'}-${ACTIVE.has(job.status) ? 'active' : 'finished'}`;
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

// what is left for new jobs: the total minus whichever is larger, what is used or what Slurm has reserved
function free(total, used, reserved) {
  if (total == null) return null;
  return Math.max(0, total - Math.max(used || 0, reserved || 0));
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

// One job's share of a resource: used / reserved, with a bar of used against reserved when it has a reservation.
function shareItem(id, job, used, reserved, fmt, extra) {
  const item = new vscode.TreeItem(job.name);
  item.id = id;
  const r = job.reserved;
  const usedText = !job.measured ? 'no data' : used == null ? '?' : fmt(used);
  const fraction = job.measured && used != null && reserved ? used / reserved : null;
  item.description = [fraction != null ? bar(fraction, 6) : '', reserved != null ? `${usedText} / ${fmt(reserved)}` : `${usedText} · no budget`,
    extra].filter(Boolean).join(' ');
  item.tooltip = `${job.name} (${job.id})${job.group ? `\ngroup: ${job.group}` : ''}\n` +
    (r ? `reserved: ${cores(r.cpus)} CPUs · ${bytes(r.mem)} RAM · ${cores(r.gpus)} GPU\nfrom: ${r.source.split('+').map(k => SOURCES[k] || k).join(', ')}`
      : 'No budget: not a Slurm job, no JOBRUN_BUDGET line or --cpus/--mem/--gpus, no cgroup limit.') +
    (job.kind === 'slurm' ? '\nUsage from sstat.' : '\nRAM is the resident memory of its processes (shared pages count once per process).');
  const running = job.pool ? (job.role === 'monitor' ? 'eye' : 'gear~spin') : 'sync~spin';  // as in the Jobs view
  item.iconPath = new vscode.ThemeIcon(fraction != null && fraction > 0.9 ? 'warning' : running,
    new vscode.ThemeColor(fraction != null && fraction > 0.9 ? 'charts.orange' : 'charts.blue'));
  return item;
}

function plainItem(id, label, description, tooltip, icon) {
  const item = new vscode.TreeItem(label);
  item.id = id;
  item.description = description;
  item.tooltip = tooltip;
  item.iconPath = new vscode.ThemeIcon(icon);
  return item;
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
    if (!element) {
      if (this.error) {
        const item = new vscode.TreeItem(`jobrun res failed: ${this.error}`);
        item.iconPath = new vscode.ThemeIcon('error');
        item.tooltip = this.error;
        return [item];
      }
      if (!this.data) return [new vscode.TreeItem('Measuring…')];
      return (this.data.nodes || []).map(node => this.nodeItem(node));
    }
    if (element.node) return this.nodeChildren(element.node);
    return element.children || [];
  }

  nodeItem(node) {
    const item = new vscode.TreeItem(node.host, vscode.TreeItemCollapsibleState.Expanded);
    item.id = `node:${node.host}`;
    item.node = node;
    const reserved = node.reserved || {};
    const left = [
      free(node.cpus, node.cpu, reserved.cpus) != null && `${cores(free(node.cpus, node.cpu, reserved.cpus))} cores`,
      free(node.mem_total, node.mem, reserved.mem) != null && `${bytes(free(node.mem_total, node.mem, reserved.mem))} RAM`,
      node.gpu_count ? `${free(node.gpu_count, null, reserved.gpus)} GPU` : '',
    ].filter(Boolean);
    const where = node.local ? 'this machine' : 'via Slurm';
    item.description = `${where} · ${node.jobs.length} job${node.jobs.length === 1 ? '' : 's'} · free ${left.join(', ')}`;
    item.tooltip = `${node.host} (${where})\nFree = total minus the larger of what is used and what is reserved ` +
      `(Slurm allocations plus the budgets of your other jobs).\nExpand CPU, RAM or GPU to see each job's share: used / reserved.\n` +
      `Means over the last ${duration(this.data.interval_s)}; RAM and VRAM are current.` +
      (node.coarse ? '\nCPU and RAM use come from slurmd (load average, free memory) and lag by minutes.' : '');
    item.iconPath = new vscode.ThemeIcon(node.local ? 'vm' : 'server');
    return item;
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
        'Everything not started through jobrun: other users, Slurm jobs of others, the system.', 'person'));
      if (others.length && othersReserved(key === 'vram' ? 'gpus' : key)) {
        items.push(plainItem(`${id}:${key}:slurm-others`, `Slurm jobs of others (${others.length})`,
          `${fmt(othersReserved(key === 'vram' ? 'gpus' : key))} reserved`, [...new Set(others.map(job => job.user))].join(', '), 'organization'));
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
      item.description = [job.gpu_util != null ? `${bar(job.gpu_util / 100, 6)} ${Math.round(job.gpu_util)}%` : job.measured ? '' : 'no data',
        job.vram ? `VRAM ${bytes(job.vram)}` : '', r ? `${cores(r)} GPU reserved` : 'no GPU reserved'].filter(Boolean).join(' · ');
      return item;
    };
    const items = [
      resourceItem(`${id}:cpu`, 'CPU', node.cpus && node.cpu != null ? node.cpu / node.cpus : null,
        `${cores(node.cpu)} / ${node.cpus || '?'} cores${node.coarse ? ' (load)' : ''} · ${cores(reserved.cpus)} reserved`,
        null, 'pulse', cpuItems),
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
  const run = (args, message) => jobrun(args)
    .then(out => { if (message) vscode.window.setStatusBarMessage(message || out.trim(), 4000); })
    .catch(err => vscode.window.showErrorMessage(`jobrun ${args[0]}: ${err.message}`))
    .finally(() => provider.refresh(true));

  const resources = new ResourcesProvider();
  const resourceView = vscode.window.createTreeView('jobboard.resources', { treeDataProvider: resources });
  resources.view = resourceView;

  let timer, resourceTimer;
  const schedule = () => {
    clearInterval(timer);
    clearInterval(resourceTimer);
    timer = setInterval(() => provider.refresh(), Math.max(2, config().get('refreshSeconds')) * 1000);
    resourceTimer = setInterval(() => resources.refresh(), Math.max(5, config().get('resourceSeconds')) * 1000);
  };
  schedule();
  syncSharedSettings().finally(() => provider.refresh());
  resources.refresh();
  offerSetup(context);

  context.subscriptions.push(
    view, resourceView, provider.statusBar, { dispose: () => { clearInterval(timer); clearInterval(resourceTimer); } },
    resourceView.onDidChangeVisibility(event => { if (event.visible) resources.refresh(true); }),
    vscode.commands.registerCommand('jobboard.refreshResources', () => resources.refresh(true)),
    vscode.workspace.onDidChangeConfiguration(async event => {
      if (!event.affectsConfiguration('jobboard')) return;
      schedule();
      if (Object.keys(SHARED_SETTINGS).some(key => event.affectsConfiguration(`jobboard.${key}`))) await syncSharedSettings(event);
      provider.refresh(true);
      resources.refresh();
    }),
    view.onDidChangeVisibility(event => { if (event.visible) provider.refresh(true); }),
    vscode.window.onDidChangeWindowState(state => {
      if (state.focused && provider.stale) provider.refresh(true);
      if (state.focused) resources.refresh();
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
