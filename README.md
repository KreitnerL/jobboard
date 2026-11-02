# jobboard

**See all your long-running jobs in one place: local scripts, background processes and Slurm jobs, with
progress, rate and ETA, right in the VS Code sidebar.**

```
JOBS                                                        ⟳  ⌫
▾ Running (2)
  ▸ ◫ tuning 2026-11-02   1/3 jobs · ██████░░░░ 58% · 174/300 · 2.1/min · ETA 16:40 · 1h22m
  ▸ ⟳ train unet fold0    █████████░ 91% · 91/100 · 4.2/h · ETA 17:05 · 21h40m        ⎘  ■
▾ Finished (1)
  ▸ ✓ eval baseline       done · 600/600 · 12m03s · ended 13:58
```

- **No code changes.** Progress is read from the job's log: tqdm bars, `[12/100]` counters, or lines you count.
- **Anything that runs.** Local commands, processes that are already running, Slurm jobs.
- **Survives everything.** State lives in small files, not in the editor, so jobs stay tracked when VS Code
  closes, an SSH session drops or an agent's watcher dies.
- **Groups.** Run a sweep as a group and watch total progress, with an ETA set by the slowest job.
- **Tiny.** A single Python file that needs nothing beyond the standard library, plus a VS Code extension
  written in plain JavaScript.
- **Agent-friendly.** Comes with a skill, so Claude Code, Codex or Cursor register every job they start.

## Install

```bash
pipx install git+https://github.com/KreitnerL/jobboard     # or: pip install --user git+https://...
```

Or copy the single file: `curl -o ~/.local/bin/jobrun https://raw.githubusercontent.com/KreitnerL/jobboard/main/jobrun.py && chmod +x ~/.local/bin/jobrun`.

**VS Code extension:** download `jobboard.vsix` from the [releases](https://github.com/KreitnerL/jobboard/releases) and run
`code --install-extension jobboard.vsix`. With Remote-SSH, install it on the remote, where your jobs run.
A **Jobs** icon appears in the activity bar.

## Quick start

```bash
jobrun run --name "eval baseline" -- python eval.py --split test   # starts detached, prints id + log path
jobrun ls                                                          # or look at the sidebar
jobrun log "eval baseline" -f                                      # ids match by exact name or id substring
jobrun stop "eval baseline"
```

```
ID           STATUS   NAME            PROGRESS      RATE    ETA    ELAPSED  WHERE
261102-a28f  running  eval baseline    41% 246/600  1.6/s   14:34  2m31s    ws01
```

## Progress

Detected automatically from the newest matching log line:

| Log line | Example |
|---|---|
| tqdm bar | `246/600 [02:31<03:40, 1.6it/s]` |
| bracketed counter | `[246/600] case_042 done` |
| explicit marker | `JOBRUN_PROGRESS 246/600` |

Otherwise, say what to look for:

```bash
--count-regex 'Trial \d+ completed' --total 100        # progress = number of matching lines
--progress-regex 'case (?P<done>\d+) of (?P<total>\d+)'
```

The rate covers the last 15 minutes, and the ETA is the remaining work divided by that rate. For loops with
millions of steps, report by time, not per step, so the log stays small:

```python
if time.monotonic() - last >= 10:
    print(f"JOBRUN_PROGRESS {done}/{total}", flush=True); last = time.monotonic()
```

## Groups

```bash
for v in small base large; do
  jobrun run --group "sweep 2026-11-02" --name "$v" --count-regex 'Trial \d+ completed' --total 100 \
    -- python tune.py --variant "$v"
done
jobrun stop --group "sweep 2026-11-02"
```

The sidebar shows one row per group: jobs finished, progress summed over all jobs, combined rate, and the ETA of
the slowest job. You get one notification when the group finishes, plus one for each job that fails.

## Slurm

```bash
jobrun sbatch --name "train fold0" -- --time=04:00:00 train.sbatch     # submit and track
jobrun track --slurm 324562                                            # track an existing job
jobrun sbatch --log 'logs/%x-%j.err' -- train.sbatch                   # read progress from stderr
```

States come from `sacct`, so this also works where `scontrol` is disabled. Results are cached for 30 s, so open
dashboards don't keep querying Slurm. Stop uses `scancel`.

## Wait for jobs

```bash
jobrun wait ID            # or --group G; exits 0 only if everything finished successfully
```

This is useful in scripts and for agents. Waiting is separate from running: if the waiting shell dies, the job
keeps running and stays tracked.

## Logs

- `jobrun run` writes the command's output to `~/.jobs/logs/<id>.log`, or to `--log FILE`.
- `jobrun sbatch` writes the Slurm log there too, unless you or the script set `--output`.
- Removing a job (`rm`, `clean`, or the trash icon) also deletes the logs jobrun **wrote**. Logs it only
  **read**, such as your experiment's own log files, are never deleted.
- Logs jobrun writes are capped at 512 MB (`--max-log-mb`). Beyond that, only the last 1 MB is kept.

## Use with AI coding agents

Copy [`skills/jobrun`](skills/jobrun/SKILL.md) into your agent's skills folder (`~/.claude/skills/`,
`~/.agents/skills/`, …) and add one line to your `AGENTS.md` / `CLAUDE.md`:

```markdown
Launch every command expected to run longer than ~2 minutes through `jobrun` (see the jobrun skill).
```

The agent then names its jobs, groups sweeps, picks a sensible progress unit, and checks `jobrun ls` instead of
asking you to look.

## Commands

| Command | |
|---|---|
| `run [--fg] -- CMD` | Run under supervision: detached by default; `--fg` stays in the foreground and also prints the output |
| `sbatch -- ARGS` | Submit with `sbatch` and track |
| `track --pid PID` / `--slurm ID` | Adopt a running process or Slurm job; a process's log is detected from its stdout |
| `ls [--all] [--json]` | List jobs (finished ones from the last 3 days unless `--all`) |
| `show ID` · `log ID [-f]` | Details as JSON · tail the log |
| `wait ID` / `--group G` | Block until finished |
| `stop ID` / `--group G` | SIGTERM to the whole process tree, then SIGKILL after 10 s; `scancel` for Slurm |
| `rm ID` · `clean [--days 7]` | Remove from the list (with the logs jobrun wrote) |

Common options: `--name`, `--group`, `--log`, `--count-regex`, `--progress-regex`, `--total`.

## How it works

Each job is a few small files in `~/.jobs` (`$JOBRUN_HOME`). Point it at a shared filesystem and jobs on cluster
nodes show up too.

- A small **supervisor** process starts the command, writes its output to the log, writes a heartbeat file every
  30 s and records the exit code. It uses about 13 MB of memory and practically no CPU.
- **Readers** (`jobrun ls`, the sidebar every 10 s) work out status and progress: from the pid or heartbeat, from
  `sacct`, and from the last 256 KB of the log. Finished jobs are cached. One refresh with 200 jobs takes about
  0.06 s of CPU.
- A job whose supervisor disappears without recording an exit (e.g. after a reboot) shows as **lost** instead of
  running forever.

Requires Linux (`/proc`) and Python ≥ 3.9. Slurm is optional.

## Development

```bash
python -m pytest -q tests
cd vscode && npx @vscode/vsce package -o jobboard.vsix && code --install-extension jobboard.vsix
```

## License

MIT
