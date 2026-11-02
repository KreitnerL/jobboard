# jobboard

A minimal registry of long-running jobs (local processes, background jobs, Slurm) with progress,
rate and ETA, shown in a VS Code side panel.

- `jobrun.py`: CLI, stdlib only. Registry in `$JOBRUN_HOME` (default `~/.jobs`, shared via /vol/miltank).
- `vscode/`: the "Jobs" activity-bar view; polls `jobrun ls --json`.

## Usage

```bash
jobrun run --name "tune ilp" -- python tune.py ...      # detached; log in ~/.jobs/logs/<id>.log
jobrun run --log out/run.log -- python eval.py           # log elsewhere
jobrun run --fg -- python eval.py                        # foreground, output also on stdout (sbatch scripts)
jobrun sbatch --name rollout -- job.sbatch               # submit and track; log path from sacct
jobrun track --pid 12345 [--log file]                    # adopt a running process (log from its stdout if a file)
jobrun track --slurm 324562                              # adopt an existing Slurm job
jobrun ls [--all] [--json]   jobrun show ID   jobrun log ID [-f]   jobrun stop ID [--force]
jobrun rm ID [--keep-log]    jobrun clean [--days 7]
```

IDs match by exact id, exact name, or unique id substring.

## Groups

`--group NAME` on `run`/`sbatch`/`track` puts jobs into a set, e.g. one tuning run per variant:

```bash
for v in ilp slogs2d smac; do
  jobrun run --group "tuning 2026-11-02" --name "$v" --count-regex 'Trial \d+ completed' --total 100 -- python tune.py --variant $v
done
jobrun stop --group "tuning 2026-11-02"      # also: jobrun rm --group ...
```

The dashboard shows the group as a parent row: jobs finished, progress summed over all members
(e.g. 412/600 trials), summed rate, and the ETA of the slowest member. Finished members of a group stay quiet;
failures and the group's completion each raise one notification.

## Logs

jobrun deletes (on `rm`/`clean`/dashboard remove) only logs **it wrote**: the output of `jobrun run`
(default `~/.jobs/logs/<id>.log`, or wherever `--log` points) and the Slurm output of `jobrun sbatch`, which goes to
`~/.jobs/logs/<id>.log` unless the command line or the script's `#SBATCH` header sets `--output`. Logs it only
**reads** (`track --log`, `--progress-file`, a Slurm output you chose) belong to the experiment and are never deleted.

Logs jobrun writes are capped (`--max-log-mb`, default 512, env `JOBRUN_MAX_LOG_MB`): beyond the cap the
supervisor keeps the last 1 MB and notes the trim in the log. Count mode keeps its count across trims.

## Progress

Parsed from the newest matching line of the log tail, so scripts need no changes:

- tqdm bars: `412/1000 [01:23<04:56, ...]`
- bracketed counters: `[412/1000] ...`, `[cfg 412/1000] ...`
- explicit lines: `print(f"JOBRUN_PROGRESS {done}/{total}", flush=True)`

Otherwise: `--progress-regex 'Trial (?P<done>\d+)' --total 200`, or count mode
`--count-regex ' completed ' --total 60` (number of matching lines).
Rate is measured over the last 15 min (run average until then); ETA = remaining / rate.
Large counts display compactly (412M/1G).

For loops with millions or billions of steps, do not print per step; report by time so the log stays small
regardless of speed (≈ 360 lines per hour at 10 s):

```python
import time

class Progress:
    """Prints 'JOBRUN_PROGRESS done/total' at most every `every_s` seconds, and at the end."""
    def __init__(self, total, every_s=10):
        self.total, self.every_s, self.done, self.last = total, every_s, 0, 0.0

    def update(self, n=1):
        self.done += n
        if time.monotonic() - self.last >= self.every_s or self.done >= self.total:
            self.last = time.monotonic()
            print(f"JOBRUN_PROGRESS {self.done}/{self.total}", flush=True)
```

tqdm already throttles its own output (10 updates/s by default; `mininterval=10` for long runs).

## Status

`starting pending running unknown` are active; `done failed killed timeout oom ended lost` are final.
Supervised jobs on this host are checked by pid, on other hosts by a 30 s heartbeat; `lost` means the
supervisor vanished without recording an exit. Slurm jobs come from `sacct` (`scontrol` is disabled).

## Footprint

Measured on wskreitner with 20 running and 200 finished jobs:

- supervisor per running job: idle Python process, ~13 MB proportional memory (26 MB RSS incl. shared interpreter),
  no measurable CPU; wakes every 5 s for the heartbeat and log-size check. Output goes straight from the command
  to the log file (no copying), except in `--fg` mode.
- `jobrun ls` (one dashboard refresh): ~0.06 s CPU, 0.35–0.55 s wall (NFS), 25 MB peak; the dashboard runs it
  every 10 s. Per active job it reads the last 256 KB of the log; finished jobs are cached; count mode reads only
  new bytes, in 8 MB chunks, at most 256 MB per refresh.
- `sacct` results are cached for 30 s across all readers.
- registry: a few KB per job (2 MB for 220 jobs); `jobrun clean` drops finished jobs older than 7 days.

## Development

```bash
python -m pytest -q tests
cd vscode && npx @vscode/vsce package --allow-missing-repository --skip-license -o jobboard.vsix \
  && code --install-extension jobboard.vsix    # then "Developer: Reload Window"
```
