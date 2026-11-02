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
jobrun rm ID   jobrun clean [--days 7]
```

IDs match by exact id, unique substring, or exact name.

## Progress

Parsed from the newest matching line of the log tail, so scripts need no changes:

- tqdm bars: `412/1000 [01:23<04:56, ...]`
- bracketed counters: `[412/1000] ...`, `[cfg 412/1000] ...`
- explicit lines: `print(f"JOBRUN_PROGRESS {done}/{total}", flush=True)`

Otherwise: `--progress-regex 'Trial (?P<done>\d+)' --total 200`, or count mode
`--count-regex ' completed ' --total 60` (number of matching lines).
Rate is measured over the last 15 min (run average until then); ETA = remaining / rate.

## Status

`starting pending running unknown` are active; `done failed killed timeout oom ended lost` are final.
Supervised jobs on this host are checked by pid, on other hosts by a 30 s heartbeat; `lost` means the
supervisor vanished without recording an exit. Slurm jobs come from `sacct` (`scontrol` is disabled).

## Development

```bash
python -m pytest -q tests
cd vscode && npx @vscode/vsce package --allow-missing-repository --skip-license -o jobboard.vsix \
  && code --install-extension jobboard.vsix    # then "Developer: Reload Window"
```
