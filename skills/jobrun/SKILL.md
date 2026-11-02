---
name: jobrun
description: Register long-running jobs (local scripts, background processes, Slurm jobs, tuning sweeps, rollouts, training, evaluation) with the jobrun CLI so the user can follow progress, rate and ETA on their Job Board dashboard without asking. Use whenever launching a command expected to run longer than ~2 minutes, submitting sbatch jobs, starting several related runs, or when the user asks how a running job is doing.
---

# jobrun

The user watches all long jobs on a dashboard fed by `jobrun`. A job that is not registered is invisible to them.

## Rules

1. Launch every command expected to take more than ~2 minutes through `jobrun`; submit Slurm jobs with
   `jobrun sbatch`. Adopt jobs that are already running with `jobrun track`.
2. Always pass a short, specific `--name` (variant, dataset, N), and `--group "<what> <YYYY-MM-DD>"` for a set of
   related jobs. Use one job per independently running process (e.g. one per tuning variant).
3. Measure progress in the unit the user would ask about (completed trials, cases, epochs), not inner steps.
   Use count mode (`--count-regex ... --total N`) for items that finish out of order, e.g. parallel trials.
   Cover every phase: check what the script does after its main loop (re-timing, final evaluation, export)
   and count those lines too, e.g. `--count-regex 'Trial \d+ (completed|re-timed)' --total 105` for 100 trials
   plus 5 re-timed finalists. A job at 100% that keeps running shows as "finishing" on the dashboard.
4. If no existing log line fits, make the script print `JOBRUN_PROGRESS <done>/<total>` throttled by time
   (at most every ~10 s), never once per step of a fast loop.
5. After launching, tell the user the job id (or group). To check on jobs use `jobrun ls` / `jobrun show ID`
   instead of reading logs.
6. To wait for completion, run `jobrun wait ID` (or `--group G`) in the background; the job keeps running and
   stays on the dashboard even if the waiting shell dies.
7. Do not stop or remove jobs you did not start unless the user asks.

## Commands

```bash
jobrun run --name NAME [--group G] [progress options] -- CMD ...   # detached; prints id and log path
jobrun run --fg ...                                                # foreground, output also on stdout
jobrun sbatch --name NAME [--group G] [--log PATTERN] -- SBATCH_ARGS... SCRIPT
jobrun track --pid PID [--log FILE] | --slurm ID                   # adopt a running job
jobrun ls | show ID | wait ID | log ID [-f] | stop ID | stop --group G | rm ID
```

Progress options (default: auto-detect tqdm bars `12/100 [`, counters `[12/100]`, `JOBRUN_PROGRESS 12/100`):

| Situation | Option |
|---|---|
| Lines like `Trial 37 completed`, finishing out of order | `--count-regex 'Trial \d+ completed' --total 100` |
| A counter the defaults miss | `--progress-regex 'case (?P<done>\d+) of (?P<total>\d+)'` |
| Progress in another file than stdout | `run --progress-file FILE`, `track/sbatch --log FILE` |
| Slurm job with stderr in a separate file (tqdm writes to stderr) | `sbatch --log '/path/%x-%j.err'` |

## Examples

```bash
# Tuning sweep: one job per variant, progress = completed trials
for v in ilp slogs2d smac; do
  jobrun run --group "tuning 2026-11-02" --name "$v" \
    --count-regex 'Trial \d+ completed' --total 100 -- python tune.py --variant "$v"
done

# Slurm
jobrun sbatch --name "train unet fold0" -- job.sbatch

# Wait in the background, then report
jobrun wait --group "tuning 2026-11-02"
```

If `jobrun` is not on PATH, the job runs unmonitored: say so to the user instead of silently skipping registration.
