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
3. Do not research a program's internals to register it: name + group is enough. Progress is detected
   automatically (tqdm bars, `[12/100]` counters, `JOBRUN_STAGE` / `JOBRUN_PROGRESS` lines). Add
   `--count-regex '...' --total N` only when the progress line is obvious from the log (e.g. `Trial 7 completed`,
   finishing out of order). A job past its total shows as "finishing"; that is fine.
4. Multi-step work:
   - One script with several phases: it prints `JOBRUN_STAGE <name> [i/n]` before each phase (shell:
     `echo "JOBRUN_STAGE freeze 2/3"`); progress and ETA then apply per stage. Optional `--stages a,b,c` lists
     planned stages before they start.
   - Separate commands that depend on each other: one job each, chained with `--after <id or name>`. Never write
     polling loops (`while pgrep ...`); jobrun waits (status "waiting", no elapsed time), starts the job when the
     dependency succeeded, and marks it "cancelled" if the dependency failed.
   - If a program you will run repeatedly prints no usable progress, propose adding marker lines to its code once
     (`print(f"JOBRUN_PROGRESS {done}/{total}", flush=True)`, throttled to ~10 s in fast loops) instead of
     encoding its internals in regexes.
5. After launching, tell the user the job id (or group). To check on jobs use `jobrun ls` / `jobrun show ID`
   instead of reading logs.
6. To wait for completion, run `jobrun wait ID` (or `--group G`) in the background; the job keeps running and
   stays on the dashboard even if the waiting shell dies.
7. Do not stop or remove jobs you did not start unless the user asks.
8. Before starting jobs that need many cores, much RAM or a GPU, check `jobrun res`: per node it shows what is
   used and reserved, and each job's used / reserved, so you can size new jobs to what is free.
9. Give every job a CPU reservation when its worker count is known (Slurm jobs have one already). Best: the
   program prints `JOBRUN_BUDGET cpus=N` (also `mem=96G`, `gpus=1`; again when a stage uses a different number).
   Otherwise pass it at launch: `jobrun run --cpus 10 --mem 96G -- …`, or `jobrun edit ID --cpus 10` later.
   A memory cap set via `systemd-run --scope -p MemoryMax=…` is picked up automatically.
8. To fix a registration (name, group, `--after`, stages, progress options), use `jobrun edit ID ...`; it never
   touches the running command. For a script that prints no stage markers and cannot be changed while it runs,
   `--stage-at NAME=REGEX` starts stage NAME at the first log line matching REGEX.

## Commands

```bash
jobrun run --name NAME [--group G] [--after JOB] [--stages a,b] -- CMD ...   # detached; prints id + log
jobrun run --fg ...                                                # foreground, output also on stdout
jobrun sbatch --name NAME [--group G] [--after SLURM_JOB] [--log PATTERN] -- SBATCH_ARGS... SCRIPT
jobrun track --pid PID [--log FILE] | --slurm ID                   # adopt a running job
jobrun ls | res | show ID | wait ID | log ID [-f] | edit ID [options] | stop ID | stop --group G | rm ID
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

# Dataset (two stages printed by the script) -> tuning that needs it
jobrun run --group "wp3 2026-11-02" --name "prepare n100" -- python prepare.py --n 100
jobrun run --group "wp3 2026-11-02" --name "tune ilp" --after "prepare n100" -- python tune.py ilp.yml

# Slurm
jobrun sbatch --name "train unet fold0" -- job.sbatch

# Wait in the background, then report
jobrun wait --group "tuning 2026-11-02"
```

If `jobrun` is not on PATH, the job runs unmonitored: say so to the user instead of silently skipping registration,
and point them to the installer: `curl -fsSL https://raw.githubusercontent.com/KreitnerL/jobboard/main/install.sh | sh`
(or, with the Job Board VS Code extension, the command it shows).
