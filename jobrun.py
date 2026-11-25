#!/usr/bin/env python3
"""jobrun: register long-running jobs and follow their progress, rate and ETA.

The registry is a directory of small files ($JOBRUN_HOME, default ~/.jobs):

    jobs/<id>.json        metadata, written at registration
    jobs/<id>.exit.json   exit code and end time, written by the supervisor
    jobs/<id>.hb          heartbeat, touched by the supervisor while the command runs
    jobs/<id>.stopped     marker written by `jobrun stop`
    jobs/<id>.state.json  the last rate and the cached final row, written by readers
    hosts/<host>.json     the last resource sample of that host (`jobrun res`), the baseline for the next mean
    logs/<id>.log         default log of supervised commands
    removed.log           one JSON line per job removed by `rm`, `clean` or auto_clean_days: its status and end
                          time (so waits and dependencies still see how it ended) and the process that removed it

Progress is parsed from the job's log, so commands need no changes. Recognised by default:
tqdm bars ("412/1000 [01:23<..."), bracketed counters ("[412/1000] ...") and explicit
"JOBRUN_PROGRESS 412/1000" lines. Stdlib only, so it runs under any python3 >= 3.8.

A pool (kind "pool") is a job without a process of its own: several workers, on any hosts, work through one shared
list. Workers are ordinary jobs whose metadata names the pool; the pool's row is computed from theirs (pool_row).
"""
from __future__ import annotations

import argparse
import ctypes
import getpass
import hashlib
import json
import os
import re
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path

HOME = Path(os.environ.get("JOBRUN_HOME") or Path.home() / ".jobs")
JOBS = HOME / "jobs"
LOGS = HOME / "logs"
HOST = socket.gethostname()

HEARTBEAT_S = float(os.environ.get("JOBRUN_HEARTBEAT_S", 30))
STALE_S = 5 * HEARTBEAT_S
CHECK_S = min(HEARTBEAT_S, 2)  # supervisor wake-up: log scan (stages, progress), heartbeat, log size check
LAST_LINE_BYTES = 16 * 1024  # tail read for the last log line
WAIT_POLL_S = float(os.environ.get("JOBRUN_WAIT_POLL_S", 15))  # dependency checks of a waiting job
COUNT_CHUNK_BYTES = 8 << 20  # count mode reads in chunks ...
COUNT_BUDGET_BYTES = 256 << 20  # ... and at most this much per refresh
SACCT_TTL_S = 30
# Rate and ETA use the last RATE_STEPS completed steps of the current stage. Backtested on 173 finished tuning and
# rollout runs (2026-11-06): lower median ETA error than the run average or a 15-minute window, and nearly unbiased
# when early steps are fast (parallel workers finish their short items first).
RATE_STEPS = 15
# User settings, in $JOBRUN_HOME/config.json (`jobrun config`, or the Job Board's VS Code settings, which write it):
# one file for every machine that shares the registry. Environment variables JOBRUN_<KEY> take precedence.
SETTINGS = {
    # a running job is "stalled" when its last step is longer ago than all three: stall_factor average steps,
    # stall_gap_factor times the longest gap between its recent steps, and stall_min_s. A hint that it may hang,
    # shown next to the time since the last step. The gap term keeps jobs whose steps arrive in bursts (parallel
    # workers finishing together, a mean step far shorter than the usual pause between bursts) from being flagged.
    "stall_factor": (4.0, "a job is stalled after this many average step times without progress"),
    "stall_gap_factor": (2.0, "... and this many times the longest gap between its recent steps (0: ignore gaps)"),
    "stall_min_s": (300.0, "... and at least this many seconds"),
    # logs jobrun writes: beyond max_log_mb, the middle is cut; the first log_keep_head_mb and the newest
    # max_log_mb / 2 are kept
    "max_log_mb": (512.0, "cap for the logs jobrun writes (0: no cap); beyond it the middle of the log is cut"),
    "log_keep_head_mb": (16.0, "start of a capped log that is always kept (at most a quarter of max_log_mb)"),
    "auto_clean_days": (0.0, "delete finished jobs older than this many days, with the logs jobrun wrote (0: never)"),
}
RES_MIN_S = float(os.environ.get("JOBRUN_RES_MIN_S", 3))  # `jobrun res` within this many seconds of the last sample returns that sample (several dashboards)
RES_FIRST_S = 0.5  # without an earlier sample, the means cover this short first interval
JSON_SCHEMA = 2  # version of `ls --json`; bump on incompatible changes (the VS Code extension checks it)
VERSION = "0.8.0"  # keep in sync with pyproject.toml and vscode/package.json (tested)
SUPERVISOR_VERSION = 2  # supervisors from 2 on reload edited settings
ACTIVE = ("starting", "waiting", "running", "unknown")
# "lost" may recover (NFS hiccup, supervisor between child exit and exit file), so it is never cached.
FINAL = ("done", "error", "stopped", "cancelled", "ended")
# done: exit 0. error: non-zero exit, signal, time limit, out of memory (see the row's reason). stopped: by `jobrun stop`
# (or scancel) while running. cancelled: never started (dependency failed, or stopped while waiting). ended: a
# tracked process ended with an unknown exit code. lost: the supervisor vanished without recording an exit.
LEGACY_STATUS = {"failed": "error", "timeout": "error", "oom": "error", "killed": "stopped", "skipped": "cancelled",
                 "pending": "waiting"}  # names used before 0.3, e.g. in cached rows

PROGRESS_PATTERNS = [
    re.compile(r"JOBRUN_PROGRESS\s+(?P<done>\d+)\s*/\s*(?P<total>\d+)"),
    re.compile(r"(?P<done>\d+)/(?P<total>\d+)\s*\[\d"),  # tqdm: " 412/1000 [01:23<04:56, 3.21it/s]"
    re.compile(r"\[(?:[^\[\]]*\s)?(?P<done>\d+)/(?P<total>\d+)\]"),  # "[412/1000] ..." or "[cfg 412/1000] ..."
]
STAGE_MARKER = re.compile(r"JOBRUN_STAGE\s+(?P<name>.+?)(?:\s+(?P<index>\d+)/(?P<of>\d+))?\s*$")
# "JOBRUN_BUDGET cpus=12 mem=96G gpus=1": what the job has been given from now on (any subset of the keys)
BUDGET_MARKER = re.compile(r"JOBRUN_BUDGET((?:\s+(?:cpus|mem|gpus)=[\w.]+)+)")
# "JOBRUN_EVENT error <message>" / "JOBRUN_EVENT warn <message>": a problem the job reports while it keeps running
EVENT_MARKER = re.compile(r"JOBRUN_EVENT\s+(?P<level>error|warn(?:ing)?)\b:?\s*(?P<message>.*?)\s*$", re.I)
EVENTS_KEPT = 20  # the newest events kept per job; event_count counts all of them
EVENT_LEVELS = ("warn", "error")  # in order of severity
# "JOBRUN_ITEM [label]": the worker that prints it finished one item of its pool's shared list
ITEM_MARKER = re.compile(r"JOBRUN_ITEM\b(?:\s+(?P<label>.*?))?\s*$")
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
INTERPRETERS = {"uv", "run", "bash", "sh", "srun", "nohup", "env", "time", "exec"}
SLURM_STATES = {  # sacct state -> (status, reason)
    "PENDING": ("waiting", None), "REQUEUED": ("waiting", "requeued"), "SUSPENDED": ("waiting", "suspended"),
    "CONFIGURING": ("starting", None), "RUNNING": ("running", None), "COMPLETING": ("running", None),
    "COMPLETED": ("done", None), "FAILED": ("error", None), "NODE_FAIL": ("error", "node failure"),
    "BOOT_FAIL": ("error", "boot failure"), "DEADLINE": ("error", "deadline"), "TIMEOUT": ("error", "time limit"),
    "OUT_OF_MEMORY": ("error", "out of memory"), "CANCELLED": ("stopped", None), "PREEMPTED": ("stopped", "preempted"),
}


# ---------------------------------------------------------------- files

def _file(job_id: str, suffix: str) -> Path:
    return JOBS / f"{job_id}{suffix}"


CONFIG_PATH = HOME / "config.json"
_config_cache: tuple = (None, {})


def config_file() -> dict:
    """The registry's config.json, re-read when it changes (a supervisor runs for days)."""
    global _config_cache
    mtime = _mtime(CONFIG_PATH)
    if mtime != _config_cache[0]:
        data = read_json(CONFIG_PATH, {}) or {}
        _config_cache = (mtime, data if isinstance(data, dict) else {})
    return _config_cache[1]


def setting(key: str) -> float:
    """JOBRUN_<KEY> from the environment, else config.json, else the default."""
    default = SETTINGS[key][0]
    for source in (os.environ.get(f"JOBRUN_{key.upper()}"), config_file().get(key)):
        try:
            if source is not None and source != "":
                return float(source)
        except (TypeError, ValueError):
            pass
    return default


def read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default


def write_json(path: Path, data) -> None:
    tmp = path.with_name(f".{path.name}.{HOST}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, indent=1))
    os.replace(tmp, path)


def _mtime(path: Path | None) -> float | None:
    try:
        return path.stat().st_mtime if path else None
    except OSError:
        return None


def allocate_id() -> str:
    JOBS.mkdir(parents=True, exist_ok=True)
    while True:
        job_id = f"{datetime.now():%y%m%d}-{secrets.token_hex(2)}"
        if not _file(job_id, ".json").exists():
            return job_id


def load_jobs() -> list[dict]:
    if not JOBS.is_dir():
        return []
    metas = []
    for path in JOBS.glob("*.json"):
        if path.name.endswith((".exit.json", ".state.json")):
            continue
        meta = read_json(path)
        if meta and "id" in meta:
            metas.append(meta)
    return sorted(metas, key=lambda m: m.get("started") or 0)


def resolve(keys: list[str], missing_ok: bool = False) -> list[dict]:
    """Jobs by exact id, exact name, or unique id substring; missing_ok skips keys that match nothing."""
    metas = load_jobs()
    found = []
    for key in keys:
        hits = ([m for m in metas if m["id"] == key] or [m for m in metas if m.get("name") == key]
                or [m for m in metas if key in m["id"]])
        if not hits and missing_ok:
            print(f"jobrun: {key!r} not found (already removed?)", file=sys.stderr)
            continue
        if len(hits) != 1:
            listing = "".join(f"\n  {m['id']}  {m.get('name')}" for m in hits)
            sys.exit(f"jobrun: {key!r} matches {len(hits)} jobs{listing}")
        found.append(hits[0])
    return found


# ---------------------------------------------------------------- processes

def _proc_stat(pid) -> list[str] | None:
    """Fields after the command name: [0] state, [1] ppid, [2] pgrp, [19] starttime."""
    try:
        return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    except (OSError, IndexError):
        return None


def proc_start(pid) -> int | None:
    fields = _proc_stat(pid)
    return int(fields[19]) if fields else None


def pid_alive(pid, start: int | None = None) -> bool:
    fields = _proc_stat(pid)
    return bool(fields) and fields[0] != "Z" and (start is None or int(fields[19]) == start)


def proc_started_at(pid) -> float:
    btime = next(int(line.split()[1]) for line in Path("/proc/stat").read_text().splitlines()
                 if line.startswith("btime"))
    return btime + proc_start(pid) / os.sysconf("SC_CLK_TCK")


def descendants(pid: int) -> list[int]:
    children: dict[int, list[int]] = {}
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            fields = _proc_stat(entry.name)
            if fields:
                children.setdefault(int(fields[1]), []).append(int(entry.name))
    found, todo = [], [pid]
    while todo:
        for child in children.get(todo.pop(), []):
            found.append(child)
            todo.append(child)
    return found


def _stdout_file(pid) -> str | None:
    try:
        target = os.readlink(f"/proc/{pid}/fd/1")
    except OSError:
        return None
    return target if target.startswith("/") and os.path.isfile(target) else None


def default_name(cmd: list[str]) -> str:
    tokens = []
    for part in cmd:
        try:
            tokens += shlex.split(part) if " " in part else [part]
        except ValueError:
            tokens.append(part)
    for token in tokens:
        base = os.path.basename(token)
        if (base not in INTERPRETERS and not re.fullmatch(r"python[\d.]*", base)
                and not token.startswith("-") and "=" not in token):
            return base
    return os.path.basename(tokens[0]) if tokens else "job"


# ---------------------------------------------------------------- slurm

def _slurm_time(value: str | None) -> float | None:
    try:
        return datetime.fromisoformat(value).timestamp() if value else None
    except ValueError:  # "Unknown", "None"
        return None


def query_slurm(ids: list[str]) -> dict[str, dict]:
    """sacct states, cached for SACCT_TTL_S across all readers so open dashboards don't hammer slurmdbd."""
    if not ids or not shutil.which("sacct"):
        return {}
    cache_path = HOME / "sacct-cache.json"
    cache = read_json(cache_path, {}) or {}
    if time.time() - cache.get("time", 0) < SACCT_TTL_S and set(ids) <= set(cache.get("info", {})):
        return cache["info"]
    try:
        out = subprocess.run(["sacct", "-X", "-n", "-P", "-j", ",".join(ids),
                              "-o", "JobIDRaw,State,ExitCode,Start,End,NodeList"],
                             capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError):
        return {}
    info = {}
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) < 6:
            continue
        slurm_id, state, code, start, end, nodes = parts[:6]
        status, reason = SLURM_STATES.get(state.split()[0] if state else "", ("unknown", None))
        exit_code = int(code.split(":")[0]) if code[:1].isdigit() else None
        if status == "stopped" and _slurm_time(start) is None:
            status = "cancelled"  # cancelled before it ever started
        if status == "error" and reason is None and exit_code:
            reason = f"exit code {exit_code}"
        info[slurm_id] = {
            "status": status, "reason": reason, "exit_code": exit_code, "started": _slurm_time(start),
            "ended": None if status in ACTIVE else _slurm_time(end),
            "host": None if nodes in ("", "None assigned") else nodes,
        }
    try:
        write_json(cache_path, {"time": time.time(), "info": info})
    except OSError:
        pass
    return info


def _slurm_log(pattern: str, info: dict) -> str:
    """Expand sbatch filename patterns (%j, %x, %u, %A, %a, %N, %%) against sacct fields."""
    pattern = pattern or "slurm-%j.out"
    master, _, task = info["JobID"].partition("_")
    values = {"j": info["JobIDRaw"], "x": info["JobName"], "u": os.environ.get("USER", ""), "A": master,
              "a": task, "N": info["NodeList"], "%": "%"}
    expanded = re.sub(r"%(\d*)([jxuAaN%])", lambda m: values[m.group(2)].zfill(int(m.group(1) or 0)), pattern)
    return os.path.join(info["WorkDir"] or "", expanded)


def slurm_meta(slurm_id: str, attempts: int = 10, log_pattern: str | None = None) -> dict:
    """Metadata from sacct (scontrol is disabled on some clusters); retries until the job shows up.

    The log is the job's StdOut, or log_pattern (sbatch patterns like %x-%j.err) to follow e.g. stderr.
    """
    fields = ["JobIDRaw", "JobID", "JobName", "StdOut", "WorkDir", "NodeList", "Submit", "SubmitLine"]
    for attempt in range(attempts):
        out = subprocess.run(["sacct", "-X", "-n", "-P", "-j", slurm_id, "-o", ",".join(fields)],
                             capture_output=True, text=True)
        line = next((l for l in out.stdout.splitlines() if l.startswith(f"{slurm_id}|")), None)
        if line:
            info = dict(zip(fields, line.split("|", len(fields) - 1)))
            return {
                "kind": "slurm", "slurm_id": slurm_id, "name": info["JobName"],
                "cmd": [info["SubmitLine"]] if info["SubmitLine"] else [], "cwd": info["WorkDir"],
                "log": _slurm_log(log_pattern or info["StdOut"], info),
                "started": _slurm_time(info["Submit"]) or time.time(),
            }
        if attempt + 1 < attempts:
            time.sleep(0.5)
    return {"kind": "slurm", "slurm_id": slurm_id, "name": f"slurm-{slurm_id}", "started": time.time()}


# ---------------------------------------------------------------- supervisor

def dropped_bytes(job_id: str) -> int:
    """Bytes trimmed from the head of the job's log so far (logical offset = physical + dropped)."""
    try:
        return int(_file(job_id, ".trim").read_text())
    except (OSError, ValueError):
        return 0


TRIM_CHUNK_BYTES = 8 << 20  # the kept tail moves down in chunks of this size


def _line_start(handle, position: int, limit: int) -> int:
    """The first line start at or after position, looking at most limit bytes ahead (else position itself)."""
    handle.seek(max(0, position - 1))
    window = handle.read(min(limit, LAST_LINE_BYTES) + 1)
    newline = window.find(b"\n")
    return max(0, position - 1) + newline + 1 if newline >= 0 else position


def trim_log(job_id: str, log: str, max_bytes: int, head_bytes: int, scanned: int | None = None) -> int:
    """Cut the middle out of a log beyond max_bytes: keep the first head_bytes (at most a quarter of the cap) and
    the newest max_bytes / 2, with a line saying what was dropped in between. Returns the bytes dropped.

    The job writes with O_APPEND, so it keeps appending at the end; the tail is moved down in place, chunk by
    chunk, and the file is shortened once the copy has caught up with the end. Output appended between that last
    check and the truncation is lost: a few lines at most, and only once the log has reached the cap.
    scanned: how far (physical bytes) the progress scan has read; nothing it has not read yet is cut.
    """
    try:
        size = os.path.getsize(log)
        if not max_bytes or size <= max_bytes:
            return 0
        head = min(head_bytes, max_bytes // 4)
        with open(log, "r+b") as handle:
            cut = _line_start(handle, head, max_bytes // 8) if head else 0
            source = _line_start(handle, size - max_bytes // 2, max_bytes // 8)
            if scanned is not None and scanned < source:
                return 0  # the scan is behind: trim once it has caught up
            dropped = source - cut
            marker = (f"\njobrun: ... {dropped / (1 << 20):.0f} MB dropped here: the log exceeded {max_bytes / (1 << 20):g} MB; "
                      f"jobrun keeps its start and its newest part ...\n\n").encode()
            if dropped <= len(marker):
                return 0
            handle.seek(cut)
            handle.write(marker)
            target = cut + len(marker)
            while True:
                handle.seek(source)
                chunk = handle.read(TRIM_CHUNK_BYTES)
                if not chunk:
                    if source >= os.fstat(handle.fileno()).st_size:
                        handle.truncate(target)
                        break
                    continue
                handle.seek(target)
                handle.write(chunk)
                source += len(chunk)
                target += len(chunk)
            handle.flush()
        removed = source - target  # logical offset = physical + everything removed so far
        _file(job_id, ".trim").write_text(str(dropped_bytes(job_id) + removed))
        return removed
    except OSError:
        return 0


def update_meta(job_id: str, **fields) -> dict:
    """Set fields on a fresh read of the job's metadata, so concurrent writers (supervisor, `jobrun edit`)
    don't overwrite each other's fields; returns the updated metadata."""
    path = _file(job_id, ".json")
    meta = read_json(path) or {}
    meta.update(fields)
    write_json(path, meta)
    return meta


def _beat(job_id: str) -> None:
    try:  # registry trouble (NFS, a removed folder) must never kill the supervisor
        _file(job_id, ".hb").touch()
    except OSError:
        pass


def _finish(job_id: str, info: dict) -> None:
    try:
        write_json(_file(job_id, ".exit.json"), {"ended": time.time(), **info})
    except OSError:
        pass


def wait_for_dependencies(job_id: str) -> str | None:
    """Block until every job in the job's "after" list (re-read, so edits apply) is done. Returns why the job
    must not start, or None."""
    stop = []
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, lambda *_: stop.append(True))
    while True:
        _beat(job_id)
        if stop:
            return "stopped while waiting"
        after = (read_json(_file(job_id, ".json")) or {}).get("after") or []
        deps = [read_json(_file(dep, ".json")) for dep in after]
        gone = [dep for dep, meta in zip(after, deps) if not meta]
        if gone:  # removed from the list: what matters is how it had ended
            removed = removed_jobs()
            failed = next((dep for dep in gone if (removed.get(dep) or {}).get("status") != "done"), None)
            if failed:
                status = (removed.get(failed) or {}).get("status")
                return f"dependency {failed} was removed" + (f" ({status})" if status else "")
        rows = snapshots([m for m in deps if m])
        bad = next((r for r in rows if r["status"] not in ACTIVE and r["status"] != "done"), None)
        if bad:
            return f"dependency {bad['name']}: {bad['status']}"
        if all(r["status"] == "done" for r in rows):
            return None
        for _ in range(int(WAIT_POLL_S / 0.5)):  # sleep, but react to stop quickly
            if stop:
                break
            time.sleep(0.5)


def supervise(job_id: str, tee: bool = False) -> int:
    """Wait for dependencies, run the job's command, keep a heartbeat, scan its log, record its exit.

    With tee, also copy output to stdout. The supervisor owns the job's log scan (stages, progress), so stage
    times stay accurate while no dashboard is open.
    """
    meta_path = _file(job_id, ".json")
    meta = update_meta(job_id, supervisor_pid=os.getpid(), supervisor_start=proc_start(os.getpid()), supervisor_scans=True,
                       supervisor_version=SUPERVISOR_VERSION, waiting=bool((read_json(meta_path) or {}).get("after")))
    if meta.get("after"):
        reason = wait_for_dependencies(job_id)
        if reason:
            _finish(job_id, {"exit_code": None, "skipped": reason})
            return 1
    meta = update_meta(job_id, waiting=False, **({"started": time.time()} if meta.get("after") else {}))
    cmd = meta["cmd"]
    env = dict(os.environ, JOBRUN_ID=job_id, PYTHONUNBUFFERED="1")
    with open(meta["log"], "ab", buffering=0) as log:
        try:
            proc = subprocess.Popen(cmd[0] if len(cmd) == 1 else cmd, shell=len(cmd) == 1,
                                    cwd=meta["cwd"], env=env, stdin=subprocess.DEVNULL,
                                    stdout=subprocess.PIPE if tee else log, stderr=subprocess.STDOUT,
                                    start_new_session=True)
        except OSError as exc:
            log.write(f"jobrun: cannot start {cmd}: {exc}\n".encode())
            _finish(job_id, {"exit_code": 127})
            return 127
        meta = update_meta(job_id, pid=proc.pid, pgid=proc.pid, proc_start=proc_start(proc.pid))

        def forward(signum, _frame):
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

        for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(signum, forward)

        pump = None
        if tee:
            def copy():
                for chunk in iter(lambda: os.read(proc.stdout.fileno(), 65536), b""):
                    log.write(chunk)
                    try:
                        sys.stdout.buffer.write(chunk)
                        sys.stdout.buffer.flush()
                    except OSError:
                        pass

            pump = threading.Thread(target=copy, daemon=True)
            pump.start()

        meta_mtime = _mtime(meta_path)
        while True:
            _beat(job_id)
            if _mtime(meta_path) != meta_mtime:  # `jobrun edit` changed the settings
                meta, meta_mtime = read_json(meta_path) or meta, _mtime(meta_path)
            scan = update_scan(meta)  # before trimming, so no stage marker is trimmed unseen
            # a cap given to `jobrun run --max-log-mb` is the job's own; otherwise the current setting applies
            cap_mb = meta["max_log_mb"] if meta.get("max_log_mb") is not None else setting("max_log_mb")
            if cap_mb:
                scanned = None if meta.get("progress_file") else scan.get("offset", 0) - dropped_bytes(job_id)
                trim_log(job_id, meta["log"], int(cap_mb * (1 << 20)), int(setting("log_keep_head_mb") * (1 << 20)),
                         scanned)
            try:
                code = proc.wait(timeout=CHECK_S)
                break
            except subprocess.TimeoutExpired:
                pass
        if pump:
            pump.join(timeout=5)
    update_scan(meta, final=True)
    _finish(job_id, {"exit_code": code})
    return code if code >= 0 else 128 - code


# ---------------------------------------------------------------- status and progress

def _signal_name(number: int) -> str:
    try:
        return signal.Signals(number).name
    except ValueError:
        return f"signal {number}"


def job_status(meta: dict, slurm: dict, now: float) -> tuple[str, float | None, int | None, str | None]:
    """(status, ended, exit_code, reason) from the exit file, Slurm, or the process / heartbeat."""
    job_id = meta["id"]
    stopped = _file(job_id, ".stopped").exists()
    exit_info = read_json(_file(job_id, ".exit.json"))
    if exit_info:
        code, ended = exit_info.get("exit_code"), exit_info.get("ended")
        if exit_info.get("skipped"):
            return "cancelled", ended, None, exit_info["skipped"]
        if code == 0:
            return "done", ended, code, None
        if stopped:
            return "stopped", ended, code, None
        if code is not None and code < 0:
            return "error", ended, code, f"killed by {_signal_name(-code)}"
        return "error", ended, code, f"exit code {code}"
    kind, pid = meta.get("kind"), meta.get("pid")
    if kind == "slurm":
        info = slurm.get(str(meta.get("slurm_id")))
        if not info:
            return "unknown", None, None, None
        return info["status"], info["ended"], info["exit_code"], info.get("reason")
    if kind == "process":
        heartbeat = _file(job_id, ".hb")
        if meta.get("host") == HOST:
            supervisor = bool(meta.get("supervisor_pid")) and pid_alive(meta["supervisor_pid"], meta.get("supervisor_start"))
            alive = pid_alive(pid, meta.get("proc_start")) if pid is not None else supervisor
        else:  # the supervisor touches the heartbeat until it has written the exit file
            beat = _mtime(heartbeat)
            supervisor = alive = beat is not None and now - beat < STALE_S
        if pid is None:
            if meta.get("waiting") and alive:
                return "waiting", None, None, None
            if now - (meta.get("started") or 0) < STALE_S:
                return "starting", None, None, None
        elif alive:
            return "running", None, None, None
        elif stopped:
            return "stopped", _mtime(heartbeat), None, None
        elif supervisor:
            # The command has exited, but its supervisor is still alive: it scans the rest of the log and then
            # writes the exit file. Not lost; dependents must keep waiting for that exit code.
            return "running", None, None, None
        if _file(job_id, ".exit.json").exists():
            # The supervisor wrote the exit file and exited between our first look at it and the checks above.
            return job_status(meta, slurm, now)
        if pid is None:
            return "lost", None, None, "the supervisor vanished before the command started"
        return "lost", _mtime(heartbeat), None, "the supervisor vanished without recording an exit code"
    if meta.get("host") != HOST:
        return "unknown", None, None, None
    if pid_alive(pid, meta.get("proc_start")):
        return "running", None, None, None
    log = meta.get("log")
    ended = (_mtime(Path(log)) if log else None) or now
    return ("stopped" if stopped else "ended"), ended, None, None


def _match(match: re.Match, total: int | None) -> tuple[int, int | None]:
    groups = match.groupdict()
    if "done" in groups:
        done, found = groups["done"], groups.get("total")
    else:
        done, found = match.group(1), match.group(2) if match.re.groups >= 2 else None
    return int(done), int(found) if found else total


def _new_stage(name: str | None, started: float | None, index=None, of=None) -> dict:
    """history: [time, done] whenever done changed (time known), trimmed to the rate window; see _set_progress."""
    return {"name": name, "index": index, "of": of, "started": started, "ended": None, "done": None, "total": None,
            "history": [[started, 0]] if started is not None else [], "progress_at": None}


def _set_progress(stage: dict, done: int, total: int | None, when: float | None) -> None:
    """Record a progress value; when=None means its time is unknown (catching up on an old log)."""
    previous = stage["done"]
    stage["done"], stage["total"] = done, total
    if done == previous:
        return
    history = stage.setdefault("history", [])
    if previous is not None and done < previous:
        history.clear()  # the counter restarted
    if when is None:
        return
    stage["progress_at"] = when
    if previous is None and history == [[stage["started"], 0]] and when - (stage["started"] or when) < 10 and done > 1:
        history.clear()  # reported right at the start (e.g. a resumed run): a baseline, not steps done in no time
    if history and history[-1][0] == when:
        history[-1][1] = done
    else:
        history.append([when, done])
    while len(history) > 1 and history[1][1] <= done - RATE_STEPS:
        history.pop(0)  # keep one anchor at or below done - RATE_STEPS


def stage_rate(stage: dict, now: float, job_started: float) -> float | None:
    """Steps per second over the last RATE_STEPS steps, counted up to now (a stall lowers the rate). Falls back
    to the stage's (or job's) average when no step times are known, e.g. for logs written before the first scan."""
    done = stage["done"]
    if not done:
        return None
    history = stage.get("history") or []
    anchor = next((h for h in reversed(history) if h[1] <= done - RATE_STEPS), history[0] if history else None)
    t0, d0 = anchor if anchor else (stage["started"] or job_started, 0)
    return (done - d0) / (now - t0) if done > d0 and now > t0 else None


def step_seconds(stage: dict) -> float | None:
    """Average seconds per completed step over the last RATE_STEPS steps (only steps with known times); unlike
    stage_rate it ends at the last step, so it is the yardstick for how long the current step is taking."""
    history = stage.get("history") or []
    if len(history) < 2:
        return None
    (t0, d0), (t1, d1) = history[0], history[-1]
    return (t1 - t0) / (d1 - d0) if d1 > d0 and t1 > t0 else None


def longest_gap(stage: dict) -> float:
    """The longest time between two consecutive progress updates in the rate window (0 when unknown)."""
    history = stage.get("history") or []
    return max((b[0] - a[0] for a, b in zip(history, history[1:])), default=0.0)


def is_stalled(stage: dict, idle_s: float | None, step_s: float | None) -> bool:
    """Whether the time since the last step exceeds stall_factor average steps, stall_gap_factor times the longest
    recent gap and stall_min_s. The average alone flags jobs whose steps arrive in bursts: many steps at once, then
    a pause several times the mean step."""
    if not idle_s or not step_s:
        return False
    return idle_s > max(setting("stall_factor") * step_s, setting("stall_gap_factor") * longest_gap(stage),
                        setting("stall_min_s"))


def scan_log(meta: dict, scan: dict, now: float, final: bool = False) -> None:
    """Advance over the log's new lines: stage markers, count-mode matches and progress lines (newest wins).

    scan = {"offset": logical bytes read, "stages": [...]}; progress always belongs to the current (last) stage.
    Lines end at \\n or \\r, so tqdm updates count before the bar finishes. The unfinished last line still
    updates progress (without being consumed); with final, it is consumed like any other line.
    """
    first = (meta.get("stages") or [None])[0]  # a planned first stage names the time before any marker
    stages = scan.setdefault("stages", [])
    if not stages:
        stages.append(_new_stage(first, meta.get("started") or now))
    path = meta.get("progress_file") or meta.get("log")
    if not path:
        return
    try:
        handle = open(path, "rb")
    except OSError:
        return
    count = re.compile(meta["count_regex"]) if meta.get("count_regex") else None
    patterns = [re.compile(meta["progress_regex"])] if meta.get("progress_regex") else PROGRESS_PATTERNS
    default_total = meta.get("total")
    rules = [(name, re.compile(rule)) for name, rule in meta.get("stage_rules") or []]
    item_rule = re.compile(meta["item_regex"]) if meta.get("item_regex") else None
    # Catching up on a log that was written long before this first scan (`jobrun edit`, a tracked job): the times
    # of the stage changes in it are unknown, so they are recorded as None rather than as now.
    backfill = not scan.get("offset") and now - (meta.get("started") or now) > 60
    when = None if backfill else now

    def enter(name: str, index: int | None = None, of: int | None = None) -> None:
        stage, new = stages[-1], _new_stage(name, when, index, of)
        if stage["done"] is None and stage["name"] in (None, name):
            new["started"] = stage["started"] if backfill else now  # nothing happened in the placeholder stage
            stages[-1] = new
        else:
            stage["ended"] = when
            stages.append(new)

    def handle_line(line: str) -> None:
        item = ITEM_MARKER.search(ANSI.sub("", line)) if "JOBRUN_ITEM" in line else None
        if item or (item_rule and item_rule.search(line)):  # one item of a pool's list, counted per worker
            items = scan.get("items") or _new_stage("items", meta.get("started") or now)
            _set_progress(items, (items["done"] or 0) + 1, None, when)
            items["label"] = (item["label"] or None) if item else ANSI.sub("", line).strip()[:200]
            scan["items"] = items
            if item:
                return
        if "JOBRUN_EVENT" in line:
            marker = EVENT_MARKER.search(ANSI.sub("", line))
            if marker:
                level = "warn" if marker["level"].lower().startswith("warn") else "error"
                scan["event_count"] = scan.get("event_count", 0) + 1
                events = scan.setdefault("events", [])
                events.append({"n": scan["event_count"], "level": level, "message": marker["message"][:300], "at": when})
                del events[:-EVENTS_KEPT]
                scan["degraded"] = max(level, scan.get("degraded") or level, key=EVENT_LEVELS.index)
                return
        if "JOBRUN_BUDGET" in line:
            marker = BUDGET_MARKER.search(ANSI.sub("", line))
            if marker:
                scan["budget"] = parse_budget(marker.group(1))  # replaces the previous one: e.g. per stage
                return
        if "JOBRUN_STAGE" in line:
            marker = STAGE_MARKER.search(ANSI.sub("", line))
            if marker:
                index, of = marker.group("index"), marker.group("of")
                enter(marker.group("name"), int(index) if index else None, int(of) if of else None)
                return
        for name, rule in rules:  # --stage-at: stage boundaries of programs that print no markers
            if all(s["name"] != name for s in stages) and rule.search(line):
                enter(name)
        stage = stages[-1]
        if "JOBRUN_PROGRESS" in line:  # explicit progress wins in every mode
            explicit = PROGRESS_PATTERNS[0].search(line)
            if explicit:
                _set_progress(stage, int(explicit["done"]), int(explicit["total"]), when)
                return
        if count:
            if count.search(line):
                _set_progress(stage, (stage["done"] or 0) + 1, default_total, when)
        elif patterns is not PROGRESS_PATTERNS or "/" in line:  # every default pattern contains a slash
            match = next((m for m in (p.search(line) for p in patterns) if m), None)
            if match:
                _set_progress(stage, *_match(match, default_total), when)

    with handle:
        size = os.fstat(handle.fileno()).st_size
        dropped = dropped_bytes(meta["id"]) if meta.get("log_owned") and path == meta.get("log") else 0
        position = max(0, scan.get("offset", 0) - dropped)
        if position > size:  # replaced by a shorter file: start over
            position, stages[:] = 0, [_new_stage(first, meta.get("started") or now)]
        handle.seek(position)
        budget = COUNT_BUDGET_BYTES
        while budget > 0:
            chunk = handle.read(min(COUNT_CHUNK_BYTES, budget))
            complete = max(chunk.rfind(b"\n"), chunk.rfind(b"\r")) + 1
            if final and len(chunk) < COUNT_CHUNK_BYTES:
                complete = len(chunk)
            if not complete:
                if len(chunk) < COUNT_CHUNK_BYTES:
                    break  # partial last line, consumed once it is complete
                complete = len(chunk)  # one enormous line: skip it
            for line in re.split(r"[\r\n]+", chunk[:complete].decode(errors="replace")):
                if line:
                    handle_line(line)
            position += complete
            budget -= complete
            handle.seek(position)
            if complete < len(chunk) and not count and len(chunk) - complete <= LAST_LINE_BYTES:
                partial = ANSI.sub("", chunk[complete:].decode(errors="replace"))
                match = "JOBRUN_STAGE" not in partial and next((m for m in (p.search(partial) for p in patterns) if m), None)
                if match:
                    _set_progress(stages[-1], *_match(match, default_total), when)
        scan["offset"] = position + dropped
    if stages[-1]["total"] is None and default_total:
        stages[-1]["total"] = default_total


def scan_path(meta: dict) -> Path:
    """Each `jobrun edit` of the progress settings starts a fresh scan file (scan_rev)."""
    rev = meta.get("scan_rev") or 0
    return _file(meta["id"], f".scan.r{rev}.json" if rev else ".scan.json")


def update_scan(meta: dict, final: bool = False) -> dict:
    """Scan new log lines and persist the result; returns the scan."""
    path = scan_path(meta)
    scan = read_json(path, {}) or {}
    before = json.dumps(scan, sort_keys=True)
    scan_log(meta, scan, time.time(), final)
    if json.dumps(scan, sort_keys=True) != before:  # most wake-ups see no new output: skip the NFS write
        try:
            write_json(path, scan)
        except OSError:
            pass
    return scan


def last_log_line(meta: dict) -> str | None:
    path = meta.get("progress_file") or meta.get("log")
    try:
        with open(path, "rb") as handle:
            handle.seek(max(0, os.fstat(handle.fileno()).st_size - LAST_LINE_BYTES))
            tail = handle.read().decode(errors="replace")
    except (OSError, TypeError):
        return None
    lines = [line for line in (ANSI.sub("", raw).strip() for raw in re.split(r"[\r\n]+", tail)) if line]
    return lines[-1][:300] if lines else None


def stage_rows(meta: dict, stages: list[dict], status: str, ended: float | None) -> list[dict]:
    """Stages for display: finished, current, and planned (--stages) ones not reached yet."""
    if len(stages) == 1 and (stages[0]["of"] or 1) <= 1 and len(meta.get("stages") or []) <= 1:
        return []  # a single stage with nothing announced after it carries no information ("1/?")
    rows = []
    for stage in stages:
        last = stage is stages[-1]
        state = "done" if not last else "running" if status in ACTIVE else status
        rows.append({"name": stage["name"] or "(start)", "status": state, "done": stage["done"],
                     "total": stage["total"], "started": stage["started"],
                     "ended": stage["ended"] if not last else None if status in ACTIVE else ended})
    reached = {s["name"] for s in stages}
    rows += [{"name": name, "status": "pending", "done": None, "total": None, "started": None, "ended": None}
             for name in meta.get("stages") or [] if name not in reached]
    return rows


def snapshot(meta: dict, slurm: dict) -> dict:
    """One display row per job; finished jobs are computed once and cached."""
    job_id = meta["id"]
    state_path = _file(job_id, ".state.json")
    state = read_json(state_path, {}) or {}
    if state.get("final"):
        row = state["final"]
        row["status"] = LEGACY_STATUS.get(row["status"], row["status"])
        if len(row.get("stages") or []) == 1 and (row.get("stage_of") or 1) <= 1:  # cached before lone stages were hidden
            row.update(stage=None, stage_no=None, stage_of=None, stages=[])
        return row
    now = time.time()
    status, ended, exit_code, reason = job_status(meta, slurm, now)
    slurm_info = (slurm.get(str(meta.get("slurm_id"))) or {}) if meta.get("kind") == "slurm" else {}
    started = slurm_info.get("started") or meta.get("started") or now
    # A running supervisor keeps the scan current, unless it predates `jobrun edit` and the settings were edited.
    if (meta.get("supervisor_scans") and status in ACTIVE
            and (not meta.get("scan_rev") or meta.get("supervisor_version", 1) >= SUPERVISOR_VERSION)):
        scan = read_json(scan_path(meta), {}) or {}
        scan.setdefault("stages", [_new_stage((meta.get("stages") or [None])[0], started)])
    else:
        scan = update_scan(meta)
    stages = scan["stages"]
    current = stages[-1]
    done, total = current["done"], current["total"]
    rate = state.get("rate")
    if done is not None and status in ACTIVE:
        rate = state["rate"] = stage_rate(current, now, started)
    eta = None
    if status in ACTIVE and rate and total and done is not None and done < total:
        eta = now + (total - done) / rate
    step_s = step_seconds(current) if status in ACTIVE else None
    # time since the last step, or since the stage began when it has no step yet
    since = current.get("progress_at") or (current.get("started") if done is None and len(stages) > 1 else None)
    # the supervisor may record a step after `now` was taken: never a negative time since it
    idle_s = max(0.0, now - since) if status == "running" and since else None
    planned = meta.get("stages") or []
    stage_list = stage_rows(meta, stages, status, ended)
    row = {key: meta.get(key) for key in ("id", "name", "group", "kind", "host", "slurm_id", "pid", "cwd", "log")}
    # declared budget: the newest JOBRUN_BUDGET line, with --cpus/--mem/--gpus (run, edit) taking precedence per key
    row["budget"] = {**(scan.get("budget") or {}), **(meta.get("budget") or {})} or None
    row.update(
        log_owned=bool(meta.get("log_owned")), after=meta.get("after") or [],
        cmd=shlex.join(meta.get("cmd") or []), status=status, exit_code=exit_code, started=started, ended=ended,
        elapsed_s=None if status == "waiting" or (meta.get("after") and meta.get("pid") is None and
                                                   meta.get("kind") == "process") else (ended or now) - started,
        done=done, total=total, fraction=done / total if done is not None and total else None,
        rate_per_s=rate, eta=eta, last_line=last_log_line(meta), progress_at=current.get("progress_at"),
        step_s=step_s, idle_s=idle_s,
        # at or past the total but still running: an untracked final phase (or a too small --total)
        finishing=status in ACTIVE and bool(total) and done is not None and done >= total,
        stage=current["name"] if stage_list else None,
        stage_no=(current["index"] or (planned.index(current["name"]) + 1 if current["name"] in planned
                                       else len(stages))) if stage_list else None,
        stage_of=(current["of"] or len(planned) or None) if stage_list else None,
        stages=stage_list, reason=reason,
    )
    # a finishing job has no steps left to time its silence against
    row["stalled"] = not row["finishing"] and is_stalled(current, idle_s, step_s)
    # problems the job reported itself (JOBRUN_EVENT lines); degraded is the most severe level seen
    events = scan.get("events") or []
    row.update(events=events[-5:], event_count=scan.get("event_count", 0), degraded=scan.get("degraded"))
    # a pool's worker: the items it finished itself (JOBRUN_ITEM, --item-regex), their rate and the last one
    items = scan.get("items")
    item_idle = now - items["progress_at"] if items and items.get("progress_at") and status == "running" else None
    row.update(pool=meta.get("pool"), role=meta.get("role"), items=items["done"] if items else None,
               item_rate=stage_rate(items, now, started) if items and status in ACTIVE else None,
               item_label=items.get("label") if items else None, item_at=items.get("progress_at") if items else None,
               item_idle_s=item_idle,
               item_stalled=bool(items) and status == "running" and is_stalled(items, item_idle, step_seconds(items)))
    row["host"] = slurm_info.get("host") or row["host"]
    try:  # the log's size, and what jobrun cut out of a log it writes (see trim_log)
        row["log_bytes"] = os.path.getsize(meta["log"]) if meta.get("log") else None
    except OSError:
        row["log_bytes"] = None
    row["log_dropped"] = dropped_bytes(job_id) if meta.get("log_owned") else 0
    if status in FINAL:
        state["final"] = row
    try:
        write_json(state_path, state)
    except OSError:
        pass
    return row


def snapshots(metas: list[dict]) -> list[dict]:
    """Rows in the order of metas. Pools are computed from their workers' rows, loaded as needed."""
    order = [m["id"] for m in metas]
    pools = [m for m in metas if m.get("kind") == "pool"]
    metas = [m for m in metas if m.get("kind") != "pool"]
    open_slurm = [m for m in metas
                  if m.get("kind") == "slurm" and not (read_json(_file(m["id"], ".state.json"), {}) or {}).get("final")]
    for meta in open_slurm:
        if not meta.get("log"):  # sacct had no record yet at registration
            fresh = slurm_meta(str(meta["slurm_id"]), attempts=1, log_pattern=meta.get("log_pattern"))
            if fresh.get("log"):
                meta.update({k: v for k, v in fresh.items() if not meta.get(k) or k == "name" and
                             meta["name"] == f"slurm-{meta['slurm_id']}"})
                write_json(_file(meta["id"], ".json"), meta)
    slurm = query_slurm([str(m["slurm_id"]) for m in open_slurm])
    rows = [snapshot(m, slurm) for m in metas]
    by_id = {r["id"]: r for r in rows}
    now = time.time()
    for row in rows:
        row.setdefault("after", [])  # final rows cached by older versions lack these
        row.setdefault("events", [])
        row.setdefault("event_count", 0)
        row.setdefault("degraded", None)
        for key in ("pool", "role", "items", "item_rate", "item_label", "item_at", "item_idle_s"):
            row.setdefault(key, None)
        row.setdefault("item_stalled", False)
        deps = [d for d in (by_id.get(dep) or snapshot_or_none(dep, slurm) for dep in row["after"]) if d]
        row["after_names"] = [d["name"] for d in deps]
        if row["status"] == "running" and any(d["status"] in ACTIVE for d in deps):
            # registered with --after but started early: the command waits for its dependency by itself
            row.update(status="waiting", elapsed_s=None, eta=None, rate_per_s=None, idle_s=None, stalled=False)
        if row["status"] == "waiting":
            row["waiting_for"] = [d["name"] + ("" if d["status"] in ACTIVE else f" ({d['status']})")
                                  for d in deps if d["status"] != "done"] or (
                ["Slurm queue"] if row["kind"] == "slurm" else [])
        elif deps and row["status"] in ("stopped", "error", "lost") and any(
                d["status"] in ACTIVE or (d["ended"] or now) > (row["ended"] or now) for d in deps):
            # ended before its dependencies did: it was still waiting (inside its own script), never started
            row.update(status="cancelled", elapsed_s=None, reason=f"{row['status']} while waiting")
        elif deps and all(d["status"] in ("done", "ended") for d in deps) and row["status"] != "cancelled":
            start = max(d["ended"] or 0 for d in deps)  # time spent waiting is not part of the job's runtime
            if start > (row["started"] or 0):
                row["started"], row["elapsed_s"] = start, (row["ended"] or now) - start
    if pools:
        ids = {p["id"] for p in pools}
        extra = [m for m in load_jobs() if m.get("pool") in ids and m["id"] not in by_id and m.get("kind") != "pool"]
        workers = rows + (snapshots(extra) if extra else [])
        for pool in pools:
            by_id[pool["id"]] = pool_row(pool, [r for r in workers if r.get("pool") == pool["id"]], time.time(),
                                         _file(pool["id"], ".stopped").exists())
    return [by_id[job_id] for job_id in order]


def snapshot_or_none(job_id: str, slurm: dict) -> dict | None:
    meta = read_json(_file(job_id, ".json"))
    if not meta:
        return None
    return snapshots([meta])[0] if meta.get("kind") == "pool" else snapshot(meta, slurm)


POOL_EMPTY_S = 600  # a pool no job joined (e.g. its workers failed to launch) is cancelled after this long


def pool_row(meta: dict, members: list[dict], now: float, stopped: bool = False) -> dict:
    """A pool's row from its members' rows (and stopped: `jobrun stop` was called on the pool).

    Progress is the newest reading of the whole list that any member reports (a progress bar, counter or
    JOBRUN_PROGRESS line); without one, the sum of the workers' items (JOBRUN_ITEM) out of the pool's total.
    Members with role "monitor" only report progress; all others are workers. The pool runs while any worker runs;
    a worker that fails is a warning, the pool fails only when no worker succeeded or items are left after a
    failure. A pool without members waits for its first worker, at most POOL_EMPTY_S; a stopped one is stopped.
    All workers succeeding with items left is a warning too: the count may be stale, or items were skipped.
    Sets each worker's share of the items on its row.
    """
    workers = [r for r in members if r.get("role") != "monitor"]
    active = [r for r in workers if r["status"] in ACTIVE]
    readings = [r for r in members if r.get("done") is not None and r.get("total")]
    source = max(readings, key=lambda r: (r.get("progress_at") or 0, r["done"])) if readings else None
    counted = [r for r in workers if r.get("items") is not None]
    items = sum(r["items"] for r in counted) if counted else None
    for r in workers:
        r["share"] = r["items"] / items if items and r.get("items") is not None else None
    done, total = (source["done"], source["total"]) if source else (items, meta.get("total"))
    live = source if source and source["status"] in ACTIVE else None
    item_rate = sum(r.get("item_rate") or 0 for r in active) or None
    rate = ((live or {}).get("rate_per_s") or item_rate) if active else None
    open_items = bool(total) and done is not None and done < total
    starts = [r["started"] for r in members if r.get("started")]
    started = min(starts) if starts else meta.get("started") or now
    ended = max(r.get("ended") or r.get("started") or now for r in workers) if workers and not active else None
    events = [{**e, "message": f"{r['name']}: {e['message']}"} for r in members for e in r.get("events") or []]
    if not stopped:
        for r in members:
            if r["status"] in ("error", "lost"):
                cause = f"{r['status']}: {r['reason']}" if r.get("reason") else r["status"]
                what = "monitor" if r.get("role") == "monitor" else "worker"
                events.append({"level": "warn", "at": r.get("ended"),
                               "message": f"{what} {r['name']} on {r.get('host') or '?'} ended ({cause})"})
    reason, incomplete = None, False
    if active:
        status = next(s for s in ("running", "starting", "waiting", "unknown") if any(r["status"] == s for r in active))
    elif stopped and (not workers or any(r["status"] == "stopped" for r in workers)):
        status = "stopped"
        if not workers:
            reason = "stopped before any worker joined"
    elif not members and now - (meta.get("started") or now) > POOL_EMPTY_S:
        status, reason = "cancelled", f"no workers joined within {fmt_duration(POOL_EMPTY_S)}"
    elif not workers:
        status = "waiting"
    else:
        succeeded = [r for r in workers if r["status"] in ("done", "ended")]
        if not succeeded:
            status = next(s for s in ("error", "lost", "stopped", "cancelled") if any(r["status"] == s for r in workers))
        elif open_items and len(succeeded) < len(workers):
            status, reason = "error", f"no workers left; {total - done} of {total} items open"
        else:
            status = "done" if any(r["status"] == "done" for r in succeeded) else "ended"
            if open_items:
                incomplete, reason = True, f"all workers ended at {done}/{total}"
                events.append({"level": "warn", "at": ended, "message":
                               f"all workers ended, but progress is {done}/{total}: items may be unprocessed, or the "
                               f"count is stale; check the list"})
    events.sort(key=lambda e: e.get("at") or 0)
    hidden = sum(r.get("event_count") or 0 for r in members) - sum(len(r.get("events") or []) for r in members)
    events = [{**e, "n": hidden + i + 1} for i, e in enumerate(events)]
    levels = [e["level"] for e in events] + [r["degraded"] for r in members if r.get("degraded")]
    if live:
        stalled, idle_s, step_s = bool(live.get("stalled")), live.get("idle_s"), live.get("step_s")
        idle_s = max(0.0, idle_s) if idle_s is not None else None
    else:
        counting = [r for r in active if r.get("items")]
        idles = [r["item_idle_s"] for r in counting if r.get("item_idle_s") is not None]
        stalled = bool(counting) and all(r.get("item_stalled") for r in counting)
        idle_s, step_s = (min(idles) if idles else None), None
    finishing = bool(active) and bool(total) and done is not None and done >= total
    newest = max((r for r in workers if r.get("item_at")), key=lambda r: r["item_at"], default=None)
    last_line = (f"{newest['name']}: {newest['item_label']}" if newest and newest.get("item_label")
                 else (source or {}).get("last_line"))
    waiting_for = [w for r in active if r["status"] == "waiting" for w in r.get("waiting_for") or []]
    return {
        "id": meta["id"], "name": meta.get("name"), "group": meta.get("group"), "kind": "pool", "host": None,
        "slurm_id": None, "pid": None, "cwd": meta.get("cwd"), "log": None, "budget": None, "log_owned": False,
        "after": [], "after_names": [], "cmd": "", "status": status, "exit_code": None, "started": started,
        "ended": ended, "elapsed_s": None if status == "waiting" else (ended or now) - started,
        "done": done, "total": total, "fraction": done / total if done is not None and total else None,
        "rate_per_s": rate, "eta": now + (total - done) / rate if rate and open_items and active else None,
        "last_line": last_line,
        "progress_at": max([r.get("progress_at") or 0 for r in members] + [r.get("item_at") or 0 for r in members],
                           default=0) or None,
        "step_s": step_s, "idle_s": idle_s if status == "running" else None, "finishing": finishing,
        "stage": None, "stage_no": None, "stage_of": None, "stages": [], "reason": reason,
        "stalled": stalled and status == "running" and not finishing,
        "events": events[-5:], "event_count": hidden + len(events),
        "degraded": max(levels, key=EVENT_LEVELS.index) if levels else None,
        "log_bytes": None, "log_dropped": 0, "pool": None, "role": None, "items": items,
        "item_rate": item_rate if active else None, "workers": [r["id"] for r in workers],
        "monitors": [r["id"] for r in members if r.get("role") == "monitor"], "active_workers": len(active),
        "hosts": sorted({r["host"] for r in (active or workers) if r.get("host")}), "incomplete": incomplete,
        **({"waiting_for": waiting_for or ["workers"]} if status == "waiting" else {}),
    }


def group_rows(rows: list[dict]) -> list[dict]:
    """One summary per group: progress summed over members, ETA of the slowest active member."""
    members: dict[str, list[dict]] = {}
    for row in rows:
        if row.get("group"):
            members.setdefault(row["group"], []).append(row)
    groups = []
    for name, jobs in members.items():
        active = [r for r in jobs if r["status"] in ACTIVE]
        bad = [r for r in jobs if r["status"] not in ACTIVE and r["status"] != "done"]
        if active:
            status = next(s for s in ("running", "starting", "waiting", "unknown") if any(r["status"] == s for r in active))
        else:  # the most severe outcome
            status = next((s for s in ("error", "lost", "stopped", "cancelled", "ended") if any(r["status"] == s for r in jobs)),
                          "done")
        # summing only makes sense when every member counts the same kind of thing towards a known total
        total = sum(r["total"] for r in jobs) if all(r["total"] for r in jobs) else None
        done = sum(min(r["done"] or 0, r["total"]) for r in jobs) if total else None
        etas = [r["eta"] for r in active if r["eta"]]
        started = min(r["started"] for r in jobs)
        ended = None if active else max(r["ended"] or r["started"] for r in jobs)
        groups.append({
            "name": name, "status": status, "jobs": len(jobs), "finished": len(jobs) - len(active),
            "failed": len(bad), "ids": [r["id"] for r in jobs], "done": done, "total": total,
            "counts": {st: sum(r["status"] == st for r in jobs) for st in dict.fromkeys(r["status"] for r in jobs)},
            "fraction": done / total if done is not None and total else None,
            "rate_per_s": sum(r["rate_per_s"] or 0 for r in active) or None,
            "eta": max(etas) if etas else None, "started": started, "ended": ended,
            "finishing": bool(active) and all(r.get("finishing") for r in active),
            "stalled": any(r.get("stalled") for r in active),
            "degraded": max((r["degraded"] for r in jobs if r.get("degraded")), key=EVENT_LEVELS.index, default=None),
            "elapsed_s": (ended or time.time()) - started,
        })
    return groups


# ---------------------------------------------------------------- resources
# `jobrun res`: CPU, RAM, GPU and VRAM of every node that runs an active job (and this one), what Slurm has reserved
# there, and which job uses what. Nothing samples in the background: each call measures the means since the previous
# call from cumulative counters (/proc CPU ticks, NVML's utilization samples since a timestamp, Slurm's CPU time),
# so the cost is one pass over /proc, one NVML query and two short Slurm calls per refresh. RAM and VRAM are current
# values. Nodes other than this one are seen through Slurm only: reservations, a coarse load and free memory as
# reported by slurmd, and the usage of your own Slurm jobs (sstat); their GPU utilization is not available.

def _size(value: str | None, unit: str = "") -> float | None:
    """Bytes from Slurm sizes like 85356K, 48G, 249856 (with unit as the default suffix)."""
    match = re.fullmatch(r"([\d.]+)([KMGTP]?)", (value or "").strip())
    if not match:
        return None
    return float(match.group(1)) * 1024 ** "_KMGTP".index(match.group(2) or unit or "_")


def _slurm_seconds(value: str) -> float:
    """CPU time like 1-02:03:04, 02:03:04, 03:04.123 -> seconds."""
    days, _, rest = value.rpartition("-")
    seconds = 0.0
    for part in rest.split(":"):
        seconds = seconds * 60 + float(part or 0)
    return seconds + (int(days) * 86400 if days else 0)


def _tres(text: str) -> dict[str, str]:
    return dict(item.split("=", 1) for item in text.split(",") if "=" in item)


def expand_hosts(nodelist: str | None) -> list[str]:
    """Slurm hostlists like "node[01-03,07],gpu1" -> names."""
    hosts = []
    for prefix, ranges in re.findall(r"([^,\[]+)(?:\[([^\]]*)\])?", nodelist or ""):
        if not ranges:
            hosts.append(prefix)
            continue
        for part in ranges.split(","):
            start, _, end = part.partition("-")
            for n in range(int(start), int(end or start) + 1):
                hosts.append(f"{prefix}{n:0{len(start)}d}")
    return hosts


def _gpu_count(gres: str) -> int:
    return sum(int(n) for n in re.findall(r"(?:^|,)gpu(?::[^:,(]+)?:(\d+)", gres or ""))


class Nvml:
    """The few NVML calls needed, through ctypes (no pynvml). Raises OSError when there is no NVIDIA driver."""

    class _Memory(ctypes.Structure):
        _fields_ = [("total", ctypes.c_ulonglong), ("free", ctypes.c_ulonglong), ("used", ctypes.c_ulonglong)]

    class _Sample(ctypes.Structure):  # nvmlSample_t; the value union is 8 bytes, GPU utilization is an unsigned int
        _fields_ = [("time", ctypes.c_ulonglong), ("value", ctypes.c_ulonglong)]

    class _Process(ctypes.Structure):  # nvmlProcessInfo_t (v2/v3)
        _fields_ = [("pid", ctypes.c_uint), ("memory", ctypes.c_ulonglong), ("gi", ctypes.c_uint), ("ci", ctypes.c_uint)]

    class _ProcessUtil(ctypes.Structure):  # nvmlProcessUtilizationSample_t
        _fields_ = [("pid", ctypes.c_uint), ("time", ctypes.c_ulonglong), ("sm", ctypes.c_uint), ("mem", ctypes.c_uint),
                    ("enc", ctypes.c_uint), ("dec", ctypes.c_uint)]

    def __init__(self):
        self.lib = ctypes.CDLL("libnvidia-ml.so.1")
        if self.lib.nvmlInit_v2():
            raise OSError("nvmlInit failed")

    def close(self):
        self.lib.nvmlShutdown()

    def devices(self, since_us: int) -> list[dict]:
        """Per GPU: mean utilization of the samples after since_us (epoch µs), memory, and per process the used
        memory and mean SM utilization."""
        lib, count = self.lib, ctypes.c_uint()
        lib.nvmlDeviceGetCount_v2(ctypes.byref(count))
        gpus = []
        for index in range(count.value):
            handle, name, memory = ctypes.c_void_p(), ctypes.create_string_buffer(96), self._Memory()
            if lib.nvmlDeviceGetHandleByIndex_v2(index, ctypes.byref(handle)):
                continue
            lib.nvmlDeviceGetName(handle, name, 96)
            lib.nvmlDeviceGetMemoryInfo(handle, ctypes.byref(memory))
            last = since_us
            kind, n = ctypes.c_int(), ctypes.c_uint(0)
            samples = []
            if lib.nvmlDeviceGetSamples(handle, 1, ctypes.c_ulonglong(last), ctypes.byref(kind), ctypes.byref(n), None) == 0 and n.value:
                buffer = (self._Sample * n.value)()
                if lib.nvmlDeviceGetSamples(handle, 1, ctypes.c_ulonglong(last), ctypes.byref(kind), ctypes.byref(n), buffer) == 0:
                    samples = [b.value & 0xFFFFFFFF for b in buffer[:n.value] if b.time > last]
            processes = {}
            n = ctypes.c_uint(0)
            if lib.nvmlDeviceGetComputeRunningProcesses_v3(handle, ctypes.byref(n), None) in (0, 7) and n.value:
                buffer = (self._Process * (n.value + 4))()
                n = ctypes.c_uint(n.value + 4)
                if lib.nvmlDeviceGetComputeRunningProcesses_v3(handle, ctypes.byref(n), buffer) == 0:
                    for proc in buffer[:n.value]:
                        processes[proc.pid] = {"vram": proc.memory if proc.memory < 1 << 62 else None, "util": None}
            n = ctypes.c_uint(0)
            if lib.nvmlDeviceGetProcessUtilization(handle, None, ctypes.byref(n), ctypes.c_ulonglong(last)) in (0, 7) and n.value:
                buffer = (self._ProcessUtil * n.value)()
                if lib.nvmlDeviceGetProcessUtilization(handle, buffer, ctypes.byref(n), ctypes.c_ulonglong(last)) == 0:
                    per_pid: dict[int, list[int]] = {}
                    for sample in buffer[:n.value]:
                        if sample.pid and sample.time > last:
                            per_pid.setdefault(sample.pid, []).append(sample.sm)
                    for pid, values in per_pid.items():
                        processes.setdefault(pid, {"vram": None})["util"] = sum(values) / len(values)
            gpus.append({"index": index, "name": name.value.decode(errors="replace"),
                         "util": sum(samples) / len(samples) if samples else 0.0,
                         "vram": memory.used, "vram_total": memory.total, "processes": processes})
        return gpus


def _proc_table() -> dict[int, tuple[int, int, int]]:
    """pid -> (ppid, cumulative CPU ticks incl. reaped children, RSS bytes), from one pass over /proc."""
    page = os.sysconf("SC_PAGE_SIZE")
    table = {}
    for entry in os.scandir("/proc"):
        if entry.name.isdigit():
            fields = _proc_stat(entry.name)
            if fields and len(fields) > 21:
                ticks = sum(int(v) for v in fields[11:15])  # utime stime cutime cstime
                table[int(entry.name)] = (int(fields[1]), ticks, int(fields[21]) * page)
    return table


def cgroup_limits(pid: int) -> dict:
    """RAM and CPU limits of the job's own cgroup (e.g. `systemd-run --scope -p MemoryMax=96G`, or a Slurm job), found
    by walking up from the process's cgroup v2 path until a limit or a shared cgroup (a slice, a service, Slurm's
    scope of all jobs) is reached, so a user-wide limit is never taken for the job's."""
    try:
        path = Path(f"/proc/{pid}/cgroup").read_text().split("::", 1)[1].strip()
    except (OSError, IndexError):
        return {}
    limits: dict[str, float] = {}
    while path not in ("", "/"):
        name = path.rsplit("/", 1)[-1]
        if name.endswith((".slice", ".service")) or name == "slurmstepd.scope":
            break
        base = Path("/sys/fs/cgroup" + path)
        try:
            memory = (base / "memory.max").read_text().strip()
            if "mem" not in limits and memory != "max":
                limits["mem"] = float(memory)
        except (OSError, ValueError):
            pass
        try:
            quota, period = (base / "cpu.max").read_text().split()
            if "cpus" not in limits and quota != "max":
                limits["cpus"] = float(quota) / float(period)
        except (OSError, ValueError):
            pass
        path = path.rsplit("/", 1)[0]
    return limits


def holds_gpu(pids: list[int]) -> bool:
    """Whether any of the processes has an NVIDIA device open (every CUDA process does). Lets `res` skip NVML, which
    wakes an idle GPU's driver (~2 s) when nothing uses it."""
    for pid in pids:
        try:
            fds = os.scandir(f"/proc/{pid}/fd")
        except OSError:
            continue
        with fds:
            for fd in fds:
                try:
                    if os.readlink(fd.path).startswith("/dev/nvidia"):
                        return True
                except OSError:
                    pass
    return False


def _local_gpu_count() -> int:
    try:
        return len(os.listdir("/proc/driver/nvidia/gpus"))
    except OSError:
        return 0


def _cpu_ticks() -> tuple[int, int]:
    """(busy, total) jiffies of the whole machine."""
    with open("/proc/stat") as f:
        values = [int(v) for v in f.readline().split()[1:9]]
    return sum(values) - values[3] - values[4], sum(values)


def _meminfo() -> dict[str, int]:
    info = {}
    with open("/proc/meminfo") as f:
        for line in f:
            key, _, rest = line.partition(":")
            if key in ("MemTotal", "MemAvailable"):
                info[key] = int(rest.split()[0]) * 1024
    return info


def _slurm_nodes(hosts: list[str]) -> dict[str, dict]:
    """Totals, Slurm's coarse load / free memory, and the reservations of all running jobs, per node."""
    if not hosts or not shutil.which("sinfo"):
        return {}
    try:
        out = subprocess.run(["sinfo", "-N", "-h", "-n", ",".join(hosts), "-O",
                              "NodeList:200,CPUsState:30,CPUsLoad:12,Memory:16,FreeMem:16,Gres:300,StateLong:30"],
                             capture_output=True, text=True, timeout=15).stdout
        jobs = subprocess.run(["squeue", "-h", "-t", "R", "-w", ",".join(hosts), "-O",
                               "JobID:30,UserName:40,NodeList:300,tres-alloc:400"],
                              capture_output=True, text=True, timeout=15).stdout
    except (OSError, subprocess.SubprocessError):
        return {}
    nodes = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 7 or parts[0] in nodes:  # a node in several partitions is listed once per partition
            continue
        name, cpus, load, memory, free, gres, state = parts[:7]
        total_cpus = int(cpus.split("/")[-1]) if cpus.count("/") == 3 else None
        nodes[name] = {"cpus": total_cpus, "load": float(load) if load[:1].isdigit() else None,
                       "mem": _size(memory, "M"), "mem_free": _size(free, "M"), "gpus": _gpu_count(gres),
                       "state": state, "reserved": {"cpus": 0, "mem": 0.0, "gpus": 0}, "slurm_jobs": []}
    user = getpass.getuser()
    for line in jobs.splitlines():
        parts = line.split()
        if len(parts) < 4:
            continue
        job_id, owner, nodelist, tres = parts[:4]
        names = expand_hosts(nodelist)
        alloc = _tres(tres)
        share = {"cpus": int(alloc.get("cpu", 0)) / len(names), "mem": (_size(alloc.get("mem"), "M") or 0) / len(names),
                 "gpus": int(alloc.get("gres/gpu", 0)) / len(names)}
        for name in names:
            if name in nodes:
                for key, value in share.items():
                    nodes[name]["reserved"][key] += value
                nodes[name]["slurm_jobs"].append({"slurm_id": job_id, "user": owner, "mine": owner == user, **share})
    return nodes


def _sstat(ids: list[str]) -> dict[str, dict]:
    """Cumulative CPU seconds and current RAM / VRAM / GPU utilization of running Slurm jobs, summed over steps."""
    if not ids or not shutil.which("sstat"):
        return {}
    try:
        out = subprocess.run(["sstat", "-a", "-n", "-P", "-j", ",".join(ids), "-o", "JobID,TRESUsageInTot"],
                             capture_output=True, text=True, timeout=15).stdout
    except (OSError, subprocess.SubprocessError):
        return {}
    usage: dict[str, dict] = {}
    for line in out.splitlines():
        step, _, tres = line.partition("|")
        values = _tres(tres.rstrip("|"))
        if not values:
            continue
        job = usage.setdefault(step.split(".")[0], {"cpu_s": 0.0, "mem": 0.0, "vram": 0.0, "gpu_util": None})
        job["cpu_s"] += _slurm_seconds(values["cpu"]) if values.get("cpu") else 0.0
        job["mem"] += _size(values.get("mem")) or 0.0
        job["vram"] += _size(values.get("gres/gpumem")) or 0.0
        if values.get("gres/gpuutil"):
            job["gpu_util"] = max(job["gpu_util"] or 0.0, float(values["gres/gpuutil"]))
    return usage


def resources(rows: list[dict]) -> dict:
    """See the section comment. rows: job rows from snapshots(); only running ones are measured."""
    path = HOME / "hosts" / f"{HOST}.json"
    previous = read_json(path, {}) or {}
    now = time.time()
    if now - previous.get("time", 0) < RES_MIN_S and previous.get("result"):
        return previous["result"]
    running = [r for r in rows if r["status"] == "running"]
    local = [r for r in running if r.get("kind") in ("process", "pid") and r.get("host") == HOST and r.get("pid")]
    slurm_rows = [r for r in running if r.get("kind") == "slurm" and r.get("slurm_id")]
    if not previous.get("time"):  # no baseline yet: measure a short first interval
        previous = {"time": now, "cpu": _cpu_ticks(), "jobs": {}}
        table = _proc_table()
        children = _children(table)
        previous["jobs"] = {r["id"]: _tree_usage(table, children, r["pid"])[0] for r in local}
        time.sleep(RES_FIRST_S)
        now = time.time()
    interval = now - previous["time"]
    hz = os.sysconf("SC_CLK_TCK")

    # this machine, from /proc and NVML
    busy, total = _cpu_ticks()
    prev_busy, prev_total = previous.get("cpu") or (busy, total)
    memory = _meminfo()
    table = _proc_table()
    children = _children(table)
    cpus = os.cpu_count() or 1
    jobs_usage, job_ticks, owner, limits = {}, {}, {}, {}
    for r in local:
        ticks, rss, pids = _tree_usage(table, children, r["pid"])
        job_ticks[r["id"]] = ticks
        before = (previous.get("jobs") or {}).get(r["id"])
        cpu = (ticks - before) / hz / interval if before is not None and ticks >= before else None
        jobs_usage[r["id"]] = {"cpu": cpu, "mem": rss, "gpu_util": None, "vram": None}
        owner.update({pid: r["id"] for pid in pids})
        limits[r["id"]] = cgroup_limits(r["pid"])
    # NVML only while one of our jobs holds the GPU: querying an idle GPU wakes its driver
    gpu_queried = holds_gpu(list(owner))
    gpus = []
    if gpu_queried:
        try:
            nvml = Nvml()
        except OSError:
            nvml = None
        if nvml:
            try:
                gpus = nvml.devices(int(previous["time"] * 1e6))
            finally:
                nvml.close()
    for gpu in gpus:
        for pid, proc in gpu.pop("processes").items():
            job = owner.get(pid)
            if job:
                usage = jobs_usage[job]
                usage["vram"] = (usage["vram"] or 0) + (proc.get("vram") or 0)
                if proc.get("util") is not None:
                    usage["gpu_util"] = (usage["gpu_util"] or 0) + proc["util"]
                usage.setdefault("gpus", []).append(gpu["index"])
    here = {"host": HOST, "local": True, "cpus": cpus,
            "cpu": (busy - prev_busy) / max(total - prev_total, 1) * cpus,
            "mem": memory.get("MemTotal", 0) - memory.get("MemAvailable", 0), "mem_total": memory.get("MemTotal"),
            "gpus": gpus, "gpu_count": len(gpus) or _local_gpu_count(), "gpu_queried": bool(gpus)}

    # Slurm: reservations on every node involved, usage of our running Slurm jobs
    hosts = [HOST] + sorted({h for r in running if r.get("host") for h in expand_hosts(r["host"])} - {HOST})
    slurm_nodes = _slurm_nodes(hosts) if shutil.which("sinfo") else {}
    stats = _sstat([str(r["slurm_id"]) for r in slurm_rows])
    slurm_cpu = {}
    for r in slurm_rows:
        stat = stats.get(str(r["slurm_id"]))
        if not stat:
            continue
        slurm_cpu[r["id"]] = stat["cpu_s"]
        before = (previous.get("slurm_cpu") or {}).get(r["id"])
        if before is not None and stat["cpu_s"] >= before:
            cpu = (stat["cpu_s"] - before) / interval
        else:  # first sight: the mean over the job's runtime so far
            cpu = stat["cpu_s"] / r["elapsed_s"] if r.get("elapsed_s") else None
        jobs_usage[r["id"]] = {"cpu": cpu, "mem": stat["mem"], "vram": stat["vram"] or None,
                               "gpu_util": stat["gpu_util"]}

    nodes = []
    for host in hosts:
        node = dict(here) if host == HOST else {"host": host, "local": False, "gpus": []}
        info = slurm_nodes.get(host)
        if info:
            node["slurm"] = {"state": info["state"], "reserved": info["reserved"], "jobs": info["slurm_jobs"]}
            if host != HOST:
                node.update(cpus=info["cpus"], cpu=info["load"], mem_total=info["mem"], gpu_count=info["gpus"],
                            mem=info["mem"] - info["mem_free"] if info["mem"] and info["mem_free"] is not None else None,
                            coarse=True)  # slurmd's load average and free memory, updated every few minutes
        reservations = {j["slurm_id"]: j for j in (info or {}).get("slurm_jobs", [])}
        members = []
        for r in running:
            on = expand_hosts(r.get("host")) if r.get("kind") == "slurm" else [r.get("host")]
            if host not in on:
                continue
            usage = dict(jobs_usage.get(r["id"]) or {})
            if len(on) > 1:  # Slurm reports a multi-node job as a whole
                usage = {k: (v / len(on) if isinstance(v, float) and k != "gpu_util" else v) for k, v in usage.items()}
            members.append({"id": r["id"], "name": r["name"], "group": r.get("group"), "kind": r.get("kind"),
                            "pool": r.get("pool"), "role": r.get("role"),
                            "measured": r["id"] in jobs_usage, **usage,
                            "reserved": job_reservation(r, reservations.get(str(r.get("slurm_id"))), limits.get(r["id"]))})
        node["jobs"] = members
        if node.get("coarse"):
            # slurmd's load average and free memory lag by minutes and can read near zero on a busy node; the node
            # uses at least what our jobs on it measurably use (sstat)
            for key, flag in (("cpu", "cpu_from_jobs"), ("mem", "mem_from_jobs")):
                measured = sum(m.get(key) or 0 for m in members)
                node[flag] = bool(measured) and (node.get(key) is None or measured > node[key])
                if node[flag]:
                    node[key] = measured
        # reserved on the node: Slurm's allocations (all users) plus the budgets of our jobs Slurm does not manage
        slurm_reserved = (node.get("slurm") or {}).get("reserved") or {"cpus": 0, "mem": 0, "gpus": 0}
        own = [m["reserved"] for m in members if m["reserved"] and m["reserved"]["source"] != "slurm"]
        node["reserved"] = {k: slurm_reserved[k] + sum(o.get(k) or 0 for o in own) for k in ("cpus", "mem", "gpus")}
        if node.get("local"):  # everything not started through jobrun (other users, Slurm jobs of others, the OS)
            node["other"] = {"cpu": max(0.0, node["cpu"] - sum(m.get("cpu") or 0 for m in members)),
                             "mem": max(0, node["mem"] - sum(m.get("mem") or 0 for m in members)),
                             "vram": max(0, sum(g["vram"] for g in gpus) - sum(m.get("vram") or 0 for m in members))
                             if gpus else None}
        nodes.append(node)

    result = {"time": now, "interval_s": interval, "nodes": nodes}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        write_json(path, {"time": now, "cpu": [busy, total], "jobs": job_ticks, "slurm_cpu": slurm_cpu,
                          "result": result})
    except OSError:
        pass
    return result


def job_reservation(row: dict, slurm: dict | None, limits: dict | None) -> dict | None:
    """What the job was given: its Slurm allocation, else the --cpus/--mem/--gpus budget, else its cgroup's limits
    (per resource, so `--cpus 10` and a MemoryMax scope combine)."""
    if slurm:
        return {"cpus": slurm["cpus"], "mem": slurm["mem"], "gpus": slurm["gpus"], "source": "slurm"}
    declared, limits = row.get("budget") or {}, limits or {}
    reserved = {k: declared.get(k) if declared.get(k) is not None else limits.get(k) for k in ("cpus", "mem", "gpus")}
    if all(v is None for v in reserved.values()):
        return None
    sources = {"budget" if declared.get(k) is not None else "cgroup" for k, v in reserved.items() if v is not None}
    return {**reserved, "source": "+".join(sorted(sources))}


def _children(table: dict) -> dict[int, list[int]]:
    children: dict[int, list[int]] = {}
    for pid, (ppid, _, _) in table.items():
        children.setdefault(ppid, []).append(pid)
    return children


def _tree_usage(table: dict, children: dict, root: int) -> tuple[int, int, list[int]]:
    """CPU ticks (live processes plus reaped children, so nothing is lost when a worker exits), RSS (shared pages
    count once per process) and pids of a process and its descendants."""
    pids, todo = [], [root] if root in table else []
    while todo:
        pid = todo.pop()
        pids.append(pid)
        todo.extend(children.get(pid, []))
    return sum(table[p][1] for p in pids), sum(table[p][2] for p in pids), pids


# ---------------------------------------------------------------- setup, uninstall, demo
# `jobrun setup` installs the pieces around the CLI: the CLI itself on PATH, the VS Code extension, and the agent skill
# (plus an optional one-line instruction) for the agents the user picks. Every file, link, text block and extension
# it creates is recorded in DATA/install.json, so `jobrun uninstall` removes exactly those and nothing else. Front-ends
# (install.sh, a clone, pipx, the VS Code extension) all call this one implementation.

REPO = "https://github.com/KreitnerL/jobboard"
RAW = "https://raw.githubusercontent.com/KreitnerL/jobboard"
EXTENSION_ID = "kreitner.jobboard"
EDITORS = {"code": "VS Code", "code-insiders": "VS Code Insiders", "cursor": "Cursor", "codium": "VSCodium",
           "windsurf": "Windsurf"}  # CLIs that take --install-extension -> editor name
INSTRUCTION = "Launch every command expected to run longer than ~2 minutes through `jobrun` (see the jobrun skill)."
# Agents whose global skill folder is known, keyed by the identifiers of the Agent Skills CLI (`npx skills add -a`),
# with the same global folders it uses: id -> label, skill folder, global instruction file (None: the agent has
# none), signs that the agent is installed. `npx skills add KreitnerL/jobboard -g` covers the many others.
AGENT_TARGETS = {
    "claude-code": ("Claude Code", "~/.claude/skills", "~/.claude/CLAUDE.md", ("~/.claude",), "claude"),
    "codex": ("Codex", "~/.agents/skills", "~/.codex/AGENTS.md", ("~/.codex",), "codex"),
    "cursor": ("Cursor", "~/.cursor/skills", None, ("~/.cursor",), "cursor-agent"),
    "github-copilot": ("GitHub Copilot", "~/.copilot/skills", None, ("~/.copilot",), None),
    "gemini-cli": ("Gemini CLI", "~/.gemini/skills", "~/.gemini/GEMINI.md", ("~/.gemini",), "gemini"),
    "opencode": ("OpenCode", "~/.config/opencode/skills", None, ("~/.config/opencode",), "opencode"),
    "windsurf": ("Windsurf", "~/.codeium/windsurf/skills", None, ("~/.codeium/windsurf",), "windsurf"),
}
AGENT_ALIASES = {"claude": "claude-code", "copilot": "github-copilot", "gemini": "gemini-cli"}


def data_dir() -> Path:
    return Path(os.environ.get("XDG_DATA_HOME") or Path.home() / ".local" / "share") / "jobboard"


def bin_dir() -> Path:
    return Path.home() / ".local" / "bin"


def _expand(path: str) -> Path:
    return Path(os.path.expanduser(path))


def _sha(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def say(text: str = "") -> None:
    """print, with the home directory shown as ~."""
    print(str(text).replace(str(Path.home()), "~"), flush=True)


class Ask:
    """Questions on the terminal (also under `curl | sh`, via /dev/tty); with --yes or without a terminal the
    defaults are taken."""

    def __init__(self, assume_yes: bool):
        self.stream = None
        if not assume_yes:
            if sys.stdin.isatty():
                self.stream = sys.stdin
            else:
                try:
                    self.stream = open("/dev/tty")
                except OSError:
                    self.stream = None

    @property
    def interactive(self) -> bool:
        return self.stream is not None

    def line(self, prompt: str) -> str:
        print(prompt.replace(str(Path.home()), "~"), end="", flush=True)
        return (self.stream.readline() if self.stream else "\n").strip()

    def yes(self, question: str, default: bool = True) -> bool:
        if not self.stream:
            return default
        answer = self.line(f"{question} [{'Y/n' if default else 'y/N'}] ").lower()
        return default if not answer else answer.startswith("y")

    def pick(self, question: str, options: list[tuple[str, str, bool]]) -> list[str]:
        """options: (key, label, default). Returns the chosen keys."""
        if not self.stream:
            return [key for key, _, default in options if default]
        say(question)
        for n, (_, label, default) in enumerate(options, 1):
            say(f"  {n}) [{'x' if default else ' '}] {label}")
        marked = any(default for _, _, default in options)
        answer = self.line("Numbers separated by spaces" + (", Enter for the marked ones, 0 for none: " if marked
                                                            else ", Enter for none: "))
        if not answer:
            return [key for key, _, default in options if default]
        picked = {int(n) for n in re.findall(r"\d+", answer)}
        return [key for n, (key, _, _) in enumerate(options, 1) if n in picked]


class InstallRecord:
    """What setup created: [{kind: file|link|block|extension, ...}], in creation order."""

    def __init__(self):
        self.path = data_dir() / "install.json"
        data = read_json(self.path, {}) or {}
        self.items: list[dict] = data.get("items", [])

    def add(self, item: dict) -> None:
        key = {k: item.get(k) for k in ("kind", "path", "editor")}
        self.items = [i for i in self.items if {k: i.get(k) for k in key} != key] + [item]

    def created(self, path: Path) -> bool:
        """Whether setup created this file (a later setup run then still knows it may delete it)."""
        return any(i.get("path") == str(path) and i.get("created") for i in self.items)

    def owns(self, kind: str, path: Path) -> bool:
        return any(i["kind"] == kind and i.get("path") == str(path) for i in self.items)

    def mkdir(self, path: Path) -> None:
        """mkdir -p, recording the directories it creates (uninstall removes them again when empty)."""
        missing = []
        for parent in [path, *path.parents]:
            if parent.exists():
                break
            missing.append(parent)
        path.mkdir(parents=True, exist_ok=True)
        for directory in reversed(missing):
            self.add({"kind": "dir", "path": str(directory)})

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        write_json(self.path, {"version": VERSION, "items": self.items})


BLOCK_BEGIN = "jobrun:begin (added by `jobrun setup`, removed by `jobrun uninstall`)"
BLOCK_END = "jobrun:end"


def _add_block(path: Path, text: str, comment: tuple[str, str]) -> bool:
    """Append text between begin/end marker lines; an existing block is replaced. Returns whether the file is new."""
    created = not path.exists()
    kept = "" if created else _strip_block(path.read_text())
    gap = "" if not kept else "\n" if kept.endswith("\n") else "\n\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{kept}{gap}{comment[0]} {BLOCK_BEGIN}{comment[1]}\n{text}\n{comment[0]} {BLOCK_END}{comment[1]}\n")
    return created


def _strip_block(text: str) -> str:
    """The text without the marker block (and the blank line _add_block put before it)."""
    if BLOCK_BEGIN not in text:
        return text
    kept, inside = [], False
    for line in text.splitlines(keepends=True):
        if BLOCK_BEGIN in line:
            inside = True
        elif inside and BLOCK_END in line:
            inside = False
        elif not inside:
            kept.append(line)
    rest = "".join(kept).rstrip("\n")
    return rest + "\n" if rest else ""


def _download(url: str, dest: Path) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = response.read()
    except (OSError, ValueError):
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    return True


def _skill_source(scratch: Path) -> Path | None:
    """The skill shipped next to this file (clone, release tarball, VS Code bundle), else downloaded for VERSION."""
    here = Path(__file__).resolve().parent / "skills" / "jobrun" / "SKILL.md"
    if here.exists():
        return here
    for ref in (f"v{VERSION}", "main"):
        if _download(f"{RAW}/{ref}/skills/jobrun/SKILL.md", scratch / "SKILL.md"):
            return scratch / "SKILL.md"
    return None


def _vsix_source(scratch: Path) -> Path | None:
    """A built extension: next to this file, from the GitHub release of VERSION (or the latest), or built with npx."""
    here = Path(__file__).resolve().parent
    if (here / "vscode" / "jobboard.vsix").exists():
        return here / "vscode" / "jobboard.vsix"
    for url in (f"{REPO}/releases/download/v{VERSION}/jobboard.vsix", f"{REPO}/releases/latest/download/jobboard.vsix"):
        if _download(url, scratch / "jobboard.vsix"):
            return scratch / "jobboard.vsix"
    if (here / "vscode" / "package.json").exists() and shutil.which("npx"):
        print("  building the extension with npx @vscode/vsce ...")
        out = subprocess.run(["npx", "--yes", "@vscode/vsce", "package", "-o", "jobboard.vsix"], cwd=here / "vscode",
                             capture_output=True, text=True)
        if out.returncode == 0:
            return here / "vscode" / "jobboard.vsix"
    return None


def _running_inside_extension() -> bool:
    package = Path(__file__).resolve().parent.parent / "package.json"
    return (read_json(package, {}) or {}).get("name") == "jobboard"


def cmd_setup(args) -> int:
    ask = Ask(args.yes)
    record = InstallRecord()
    done: list[str] = []
    missed: list[str] = []  # wanted but not done, repeated in the summary so it is not lost in the output
    scratch = data_dir() / "download"
    say(f"jobrun {VERSION} setup. Each step can be skipped; `jobrun uninstall` undoes everything it does.\n")

    # 1. the CLI on PATH
    source = Path(__file__).resolve()
    link = bin_dir() / "jobrun"
    on_path = shutil.which("jobrun")
    if (link.exists() or link.is_symlink()) and not record.owns("link", link):
        say(f"1. CLI: {link} already exists{' -> ' + str(link.resolve()) if link.is_symlink() else ''} "
              "(not made by setup); left alone.")
    elif on_path and Path(on_path).resolve() != link.resolve():
        say(f"1. CLI: `jobrun` is already on PATH ({on_path}, e.g. pipx); kept.")
    elif ask.yes(f"1. Install the `jobrun` command in {bin_dir()}?"):
        if args.link:
            target = source
        else:  # a stable copy, so the command survives moving the clone or updating the VS Code extension
            app = data_dir() / "app"
            record.mkdir(app)
            target = app / "jobrun.py"
            if source != target:
                shutil.copy2(source, target)
            target.chmod(0o755)
            skill = source.parent / "skills" / "jobrun" / "SKILL.md"
            if skill.exists() and skill.resolve() != (app / "skills" / "jobrun" / "SKILL.md").resolve():
                record.mkdir(app / "skills" / "jobrun")
                shutil.copy2(skill, app / "skills" / "jobrun" / "SKILL.md")
        record.mkdir(link.parent)
        if link.is_symlink():
            link.unlink()
        link.symlink_to(target)
        record.add({"kind": "link", "path": str(link), "target": str(target)})
        done.append(f"command: {link} -> {target}")
        if str(bin_dir()) not in os.environ.get("PATH", "").split(os.pathsep):
            shell = Path(os.environ.get("SHELL", "")).name
            rc = Path.home() / {"bash": ".bashrc", "zsh": ".zshrc"}.get(shell, ".profile")
            if ask.yes(f"   {bin_dir()} is not on your PATH. Add it in {rc}?"):
                created = _add_block(rc, 'export PATH="$HOME/.local/bin:$PATH"', ("#", ""))
                record.add({"kind": "block", "path": str(rc), "created": created or record.created(rc)})
                done.append(f"PATH: {rc} (open a new terminal)")
    record.save()

    # 2. the VS Code extension
    editors = [e for e in EDITORS if shutil.which(e)]
    label = {e: f"{EDITORS[e]} (`{e}`)" for e in editors}
    if args.no_vscode:
        pass
    elif _running_inside_extension():
        say("2. VS Code extension: already installed (setup runs from it).")
    elif not editors:
        say("2. VS Code extension: no editor command found (code, cursor, ...). In VS Code run "
              "'Shell Command: Install code command in PATH', or install jobboard.vsix from the releases page.")
    else:
        if len(editors) == 1:
            chosen = editors if ask.yes(f"2. Install the Job Board extension into {label[editors[0]]}?") else []
        else:
            chosen = ask.pick("2. Install the Job Board extension into these editors:",
                              [(e, label[e], e == editors[0]) for e in editors])
        vsix = _vsix_source(scratch) if chosen else None
        if chosen and not vsix:
            say(f"   no jobboard.vsix found or downloadable; see {REPO}/releases")
            missed.append(f"extension: no jobboard.vsix found or downloadable; install it from {REPO}/releases")
        for editor in chosen if vsix else []:
            out = subprocess.run([editor, "--install-extension", str(vsix), "--force"], capture_output=True, text=True)
            if out.returncode == 0:
                record.add({"kind": "extension", "editor": editor, "id": EXTENSION_ID})
                done.append(f"extension: {editor} (reload the window; under Remote-SSH it is installed on this host)")
            else:
                say(f"   {editor}: {(out.stderr or out.stdout).strip()[:300]}")
                missed.append(f"extension: `{editor} --install-extension` failed (see above)")
    record.save()

    # 3. agents: the skill, and optionally one line in the global instructions
    wanted = None if args.agents is None else [AGENT_ALIASES.get(a.strip(), a.strip()) for a in args.agents.split(",")
                                               if a.strip() not in ("", "none")]
    found = [key for key, (_, _, _, signs, command) in AGENT_TARGETS.items()
             if any(_expand(s).exists() for s in signs) or (command and shutil.which(command))]
    if wanted is not None:
        agents = wanted
    elif not found:
        say("3. Coding agents: none found (looked for " + ", ".join(t[0] for t in AGENT_TARGETS.values()) + ").")
        agents = []
    else:
        agents = ask.pick("3. Teach these coding agents to use jobrun (installs the skill); found on this machine:",
                          [(key, AGENT_TARGETS[key][0], False) for key in found])
    if wanted is None:
        say(f"   An agent not listed here, or later: `jobrun setup --agents ID,...` (IDs: {', '.join(AGENT_TARGETS)}),\n"
            "   or for any other agent `npx skills add KreitnerL/jobboard -g` (the Agent Skills installer, 70+ agents).")
    unknown = set(agents) - set(AGENT_TARGETS)
    if unknown:
        sys.exit(f"jobrun setup: unknown agent(s) {', '.join(sorted(unknown))}; one of {', '.join(AGENT_TARGETS)}")
    skill = _skill_source(scratch) if agents else None
    if agents and not skill:
        say("   the skill could not be found or downloaded; skipped")
        missed.append("skill: could not be found or downloaded")
    for skill_root in dict.fromkeys(AGENT_TARGETS[a][1] for a in agents) if skill else []:
        dest = _expand(skill_root) / "jobrun" / "SKILL.md"
        if dest.exists() and not record.owns("file", dest):
            say(f"   {dest} exists and was not made by setup; left alone.")
            continue
        record.mkdir(dest.parent)
        shutil.copy2(skill, dest)
        record.add({"kind": "file", "path": str(dest), "sha256": _sha(dest)})
        done.append(f"skill: {dest}")
    files = [_expand(AGENT_TARGETS[a][2]) for a in agents if AGENT_TARGETS[a][2]]
    fresh = [f for f in files if record.owns("block", f) or "jobrun" not in (f.read_text() if f.exists() else "")]
    for f in sorted(set(files) - set(fresh)):
        say(f"   {f} already mentions jobrun; left alone.")
    if fresh and (args.instructions if args.instructions is not None else
                  ask.yes(f"   Also add this line to {', '.join(map(str, fresh))}?\n     \"{INSTRUCTION}\"\n  ")):
        for f in fresh:
            record.mkdir(f.parent)
            created = _add_block(f, INSTRUCTION, ("<!--", " -->"))
            record.add({"kind": "block", "path": str(f), "created": created or record.created(f)})
            done.append(f"instruction: {f}")
    record.save()

    say("\nDone." if done else "\nNothing changed.")
    for line in done:
        say(f"  {line}")
    if missed:
        say("\nNot done:")
        for line in missed:
            say(f"  {line}")
    say("\nTry it: `jobrun demo` starts a few short example jobs; watch them with `jobrun ls` or in the Jobs view.")
    return 0


def _remove_item(item: dict, force: bool) -> str | None:
    """Undo one recorded step; returns a note when something was kept."""
    path = Path(item["path"]) if item.get("path") else None
    if item["kind"] == "link":
        if path.is_symlink() and (force or str(path.resolve()) == str(Path(item["target"]).resolve())):
            path.unlink()
        elif path.exists() or path.is_symlink():
            return f"kept {path}: it no longer points to {item['target']}"
    elif item["kind"] == "file":
        if path.exists() and (force or _sha(path) == item.get("sha256")):
            path.unlink()
            try:
                path.parent.rmdir()
            except OSError:
                pass
        elif path.exists():
            return f"kept {path}: changed since setup (--force removes it)"
    elif item["kind"] == "block":
        if path.exists():
            rest = _strip_block(path.read_text())
            if not rest.strip() and item.get("created"):
                path.unlink()  # setup made this file for the block alone
            else:
                path.write_text(rest)
    elif item["kind"] == "dir":
        try:
            path.rmdir()  # only when empty
        except OSError:
            pass
    elif item["kind"] == "extension":
        if shutil.which(item["editor"]):
            subprocess.run([item["editor"], "--uninstall-extension", item["id"]], capture_output=True)
    return None


def cmd_uninstall(args) -> int:
    ask = Ask(args.yes)
    record = InstallRecord()
    active = [r for r in snapshots(load_jobs()) if r["status"] in ACTIVE] if JOBS.exists() else []
    say(f"jobrun {VERSION} uninstall\n")
    if not record.items:
        say(f"No install record ({record.path}): nothing set up by `jobrun setup` here.")
    else:
        say("This removes what `jobrun setup` installed:")
        for item in record.items:
            what = {"link": "command", "file": "skill", "block": "lines in", "extension": "extension in"}.get(item["kind"])
            if what:
                say(f"  {what} {item.get('path') or item.get('editor')}")
    if active:
        say(f"\n{len(active)} job(s) still running; they keep running and finish normally, but `jobrun` and the "
              "Jobs view will be gone.")
    if record.items and not ask.yes("\nContinue?"):
        return 1
    # the app copy and the record first, so the directories setup created are empty when their turn comes
    shutil.rmtree(data_dir() / "app", ignore_errors=True)
    shutil.rmtree(data_dir() / "download", ignore_errors=True)
    if record.path.exists():
        record.path.unlink()
    for item in reversed(record.items):
        note = _remove_item(item, args.force)
        if note:
            say(f"  {note}")
    try:
        data_dir().rmdir()
    except OSError:
        pass
    if HOME.exists():
        jobs = len(list(JOBS.glob("*.json"))) if JOBS.exists() else 0
        if args.purge and active:
            say(f"\nKept the registry {HOME}: jobs are still running. Run `jobrun uninstall --purge` again later.")
        elif args.purge or (record.items and ask.yes(f"\nAlso delete the job registry {HOME} ({jobs} files, "
                                                         "with the logs jobrun wrote)?", default=False)):
            shutil.rmtree(HOME, ignore_errors=True)
            say(f"  deleted {HOME}")
        else:
            say(f"\nKept the job registry {HOME} (delete it with `rm -r {HOME}`).")
    here = Path(__file__).resolve()
    if "site-packages" in here.parts or "dist-packages" in here.parts:
        say("\njobrun itself was installed as a Python package; remove it with `pipx uninstall jobboard`, "
              "`uv tool uninstall jobboard` or `pip uninstall jobboard`.")
    say("\nUninstalled.")
    return 0


DEMO_JOBS = [
    ("demo: train model", ["--stages", "prepare,train,evaluate"], """
import sys, time
print("JOBRUN_BUDGET cpus=1 mem=1G", flush=True)
for (name, idx, total, pause) in (("prepare", 1, 10, 0.6), ("train", 2, 60, 0.9), ("evaluate", 3, 20, 0.5)):
    print(f"JOBRUN_STAGE {name} {idx}/3", flush=True)
    for i in range(1, total + 1):
        time.sleep(pause)
        sys.stderr.write(f"\\r{name}: {i}/{total} [00:00<00:00, 1.00it/s]"); sys.stderr.flush()
    sys.stderr.write("\\n")
print("done", flush=True)
"""),
    ("demo: tuning sweep", ["--count-regex", "Trial \\d+ completed", "--total", "40"], """
import random, time
print("JOBRUN_BUDGET cpus=2", flush=True)
for trial in range(40):
    time.sleep(random.uniform(0.5, 2.5))
    print(f"Trial {trial} completed with value {random.random():.3f}", flush=True)
"""),
    ("demo: report (waits for the model)", ["--after", "demo: train model"], """
import time
for i in range(1, 6):
    time.sleep(1)
    print(f"JOBRUN_PROGRESS {i}/5", flush=True)
"""),
]


def cmd_demo(args) -> int:
    """Start a few short example jobs in one group: stages, a tqdm bar, counted lines, a dependency, budgets."""
    group = "jobrun demo"
    me = [sys.executable, str(Path(__file__).resolve())]
    for name, options, script in DEMO_JOBS:
        subprocess.run(me + ["run", "--group", group, "--name", name, *options, "--", sys.executable, "-c",
                             script.strip()], check=True, stdout=subprocess.DEVNULL)
    print(f"Started {len(DEMO_JOBS)} demo jobs (about 2 minutes) in the group \"{group}\".\n"
          "Watch them: the Jobs view in VS Code, `jobrun ls`, `jobrun res`.\n"
          f"Remove them afterwards: `jobrun rm --group \"{group}\"` (or the trash icon).")
    return 0


# ---------------------------------------------------------------- formatting

def fmt_count(n: int | None) -> str:
    if n is None:
        return ""
    for suffix, scale in (("G", 1e9), ("M", 1e6), ("k", 1e3)):
        if n >= 10 * scale if suffix == "k" else n >= scale:
            return f"{n / scale:.3g}{suffix}"
    return str(n)


def fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return ""
    s = int(seconds)
    if s >= 86400:
        return f"{s // 86400}d{s % 86400 // 3600:02d}h"
    if s >= 3600:
        return f"{s // 3600}h{s % 3600 // 60:02d}m"
    return f"{s // 60}m{s % 60:02d}s"


def fmt_rate(rate: float | None) -> str:
    if not rate:
        return ""
    for unit, scale in (("s", 1), ("min", 60), ("h", 3600)):
        if rate * scale >= 1:
            return f"{rate * scale:.3g}/{unit}"
    return f"{rate * 86400:.3g}/d"


def fmt_eta(eta: float | None) -> str:
    if not eta:
        return ""
    when = datetime.fromtimestamp(eta)
    return f"{when:%H:%M}" if when.date() == datetime.now().date() else f"{when:%a %H:%M}"


def fmt_idle(row: dict) -> str:
    """Time since the last step, with the average step time when known; "!" marks a likely stall."""
    if row.get("idle_s") is None:
        return ""
    average = f" (avg {fmt_duration(row['step_s'])})" if row.get("step_s") else ""
    return f"{fmt_duration(row['idle_s'])} ago{average}{' !' if row.get('stalled') else ''}"


def fmt_event(row: dict) -> str:
    """The newest problem the job reported (JOBRUN_EVENT), for a group the most severe level among its jobs."""
    if not row.get("degraded"):
        return ""
    newest = (row.get("events") or [None])[-1]
    if not newest:
        return f"[{row['degraded']}]"
    more = f" (+{row['event_count'] - 1} more)" if row.get("event_count", 0) > 1 else ""
    return f"[{newest['level']}: {newest['message'][:60]}{more}]"


def pool_where(row: dict) -> str:
    """"2 workers + monitor on ws-1, node-b": the pool's workers (running ones while any run), its monitors, and the
    machines its workers run on."""
    n, monitors, hosts = row["active_workers"] or len(row["workers"]), len(row.get("monitors") or []), row["hosts"]
    text = f"{n} worker{'s' * (n != 1)}"
    if monitors:
        text += " + monitor" if monitors == 1 else f" + {monitors} monitors"
    if hosts:
        text += f" on {', '.join(hosts)}" if len(hosts) <= 3 else f" on {len(hosts)} hosts"
    return text


def fmt_progress(row: dict) -> str:
    text = _fmt_progress(row)
    return f"{text}  {fmt_event(row)}".strip() if row.get("degraded") else text


def _fmt_progress(row: dict) -> str:
    if row.get("status") == "waiting":
        return "waiting for " + ", ".join(row.get("waiting_for") or ["dependencies"])
    if row.get("status") == "cancelled":
        return row.get("reason") or "never started"
    if row.get("pool"):  # a pool's worker: the list's progress is on the pool's row only
        reason = f" ({row['reason']})" if row.get("reason") else ""
        return f"monitor{reason}" if row.get("role") == "monitor" else reason.strip()
    stage = ""
    if row.get("stage"):
        stage = f"[{row['stage_no']}/{row['stage_of'] or '?'} {row['stage']}] "
    reason = f" ({row['reason']})" if row.get("reason") else ""
    if row["done"] is None:
        return (stage + reason).strip()
    if row["fraction"] is not None:
        return f"{stage}{row['fraction'] * 100:3.0f}% {fmt_count(row['done'])}/{fmt_count(row['total'])}{reason}"
    return stage + fmt_count(row["done"]) + reason


def dependency_order(rows: list[dict]) -> list[dict]:
    """Rows in their order, except that every job comes after the listed jobs it waits for (--after)."""
    by_id, placed, ordered = {r["id"]: r for r in rows}, set(), []

    def place(row):
        if row["id"] not in placed:
            placed.add(row["id"])
            for dep in row.get("after") or []:
                if dep in by_id:
                    place(by_id[dep])
            ordered.append(row)

    for row in rows:
        place(row)
    return ordered


def visible(row: dict, days: float | None) -> bool:
    if days is None or row["status"] not in FINAL + ("lost",):
        return True
    return time.time() - (row["ended"] or row["started"] or 0) <= days * 86400


# ---------------------------------------------------------------- commands

def budget(args, current: dict | None = None) -> dict | None:
    """--cpus / --mem / --gpus as stored in the metadata (bytes for RAM); None when none is given."""
    given = {"cpus": args.cpus, "mem": _size(args.mem.upper().rstrip("B"), "G") if args.mem else None, "gpus": args.gpus}
    if args.mem and given["mem"] is None:
        sys.exit(f"jobrun: --mem expects a size like 20G, got {args.mem!r}")
    if all(v is None for v in given.values()):
        return None
    return {**(current or {}), **{k: v for k, v in given.items() if v is not None}}


def parse_budget(text: str) -> dict:
    """{"cpus": 12.0, "mem": bytes, "gpus": 1.0} from "cpus=12 mem=96G gpus=1"; malformed values are skipped."""
    found = {}
    for key, value in re.findall(r"(cpus|mem|gpus)=([\w.]+)", text):
        try:
            found[key] = _size(value.upper().rstrip("B"), "G") if key == "mem" else float(value)
        except ValueError:
            continue
    return {k: v for k, v in found.items() if v is not None}


def progress_options(args) -> dict:
    options = {"budget": budget(args), "group": args.group, "progress_regex": args.progress_regex,
               "count_regex": args.count_regex, "item_regex": args.item_regex, "total": args.total,
               "stages": [s.strip() for s in args.stages.split(",")] if args.stages else None,
               "stage_rules": stage_rules(args.stage_at), "role": None if args.role in (None, "worker") else args.role}
    if args.pool:  # the group belongs to the pool, which shows as one job in it
        options.update(pool=join_pool(args.pool, args.group), group=None)
    return options


def find_pool(key: str) -> dict | None:
    """The pool with this id, else the newest pool with this name."""
    pools = [m for m in load_jobs() if m.get("kind") == "pool"]
    hits = [m for m in pools if m["id"] == key] or [m for m in pools if m.get("name") == key]
    return hits[-1] if hits else None


def create_pool(name: str, group: str | None, total: int | None) -> dict:
    meta = {"id": allocate_id(), "name": name, "kind": "pool", "group": group or None, "total": total,
            "started": time.time(), "host": HOST, "cwd": os.getcwd()}
    write_json(_file(meta["id"], ".json"), meta)
    return meta


def join_pool(key: str, group: str | None, inherit: str | None = None) -> str:
    """The id of the pool named key (id or name), created when there is none. group: the pool's group, which
    must match an existing pool's; inherit: a group for a pool that has none (a job moved into the pool)."""
    pool = find_pool(key)
    if pool is None:
        pool = create_pool(key, group or inherit, None)
        print(f"jobrun: new pool {pool['id']}  {key}", file=sys.stderr)
        return pool["id"]
    if group and pool.get("group") not in (None, group):
        sys.exit(f"jobrun: pool {key!r} is in group {pool['group']!r}; leave out --group (workers take their "
                 f"pool's group) or use another pool")
    if not pool.get("group") and (group or inherit):
        pool = update_meta(pool["id"], group=group or inherit)
    workers = sum(m.get("pool") == pool["id"] and m.get("role") != "monitor" for m in load_jobs())
    print(f"jobrun: joining pool {pool['id']}  {pool['name']} ({workers} workers so far, created "
          f"{fmt_duration(time.time() - pool['started'])} ago)", file=sys.stderr)
    return pool["id"]


def cmd_pool(args) -> int:
    """Create a pool; workers join it with --pool."""
    existing = find_pool(args.name)
    if existing and existing["id"] != args.name:
        print(f"jobrun: another pool is named {args.name!r} ({existing['id']}); --pool {args.name!r} joins the "
              f"new one", file=sys.stderr)
    pool = create_pool(args.name, args.group, args.total)
    print(f"{pool['id']}  {pool['name']}")
    return 0


def stage_rules(values: list[str] | None) -> list[list[str]] | None:
    """--stage-at NAME=REGEX: the stage NAME starts at the first log line matching REGEX."""
    rules = []
    for value in values or []:
        name, sep, rule = value.partition("=")
        if not sep or not name.strip():
            sys.exit(f"jobrun: --stage-at expects NAME=REGEX, got {value!r}")
        re.compile(rule)
        rules.append([name.strip(), rule])
    return rules or None


def dependencies(args) -> list[str]:
    """Job ids from --after (repeatable, comma-separated ids or names)."""
    keys = [k.strip() for value in args.after or [] for k in value.split(",") if k.strip()]
    return [m["id"] for m in resolve(keys)] if keys else []


def cmd_run(args) -> int:
    cmd = args.cmd[1:] if args.cmd[:1] == ["--"] else args.cmd
    if not cmd:
        sys.exit("jobrun run: no command given")
    auto_clean()
    job_id = allocate_id()
    log = Path(args.log).expanduser().resolve() if args.log else LOGS / f"{job_id}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    meta = {"id": job_id, "name": args.name or default_name(cmd), "kind": "process", "cmd": cmd,
            "cwd": os.getcwd(), "host": HOST, "log": str(log), "started": time.time(),
            "slurm_id": os.environ.get("SLURM_JOB_ID"), "log_owned": True,
            "max_log_mb": args.max_log_mb,
            "progress_file": str(Path(args.progress_file).expanduser().resolve()) if args.progress_file else None,
            "after": dependencies(args), **progress_options(args)}
    write_json(_file(job_id, ".json"), meta)
    if args.fg:
        print(f"jobrun: {job_id}  {meta['name']}  log: {log}", file=sys.stderr, flush=True)
        return supervise(job_id, tee=True)
    subprocess.Popen([sys.executable, os.path.abspath(__file__), "_supervise", job_id],
                     start_new_session=True, stdin=subprocess.DEVNULL,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"{job_id}  {meta['name']}  log: {log}")
    return 0


def cmd_track(args, job_id: str | None = None, log_owned: bool = False, after: list[str] | None = None) -> int:
    job_id = job_id or allocate_id()
    if args.pid is not None:
        pid = args.pid
        if not pid_alive(pid):
            sys.exit(f"jobrun track: no running process {pid}")
        cmd = [c.decode(errors="replace") for c in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0") if c]
        try:
            cwd = os.readlink(f"/proc/{pid}/cwd")
        except OSError:
            cwd = None
        log = args.log or _stdout_file(pid)
        meta = {"kind": "pid", "name": default_name(cmd), "cmd": cmd, "cwd": cwd, "host": HOST,
                "log": str(Path(log).expanduser().resolve()) if log else None,
                "started": proc_started_at(pid), "pid": pid, "proc_start": proc_start(pid)}
    else:
        pattern = str(Path(args.log).expanduser().resolve()) if args.log else None
        meta = slurm_meta(str(args.slurm), log_pattern=pattern)
        meta["log_pattern"] = pattern
    meta.update(id=job_id, log_owned=log_owned, after=after or [], **progress_options(args))
    if args.name:
        meta["name"] = args.name
    write_json(_file(job_id, ".json"), meta)
    print(f"{job_id}  {meta['name']}  log: {meta.get('log')}")
    return 0


def _sets_output(sbatch_args: list[str]) -> bool:
    """Whether the command line or the batch script's #SBATCH header chooses the output file."""
    if any(a == "-o" or a.startswith(("--output", "-o")) for a in sbatch_args):
        return True
    script = next((a for a in sbatch_args if not a.startswith("-") and os.path.isfile(a)), None)
    if not script:
        return False
    with open(script, errors="replace") as handle:
        for line in handle:
            if line.startswith("#SBATCH"):
                if re.search(r"\s(-o|--output)\b", line):
                    return True
            elif line.strip() and not line.startswith("#"):
                break  # sbatch stops reading directives at the first command
    return False


def cmd_sbatch(args) -> int:
    """Submit and track; without a chosen --output the Slurm log goes to the registry and is jobrun's."""
    rest = args.sbatch_args[1:] if args.sbatch_args[:1] == ["--"] else args.sbatch_args
    after = dependencies(args)
    if after:
        slurm_ids = [str((read_json(_file(dep, ".json")) or {}).get("slurm_id") or "") for dep in after]
        if not all(slurm_ids):
            sys.exit("jobrun sbatch --after: dependencies must be Slurm jobs (use `jobrun run --after` otherwise)")
        rest = [f"--dependency=afterok:{':'.join(slurm_ids)}", *rest]
    job_id, owned = allocate_id(), not _sets_output(rest)
    if owned:
        LOGS.mkdir(parents=True, exist_ok=True)
        rest = [f"--output={LOGS / job_id}.log", *rest]
    out = subprocess.run(["sbatch", "--parsable", *rest], capture_output=True, text=True)
    if out.returncode:
        sys.stderr.write(out.stderr)
        return out.returncode
    args.slurm, args.pid = out.stdout.strip().split(";")[0], None
    print(f"Submitted batch job {args.slurm}")
    return cmd_track(args, job_id, log_owned=owned, after=after)


def visible_rows(days: float | None) -> list[dict]:
    """Rows to show; a group or pool stays complete as long as any member is visible."""
    rows = snapshots(load_jobs())

    def with_pools(keep: set) -> set:
        pools = {r.get("pool") or r["id"] for r in rows if r["id"] in keep and (r.get("pool") or r["kind"] == "pool")}
        return keep | {r["id"] for r in rows if (r.get("pool") or r["id"]) in pools}

    keep = with_pools({r["id"] for r in rows if visible(r, days)})
    groups = {r["group"] for r in rows if r.get("group") and r["id"] in keep}
    keep = with_pools(keep | {r["id"] for r in rows if r.get("group") in groups})
    return [r for r in rows if r["id"] in keep]


def cmd_ls(args) -> int:
    auto_clean()
    rows = visible_rows(None if args.all else args.days)
    groups = group_rows(rows)
    if args.json:
        print(json.dumps({"schema": JSON_SCHEMA, "jobs": rows, "groups": groups,
                          "settings": {key: setting(key) for key in SETTINGS}}))
        return 0
    if not rows:
        print("no jobs")
        return 0
    table = [("ID", "STATUS", "NAME", "PROGRESS", "RATE", "ETA", "LAST STEP", "ELAPSED", "WHERE")]

    def line(r, job_id, name, where=""):
        worker = bool(r.get("pool"))  # a pool's worker: progress, rate and ETA are the pool's
        table.append((job_id, r["status"], name, fmt_progress(r), "" if worker else fmt_rate(r["rate_per_s"]),
                      "" if worker else "finishing" if r.get("finishing") else fmt_eta(r["eta"]),
                      "" if worker else fmt_idle(r),
                      fmt_duration(r["elapsed_s"]), where))

    def where(r):
        if r.get("kind") == "pool":
            return pool_where(r)
        return ((r["host"] or "") + (f" slurm:{r['slurm_id']}" if r["slurm_id"] else "")).strip()

    pools = {r["id"] for r in rows if r.get("kind") == "pool"}
    by_name = {g["name"]: g for g in groups}
    for r in dependency_order(rows):
        if r.get("pool") in pools:
            continue  # listed under its pool
        group = r.get("group")
        if group and group in by_name:
            g = by_name.pop(group)
            line(g, "group", f"{group[:36]} ({g['finished']}/{g['jobs']} jobs)")
            members = dependency_order([m for m in rows if m.get("group") == group])
        elif group:
            continue
        else:
            members = [r]
        for member in members:
            indent = "  " if group else ""
            line(member, member["id"], indent + (member["name"] or "")[:40], where(member))
            if member.get("kind") == "pool":
                members_of = [w for w in rows if w.get("pool") == member["id"]]
                for worker in sorted(dependency_order(members_of), key=lambda w: w.get("role") != "monitor"):
                    line(worker, worker["id"], f"{indent}  └ {(worker['name'] or '')[:36]}", where(worker))
    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    for row in table:
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths)).rstrip())
    return 0


def fmt_bytes(n: float | None) -> str:
    if n is None:
        return "?"
    for unit, scale in (("T", 1 << 40), ("G", 1 << 30), ("M", 1 << 20)):
        if n >= scale:
            return f"{n / scale:.3g}{unit}"
    return f"{n / 1024:.0f}K" if n else "0"


def fmt_usage(u: dict) -> str:
    """CPU cores, RAM, GPU utilization and VRAM of a job or of "other"."""
    parts = [f"cpu {u['cpu']:.1f}" if u.get("cpu") is not None else "cpu ?", f"ram {fmt_bytes(u.get('mem'))}"]
    if u.get("gpu_util") is not None:
        parts.append(f"gpu {u['gpu_util']:.0f}%")
    if u.get("vram"):
        parts.append(f"vram {fmt_bytes(u['vram'])}")
    return "  ".join(parts)


def cmd_res(args) -> int:
    rows = [r for r in snapshots(load_jobs()) if r["status"] in ACTIVE]
    result = resources(rows)
    if args.json:
        print(json.dumps({"schema": JSON_SCHEMA, **result}))
        return 0
    print(f"means over the last {fmt_duration(result['interval_s'])}; RAM and VRAM now. used / reserved per job")
    for node in result["nodes"]:
        reserved = node["reserved"]
        coarse = (" (CPU, RAM: Slurm's load average and free memory, minutes old, or at least what your jobs use)"
                  if node.get("coarse") else "")
        print(f"\n{node['host']}{' (this machine)' if node.get('local') else ''}{coarse}")
        cpu = f"{node['cpu']:.1f}" if node.get("cpu") is not None else "?"
        jobs_note = lambda flag: " (your jobs; Slurm reports less)" if node.get(flag) else ""
        if node.get("coarse"):  # a Slurm node: reserved first, a reserved core is not free whatever the load says
            print(f"  CPU    reserved {reserved['cpus']:>4g} / {node.get('cpus') or '?'} cores   used {cpu}"
                  f"{jobs_note('cpu_from_jobs')}")
            print(f"  RAM    reserved {fmt_bytes(reserved['mem']):>4} / {fmt_bytes(node.get('mem_total'))}   used "
                  f"{fmt_bytes(node.get('mem'))}{jobs_note('mem_from_jobs')}")
        else:
            print(f"  CPU    {cpu:>6} / {node.get('cpus') or '?'} cores   reserved {reserved['cpus']:g}")
            print(f"  RAM    {fmt_bytes(node.get('mem')):>6} / {fmt_bytes(node.get('mem_total'))}   reserved "
                  f"{fmt_bytes(reserved['mem'])}")
        for gpu in node.get("gpus") or []:
            print(f"  GPU{gpu['index']}   {gpu['util']:5.0f}%   vram {fmt_bytes(gpu['vram'])} / {fmt_bytes(gpu['vram_total'])}"
                  f"   {gpu['name']}")
        if node.get("gpu_count"):
            why = "" if node.get("gpus") else (" (not measured: no job holds a GPU)" if node.get("local")
                                                else " (utilization not available)")
            print(f"  GPUs   reserved {reserved['gpus']:g} / {node['gpu_count']}{why}")
        for job in node["jobs"]:
            r = job["reserved"] or {}
            pair = (lambda used, rsv, fmt: f"{fmt(used) if used is not None else '?'}"
                    f"{'/' + fmt(rsv) if rsv is not None else ''}")
            parts = [f"cpu {pair(job.get('cpu'), r.get('cpus'), lambda v: f'{v:.1f}')}",
                     f"ram {pair(job.get('mem'), r.get('mem'), fmt_bytes)}"]
            if job.get("gpu_util") is not None or r.get("gpus"):
                util = "?" if job.get("gpu_util") is None else f"{job['gpu_util']:.0f}%"
                parts.append(f"gpu {util}" + (f" of {r['gpus']:g}" if r.get("gpus") else ""))
            if job.get("vram"):
                parts.append(f"vram {fmt_bytes(job['vram'])}")
            source = f"  [{r['source']}]" if r else "  [no budget]"
            usage = "  ".join(parts) if job["measured"] else "no usage data"
            print(f"    {job['id']}  {(job['name'] or '')[:36]:<36}  {usage}{source}")
        if node.get("other"):
            print(f"    {'other':<11}  {'(not started through jobrun)':<36}  {fmt_usage(node['other'])}")
        others = [j for j in (node.get("slurm") or {}).get("jobs", []) if not j["mine"]]
        if others:
            print(f"    Slurm jobs of others: {len(others)} ({', '.join(sorted({j['user'] for j in others}))})")
    return 0


def cmd_show(args) -> int:
    print(json.dumps(snapshots(resolve([args.id]))[0], indent=1))
    return 0


def select(args, missing_ok: bool = False, workers: bool = False) -> list[dict]:
    """Jobs named by ids and/or --group; missing_ok tolerates ids and groups that are already gone. workers: also
    the members of the selected pools (to stop or remove them with their pool)."""
    if not args.ids and not args.group:
        sys.exit("jobrun: give job ids or --group")
    metas = resolve(args.ids, missing_ok) if args.ids else []
    if args.group:
        members = [m for m in load_jobs() if m.get("group") == args.group]
        if not members and not missing_ok:
            sys.exit(f"jobrun: no jobs in group {args.group!r}")
        metas += [m for m in members if m["id"] not in {x["id"] for x in metas}]
    pools = {m["id"] for m in metas if m.get("kind") == "pool"}
    if workers and pools:
        metas += [m for m in load_jobs() if m.get("pool") in pools and m["id"] not in {x["id"] for x in metas}]
    return metas


def signal_local(meta: dict, sig: int) -> list[int]:
    """Signal a local job's process group and descendants; returns the signalled pids."""
    pid = meta["pid"]
    if not pid_alive(pid, meta.get("proc_start")):
        return []
    targets = [pid] + descendants(pid)
    if meta.get("pgid"):
        try:
            os.killpg(meta["pgid"], sig)
        except ProcessLookupError:
            pass
    for target in targets:
        try:
            os.kill(target, sig)
        except ProcessLookupError:
            pass
    return targets


def cmd_stop(args) -> int:
    """SIGTERM every selected job at once, then one shared grace period before SIGKILL."""
    rc, local = 0, []
    for meta, row in zip(metas := select(args, workers=True), snapshots(metas)):
        if row["status"] not in ACTIVE:
            print(f"{meta['id']} is not running ({row['status']})")
            continue
        _file(meta["id"], ".stopped").touch()
        if meta.get("kind") == "pool":  # its workers follow in metas
            print(f"stopping pool {meta['id']}  {meta.get('name')}")
            continue
        if row["status"] == "waiting" and meta.get("kind") == "process":  # only the supervisor exists yet
            meta = {**meta, "pid": meta.get("supervisor_pid"), "pgid": None, "proc_start": None}
        if meta.get("kind") == "slurm" or (meta.get("slurm_id") and meta.get("host") != HOST):
            cmd = ["scancel", str(meta["slurm_id"])]
            (HOME / "sacct-cache.json").unlink(missing_ok=True)
        elif meta.get("host") != HOST:
            target = f"-{meta['pgid']}" if meta.get("pgid") else str(meta["pid"])
            cmd = ["ssh", "-o", "BatchMode=yes", meta["host"], "kill", "-KILL" if args.force else "-TERM", "--", target]
        else:
            local += signal_local(meta, signal.SIGKILL if args.force else signal.SIGTERM)
            print(f"stopped {meta['id']}  {meta.get('name')}")
            continue
        out = subprocess.run(cmd, capture_output=True, text=True)
        if out.returncode:
            rc = 1
            print(f"{meta['id']}: {shlex.join(cmd)} failed: {out.stderr.strip()}", file=sys.stderr)
        else:
            print(f"stopped {meta['id']}  {meta.get('name')}")
    deadline = time.time() + args.grace
    while time.time() < deadline and any(pid_alive(t) for t in local):
        time.sleep(0.2)
    for target in local:
        if pid_alive(target):
            try:
                os.kill(target, signal.SIGKILL)
            except ProcessLookupError:
                pass
    return rc


def _caller() -> str:
    """The parent process's command line, e.g. the shell, agent or VS Code extension that called jobrun."""
    try:
        raw = Path(f"/proc/{os.getppid()}/cmdline").read_bytes()
    except OSError:
        return "?"
    return " ".join(raw.decode(errors="replace").split("\0")).strip()[:200] or "?"


def record_removal(meta: dict, row: dict | None, why: str) -> None:
    """Append to removed.log: how the job ended, for waits and dependencies that look for it later, and who removed
    it, so that a job missing from the list can be traced."""
    row = row or {}
    entry = {"removed": time.time(), "id": meta["id"], "name": meta.get("name"), "status": row.get("status"),
             "ended": row.get("ended"), "pool": meta.get("pool"), "role": meta.get("role"),
             "by": f"jobrun {why}", "host": socket.gethostname(), "pid": os.getpid(), "parent": _caller()}
    try:
        with open(HOME / "removed.log", "a") as handle:
            handle.write(json.dumps(entry) + "\n")
    except OSError:
        pass


def removed_jobs() -> dict[str, dict]:
    """The removed.log entries by job id (the newest per id); lines that are not JSON are skipped."""
    found = {}
    try:
        with open(HOME / "removed.log") as handle:
            for line in handle:
                try:
                    entry = json.loads(line)
                except ValueError:
                    continue
                if isinstance(entry, dict) and entry.get("id"):
                    found[entry["id"]] = entry
    except OSError:
        pass
    return found


def remove(meta: dict, keep_log: bool = False, row: dict | None = None, why: str = "rm") -> None:
    """Drop a job from the registry, with the log if jobrun wrote it; logs it only read are kept. Every removal is
    recorded in removed.log with the job's row (status, end), why (the jobrun command) and the calling process."""
    record_removal(meta, row, why)
    for suffix in (".json", ".exit.json", ".hb", ".stopped", ".state.json", ".trim"):
        _file(meta["id"], suffix).unlink(missing_ok=True)
    for scan in JOBS.glob(f"{meta['id']}.scan*.json"):
        scan.unlink(missing_ok=True)
    log = meta.get("log")
    if log and not keep_log and (meta.get("log_owned") or Path(log).parent == LOGS):
        Path(log).unlink(missing_ok=True)


def cmd_rm(args) -> int:
    for meta, row in zip(metas := select(args, missing_ok=True, workers=True), snapshots(metas)):
        empty_pool = row.get("kind") == "pool" and not row.get("workers") and not row.get("monitors")
        if row["status"] in ACTIVE and not args.force and not empty_pool:  # an empty pool has nothing to stop
            print(f"{meta['id']} is {row['status']}; stop it first or pass --force", file=sys.stderr)
            continue
        remove(meta, args.keep_log, row, "rm" + (" --force" if args.force else ""))
        print(f"removed {meta['id']}  {meta.get('name')}")
    return 0


def clean(days: float, why: str = "clean") -> int:
    """Remove finished jobs that ended more than days ago, with the logs jobrun wrote; returns how many."""
    metas = load_jobs()
    removed = 0
    for meta, row in zip(metas, snapshots(metas)):
        if row["status"] not in ACTIVE and not visible(row, days):
            remove(meta, row=row, why=f"{why} --days {days:g}")
            removed += 1
    return removed


AUTO_CLEAN_EVERY_S = 3600


def auto_clean() -> None:
    """The auto_clean_days setting: at most once an hour, from `ls` (every open Job Board) and `run`."""
    days = setting("auto_clean_days")
    stamp = HOME / "auto-clean"
    if days <= 0 or time.time() - (_mtime(stamp) or 0) < AUTO_CLEAN_EVERY_S:
        return
    try:
        stamp.touch()  # first, so that dashboards refreshing at the same time do not all clean
        clean(days, "auto-clean (auto_clean_days)")
    except OSError:
        pass


def cmd_clean(args) -> int:
    print(f"removed {clean(args.days)} finished jobs")
    return 0


def cmd_config(args) -> int:
    """Show or change the settings in the registry's config.json."""
    data = dict(config_file())
    changed = False
    for item in args.items:
        key, sep, value = item.partition("=")
        if key not in SETTINGS:
            sys.exit(f"jobrun config: unknown setting {key}; one of {', '.join(SETTINGS)}")
        if sep:
            try:
                number = float(value)
            except ValueError:
                sys.exit(f"jobrun config: {key} needs a number, not {value!r}")
            changed = changed or data.get(key) != number  # every VS Code window syncs on start: write only changes
            data[key] = number
    for key in args.unset or []:
        if key not in SETTINGS:
            sys.exit(f"jobrun config: unknown setting {key}; one of {', '.join(SETTINGS)}")
        changed = data.pop(key, None) is not None or changed
    if changed:
        HOME.mkdir(parents=True, exist_ok=True)
        write_json(CONFIG_PATH, data)
    keys = [item for item in args.items if "=" not in item]
    if args.json:
        print(json.dumps({key: setting(key) for key in SETTINGS}))
    elif keys:
        for key in keys:
            print(f"{setting(key):g}")
    elif not args.items and not args.unset:
        for key, (default, text) in SETTINGS.items():
            source = ("env" if os.environ.get(f"JOBRUN_{key.upper()}") else
                      "config" if key in config_file() else "default")
            print(f"{key:<18} {setting(key):<8g} ({source})  {text}")
        print(f"\nfile: {CONFIG_PATH}")
    return 0


def _wait_line(row: dict, detail: str) -> str:
    return f"{row['id']}  {row['status']:<9} {row['name']}  {detail}".rstrip()


def cmd_wait(args) -> int:
    """Block until the selected jobs finish; exit 0 only if all of them are done. With --any, return as soon as
    one of them ends, reports a new JOBRUN_EVENT or starts to stall, or a worker or monitor of a selected pool
    ends; exit 0 only if every trigger is a job that finished successfully."""
    selected = select(args)
    ids = list(dict.fromkeys(m["id"] for m in selected))
    pools = {m["id"] for m in selected if m.get("kind") == "pool"}

    def load() -> dict[str, dict]:
        """Rows of the selected jobs and, with --any, of the selected pools' members (also those that joined since)."""
        metas = [m for m in (read_json(_file(i, ".json")) for i in ids) if m]
        if args.any and pools:
            metas += [m for m in load_jobs() if m.get("pool") in pools and m["id"] not in ids]
        return {r["id"]: r for r in snapshots(metas)}

    if not args.any:
        while True:
            rows = list(load().values())
            if all(r["status"] not in ACTIVE for r in rows):
                break
            time.sleep(args.interval)
        for r in rows:
            print(_wait_line(r, f"{fmt_progress(r)}  {fmt_duration(r['elapsed_s'])}"))
        # a pool whose workers all succeeded with items left (incomplete) needs a look before the next step
        return 0 if rows and all(r["status"] == "done" and not r.get("incomplete") for r in rows) else 1
    # what counts as new: events, stalls and pool members that end from now on, or, with --since, from that time on
    # (the rearm line of the previous wait), so nothing that happened between two waits is lost
    since = args.since
    checked = time.time()
    rows = load()

    def ended_by(row: dict) -> float:
        return row.get("ended") or row.get("started") or 0

    def old_events(row: dict) -> int:
        if since is None:
            return row.get("event_count", 0)
        return row.get("event_count", 0) - sum(1 for e in row.get("events") or [] if (e.get("at") or 0) > since)

    baseline = {i: (old_events(r), bool(r.get("stalled"))) for i, r in rows.items()}
    # pool members whose end is not news; a worker that fails is also a pool event (pool_row), reported through it
    members_ended = {i for i, r in rows.items() if r.get("pool") in pools and r["status"] not in ACTIVE
                     and (since is None or ended_by(r) <= since)}
    removed_seen = set()
    if since is None:  # removals before the wait started are not news either
        removed_seen = {i for i, e in removed_jobs().items() if e.get("pool") in pools or i in ids}
    previous = dict(rows)
    while True:
        lines, ok = [], True
        removed = None
        for job_id in ids:
            row = rows.get(job_id)
            if row is None:
                removed = removed if removed is not None else removed_jobs()
                entry = removed.get(job_id) or {}
                status = entry.get("status") or "?"
                lines.append(f"{job_id}  {status:<9} {entry.get('name') or ''}  removed from the registry")
                ok = ok and status == "done"
                continue
            seen_events, was_stalled = baseline.get(job_id, (0, False))
            if row["status"] not in ACTIVE:
                lines.append(_wait_line(row, f"{fmt_progress(row)}  after {fmt_duration(row['elapsed_s'])}"))
                ok = ok and row["status"] == "done" and not row.get("incomplete")
            new = row.get("event_count", 0) - seen_events
            if new > 0:
                shown = [e for e in row.get("events") or [] if e.get("n", 0) > seen_events]
                for event in shown:
                    lines.append(_wait_line(row, f"JOBRUN_EVENT {event['level']}: {event['message']}"))
                if new > len(shown):
                    lines.append(_wait_line(row, f"... and {new - len(shown)} earlier events (jobrun show {job_id})"))
                ok = False
            if row["status"] in ACTIVE and row.get("stalled") and not was_stalled:
                lines.append(_wait_line(row, f"stalled: no step for {fmt_duration(row['idle_s'])} "
                                             f"(average step {fmt_duration(row['step_s'])})"))
                ok = False
            baseline[job_id] = (baseline.get(job_id, (0, False))[0], bool(row.get("stalled")))

        def member_line(member: dict, status: str, detail: str) -> str:
            pool = rows.get(member["pool"]) or previous.get(member["pool"]) or {}
            role = "monitor" if member.get("role") == "monitor" else "worker"
            return (f"{member['id']}  {status:<9} {member.get('name') or ''}  "
                    f"{role} of pool {pool.get('name')} ({member['pool']})  {detail}").rstrip()

        for member in rows.values():
            if member.get("pool") not in pools or member["status"] in ACTIVE or member["id"] in members_ended:
                continue
            members_ended.add(member["id"])
            if member["status"] in ("error", "lost"):
                continue  # already a new event of its pool, printed above
            lines.append(member_line(member, member["status"], f"after {fmt_duration(member['elapsed_s'])}"))
            ok = ok and member["status"] == "done"
        # members removed before their end was reported: their pool no longer shows it, removed.log does
        removed = removed if removed is not None else removed_jobs()
        for job_id, entry in removed.items():
            if (entry.get("pool") not in pools or job_id in rows or job_id in members_ended or job_id in removed_seen
                    or (since is not None and job_id not in previous and (entry.get("ended") or 0) <= since)):
                continue
            removed_seen.add(job_id)
            members_ended.add(job_id)
            status = entry.get("status") or "?"
            lines.append(member_line(entry, status, "removed from the registry"))
            ok = ok and status == "done"
        if lines:
            break
        time.sleep(args.interval)
        previous = rows
        checked = time.time()
        rows = load()
    for line in lines:
        print(line)
    active = [i for i in ids if i in rows and rows[i]["status"] in ACTIVE]
    if active:
        shown = [i + (f" ({rows[i]['active_workers']} workers)" if i in pools else "") for i in active]
        print(f"still active: {' '.join(shown)}")
        # the time of the check that found these triggers: a new wait from there misses nothing that happens before
        # it starts (an end in the few seconds the check took may be reported twice)
        print(f"rearm: jobrun wait --any {' '.join(active)} --since {checked:.3f}")
    return 0 if ok else 1


def cmd_edit(args) -> int:
    """Change a registered job, also while it runs; the command itself is not touched."""
    meta = resolve([args.id])[0]
    progress = {"progress_regex": args.progress_regex, "count_regex": args.count_regex, "total": args.total,
                "stages": [s.strip() for s in args.stages.split(",") if s.strip()] if args.stages is not None else None,
                "stage_rules": stage_rules(args.stage_at) if args.stage_at else None,
                "progress_file": str(Path(args.log).expanduser().resolve()) if args.log else None,
                "item_regex": args.item_regex}
    progress = {key: value for key, value in progress.items() if value is not None}
    if args.log and meta.get("kind") != "process":
        progress["log"] = progress.pop("progress_file")  # tracked and Slurm jobs are parsed from their log
    other = {key: value for key, value in (("name", args.name), ("group", args.group)) if value is not None}
    if args.pool is not None:
        if meta.get("kind") == "pool":
            sys.exit("jobrun edit: a pool cannot join a pool")
        other["pool"] = join_pool(args.pool, args.group, inherit=meta.get("group")) if args.pool else None
        if args.pool:
            other["group"] = None  # the group belongs to the pool
    if args.role is not None:
        other["role"] = None if args.role == "worker" else args.role
    if budget(args):
        other["budget"] = budget(args, meta.get("budget"))
    if args.after is not None:
        other["after"] = dependencies(args)
        if meta["id"] in other["after"]:
            sys.exit("jobrun edit: a job cannot wait for itself")
    if not progress and not other:
        sys.exit("jobrun edit: nothing to change")
    changes = {**other, **progress}
    if "group" in changes and not changes["group"]:
        changes["group"] = None
    state_path = _file(meta["id"], ".state.json")
    state = read_json(state_path, {}) or {}
    state.pop("final", None)  # recompute the row
    if progress:  # progress from scratch under the new settings
        changes["scan_rev"] = (meta.get("scan_rev") or 0) + 1
        state = {}
    write_json(state_path, state)
    meta = update_meta(meta["id"], **changes)
    print(f"edited {meta['id']}  {meta['name']}")
    return 0


def cmd_log(args) -> int:
    log = resolve([args.id])[0].get("log")
    if not log:
        sys.exit("jobrun log: job has no log")
    if args.path:
        print(log)
        return 0
    os.execvp("tail", ["tail", "-n", str(args.lines), *(["-F"] if args.follow else []), log])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="jobrun", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def progress_args(p):
        p.add_argument("--name", help="display name (default: script name)")
        p.add_argument("--log", help="log file (run: where output goes; track: what to parse)")
        p.add_argument("--progress-regex", help="regex with groups done/total (named or 1, 2) for the log")
        p.add_argument("--count-regex", help="progress = number of log lines matching this regex")
        p.add_argument("--total", type=int, help="total for --count-regex or a done-only --progress-regex")
        p.add_argument("--group", help="group name; the dashboard sums progress over a group's jobs")
        p.add_argument("--stages", help="planned stage names, comma-separated (shown before they are reached)")
        p.add_argument("--stage-at", action="append", metavar="NAME=REGEX",
                       help="stage NAME starts at the first log line matching REGEX (for programs without markers)")
        p.add_argument("--cpus", type=float, help="CPU cores budgeted for the job (shown as reserved; not enforced)")
        p.add_argument("--mem", help="RAM budgeted for the job, e.g. 20G (shown as reserved; not enforced)")
        p.add_argument("--gpus", type=float, help="GPUs budgeted for the job (shown as reserved; not enforced)")
        p.add_argument("--pool", help="join this pool (id or name; created if there is none): a worker of a list "
                                      "that several jobs work through together")
        p.add_argument("--role", choices=("worker", "monitor"),
                       help="in a pool: monitor = only reports the list's progress, works on no items")
        p.add_argument("--item-regex", help="in a pool: each log line matching this regex is one item this worker "
                                            "finished (like a JOBRUN_ITEM line)")

    p = sub.add_parser("run", help="run a command under supervision (detached unless --fg)")
    progress_args(p)
    p.add_argument("--after", action="append", metavar="JOB",
                   help="start only after these jobs (ids or names, comma-separated) finished successfully")
    p.add_argument("--progress-file", help="parse progress from this file instead of the log")
    p.add_argument("--max-log-mb", type=float,
                   help="cap the log at this size by cutting its middle (default: the max_log_mb setting; 0: never)")
    p.add_argument("--fg", action="store_true", help="stay in the foreground and copy output to stdout")
    p.add_argument("cmd", nargs=argparse.REMAINDER, help="command (a single argument runs through the shell)")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("track", help="register an already running process or Slurm job")
    progress_args(p)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--pid", type=int)
    group.add_argument("--slurm", help="Slurm job id")
    p.set_defaults(func=cmd_track)

    p = sub.add_parser("sbatch", help="submit with sbatch and track the job")
    progress_args(p)
    p.add_argument("--after", action="append", metavar="JOB",
                   help="start only after these jobs (ids or names, comma-separated) finished successfully")
    p.add_argument("sbatch_args", nargs=argparse.REMAINDER)
    p.set_defaults(func=cmd_sbatch)

    p = sub.add_parser("pool", help="create a pool: one job that several workers, on any hosts, work through")
    p.add_argument("name")
    p.add_argument("--group", help="group of the pool (its workers take it)")
    p.add_argument("--total", type=int, help="items in the list, when no member reports the list's progress")
    p.set_defaults(func=cmd_pool)

    p = sub.add_parser("ls", help="list jobs")
    p.add_argument("--json", action="store_true")
    p.add_argument("--all", action="store_true", help="include all finished jobs")
    p.add_argument("--days", type=float, default=3, help="show finished jobs of the last N days (default 3)")
    p.set_defaults(func=cmd_ls)

    p = sub.add_parser("res", help="CPU, RAM, GPU and VRAM of the nodes running jobs: use, Slurm reservations, per job")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_res)

    p = sub.add_parser("setup", help="install the command, the VS Code extension and the agent skill (wizard)")
    p.add_argument("--yes", "-y", action="store_true", help="no questions: take the defaults")
    p.add_argument("--agents", help="comma-separated: " + ",".join(AGENT_TARGETS) + " (or none); default: ask, none selected")
    p.add_argument("--instructions", action=argparse.BooleanOptionalAction, default=None,
                   help="add the one-line jobrun instruction to the agents' global instruction files (default: ask; yes)")
    p.add_argument("--no-vscode", action="store_true", help="skip the VS Code extension")
    p.add_argument("--link", action="store_true", help="link the command to this file instead of copying it (development)")
    p.set_defaults(func=cmd_setup)

    p = sub.add_parser("uninstall", help="remove what `jobrun setup` installed")
    p.add_argument("--yes", "-y", action="store_true", help="no questions")
    p.add_argument("--purge", action="store_true", help="also delete the job registry with its logs")
    p.add_argument("--force", action="store_true", help="also remove installed files that were changed since")
    p.set_defaults(func=cmd_uninstall)

    p = sub.add_parser("demo", help="start a few short example jobs to see the Job Board in action")
    p.set_defaults(func=cmd_demo)

    p = sub.add_parser("show", help="print one job as JSON")
    p.add_argument("id")
    p.set_defaults(func=cmd_show)

    p = sub.add_parser("stop", help="stop jobs (SIGTERM, SIGKILL after --grace; scancel for Slurm)")
    p.add_argument("ids", nargs="*")
    p.add_argument("--group", help="all jobs of this group")
    p.add_argument("--force", action="store_true", help="SIGKILL right away")
    p.add_argument("--grace", type=float, default=10)
    p.set_defaults(func=cmd_stop)

    p = sub.add_parser("rm", help="remove jobs from the registry, with the logs jobrun wrote")
    p.add_argument("ids", nargs="*")
    p.add_argument("--group", help="all jobs of this group")
    p.add_argument("--force", action="store_true", help="also remove running jobs (does not stop them)")
    p.add_argument("--keep-log", action="store_true", help="keep logs even if jobrun wrote them")
    p.set_defaults(func=cmd_rm)

    p = sub.add_parser("clean", help="remove finished jobs older than --days")
    p.add_argument("--days", type=float, default=7)
    p.set_defaults(func=cmd_clean)

    p = sub.add_parser("config", help="show or change settings (stall detection, log cap, auto-clean)",
                       description="Settings in $JOBRUN_HOME/config.json, shared by every machine that uses the "
                                   "registry. JOBRUN_<KEY> environment variables take precedence.")
    p.add_argument("items", nargs="*", metavar="KEY[=VALUE]", help="show KEY, or set it to VALUE")
    p.add_argument("--unset", action="append", metavar="KEY", help="back to the default")
    p.add_argument("--json", action="store_true", help="all current values as JSON")
    p.set_defaults(func=cmd_config)

    p = sub.add_parser("edit", help="change name, group, dependencies or progress settings of a job (also running)")
    p.add_argument("id")
    progress_args(p)
    p.add_argument("--after", action="append", metavar="JOB", help="replace the dependencies ('' for none)")
    p.set_defaults(func=cmd_edit)

    p = sub.add_parser("wait", help="block until jobs finish (exit 1 if any did not succeed); --any: until one "
                                    "ends, reports a JOBRUN_EVENT or stalls")
    p.add_argument("ids", nargs="*")
    p.add_argument("--group", help="all jobs of this group")
    p.add_argument("--any", action="store_true",
                   help="return as soon as one job ends, reports a new JOBRUN_EVENT or starts to stall (one line per "
                        "trigger; exit 0 only if every trigger is a job that finished successfully)")
    p.add_argument("--interval", type=float, default=30, help="seconds between checks (default 30)")
    p.add_argument("--since", type=float, metavar="TIME",
                   help="with --any: also count events, pool members that ended and removals since this Unix time "
                        "(the `rearm:` line of the previous wait prints it), not only those after the wait starts")
    p.set_defaults(func=cmd_wait)

    p = sub.add_parser("log", help="tail a job's log")
    p.add_argument("id")
    p.add_argument("-f", "--follow", action="store_true")
    p.add_argument("-n", "--lines", type=int, default=50)
    p.add_argument("--path", action="store_true", help="only print the log path")
    p.set_defaults(func=cmd_log)

    p = sub.add_parser("_supervise")
    p.add_argument("job_id")
    p.set_defaults(func=lambda a: supervise(a.job_id))

    args = parser.parse_args(argv)
    try:
        return args.func(args) or 0
    except BrokenPipeError:  # `jobrun ls | head`
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return 0


if __name__ == "__main__":
    sys.exit(main())
