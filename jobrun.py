#!/usr/bin/env python3
"""jobrun: register long-running jobs and follow their progress, rate and ETA.

The registry is a directory of small files ($JOBRUN_HOME, default ~/.jobs):

    jobs/<id>.json        metadata, written at registration
    jobs/<id>.exit.json   exit code and end time, written by the supervisor
    jobs/<id>.hb          heartbeat, touched by the supervisor while the command runs
    jobs/<id>.stopped     marker written by `jobrun stop`
    jobs/<id>.state.json  progress samples and the cached final row, written by readers
    logs/<id>.log         default log of supervised commands

Progress is parsed from the job's log, so commands need no changes. Recognised by default:
tqdm bars ("412/1000 [01:23<..."), bracketed counters ("[412/1000] ...") and explicit
"JOBRUN_PROGRESS 412/1000" lines. Stdlib only, so it runs under any python3 >= 3.8.
"""
from __future__ import annotations

import argparse
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
from datetime import datetime
from pathlib import Path

HOME = Path(os.environ.get("JOBRUN_HOME") or Path.home() / ".jobs")
JOBS = HOME / "jobs"
LOGS = HOME / "logs"
HOST = socket.gethostname()

HEARTBEAT_S = float(os.environ.get("JOBRUN_HEARTBEAT_S", 30))
STALE_S = 5 * HEARTBEAT_S
CHECK_S = min(HEARTBEAT_S, 2)  # supervisor wake-up: log scan (stages, progress), heartbeat, log size check
MAX_LOG_MB = float(os.environ.get("JOBRUN_MAX_LOG_MB", 512))  # cap for logs jobrun writes; 0 disables
TRIM_KEEP_BYTES = 1 << 20  # tail kept when a log is trimmed
LAST_LINE_BYTES = 16 * 1024  # tail read for the last log line
WAIT_POLL_S = float(os.environ.get("JOBRUN_WAIT_POLL_S", 15))  # dependency checks of a waiting job
COUNT_CHUNK_BYTES = 8 << 20  # count mode reads in chunks ...
COUNT_BUDGET_BYTES = 256 << 20  # ... and at most this much per refresh
SACCT_TTL_S = 30
RATE_WINDOW_S = 15 * 60
JSON_SCHEMA = 2  # version of `ls --json`; bump on incompatible changes (the VS Code extension checks it)
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


def trim_log(job_id: str, log: str, max_bytes: int) -> None:
    """Keep only the last TRIM_KEEP_BYTES of whole lines once the log exceeds max_bytes.

    The command writes with O_APPEND, so it keeps appending at the new end; output written between
    reading the tail and truncating is lost, which is acceptable for a log that big.
    """
    try:
        size = os.path.getsize(log)
        if size <= max_bytes:
            return
        with open(log, "r+b") as handle:
            handle.seek(max(0, size - TRIM_KEEP_BYTES))
            tail = handle.read(TRIM_KEEP_BYTES)
            tail = tail[tail.find(b"\n") + 1:]
            marker = f"jobrun: log exceeded {max_bytes >> 20} MB, dropped the oldest {size - len(tail)} bytes\n".encode()
            handle.truncate(0)
        with open(log, "ab") as handle:
            handle.write(marker + tail)
        _file(job_id, ".trim").write_text(str(dropped_bytes(job_id) + size - len(tail) - len(marker)))
    except OSError:
        pass


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
        deps = [read_json(_file(dep, ".json")) for dep in (read_json(_file(job_id, ".json")) or {}).get("after") or []]
        if not all(deps):
            return "a dependency was removed"
        rows = snapshots(deps)
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
    meta = update_meta(job_id, supervisor_pid=os.getpid(), supervisor_scans=True, supervisor_version=SUPERVISOR_VERSION,
                       waiting=bool((read_json(meta_path) or {}).get("after")))
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

        max_bytes = meta.get("max_log_bytes") or 0
        meta_mtime = _mtime(meta_path)
        while True:
            _beat(job_id)
            if _mtime(meta_path) != meta_mtime:  # `jobrun edit` changed the settings
                meta, meta_mtime = read_json(meta_path) or meta, _mtime(meta_path)
            update_scan(meta)  # before trimming, so no stage marker is trimmed unseen
            if max_bytes:
                trim_log(job_id, meta["log"], max_bytes)
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
            alive = (pid_alive(pid, meta.get("proc_start")) if pid is not None
                     else bool(meta.get("supervisor_pid")) and pid_alive(meta["supervisor_pid"]))
        else:
            beat = _mtime(heartbeat)
            alive = beat is not None and now - beat < STALE_S
        if pid is None:
            if meta.get("waiting") and alive:
                return "waiting", None, None, None
            if now - (meta.get("started") or 0) < STALE_S:
                return "starting", None, None, None
            return "lost", None, None, "the supervisor vanished before the command started"
        if alive:
            return "running", None, None, None
        if stopped:
            return "stopped", _mtime(heartbeat), None, None
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


def _new_stage(name: str | None, started: float, index=None, of=None) -> dict:
    return {"name": name, "index": index, "of": of, "started": started, "ended": None, "done": None, "total": None}


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
                stage["done"], stage["total"] = int(explicit["done"]), int(explicit["total"])
                return
        if count:
            if count.search(line):
                stage["done"], stage["total"] = (stage["done"] or 0) + 1, default_total
        elif patterns is not PROGRESS_PATTERNS or "/" in line:  # every default pattern contains a slash
            match = next((m for m in (p.search(line) for p in patterns) if m), None)
            if match:
                stage["done"], stage["total"] = _match(match, default_total)

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
                    stages[-1]["done"], stages[-1]["total"] = _match(match, default_total)
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


def update_rate(state: dict, stage_key: int, done: int, total: int | None, now: float,
                started: float) -> float | None:
    """Items per second over the last RATE_WINDOW_S of the current stage, or its average until then."""
    samples = state.get("samples") or []
    if samples and (state.get("stage") != stage_key or done < samples[-1][1] or total != state.get("total")):
        samples = []
    state.update(stage=stage_key, total=total)
    if not samples or samples[-1][1] != done:
        samples.append([now, done])
        state["progress_at"] = now
    older = [s for s in samples if now - s[0] > RATE_WINDOW_S]
    samples = older[-1:] + [s for s in samples if now - s[0] <= RATE_WINDOW_S]
    state["samples"] = samples
    first_t, first_done = samples[0]
    if now - first_t >= 60 and done > first_done:
        return (done - first_done) / (now - first_t)
    return done / (now - started) if done and now > started else None


def stage_rows(meta: dict, stages: list[dict], status: str, ended: float | None) -> list[dict]:
    """Stages for display: finished, current, and planned (--stages) ones not reached yet."""
    if len(stages) == 1 and stages[0]["name"] is None and not meta.get("stages"):
        return []
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
        rate = state["rate"] = update_rate(state, len(stages), done, total, now, max(started, current["started"] or 0))
    eta = None
    if status in ACTIVE and rate and total and done is not None and done < total:
        eta = now + (total - done) / rate
    planned = meta.get("stages") or []
    stage_list = stage_rows(meta, stages, status, ended)
    row = {key: meta.get(key) for key in ("id", "name", "group", "kind", "host", "slurm_id", "pid", "cwd", "log")}
    row.update(
        log_owned=bool(meta.get("log_owned")), after=meta.get("after") or [],
        cmd=shlex.join(meta.get("cmd") or []), status=status, exit_code=exit_code, started=started, ended=ended,
        elapsed_s=None if status == "waiting" or (meta.get("after") and meta.get("pid") is None and
                                                   meta.get("kind") == "process") else (ended or now) - started,
        done=done, total=total, fraction=done / total if done is not None and total else None,
        rate_per_s=rate, eta=eta, last_line=last_log_line(meta), progress_at=state.get("progress_at"),
        # at or past the total but still running: an untracked final phase (or a too small --total)
        finishing=status in ACTIVE and bool(total) and done is not None and done >= total,
        stage=current["name"] if stage_list else None,
        stage_no=(current["index"] or (planned.index(current["name"]) + 1 if current["name"] in planned
                                       else len(stages))) if stage_list else None,
        stage_of=(current["of"] or len(planned) or None) if stage_list else None,
        stages=stage_list, reason=reason,
    )
    row["host"] = slurm_info.get("host") or row["host"]
    if status in FINAL:
        state["final"] = row
    try:
        write_json(state_path, state)
    except OSError:
        pass
    return row


def snapshots(metas: list[dict]) -> list[dict]:
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
        row.setdefault("after", [])  # final rows cached by older versions lack it
        deps = [d for d in (by_id.get(dep) or snapshot_or_none(dep, slurm) for dep in row["after"]) if d]
        row["after_names"] = [d["name"] for d in deps]
        if row["status"] == "running" and any(d["status"] in ACTIVE for d in deps):
            # registered with --after but started early: the command waits for its dependency by itself
            row.update(status="waiting", elapsed_s=None, eta=None, rate_per_s=None)
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
    return rows


def snapshot_or_none(job_id: str, slurm: dict) -> dict | None:
    meta = read_json(_file(job_id, ".json"))
    return snapshot(meta, slurm) if meta else None


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
            "elapsed_s": (ended or time.time()) - started,
        })
    return groups


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


def fmt_progress(row: dict) -> str:
    if row.get("status") == "waiting":
        return "waiting for " + ", ".join(row.get("waiting_for") or ["dependencies"])
    if row.get("status") == "cancelled":
        return row.get("reason") or "never started"
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

def progress_options(args) -> dict:
    return {"group": args.group, "progress_regex": args.progress_regex, "count_regex": args.count_regex,
            "total": args.total, "stages": [s.strip() for s in args.stages.split(",")] if args.stages else None,
            "stage_rules": stage_rules(args.stage_at)}


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
    job_id = allocate_id()
    log = Path(args.log).expanduser().resolve() if args.log else LOGS / f"{job_id}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    meta = {"id": job_id, "name": args.name or default_name(cmd), "kind": "process", "cmd": cmd,
            "cwd": os.getcwd(), "host": HOST, "log": str(log), "started": time.time(),
            "slurm_id": os.environ.get("SLURM_JOB_ID"), "log_owned": True,
            "max_log_bytes": int(args.max_log_mb * (1 << 20)),
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
    """Rows to show; a group stays complete as long as any member is visible."""
    rows = snapshots(load_jobs())
    shown = {r.get("group") for r in rows if r.get("group") and visible(r, days)}
    return [r for r in rows if visible(r, days) or r.get("group") in shown]


def cmd_ls(args) -> int:
    rows = visible_rows(None if args.all else args.days)
    groups = group_rows(rows)
    if args.json:
        print(json.dumps({"schema": JSON_SCHEMA, "jobs": rows, "groups": groups}))
        return 0
    if not rows:
        print("no jobs")
        return 0
    table = [("ID", "STATUS", "NAME", "PROGRESS", "RATE", "ETA", "ELAPSED", "WHERE")]

    def line(r, job_id, name, where=""):
        table.append((job_id, r["status"], name, fmt_progress(r), fmt_rate(r["rate_per_s"]),
                      "finishing" if r.get("finishing") else fmt_eta(r["eta"]), fmt_duration(r["elapsed_s"]), where))

    by_name = {g["name"]: g for g in groups}
    for r in dependency_order(rows):
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
            where = (member["host"] or "") + (f" slurm:{member['slurm_id']}" if member["slurm_id"] else "")
            line(member, member["id"], ("  " if group else "") + (member["name"] or "")[:40], where.strip())
    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    for row in table:
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths)).rstrip())
    return 0


def cmd_show(args) -> int:
    print(json.dumps(snapshots(resolve([args.id]))[0], indent=1))
    return 0


def select(args, missing_ok: bool = False) -> list[dict]:
    """Jobs named by ids and/or --group; missing_ok tolerates ids and groups that are already gone."""
    if not args.ids and not args.group:
        sys.exit("jobrun: give job ids or --group")
    metas = resolve(args.ids, missing_ok) if args.ids else []
    if args.group:
        members = [m for m in load_jobs() if m.get("group") == args.group]
        if not members and not missing_ok:
            sys.exit(f"jobrun: no jobs in group {args.group!r}")
        metas += [m for m in members if m["id"] not in {x["id"] for x in metas}]
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
    for meta, row in zip(metas := select(args), snapshots(metas)):
        if row["status"] not in ACTIVE:
            print(f"{meta['id']} is not running ({row['status']})")
            continue
        _file(meta["id"], ".stopped").touch()
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


def remove(meta: dict, keep_log: bool = False) -> None:
    """Drop a job from the registry, with the log if jobrun wrote it; logs it only read are kept."""
    for suffix in (".json", ".exit.json", ".hb", ".stopped", ".state.json", ".trim"):
        _file(meta["id"], suffix).unlink(missing_ok=True)
    for scan in JOBS.glob(f"{meta['id']}.scan*.json"):
        scan.unlink(missing_ok=True)
    log = meta.get("log")
    if log and not keep_log and (meta.get("log_owned") or Path(log).parent == LOGS):
        Path(log).unlink(missing_ok=True)


def cmd_rm(args) -> int:
    for meta, row in zip(metas := select(args, missing_ok=True), snapshots(metas)):
        if row["status"] in ACTIVE and not args.force:
            print(f"{meta['id']} is {row['status']}; stop it first or pass --force", file=sys.stderr)
            continue
        remove(meta, args.keep_log)
        print(f"removed {meta['id']}  {meta.get('name')}")
    return 0


def cmd_clean(args) -> int:
    metas = load_jobs()
    removed = 0
    for meta, row in zip(metas, snapshots(metas)):
        if row["status"] not in ACTIVE and not visible(row, args.days):
            remove(meta)
            removed += 1
    print(f"removed {removed} finished jobs")
    return 0


def cmd_wait(args) -> int:
    """Block until the selected jobs finish; exit 0 only if all of them are done."""
    ids = [m["id"] for m in select(args)]
    while True:
        rows = snapshots([m for m in (read_json(_file(i, ".json")) for i in ids) if m])
        if all(r["status"] not in ACTIVE for r in rows):
            break
        time.sleep(args.interval)
    for r in rows:
        print(f"{r['id']}  {r['status']:<7} {r['name']}  {fmt_progress(r)}  {fmt_duration(r['elapsed_s'])}".rstrip())
    return 0 if rows and all(r["status"] == "done" for r in rows) else 1


def cmd_edit(args) -> int:
    """Change a registered job, also while it runs; the command itself is not touched."""
    meta = resolve([args.id])[0]
    progress = {"progress_regex": args.progress_regex, "count_regex": args.count_regex, "total": args.total,
                "stages": [s.strip() for s in args.stages.split(",") if s.strip()] if args.stages is not None else None,
                "stage_rules": stage_rules(args.stage_at) if args.stage_at else None,
                "progress_file": str(Path(args.log).expanduser().resolve()) if args.log else None}
    progress = {key: value for key, value in progress.items() if value is not None}
    if args.log and meta.get("kind") != "process":
        progress["log"] = progress.pop("progress_file")  # tracked and Slurm jobs are parsed from their log
    other = {key: value for key, value in (("name", args.name), ("group", args.group)) if value is not None}
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

    p = sub.add_parser("run", help="run a command under supervision (detached unless --fg)")
    progress_args(p)
    p.add_argument("--after", action="append", metavar="JOB",
                   help="start only after these jobs (ids or names, comma-separated) finished successfully")
    p.add_argument("--progress-file", help="parse progress from this file instead of the log")
    p.add_argument("--max-log-mb", type=float, default=MAX_LOG_MB,
                   help=f"trim the log to its last MB beyond this size (default {MAX_LOG_MB:g}, 0: never)")
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

    p = sub.add_parser("ls", help="list jobs")
    p.add_argument("--json", action="store_true")
    p.add_argument("--all", action="store_true", help="include all finished jobs")
    p.add_argument("--days", type=float, default=3, help="show finished jobs of the last N days (default 3)")
    p.set_defaults(func=cmd_ls)

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

    p = sub.add_parser("edit", help="change name, group, dependencies or progress settings of a job (also running)")
    p.add_argument("id")
    progress_args(p)
    p.add_argument("--after", action="append", metavar="JOB", help="replace the dependencies ('' for none)")
    p.set_defaults(func=cmd_edit)

    p = sub.add_parser("wait", help="block until jobs finish (exit 1 if any did not succeed)")
    p.add_argument("ids", nargs="*")
    p.add_argument("--group", help="all jobs of this group")
    p.add_argument("--interval", type=float, default=30, help="seconds between checks (default 30)")
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
