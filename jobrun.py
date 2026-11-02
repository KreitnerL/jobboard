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
TAIL_BYTES = 256 * 1024
RATE_WINDOW_S = 15 * 60
ACTIVE = ("starting", "pending", "running", "unknown")
# "lost" may recover (NFS hiccup, supervisor between child exit and exit file), so it is never cached.
FINAL = ("done", "failed", "killed", "timeout", "oom", "ended")

PROGRESS_PATTERNS = [
    re.compile(r"JOBRUN_PROGRESS\s+(?P<done>\d+)\s*/\s*(?P<total>\d+)"),
    re.compile(r"(?P<done>\d+)/(?P<total>\d+)\s*\[\d"),  # tqdm: " 412/1000 [01:23<04:56, 3.21it/s]"
    re.compile(r"\[(?:[^\[\]]*\s)?(?P<done>\d+)/(?P<total>\d+)\]"),  # "[412/1000] ..." or "[cfg 412/1000] ..."
]
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
INTERPRETERS = {"uv", "run", "bash", "sh", "srun", "nohup", "env", "time", "exec"}
SLURM_STATES = {
    "PENDING": "pending", "CONFIGURING": "pending", "REQUEUED": "pending", "SUSPENDED": "pending",
    "RUNNING": "running", "COMPLETING": "running", "COMPLETED": "done", "FAILED": "failed",
    "NODE_FAIL": "failed", "BOOT_FAIL": "failed", "CANCELLED": "killed", "PREEMPTED": "killed",
    "DEADLINE": "timeout", "TIMEOUT": "timeout", "OUT_OF_MEMORY": "oom",
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


def resolve(keys: list[str]) -> list[dict]:
    """Jobs by exact id, unique id substring, or exact name."""
    metas = load_jobs()
    found = []
    for key in keys:
        hits = ([m for m in metas if m["id"] == key] or [m for m in metas if key in m["id"]]
                or [m for m in metas if m.get("name") == key])
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
    if not ids or not shutil.which("sacct"):
        return {}
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
        status = SLURM_STATES.get(state.split()[0] if state else "", "unknown")
        info[slurm_id] = {
            "status": status,
            "exit_code": int(code.split(":")[0]) if code[:1].isdigit() else None,
            "started": _slurm_time(start),
            "ended": None if status in ACTIVE else _slurm_time(end),
            "host": None if nodes in ("", "None assigned") else nodes,
        }
    return info


def _slurm_log(pattern: str, info: dict) -> str:
    """Expand sbatch filename patterns (%j, %x, %u, %A, %a, %N, %%) against sacct fields."""
    pattern = pattern or "slurm-%j.out"
    master, _, task = info["JobID"].partition("_")
    values = {"j": info["JobIDRaw"], "x": info["JobName"], "u": os.environ.get("USER", ""), "A": master,
              "a": task, "N": info["NodeList"], "%": "%"}
    expanded = re.sub(r"%(\d*)([jxuAaN%])", lambda m: values[m.group(2)].zfill(int(m.group(1) or 0)), pattern)
    return os.path.join(info["WorkDir"] or "", expanded)


def slurm_meta(slurm_id: str, attempts: int = 10) -> dict:
    """Metadata from sacct (scontrol is disabled on client machines); retries until the job shows up."""
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
                "log": _slurm_log(info["StdOut"], info), "started": _slurm_time(info["Submit"]) or time.time(),
            }
        if attempt + 1 < attempts:
            time.sleep(0.5)
    return {"kind": "slurm", "slurm_id": slurm_id, "name": f"slurm-{slurm_id}", "started": time.time()}


# ---------------------------------------------------------------- supervisor

def supervise(job_id: str, tee: bool = False) -> int:
    """Run the job's command, keep a heartbeat, record its exit. With tee, also copy output to stdout."""
    meta_path = _file(job_id, ".json")
    meta = read_json(meta_path)
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
            write_json(_file(job_id, ".exit.json"), {"exit_code": 127, "ended": time.time()})
            return 127
        meta.update(pid=proc.pid, pgid=proc.pid, proc_start=proc_start(proc.pid), supervisor_pid=os.getpid())
        write_json(meta_path, meta)

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

        heartbeat = _file(job_id, ".hb")
        while True:
            heartbeat.touch()
            try:
                code = proc.wait(timeout=HEARTBEAT_S)
                break
            except subprocess.TimeoutExpired:
                pass
        if pump:
            pump.join(timeout=5)
    write_json(_file(job_id, ".exit.json"), {"exit_code": code, "ended": time.time()})
    return code if code >= 0 else 128 - code


# ---------------------------------------------------------------- status and progress

def job_status(meta: dict, slurm: dict, now: float) -> tuple[str, float | None, int | None]:
    """(status, ended, exit_code) from the exit file, Slurm, or the process / heartbeat."""
    job_id = meta["id"]
    stopped = _file(job_id, ".stopped").exists()
    exit_info = read_json(_file(job_id, ".exit.json"))
    if exit_info:
        code = exit_info.get("exit_code")
        status = "done" if code == 0 else "killed" if stopped or (code or 0) < 0 else "failed"
        return status, exit_info.get("ended"), code
    kind, pid = meta.get("kind"), meta.get("pid")
    if kind == "slurm":
        info = slurm.get(str(meta.get("slurm_id")))
        return (info["status"], info["ended"], info["exit_code"]) if info else ("unknown", None, None)
    if kind == "process":
        heartbeat = _file(job_id, ".hb")
        if pid is None:
            return ("starting" if now - (meta.get("started") or 0) < STALE_S else "lost"), None, None
        if meta.get("host") == HOST:
            alive = pid_alive(pid, meta.get("proc_start"))
        else:
            beat = _mtime(heartbeat)
            alive = beat is not None and now - beat < STALE_S
        if alive:
            return "running", None, None
        return ("killed" if stopped else "lost"), _mtime(heartbeat), None
    if meta.get("host") != HOST:
        return "unknown", None, None
    if pid_alive(pid, meta.get("proc_start")):
        return "running", None, None
    log = meta.get("log")
    return ("killed" if stopped else "ended"), (_mtime(Path(log)) if log else None) or now, None


def _match(match: re.Match, total: int | None) -> tuple[int, int | None]:
    groups = match.groupdict()
    if "done" in groups:
        done, found = groups["done"], groups.get("total")
    else:
        done, found = match.group(1), match.group(2) if match.re.groups >= 2 else None
    return int(done), int(found) if found else total


def parse_progress(meta: dict, state: dict) -> tuple[int | None, int | None, str | None]:
    """(done, total, last log line). Count mode scans the log incrementally, the rest reads its tail."""
    total = meta.get("total")
    path = meta.get("progress_file") or meta.get("log")
    if not path:
        return None, total, None
    try:
        handle = open(path, "rb")
    except OSError:
        return None, total, None
    done = None
    with handle:
        size = os.fstat(handle.fileno()).st_size
        if meta.get("count_regex"):
            pattern = re.compile(meta["count_regex"])
            offset, count = state.get("offset", 0), state.get("count", 0)
            if size < offset:
                offset, count = 0, 0
            handle.seek(offset)
            chunk = handle.read(size - offset)
            complete = chunk.rfind(b"\n") + 1
            count += sum(1 for line in chunk[:complete].decode(errors="replace").splitlines()
                         if pattern.search(line))
            state.update(offset=offset + complete, count=count)
            done = count
        handle.seek(max(0, size - TAIL_BYTES))
        tail = handle.read().decode(errors="replace")
    lines = [line for line in (ANSI.sub("", raw).strip() for raw in re.split(r"[\r\n]+", tail)) if line]
    if done is None:
        patterns = [re.compile(meta["progress_regex"])] if meta.get("progress_regex") else PROGRESS_PATTERNS
        for line in reversed(lines):
            match = next((m for m in (p.search(line) for p in patterns) if m), None)
            if match:
                done, total = _match(match, total)
                break
    return done, total, lines[-1][:300] if lines else None


def update_rate(state: dict, done: int, total: int | None, now: float, started: float) -> float | None:
    """Items per second over the last RATE_WINDOW_S, or the run average until the window has data."""
    samples = state.get("samples") or []
    if samples and (done < samples[-1][1] or total != state.get("total")):
        samples = []
    state["total"] = total
    if not samples or samples[-1][1] != done:
        samples.append([now, done])
    older = [s for s in samples if now - s[0] > RATE_WINDOW_S]
    samples = older[-1:] + [s for s in samples if now - s[0] <= RATE_WINDOW_S]
    state["samples"] = samples
    first_t, first_done = samples[0]
    if now - first_t >= 60 and done > first_done:
        return (done - first_done) / (now - first_t)
    return done / (now - started) if done and now > started else None


def snapshot(meta: dict, slurm: dict) -> dict:
    """One display row per job; finished jobs are computed once and cached."""
    job_id = meta["id"]
    state_path = _file(job_id, ".state.json")
    state = read_json(state_path, {}) or {}
    if state.get("final"):
        return state["final"]
    now = time.time()
    status, ended, exit_code = job_status(meta, slurm, now)
    slurm_info = (slurm.get(str(meta.get("slurm_id"))) or {}) if meta.get("kind") == "slurm" else {}
    started = slurm_info.get("started") or meta.get("started") or now
    done, total, last_line = parse_progress(meta, state)
    rate = state.get("rate")
    if done is not None and status in ACTIVE:
        rate = state["rate"] = update_rate(state, done, total, now, started)
    eta = None
    if status in ACTIVE and rate and total and done is not None and done < total:
        eta = now + (total - done) / rate
    row = {key: meta.get(key) for key in ("id", "name", "kind", "host", "slurm_id", "pid", "cwd", "log")}
    row.update(
        cmd=shlex.join(meta.get("cmd") or []), status=status, exit_code=exit_code, started=started,
        ended=ended, elapsed_s=(ended or now) - started, done=done, total=total,
        fraction=done / total if done is not None and total else None, rate_per_s=rate, eta=eta,
        last_line=last_line,
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
            fresh = slurm_meta(str(meta["slurm_id"]), attempts=1)
            if fresh.get("log"):
                meta.update({k: v for k, v in fresh.items() if not meta.get(k) or k == "name" and
                             meta["name"] == f"slurm-{meta['slurm_id']}"})
                write_json(_file(meta["id"], ".json"), meta)
    open_slurm = [str(m["slurm_id"]) for m in open_slurm]
    slurm = query_slurm(open_slurm)
    return [snapshot(m, slurm) for m in metas]


# ---------------------------------------------------------------- formatting

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
    if row["done"] is None:
        return ""
    if row["fraction"] is not None:
        return f"{row['fraction'] * 100:3.0f}% {row['done']}/{row['total']}"
    return str(row["done"])


def visible(row: dict, days: float | None) -> bool:
    if days is None or row["status"] not in FINAL + ("lost",):
        return True
    return time.time() - (row["ended"] or row["started"] or 0) <= days * 86400


# ---------------------------------------------------------------- commands

def progress_options(args) -> dict:
    return {"progress_regex": args.progress_regex, "count_regex": args.count_regex, "total": args.total}


def cmd_run(args) -> int:
    cmd = args.cmd[1:] if args.cmd[:1] == ["--"] else args.cmd
    if not cmd:
        sys.exit("jobrun run: no command given")
    job_id = allocate_id()
    log = Path(args.log).expanduser().resolve() if args.log else LOGS / f"{job_id}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    meta = {"id": job_id, "name": args.name or default_name(cmd), "kind": "process", "cmd": cmd,
            "cwd": os.getcwd(), "host": HOST, "log": str(log), "started": time.time(),
            "slurm_id": os.environ.get("SLURM_JOB_ID"),
            "progress_file": str(Path(args.progress_file).expanduser().resolve()) if args.progress_file else None,
            **progress_options(args)}
    write_json(_file(job_id, ".json"), meta)
    if args.fg:
        print(f"jobrun: {job_id}  {meta['name']}  log: {log}", file=sys.stderr, flush=True)
        return supervise(job_id, tee=True)
    subprocess.Popen([sys.executable, os.path.abspath(__file__), "_supervise", job_id],
                     start_new_session=True, stdin=subprocess.DEVNULL,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"{job_id}  {meta['name']}  log: {log}")
    return 0


def cmd_track(args) -> int:
    job_id = allocate_id()
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
        meta = slurm_meta(str(args.slurm))
        if args.log:
            meta["log"] = str(Path(args.log).expanduser().resolve())
    meta.update(id=job_id, **progress_options(args))
    if args.name:
        meta["name"] = args.name
    write_json(_file(job_id, ".json"), meta)
    print(f"{job_id}  {meta['name']}  log: {meta.get('log')}")
    return 0


def cmd_sbatch(args) -> int:
    rest = args.sbatch_args[1:] if args.sbatch_args[:1] == ["--"] else args.sbatch_args
    out = subprocess.run(["sbatch", "--parsable", *rest], capture_output=True, text=True)
    if out.returncode:
        sys.stderr.write(out.stderr)
        return out.returncode
    args.slurm, args.pid = out.stdout.strip().split(";")[0], None
    print(f"Submitted batch job {args.slurm}")
    return cmd_track(args)


def cmd_ls(args) -> int:
    rows = [r for r in snapshots(load_jobs()) if visible(r, None if args.all else args.days)]
    if args.json:
        print(json.dumps(rows))
        return 0
    if not rows:
        print("no jobs")
        return 0
    table = [("ID", "STATUS", "NAME", "PROGRESS", "RATE", "ETA", "ELAPSED", "WHERE")]
    for r in rows:
        where = r["host"] or ""
        if r["slurm_id"]:
            where += f" slurm:{r['slurm_id']}"
        table.append((r["id"], r["status"], (r["name"] or "")[:40], fmt_progress(r), fmt_rate(r["rate_per_s"]),
                      fmt_eta(r["eta"]), fmt_duration(r["elapsed_s"]), where.strip()))
    widths = [max(len(row[i]) for row in table) for i in range(len(table[0]))]
    for row in table:
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths)).rstrip())
    return 0


def cmd_show(args) -> int:
    print(json.dumps(snapshots(resolve([args.id]))[0], indent=1))
    return 0


def kill_local(meta: dict, force: bool, grace: float) -> None:
    pid = meta["pid"]
    if not pid_alive(pid, meta.get("proc_start")):
        return
    targets = [pid] + descendants(pid)
    sig = signal.SIGKILL if force else signal.SIGTERM
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
    deadline = time.time() + grace
    while time.time() < deadline and any(pid_alive(t) for t in targets):
        time.sleep(0.2)
    for target in targets:
        if pid_alive(target):
            try:
                os.kill(target, signal.SIGKILL)
            except ProcessLookupError:
                pass


def cmd_stop(args) -> int:
    rc = 0
    for meta, row in zip(metas := resolve(args.ids), snapshots(metas)):
        if row["status"] not in ACTIVE:
            print(f"{meta['id']} is not running ({row['status']})")
            continue
        _file(meta["id"], ".stopped").touch()
        if meta.get("kind") == "slurm" or (meta.get("slurm_id") and meta.get("host") != HOST):
            cmd = ["scancel", str(meta["slurm_id"])]
        elif meta.get("host") != HOST:
            target = f"-{meta['pgid']}" if meta.get("pgid") else str(meta["pid"])
            cmd = ["ssh", "-o", "BatchMode=yes", meta["host"], "kill", "-KILL" if args.force else "-TERM", "--", target]
        else:
            kill_local(meta, args.force, args.grace)
            print(f"stopped {meta['id']}  {meta.get('name')}")
            continue
        out = subprocess.run(cmd, capture_output=True, text=True)
        if out.returncode:
            rc = 1
            print(f"{meta['id']}: {shlex.join(cmd)} failed: {out.stderr.strip()}", file=sys.stderr)
        else:
            print(f"stopped {meta['id']}  {meta.get('name')}")
    return rc


def remove(meta: dict) -> None:
    for suffix in (".json", ".exit.json", ".hb", ".stopped", ".state.json"):
        _file(meta["id"], suffix).unlink(missing_ok=True)
    log = meta.get("log")
    if log and Path(log).parent == LOGS:
        Path(log).unlink(missing_ok=True)


def cmd_rm(args) -> int:
    for meta, row in zip(metas := resolve(args.ids), snapshots(metas)):
        if row["status"] in ACTIVE and not args.force:
            print(f"{meta['id']} is {row['status']}; stop it first or pass --force", file=sys.stderr)
            continue
        remove(meta)
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

    p = sub.add_parser("run", help="run a command under supervision (detached unless --fg)")
    progress_args(p)
    p.add_argument("--progress-file", help="parse progress from this file instead of the log")
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
    p.add_argument("ids", nargs="+")
    p.add_argument("--force", action="store_true", help="SIGKILL right away")
    p.add_argument("--grace", type=float, default=10)
    p.set_defaults(func=cmd_stop)

    p = sub.add_parser("rm", help="remove jobs from the registry")
    p.add_argument("ids", nargs="+")
    p.add_argument("--force", action="store_true", help="also remove running jobs (does not stop them)")
    p.set_defaults(func=cmd_rm)

    p = sub.add_parser("clean", help="remove finished jobs older than --days")
    p.add_argument("--days", type=float, default=7)
    p.set_defaults(func=cmd_clean)

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
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
