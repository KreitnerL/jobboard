import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import jobrun as jobrun_module  # noqa: E402

JOBRUN = Path(__file__).resolve().parents[1] / "jobrun.py"


@pytest.fixture
def jobrun(tmp_path, monkeypatch):
    monkeypatch.setenv("JOBRUN_HOME", str(tmp_path / "registry"))
    monkeypatch.setenv("JOBRUN_HEARTBEAT_S", "0.5")

    def call(*args, check=True, cwd=tmp_path):
        out = subprocess.run([sys.executable, str(JOBRUN), *args], capture_output=True, text=True, cwd=cwd)
        if check and out.returncode:
            raise AssertionError(out.stderr)
        return out

    call.home = tmp_path / "registry"
    yield call
    # never leak processes, also when a test fails half-way
    active = [r["id"] for r in json.loads(call("ls", "--json", "--all").stdout)["jobs"]
              if r["status"] in ("starting", "running")]
    if active:
        call("stop", *active, "--force", check=False)


def listing(jobrun):
    return json.loads(jobrun("ls", "--json", "--all").stdout)


def rows(jobrun):
    return {r["name"]: r for r in listing(jobrun)["jobs"]}


def wait_for(jobrun, name, status, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = rows(jobrun).get(name)
        if row and row["status"] == status:
            return row
        time.sleep(0.2)
    raise AssertionError(f"{name} not {status}: {rows(jobrun).get(name)}")


def test_detached_run_parses_tqdm_and_records_exit(jobrun):
    script = "import sys,time\nfor i in range(1,6):\n sys.stderr.write(f'\\r{i}/5 [00:01<00:02, 2it/s]'); time.sleep(0.1)"
    jobrun("run", "--name", "bar", "--", sys.executable, "-c", script)
    row = wait_for(jobrun, "bar", "done")
    assert (row["done"], row["total"], row["exit_code"]) == (5, 5, 0)


def test_shell_command_failure_and_bracket_counter(jobrun):
    jobrun("run", "--name", "fails", "--", 'echo "[2/7] case a"; exit 3')
    row = wait_for(jobrun, "fails", "error")
    assert (row["done"], row["total"], row["exit_code"]) == (2, 7, 3)


def test_count_regex_counts_matching_lines(jobrun):
    jobrun("run", "--name", "count", "--count-regex", "completed", "--total", "10", "--",
           "for i in 1 2 3; do echo completed; done; echo other")
    row = wait_for(jobrun, "count", "done")
    assert (row["done"], row["total"]) == (3, 10)


def test_custom_progress_regex(jobrun):
    jobrun("run", "--name", "custom", "--progress-regex", r"Trial (?P<done>\d+)", "--total", "50", "--",
           "echo 'Trial 11 completed'; echo 'Trial 12 completed'")
    row = wait_for(jobrun, "custom", "done")
    assert (row["done"], row["total"]) == (12, 50)


def test_foreground_copies_output_and_returns_exit_code(jobrun):
    out = jobrun("run", "--fg", "--name", "fg", "--", "echo JOBRUN_PROGRESS 4/8; exit 2", check=False)
    assert out.returncode == 2
    assert "JOBRUN_PROGRESS 4/8" in out.stdout
    row = rows(jobrun)["fg"]
    assert (row["status"], row["done"], row["total"]) == ("error", 4, 8)


def test_stop_kills_process_tree(jobrun):
    jobrun("run", "--name", "tree", "--", "sleep 300 & sleep 300; wait")
    pid = wait_for(jobrun, "tree", "running")["pid"]
    time.sleep(0.3)
    children = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True).stdout.split()
    jobrun("stop", "tree", "--grace", "2")
    assert wait_for(jobrun, "tree", "stopped")["status"] == "stopped"
    time.sleep(0.3)
    assert not any(Path(f"/proc/{c}").exists() for c in children)


def test_track_pid_detects_log_and_end(jobrun, tmp_path):
    log = tmp_path / "out.log"
    with open(log, "w") as handle:
        proc = subprocess.Popen(["bash", "-c", "echo '[1/4] start'; sleep 1.5; echo '[3/4] more'"], stdout=handle)
    try:
        jobrun("track", "--pid", str(proc.pid), "--name", "tracked")
        row = rows(jobrun)["tracked"]
        assert row["log"] == str(log) and row["status"] == "running"
        proc.wait()
        row = wait_for(jobrun, "tracked", "ended")
        assert (row["done"], row["total"]) == (3, 4)
    finally:
        proc.kill()


def test_supervisor_death_is_reported_as_lost(jobrun):
    jobrun("run", "--name", "orphan", "--", "sleep", "300")
    meta = wait_for(jobrun, "orphan", "running")
    supervisor = json.loads((jobrun.home / "jobs" / f"{meta['id']}.json").read_text())["supervisor_pid"]
    os.kill(supervisor, 9)
    os.kill(meta["pid"], 9)
    assert wait_for(jobrun, "orphan", "lost")["status"] == "lost"


def test_rm_and_clean(jobrun):
    jobrun("run", "--name", "a", "--", "true")
    jobrun("run", "--name", "b", "--", "true")
    wait_for(jobrun, "a", "done")
    wait_for(jobrun, "b", "done")
    jobrun("rm", "a")
    assert set(rows(jobrun)) == {"b"}
    jobrun("clean", "--days", "0")
    assert rows(jobrun) == {}


def test_group_sums_progress_and_stops_together(jobrun):
    for name in ("v1", "v2"):
        jobrun("run", "--group", "sweep", "--name", name, "--count-regex", "Trial .* completed", "--total", "100",
               "--", "echo 'Trial 1 completed'; echo 'Trial 2 completed'; sleep 300")
    jobrun("run", "--group", "sweep", "--name", "v3", "--count-regex", "completed", "--total", "100", "--", "true")
    wait_for(jobrun, "v1", "running")
    wait_for(jobrun, "v3", "done")
    time.sleep(0.3)
    group = listing(jobrun)["groups"][0]
    assert (group["name"], group["status"], group["jobs"], group["finished"]) == ("sweep", "running", 3, 1)
    assert (group["done"], group["total"]) == (4, 300)
    assert "sweep (1/3 jobs)" in jobrun("ls").stdout
    jobrun("stop", "--group", "sweep", "--grace", "2")
    group = listing(jobrun)["groups"][0]
    assert (group["status"], group["finished"]) == ("stopped", 3)


def test_rm_deletes_only_logs_jobrun_wrote(jobrun, tmp_path):
    own = tmp_path / "out" / "own.log"
    jobrun("run", "--name", "owned", "--log", str(own), "--", "echo hi")
    borrowed = tmp_path / "worker.log"
    borrowed.write_text("[1/2] x\n")
    proc = subprocess.Popen(["sleep", "0.5"])
    jobrun("track", "--pid", str(proc.pid), "--name", "borrowed", "--log", str(borrowed))
    proc.wait()
    wait_for(jobrun, "owned", "done")
    wait_for(jobrun, "borrowed", "ended")
    jobrun("rm", "owned", "borrowed")
    assert not own.exists() and borrowed.exists()


def test_log_is_trimmed_and_count_survives_trimming(jobrun):
    script = ("import time\nprint('config: lr=0.1', flush=True)\n"
              "for i in range(30):\n print(f'completed {i} ' + 'x' * 100000, flush=True); time.sleep(0.05)\n"
              "time.sleep(1.5)")
    jobrun("run", "--name", "big", "--max-log-mb", "1", "--count-regex", "^completed", "--total", "30",
           "--", sys.executable, "-c", script)
    row = wait_for(jobrun, "big", "done")
    text = Path(row["log"]).read_text()
    assert os.path.getsize(row["log"]) <= 1 << 20
    assert text.startswith("config: lr=0.1\n")  # the start of the log is kept ...
    assert "MB dropped here" in text and "completed 29 " in text  # ... the middle is cut, the newest part kept
    assert row["done"] == 30  # the scan reads every line before it is cut
    assert row["log_dropped"] > 1 << 20


def test_trim_log_keeps_head_and_tail_and_offsets_stay_valid(tmp_path, monkeypatch):
    monkeypatch.setattr(jobrun_module, "JOBS", tmp_path)
    monkeypatch.setattr(jobrun_module, "TRIM_CHUNK_BYTES", 1000)  # move the tail in many chunks
    log = tmp_path / "x.log"
    lines = [f"line {i:05d} ".ljust(99, "x") + "\n" for i in range(1000)]  # 100 kB, 100-byte lines
    log.write_text("".join(lines))
    removed = jobrun_module.trim_log("x", str(log), 40_000, 5_000)
    text = log.read_text()
    assert text.startswith("".join(lines[:50]))  # the first 5 kB, whole lines
    assert text.endswith("".join(lines[800:]))  # the newest 20 kB
    assert "MB dropped here" in text and "line 00500" not in text
    assert removed == 100_000 - len(text.encode()) == jobrun_module.dropped_bytes("x")
    # a position read before the trim, past the cut, maps to the same line after it (logical = physical + removed)
    with open(log, "rb") as handle:
        handle.seek(900 * 100 - removed)
        assert handle.readline().decode() == lines[900]
    assert jobrun_module.trim_log("x", str(log), 40_000, 5_000) == 0  # under the cap now


def test_trim_waits_for_the_scan(tmp_path, monkeypatch):
    monkeypatch.setattr(jobrun_module, "JOBS", tmp_path)
    log = tmp_path / "y.log"
    log.write_text("z" * 99 + "\n" * 1 + ("a" * 99 + "\n") * 999)
    assert jobrun_module.trim_log("y", str(log), 40_000, 5_000, scanned=1000) == 0
    assert os.path.getsize(log) == 100_000


def test_settings_come_from_env_then_config_file_then_default(jobrun, monkeypatch):
    assert json.loads(jobrun("config", "--json").stdout)["stall_factor"] == 4
    jobrun("config", "stall_factor=5", "auto_clean_days=30")
    assert json.loads((jobrun.home / "config.json").read_text()) == {"stall_factor": 5, "auto_clean_days": 30}
    assert jobrun("config", "stall_factor").stdout.strip() == "5"
    assert listing(jobrun)["settings"]["stall_factor"] == 5
    monkeypatch.setenv("JOBRUN_STALL_FACTOR", "7")
    assert jobrun("config", "stall_factor").stdout.strip() == "7"
    monkeypatch.delenv("JOBRUN_STALL_FACTOR")
    jobrun("config", "--unset", "stall_factor")
    assert json.loads(jobrun("config", "--json").stdout)["stall_factor"] == 4
    assert jobrun("config", "nope=1", check=False).returncode


def test_auto_clean_removes_old_finished_jobs(jobrun):
    jobrun("run", "--name", "old", "--", "true")
    wait_for(jobrun, "old", "done")
    jobrun("config", "auto_clean_days=0.00001")  # about a second
    time.sleep(1.2)
    assert rows(jobrun) == {}  # `ls` cleaned
    assert (jobrun.home / "auto-clean").exists()
    jobrun("run", "--name", "new", "--", "true")
    wait_for(jobrun, "new", "done")
    time.sleep(1.2)
    assert "new" in rows(jobrun)  # not again within the hour


def test_count_mode_reads_large_logs_in_bounded_chunks(tmp_path, monkeypatch):
    log = tmp_path / "huge.log"
    log.write_text("step done\n" * 200000)
    monkeypatch.setattr(jobrun_module, "COUNT_CHUNK_BYTES", 64 * 1024)
    monkeypatch.setattr(jobrun_module, "COUNT_BUDGET_BYTES", 1 << 20)
    meta, scan = {"id": "x", "log": str(log), "count_regex": "done", "total": None, "started": 0}, {}
    jobrun_module.scan_log(meta, scan, 1.0)
    assert scan["stages"][-1]["done"] < 200000 and scan["offset"] <= 1 << 20
    while scan["stages"][-1]["done"] < 200000:
        jobrun_module.scan_log(meta, scan, 1.0)
    assert scan["stages"][-1]["done"] == 200000


def test_sbatch_output_detection(tmp_path):
    script = tmp_path / "job.sbatch"
    script.write_text("#!/bin/bash\n#SBATCH --time=1\n#SBATCH -o out/%j.log\npython x.py\n")
    plain = tmp_path / "plain.sbatch"
    plain.write_text("#!/bin/bash\n#SBATCH --time=1\npython x.py\n#SBATCH -o ignored.log\n")
    assert jobrun_module._sets_output([str(script)])
    assert not jobrun_module._sets_output([str(plain)])
    assert jobrun_module._sets_output(["--output=x.log", str(plain)])
    assert not jobrun_module._sets_output(["--wrap", "echo hi"])


def test_compact_counts():
    assert [jobrun_module.fmt_count(n) for n in (999, 12345, 4_120_000, 1_000_000_000)] == ["999", "12.3k", "4.12M", "1G"]


def test_exact_name_beats_id_substring(jobrun):
    jobrun("run", "--name", "a", "--", "true")
    job_id = rows(jobrun)["a"]["id"]
    jobrun("run", "--name", job_id[-2:], "--", "true")  # a name that is also a substring of an id
    assert json.loads(jobrun("show", job_id[-2:]).stdout)["name"] == job_id[-2:]


def test_supervisor_survives_registry_removal(jobrun):
    import shutil
    jobrun("run", "--name", "survivor", "--", "sleep", "1.5")
    row = wait_for(jobrun, "survivor", "running")
    supervisor = json.loads((jobrun.home / "jobs" / f"{row['id']}.json").read_text())["supervisor_pid"]
    shutil.rmtree(jobrun.home)
    time.sleep(1)
    assert jobrun_module.pid_alive(row["pid"])  # the command keeps running under its supervisor
    time.sleep(1.5)
    assert not jobrun_module.pid_alive(supervisor) and not jobrun_module.pid_alive(row["pid"])


def test_wait_blocks_until_done_and_reports_failure(jobrun):
    jobrun("run", "--group", "w", "--name", "ok", "--", "sleep 0.5")
    jobrun("run", "--group", "w", "--name", "bad", "--", "sleep 0.8; exit 1")
    out = jobrun("wait", "--group", "w", "--interval", "0.2", check=False)
    assert out.returncode == 1
    assert "done" in out.stdout and "error" in out.stdout
    assert jobrun("wait", "ok", "--interval", "0.2").returncode == 0


def test_wait_any_returns_when_the_first_job_fails(jobrun):
    jobrun("run", "--group", "any", "--name", "slow", "--", "sleep 300")
    jobrun("run", "--group", "any", "--name", "crash", "--", "sleep 0.5; exit 3")
    started = time.time()
    out = jobrun("wait", "--any", "--group", "any", "--interval", "0.2", check=False)
    assert time.time() - started < 20 and out.returncode == 1
    lines = out.stdout.splitlines()
    assert "error" in lines[0] and "crash" in lines[0] and "exit code 3" in lines[0]
    slow = rows(jobrun)['slow']['id']
    assert lines[-2] == f"still active: {slow}" and lines[-1].startswith(f"rearm: jobrun wait --any {slow} --since ")
    out = jobrun("wait", "--any", "crash", "--interval", "0.2", check=False)  # already ended: returns at once
    assert out.returncode == 1 and "crash" in out.stdout


def test_wait_any_exits_0_when_a_job_succeeds(jobrun):
    jobrun("run", "--name", "quick", "--", "sleep 0.3")
    jobrun("run", "--name", "slower", "--", "sleep 300")
    out = jobrun("wait", "--any", "quick", "slower", "--interval", "0.2", check=False)
    assert out.returncode == 0 and "done" in out.stdout and "still active" in out.stdout


def test_event_marker_degrades_the_job_and_wakes_wait_any(jobrun):
    script = "echo '[1/4] a'; sleep 1; echo 'JOBRUN_EVENT error pipeline case15 failed'; echo '[2/4] b'; sleep 300"
    jobrun("run", "--name", "runner", "--", script)
    out = jobrun("wait", "--any", "runner", "--interval", "0.2", check=False)
    assert out.returncode == 1
    assert out.stdout.splitlines()[0].endswith("JOBRUN_EVENT error: pipeline case15 failed")
    row = rows(jobrun)["runner"]
    assert (row["status"], row["degraded"], row["event_count"], row["done"]) == ("running", "error", 1, 2)
    assert row["events"][0]["message"] == "pipeline case15 failed" and row["events"][0]["at"]
    assert "[error: pipeline case15 failed]" in jobrun("ls").stdout
    env = dict(os.environ, JOBRUN_HOME=str(jobrun.home))
    with pytest.raises(subprocess.TimeoutExpired):  # an event seen before the wait started does not wake it
        subprocess.run([sys.executable, str(JOBRUN), "wait", "--any", "runner", "--interval", "0.2"], env=env,
                       capture_output=True, timeout=2)


def test_event_levels_are_parsed_and_the_most_severe_is_kept(tmp_path):
    log = tmp_path / "events.log"
    log.write_text("JOBRUN_EVENT warn disk low\nJOBRUN_EVENT errors are not a level\n\x1b[31mJOBRUN_EVENT ERROR: boom\x1b[0m\n"
                   "JOBRUN_EVENT warning: slow\n")
    meta, scan = {"id": "x", "log": str(log), "started": 0.0}, {}
    jobrun_module.scan_log(meta, scan, 1.0)
    assert [(e["level"], e["message"]) for e in scan["events"]] == [("warn", "disk low"), ("error", "boom"),
                                                                     ("warn", "slow")]
    assert (scan["event_count"], scan["degraded"]) == (3, "error")


def test_bursty_steps_are_not_stalled_during_their_usual_pauses(monkeypatch):
    for key in jobrun_module.SETTINGS:
        monkeypatch.delenv(f"JOBRUN_{key.upper()}", raising=False)
    monkeypatch.setattr(jobrun_module, "config_file", lambda: {})
    stage = jobrun_module._new_stage(None, 0.0)
    done = 0
    for burst in range(1, 5):  # 8 parallel workers finish together every 600 s: a mean step of 75 s
        for _ in range(8):
            done += 1
            jobrun_module._set_progress(stage, done, 100, 600.0 * burst)
    step_s = jobrun_module.step_seconds(stage)
    assert step_s == pytest.approx(75)
    assert not jobrun_module.is_stalled(stage, 900.0, step_s)  # 12 mean steps, but well within twice the usual pause
    assert jobrun_module.is_stalled(stage, 1300.0, step_s)
    steady = jobrun_module._new_stage(None, 0.0)
    for step in range(1, 21):  # one step every 10 s: the minimum decides
        jobrun_module._set_progress(steady, step, 100, 10.0 * step)
    assert not jobrun_module.is_stalled(steady, 250.0, 10.0) and jobrun_module.is_stalled(steady, 301.0, 10.0)


def _shift_scan(home: Path, job_id: str, seconds: float) -> None:
    """Pretend the job's steps happened `seconds` earlier."""
    scan_file = next(home.glob(f"jobs/{job_id}.scan*.json"))
    scan = json.loads(scan_file.read_text())
    stage = scan["stages"][-1]
    stage["history"] = [[t - seconds, d] for t, d in stage["history"]]
    stage["progress_at"] -= seconds
    scan_file.write_text(json.dumps(scan))


def test_wait_any_returns_when_a_job_starts_to_stall(jobrun):
    jobrun("run", "--name", "hangs", "--count-regex", "completed", "--total", "10", "--",
           "echo 'Trial 0 completed'; sleep 0.3; echo 'Trial 1 completed'; sleep 300")
    deadline = time.time() + 10
    while rows(jobrun).get("hangs", {}).get("done") != 2 and time.time() < deadline:
        time.sleep(0.2)
    row = rows(jobrun)["hangs"]
    assert not row["stalled"]
    env = dict(os.environ, JOBRUN_HOME=str(jobrun.home))
    waiter = subprocess.Popen([sys.executable, str(JOBRUN), "wait", "--any", "hangs", "--interval", "0.2"], env=env,
                              stdout=subprocess.PIPE, text=True)
    time.sleep(1)
    _shift_scan(jobrun.home, row["id"], 3600)
    out, _ = waiter.communicate(timeout=10)
    assert waiter.returncode == 1 and "stalled: no step for 1h00m" in out


def test_finishing_job_is_not_stalled(jobrun):
    jobrun("run", "--name", "final phase", "--", "echo '[1/2] a'; sleep 0.3; echo '[2/2] b'; sleep 300")
    deadline = time.time() + 10
    while rows(jobrun).get("final phase", {}).get("done") != 2 and time.time() < deadline:
        time.sleep(0.2)
    _shift_scan(jobrun.home, rows(jobrun)["final phase"]["id"], 3600)
    row = rows(jobrun)["final phase"]
    assert row["finishing"] and row["idle_s"] >= 3600 and not row["stalled"]


def test_slurm_log_pattern_expansion():
    info = {"JobIDRaw": "42", "JobID": "42", "JobName": "train", "NodeList": "atlas", "WorkDir": "/w", "StdOut": ""}
    assert jobrun_module._slurm_log("/logs/%x-%j.err", info) == "/logs/train-42.err"
    assert jobrun_module._slurm_log("", info) == "/w/slurm-42.out"


def test_rm_is_idempotent_and_json_is_versioned(jobrun):
    jobrun("run", "--group", "g", "--name", "once", "--", "true")
    job_id = wait_for(jobrun, "once", "done")["id"]
    jobrun("rm", job_id)
    again = jobrun("rm", job_id)  # e.g. a second click on a stale dashboard row
    assert "already removed" in again.stderr
    assert jobrun("rm", "--group", "g").returncode == 0
    assert jobrun("stop", "nonexistent", check=False).returncode != 0  # stop stays strict
    assert listing(jobrun)["schema"] == jobrun_module.JSON_SCHEMA


def test_job_past_its_total_is_finishing(jobrun):
    jobrun("run", "--name", "tail", "--count-regex", "completed", "--total", "2", "--",
           "echo 'Trial 1 completed'; echo 'Trial 0 completed'; sleep 300")
    row = wait_for(jobrun, "tail", "running")
    time.sleep(0.3)
    row = rows(jobrun)["tail"]
    assert (row["done"], row["finishing"], row["eta"]) == (2, True, None) and row["progress_at"]
    assert "finishing" in jobrun("ls").stdout


def test_stages_track_progress_per_stage(jobrun):
    script = ("echo preamble; echo 'JOBRUN_STAGE download 1/2'; echo '[3/3] file'; sleep 0.6; "
              "echo 'JOBRUN_STAGE convert 2/2'; echo '[2/5] sample'; sleep 300")
    jobrun("run", "--name", "prep", "--", script)
    deadline = time.time() + 10
    while (row := rows(jobrun)["prep"])["stage"] != "convert" and time.time() < deadline:
        time.sleep(0.3)
    assert (row["stage"], row["stage_no"], row["stage_of"], row["done"], row["total"]) == ("convert", 2, 2, 2, 5)
    download, convert = row["stages"]
    assert (download["name"], download["status"], download["done"], download["total"]) == ("download", "done", 3, 3)
    assert convert["status"] == "running" and download["ended"] >= download["started"]
    assert "[2/2 convert]" in jobrun("ls").stdout


def test_planned_stages_and_count_reset_per_stage(jobrun):
    jobrun("run", "--name", "plan", "--stages", "a,b,c", "--count-regex", "item", "--total", "4", "--",
           "echo 'JOBRUN_STAGE a'; echo item; echo item; echo 'JOBRUN_STAGE b'; echo item; sleep 300")
    deadline = time.time() + 10
    while (row := rows(jobrun)["plan"])["stage"] != "b" and time.time() < deadline:
        time.sleep(0.3)
    assert (row["stage_no"], row["stage_of"], row["done"], row["total"]) == (2, 3, 1, 4)
    assert [(s["name"], s["status"], s["done"]) for s in row["stages"]] == [
        ("a", "done", 2), ("b", "running", 1), ("c", "pending", None)]


def test_after_waits_then_starts(jobrun, monkeypatch):
    monkeypatch.setenv("JOBRUN_WAIT_POLL_S", "0.5")
    jobrun("run", "--group", "chain", "--name", "first", "--", "sleep 2")
    jobrun("run", "--group", "chain", "--name", "second", "--after", "first", "--", "echo '[1/1] ok'")
    second = wait_for(jobrun, "second", "waiting")
    assert second["waiting_for"] == ["first"] and second["elapsed_s"] is None and second["after_names"] == ["first"]
    listing_lines = jobrun("ls").stdout.splitlines()
    first_line = next(i for i, l in enumerate(listing_lines) if " first" in l)
    second_line = next(i for i, l in enumerate(listing_lines) if " second" in l)
    assert second_line == first_line + 1 and "waiting for first" in listing_lines[second_line]
    assert listing_lines[second_line].index("second") == listing_lines[first_line].index("first")  # same level
    first = wait_for(jobrun, "first", "done")
    second = wait_for(jobrun, "second", "done")
    assert second["started"] >= first["ended"] - 0.5 and second["done"] == 1


def test_after_failed_dependency_skips(jobrun, monkeypatch):
    monkeypatch.setenv("JOBRUN_WAIT_POLL_S", "0.5")
    jobrun("run", "--name", "breaks", "--", "exit 3")
    jobrun("run", "--name", "follow", "--after", "breaks", "--", "touch should-not-exist")
    row = wait_for(jobrun, "follow", "cancelled")
    assert row["reason"] == "dependency breaks: error" and row["elapsed_s"] is None
    assert rows(jobrun)["breaks"]["reason"] == "exit code 3"
    assert not (jobrun.home.parent / "should-not-exist").exists()


def test_stop_waiting_job(jobrun, monkeypatch):
    monkeypatch.setenv("JOBRUN_WAIT_POLL_S", "0.5")
    jobrun("run", "--name", "long", "--", "sleep 300")
    jobrun("run", "--name", "queued", "--after", "long", "--", "true")
    supervisor = json.loads(next((jobrun.home / "jobs").glob("*.json")).read_text())  # any meta works for the dir
    row = wait_for(jobrun, "queued", "waiting")
    pid = json.loads((jobrun.home / "jobs" / f"{row['id']}.json").read_text())["supervisor_pid"]
    jobrun("stop", "queued", "--grace", "2")
    assert wait_for(jobrun, "queued", "cancelled")["reason"] == "stopped while waiting"
    time.sleep(0.5)
    assert not jobrun_module.pid_alive(pid) and supervisor


def test_sbatch_after_requires_slurm_dependencies(jobrun):
    jobrun("run", "--name", "local", "--", "true")
    out = jobrun("sbatch", "--after", "local", "--", "--wrap", "true", check=False)
    assert out.returncode != 0 and "must be Slurm jobs" in out.stderr


def test_jobs_from_older_supervisors_are_scanned_by_readers(jobrun):
    log = jobrun.home.parent / "old.log"
    log.write_text("JOBRUN_STAGE trials\n[7/10] x\n")
    (jobrun.home / "jobs").mkdir(parents=True)
    meta = {"id": "261101-aaaa", "name": "old", "kind": "process", "cmd": ["x"], "host": "elsewhere",
            "log": str(log), "started": time.time() - 5, "pid": 1}  # no supervisor_scans flag
    (jobrun.home / "jobs" / "261101-aaaa.json").write_text(json.dumps(meta))
    (jobrun.home / "jobs" / "261101-aaaa.hb").touch()
    row = rows(jobrun)["old"]
    assert (row["status"], row["done"], row["total"]) == ("running", 7, 10)  # a lone stage is not shown


def test_rows_cached_by_older_versions_still_list(jobrun):
    jobrun("run", "--name", "old-final", "--", "true")
    row = wait_for(jobrun, "old-final", "done")
    state_path = jobrun.home / "jobs" / f"{row['id']}.state.json"
    state = json.loads(state_path.read_text())
    for key in ("after", "after_names", "stages", "stage", "reason"):
        state["final"].pop(key, None)
    state_path.write_text(json.dumps(state))
    assert rows(jobrun)["old-final"]["after_names"] == []
    assert "old-final" in jobrun("ls").stdout


def test_error_reasons(jobrun):
    jobrun("run", "--name", "signalled", "--", "kill -9 $$")
    assert wait_for(jobrun, "signalled", "error")["reason"] == "killed by SIGKILL"


def test_edit_running_job_takes_effect_without_restart(jobrun):
    script = ("for i in 1 2 3; do echo 'JOBRUN_TRIAL Trial completed'; done; sleep 1; "
              "echo 'JOBRUN_PROGRESS 0/6'; echo 'JOBRUN_PROGRESS 2/6'; sleep 300")
    jobrun("run", "--name", "pipeline", "--", script)
    row = wait_for(jobrun, "pipeline", "running")
    jobrun("edit", "pipeline", "--name", "pipeline v2", "--stages", "tune,rollout", "--count-regex", "JOBRUN_TRIAL",
           "--total", "3", "--stage-at", "rollout=JOBRUN_PROGRESS")
    deadline = time.time() + 10
    while (row := rows(jobrun).get("pipeline v2") or {}).get("stage") != "rollout" and time.time() < deadline:
        time.sleep(0.3)
    assert [(s["name"], s["status"], s["done"], s["total"]) for s in row["stages"]] == [
        ("tune", "done", 3, 3), ("rollout", "running", 2, 6)]


def test_edit_after_marks_self_waiting_command_as_waiting(jobrun):
    jobrun("run", "--name", "upstream", "--", "sleep 2")
    jobrun("run", "--name", "polls", "--", "sleep 1.5; echo '[1/1] x'; sleep 300")  # waits by itself
    wait_for(jobrun, "polls", "running")  # already started: only the display can wait
    jobrun("edit", "polls", "--after", "upstream")
    row = rows(jobrun)["polls"]
    assert (row["status"], row["waiting_for"], row["elapsed_s"]) == ("waiting", ["upstream"], None)
    upstream = wait_for(jobrun, "upstream", "done")
    row = wait_for(jobrun, "polls", "running")
    assert row["started"] == upstream["ended"] and row["after_names"] == ["upstream"]
    jobrun("edit", "polls", "--after", "")
    assert rows(jobrun)["polls"]["after"] == []


def test_legacy_statuses_in_cached_rows_are_renamed(jobrun):
    jobrun("run", "--name", "legacy", "--", "exit 1")
    row = wait_for(jobrun, "legacy", "error")
    state_path = jobrun.home / "jobs" / f"{row['id']}.state.json"
    state = json.loads(state_path.read_text())
    state["final"]["status"] = "failed"
    state_path.write_text(json.dumps(state))
    assert rows(jobrun)["legacy"]["status"] == "error"


def test_backfilled_stage_times_are_unknown(tmp_path):
    log = tmp_path / "old.log"
    log.write_text("JOBRUN_STAGE a\n[1/1] x\nJOBRUN_STAGE b\n[3/10] y\n")
    meta, scan = {"id": "x", "log": str(log), "started": 1000.0}, {}
    jobrun_module.scan_log(meta, scan, 5000.0)  # first scan long after the start
    a, b = scan["stages"]
    assert (a["started"], a["ended"], b["started"]) == (1000.0, None, None)
    with log.open("a") as handle:
        handle.write("JOBRUN_STAGE c\n")
    jobrun_module.scan_log(meta, scan, 5100.0)  # live from now on
    assert scan["stages"][-1]["started"] == 5100.0 and scan["stages"][1]["ended"] == 5100.0


def test_stopped_while_self_waiting_is_cancelled(jobrun):
    jobrun("run", "--name", "slow upstream", "--", "sleep 300")
    jobrun("run", "--name", "poller", "--", "sleep 300")
    wait_for(jobrun, "poller", "running")
    jobrun("edit", "poller", "--after", "slow upstream")
    jobrun("stop", "poller", "--grace", "2")
    row = wait_for(jobrun, "poller", "cancelled")
    assert row["reason"] == "stopped while waiting" and row["elapsed_s"] is None


def _job_between_child_exit_and_exit_file(home: Path, job_id: str, name: str) -> subprocess.Popen:
    """A job whose command has exited while its supervisor is alive and has not written .exit.json yet (it is
    still scanning the log, or about to call _finish). Returns the stand-in supervisor process."""
    child = subprocess.Popen(["sleep", "0.2"])
    child_start = jobrun_module.proc_start(child.pid)
    child.wait()
    supervisor = subprocess.Popen(["sleep", "300"])
    (home / "jobs").mkdir(parents=True, exist_ok=True)
    meta = {"id": job_id, "name": name, "kind": "process", "cmd": ["true"], "cwd": str(home), "host": socket.gethostname(),
            "log": str(home / f"{job_id}.log"), "started": time.time() - 1, "pid": child.pid, "pgid": child.pid,
            "proc_start": child_start, "supervisor_pid": supervisor.pid,
            "supervisor_start": jobrun_module.proc_start(supervisor.pid), "supervisor_scans": True}
    (home / "jobs" / f"{job_id}.json").write_text(json.dumps(meta))
    return supervisor


def test_job_whose_supervisor_is_still_finishing_is_not_lost(jobrun):
    supervisor = _job_between_child_exit_and_exit_file(jobrun.home, "261103-aaaa", "finishing up")
    try:
        assert rows(jobrun)["finishing up"]["status"] == "running"
    finally:
        supervisor.kill()
        supervisor.wait()
    assert rows(jobrun)["finishing up"]["status"] == "lost"  # a real supervisor death is still lost


def test_dependency_between_child_exit_and_exit_file_does_not_cancel(jobrun, monkeypatch):
    monkeypatch.setenv("JOBRUN_WAIT_POLL_S", "0.3")
    supervisor = _job_between_child_exit_and_exit_file(jobrun.home, "261103-bbbb", "short dependency")
    try:
        jobrun("run", "--name", "dependent", "--after", "short dependency", "--", "echo '[1/1] ok'")
        time.sleep(1.5)  # several dependency checks inside the window
        assert rows(jobrun)["dependent"]["status"] == "waiting"
        (jobrun.home / "jobs" / "261103-bbbb.exit.json").write_text(json.dumps({"exit_code": 0, "ended": time.time()}))
    finally:
        supervisor.kill()
        supervisor.wait()
    assert wait_for(jobrun, "dependent", "done")["done"] == 1


def test_single_stage_is_not_shown(jobrun):
    jobrun("run", "--name", "one stage", "--", "echo 'JOBRUN_STAGE only'; echo '[2/4] x'; sleep 300")
    jobrun("run", "--name", "announced", "--", "echo 'JOBRUN_STAGE first 1/3'; sleep 300")
    deadline = time.time() + 10
    while not ((row := rows(jobrun)["one stage"])["done"] and rows(jobrun)["announced"]["stage"]) and time.time() < deadline:
        time.sleep(0.3)
    assert (row["stage"], row["stages"], row["done"], row["total"]) == (None, [], 2, 4)
    assert "/?" not in jobrun("ls").stdout
    announced = rows(jobrun)["announced"]
    assert (announced["stage"], announced["stage_no"], announced["stage_of"]) == ("first", 1, 3)


def test_rate_uses_the_last_steps_not_the_run_average():
    stage = jobrun_module._new_stage(None, 0.0)
    for step in range(1, 51):  # 50 fast steps (1 s each), then 20 slow ones (60 s each)
        jobrun_module._set_progress(stage, step, 100, float(step))
    for step in range(51, 71):
        jobrun_module._set_progress(stage, step, 100, 50.0 + 60 * (step - 50))
    now = 50.0 + 60 * 20
    assert jobrun_module.stage_rate(stage, now, 0.0) == pytest.approx(1 / 60)  # the run average would be ~1/18
    assert len(stage["history"]) <= jobrun_module.RATE_STEPS + 1


def test_a_stall_lowers_the_rate():
    stage = jobrun_module._new_stage(None, 0.0)
    for step in range(1, 31):
        jobrun_module._set_progress(stage, step, 100, float(step))
    steps = jobrun_module.RATE_STEPS
    assert jobrun_module.stage_rate(stage, 30.0, 0.0) == pytest.approx(steps / steps)
    assert jobrun_module.stage_rate(stage, 330.0, 0.0) == pytest.approx(steps / (330.0 - 15.0))


def test_progress_reported_at_the_start_is_a_baseline():
    stage = jobrun_module._new_stage(None, 0.0)
    jobrun_module._set_progress(stage, 120, 257, 2.0)  # a resumed run reports its earlier results at once
    assert jobrun_module.stage_rate(stage, 10.0, 0.0) is None
    jobrun_module._set_progress(stage, 121, 257, 62.0)
    assert jobrun_module.stage_rate(stage, 62.0, 0.0) == pytest.approx(1 / 60)


def test_step_seconds_is_the_average_of_the_last_steps():
    stage = jobrun_module._new_stage(None, 0.0)
    assert jobrun_module.step_seconds(stage) is None  # no step yet
    for step in range(1, 51):  # 50 fast steps (1 s each), then 20 slow ones (60 s each)
        jobrun_module._set_progress(stage, step, 100, float(step))
    for step in range(51, 71):
        jobrun_module._set_progress(stage, step, 100, 50.0 + 60 * (step - 50))
    assert jobrun_module.step_seconds(stage) == pytest.approx(60)  # ends at the last step: a stall does not count


def test_a_long_silence_is_flagged_as_stalled(jobrun):
    jobrun("run", "--name", "quiet", "--count-regex", "completed", "--total", "10", "--",
           "echo 'Trial 0 completed'; sleep 0.3; echo 'Trial 1 completed'; sleep 300")
    wait_for(jobrun, "quiet", "running")
    deadline = time.time() + 10
    while rows(jobrun)["quiet"]["done"] != 2 and time.time() < deadline:
        time.sleep(0.2)
    row = rows(jobrun)["quiet"]
    assert row["idle_s"] is not None and row["step_s"] and not row["stalled"]
    scan_file = next(Path(os.environ["JOBRUN_HOME"]).glob(f"jobs/{row['id']}.scan*.json"))
    scan = json.loads(scan_file.read_text())  # pretend the last step was long ago
    stage = scan["stages"][-1]
    stage["history"] = [[t - 600, d] for t, d in stage["history"]]
    stage["progress_at"] -= 600
    scan_file.write_text(json.dumps(scan))
    row = rows(jobrun)["quiet"]
    assert row["stalled"] and row["idle_s"] >= 600
    assert "ago" in jobrun("ls").stdout and " !" in jobrun("ls").stdout


def test_unknown_step_times_fall_back_to_the_average():
    stage = jobrun_module._new_stage("old", None)
    jobrun_module._set_progress(stage, 40, 100, None)  # read from a log written before the first scan
    assert jobrun_module.stage_rate(stage, 1000.0, 200.0) == pytest.approx(40 / 800)


def test_slurm_value_parsers():
    assert jobrun_module._size("85356K") == 85356 * 1024
    assert jobrun_module._size("48G") == 48 * 2 ** 30
    assert jobrun_module._size("249856", "M") == 249856 * 2 ** 20
    assert jobrun_module._size("N/A") is None
    assert jobrun_module._slurm_seconds("1-02:03:04") == 86400 + 2 * 3600 + 3 * 60 + 4
    assert jobrun_module._slurm_seconds("03:04.500") == pytest.approx(184.5)
    assert jobrun_module.expand_hosts("node[01-03,07],gpu1") == ["node01", "node02", "node03", "node07", "gpu1"]
    assert jobrun_module.expand_hosts("workstation") == ["workstation"]
    assert jobrun_module._gpu_count("gpu:rtx_8000:1(S:0),gpu:rtx_a6000:1(S:0),gpumem:rtx_8000:no_consume:48G") == 2
    assert jobrun_module._gpu_count("gpu:a100:4(IDX:0,4,6-7),gpumem:a100:0") == 4
    tres = jobrun_module._tres("cpu=8,mem=48G,node=1,gres/gpu=1,gres/gpu:a100=1")
    assert (tres["cpu"], tres["mem"], tres["gres/gpu"]) == ("8", "48G", "1")


def test_res_attributes_cpu_and_memory_to_the_job(jobrun, monkeypatch):
    monkeypatch.setenv("JOBRUN_RES_MIN_S", "0")
    spin = f"{sys.executable} -c 'while True: pass'"
    jobrun("run", "--name", "busy", "--", f"{spin} & {spin} & wait")  # two busy processes below the job's shell
    wait_for(jobrun, "busy", "running")
    time.sleep(0.5)
    json.loads(jobrun("res", "--json").stdout)  # baseline
    time.sleep(1.5)
    result = json.loads(jobrun("res", "--json").stdout)
    node = next(n for n in result["nodes"] if n["local"])
    job = next(j for j in node["jobs"] if j["name"] == "busy")
    assert result["interval_s"] >= 1.5 and job["measured"]
    assert 1.5 < job["cpu"] < 2.5 and job["mem"] > 0
    assert node["cpu"] >= job["cpu"] - 0.3 and node["other"]["cpu"] >= 0
    assert "busy" in jobrun("res").stdout


def test_res_slurm_node_uses_at_least_what_our_jobs_use(tmp_path, monkeypatch):
    """slurmd's load and free memory can read near zero on a busy node; the node shows our jobs' measured sum."""
    monkeypatch.setattr(jobrun_module, "HOME", tmp_path)
    monkeypatch.setattr(jobrun_module, "RES_FIRST_S", 0)
    gib = 2 ** 30
    node = {"cpus": 36, "load": 0.0, "mem": 117 * gib, "mem_free": 115 * gib, "gpus": 1, "state": "mixed",
            "reserved": {"cpus": 16, "mem": 96 * gib, "gpus": 1},
            "slurm_jobs": [{"slurm_id": "7", "user": "me", "mine": True, "cpus": 12, "mem": 48 * gib, "gpus": 1}]}
    busy_node = dict(node, load=20.0, mem_free=50 * gib)
    row = {"id": "j1", "name": "w", "status": "running", "kind": "slurm", "slurm_id": "7", "host": "eris",
           "elapsed_s": 100.0}
    monkeypatch.setattr(jobrun_module, "_sstat", lambda ids: {"7": {"cpu_s": 1200.0, "mem": 40 * gib, "vram": 0,
                                                                     "gpu_util": None}})
    monkeypatch.setattr(jobrun_module.shutil, "which", lambda name: "/usr/bin/" + name)
    for info, cpu, mem, from_jobs in ((node, 12.0, 40 * gib, True), (busy_node, 20.0, 67 * gib, False)):
        monkeypatch.setattr(jobrun_module, "_slurm_nodes", lambda hosts, info=info: {"eris": info})
        (tmp_path / "hosts").mkdir(exist_ok=True)
        for f in (tmp_path / "hosts").iterdir():
            f.unlink()
        eris = next(n for n in jobrun_module.resources([row])["nodes"] if n["host"] == "eris")
        assert (eris["cpu"], eris["mem"], eris["cpu_from_jobs"], eris["mem_from_jobs"]) == (cpu, mem, from_jobs, from_jobs)


def test_reservation_prefers_slurm_then_budget_then_cgroup():
    reserve = jobrun_module.job_reservation
    slurm = {"cpus": 8, "mem": 48 * 2 ** 30, "gpus": 1}
    assert reserve({"budget": {"cpus": 2}}, slurm, {"mem": 1.0})["source"] == "slurm"
    mixed = reserve({"budget": {"cpus": 10}}, None, {"mem": 96 * 2 ** 30})
    assert (mixed["cpus"], mixed["mem"], mixed["gpus"], mixed["source"]) == (10, 96 * 2 ** 30, None, "budget+cgroup")
    assert reserve({}, None, {}) is None


def test_budget_flags_are_stored_and_editable(jobrun):
    jobrun("run", "--name", "budgeted", "--cpus", "4", "--mem", "20G", "--", "sleep 300")
    row = wait_for(jobrun, "budgeted", "running")
    assert row["budget"] == {"cpus": 4.0, "mem": 20 * 2 ** 30}
    jobrun("edit", row["id"], "--gpus", "1")
    assert rows(jobrun)["budgeted"]["budget"]["gpus"] == 1 and rows(jobrun)["budgeted"]["budget"]["cpus"] == 4.0
    node = next(n for n in json.loads(jobrun("res", "--json").stdout)["nodes"] if n["local"])
    job = next(j for j in node["jobs"] if j["name"] == "budgeted")
    assert job["reserved"]["mem"] == 20 * 2 ** 30 and job["reserved"]["source"].startswith("budget")
    assert node["reserved"]["cpus"] >= 4


def test_gpu_is_not_queried_without_a_job_holding_it(jobrun):
    assert not jobrun_module.holds_gpu([os.getpid()])
    jobrun("run", "--name", "cpu only", "--", "sleep 300")
    wait_for(jobrun, "cpu only", "running")
    node = next(n for n in json.loads(jobrun("res", "--json").stdout)["nodes"] if n["local"])
    assert node["gpus"] == [] and not node["gpu_queried"]


def test_budget_marker_from_the_log_newest_wins_and_flags_override(jobrun):
    assert jobrun_module.parse_budget(" cpus=12 mem=96G gpus=1") == {"cpus": 12.0, "mem": 96 * 2 ** 30, "gpus": 1.0}
    script = "echo 'JOBRUN_BUDGET cpus=4'; echo 'JOBRUN_BUDGET cpus=12 mem=8G'; sleep 300"
    jobrun("run", "--name", "declares", "--", script)
    jobrun("run", "--name", "overridden", "--cpus", "2", "--", script)
    wait_for(jobrun, "declares", "running")
    wait_for(jobrun, "overridden", "running")
    time.sleep(0.5)
    found = rows(jobrun)
    assert found["declares"]["budget"] == {"cpus": 12.0, "mem": 8 * 2 ** 30}
    assert found["overridden"]["budget"] == {"cpus": 2.0, "mem": 8 * 2 ** 30}


def test_versions_agree():
    import re as _re
    root = Path(__file__).resolve().parents[1]
    pyproject = _re.search(r'^version = "([^"]+)"', (root / "pyproject.toml").read_text(), _re.M).group(1)
    extension = json.loads((root / "vscode" / "package.json").read_text())["version"]
    plugin = json.loads((root / ".claude-plugin" / "plugin.json").read_text())["version"]
    assert jobrun_module.VERSION == pyproject == extension == plugin


def test_skill_is_where_every_channel_expects_it():
    """One SKILL.md for jobrun setup, the Claude Code plugin (repo root), `npx skills add` and the extension bundle."""
    root = Path(__file__).resolve().parents[1]
    skill = (root / "skills" / "jobrun" / "SKILL.md").read_text()
    assert skill.startswith("---\nname: jobrun\n")  # the name must match the folder (Agent Skills, VS Code chatSkills)
    marketplace = json.loads((root / ".claude-plugin" / "marketplace.json").read_text())
    assert marketplace["plugins"][0]["source"] == "./"
    package = json.loads((root / "vscode" / "package.json").read_text())
    assert package["contributes"]["chatSkills"] == [{"path": "./bundle/skills/jobrun/SKILL.md"}]
    assert "'skills', 'jobrun', 'SKILL.md'" in (root / "vscode" / "scripts" / "bundle.js").read_text()


def test_marker_block_round_trip(tmp_path):
    notes = tmp_path / "CLAUDE.md"
    notes.write_text("# mine\n\nbe nice\n")
    assert not jobrun_module._add_block(notes, "line one", ("<!--", " -->"))
    jobrun_module._add_block(notes, "line two", ("<!--", " -->"))  # replaces, not appends
    text = notes.read_text()
    assert text.count("jobrun:begin") == 1 and "line two" in text and "line one" not in text
    assert jobrun_module._strip_block(text) == "# mine\n\nbe nice\n"


def _sandbox(tmp_path):
    home = tmp_path / "home"
    (home / ".claude").mkdir(parents=True)
    (home / ".claude" / "CLAUDE.md").write_text("# mine\n")
    env = {"HOME": str(home), "PATH": "/usr/bin:/bin", "SHELL": "/bin/bash"}

    def call(*args):
        out = subprocess.run([sys.executable, str(JOBRUN), *args], capture_output=True, text=True, env=env,
                             stdin=subprocess.DEVNULL)
        assert out.returncode == 0, out.stderr
        return out.stdout
    return home, call


def test_setup_then_uninstall_leaves_home_as_it_was(tmp_path):
    home, call = _sandbox(tmp_path)
    before = sorted(str(p.relative_to(home)) for p in home.rglob("*"))
    call("setup", "--yes", "--no-vscode", "--agents", "claude,codex,copilot")
    call("setup", "--yes", "--no-vscode", "--agents", "claude-code,codex,github-copilot")  # idempotent; aliases
    assert (home / ".local" / "bin" / "jobrun").resolve() == home / ".local" / "share" / "jobboard" / "app" / "jobrun.py"
    assert (home / ".claude" / "skills" / "jobrun" / "SKILL.md").exists()
    assert (home / ".agents" / "skills" / "jobrun" / "SKILL.md").exists()
    assert (home / ".copilot" / "skills" / "jobrun" / "SKILL.md").exists()
    assert (home / ".claude" / "CLAUDE.md").read_text().count("through `jobrun`") == 1
    assert "jobrun:begin" in (home / ".codex" / "AGENTS.md").read_text()
    assert ".local/bin" in (home / ".bashrc").read_text()
    call("uninstall", "--yes", "--purge")
    assert sorted(str(p.relative_to(home)) for p in home.rglob("*")) == before
    assert (home / ".claude" / "CLAUDE.md").read_text() == "# mine\n"


def test_setup_leaves_foreign_files_alone(tmp_path):
    home, call = _sandbox(tmp_path)
    (home / ".claude" / "CLAUDE.md").write_text("# mine\nuse jobrun for long jobs\n")
    skill = home / ".claude" / "skills" / "jobrun" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text("my own skill\n")
    (home / ".local" / "bin").mkdir(parents=True)
    (home / ".local" / "bin" / "jobrun").symlink_to("/somewhere/else/jobrun.py")
    out = call("setup", "--yes", "--no-vscode", "--agents", "claude")
    assert out.count("left alone") == 3
    call("uninstall", "--yes")
    assert skill.read_text() == "my own skill\n" and (home / ".local" / "bin" / "jobrun").is_symlink()
    assert (home / ".claude" / "CLAUDE.md").read_text() == "# mine\nuse jobrun for long jobs\n"


def test_demo_starts_a_group(jobrun):
    out = jobrun("demo").stdout
    assert "jobrun demo" in out
    group = [r for r in listing(jobrun)["jobs"] if r.get("group") == "jobrun demo"]
    assert len(group) == len(jobrun_module.DEMO_JOBS)


# ---------------------------------------------------------------- pools

def member(name, status="running", **fields):
    """A worker row as snapshots() returns it, with only the fields pool_row reads."""
    row = {"id": f"id-{name}", "name": name, "host": "node-a", "slurm_id": None, "status": status, "reason": None,
           "started": 100.0, "ended": None if status in jobrun_module.ACTIVE else 200.0, "done": None, "total": None,
           "rate_per_s": None, "progress_at": None, "idle_s": None, "step_s": None, "stalled": False,
           "events": [], "event_count": 0, "degraded": None, "items": None, "item_rate": None, "item_label": None,
           "item_at": None, "item_idle_s": None, "item_stalled": False, "role": None, "budget": None,
           "last_line": None}
    row.update(fields)
    return row


POOL = {"id": "pool-1", "name": "queue", "kind": "pool", "group": None, "total": None, "started": 50.0, "cwd": "/"}


def test_pool_takes_the_newest_reading_of_the_whole_list_not_the_sum():
    monitor = member("monitor", role="monitor", done=40, total=100, progress_at=300.0, rate_per_s=0.5)
    worker = member("w1", done=38, total=100, progress_at=290.0, items=10, item_rate=0.1)
    row = jobrun_module.pool_row(POOL, [monitor, worker], now=310.0)
    assert (row["status"], row["done"], row["total"], row["rate_per_s"]) == ("running", 40, 100, 0.5)
    assert row["eta"] == 310.0 + 60 / 0.5
    assert (row["workers"], row["active_workers"]) == (["id-w1"], 1)  # the monitor is no worker
    assert jobrun_module.pool_where(row) == "1 worker + monitor on node-a"


def test_pool_sums_worker_items_without_a_reading_and_sets_shares():
    a = member("a", items=30, item_rate=0.2, host="node-a")
    b = member("b", items=10, item_rate=0.1, host="node-b")
    row = jobrun_module.pool_row({**POOL, "total": 100}, [a, b], now=310.0)
    assert (row["done"], row["total"], round(row["rate_per_s"], 6)) == (40, 100, 0.3)
    assert row["hosts"] == ["node-a", "node-b"]
    assert (a["share"], b["share"]) == (0.75, 0.25)


def test_pool_failed_worker_degrades_but_the_pool_keeps_running():
    crashed = member("b", status="error", reason="out of memory", items=3)
    row = jobrun_module.pool_row(POOL, [member("a", items=5), crashed], now=310.0)
    assert (row["status"], row["degraded"], row["event_count"]) == ("running", "warn", 1)
    assert "b" in row["events"][0]["message"] and "out of memory" in row["events"][0]["message"]


def test_pool_done_when_the_list_is_complete_even_if_a_worker_failed():
    row = jobrun_module.pool_row({**POOL, "total": 8}, [member("a", "done", items=5), member("b", "error", items=3)],
                                 now=310.0)
    assert (row["status"], row["incomplete"], row["degraded"]) == ("done", False, "warn")


def test_pool_all_workers_succeeded_but_items_open_warns():
    row = jobrun_module.pool_row({**POOL, "total": 10}, [member("a", "done", items=3), member("b", "done", items=3)],
                                 now=310.0)
    assert (row["status"], row["incomplete"], row["degraded"]) == ("done", True, "warn")
    assert "6/10" in row["events"][-1]["message"] and row["reason"]


def test_pool_last_worker_failed_with_items_open_is_an_error():
    row = jobrun_module.pool_row({**POOL, "total": 10}, [member("a", "done", items=3), member("b", "error", items=3)],
                                 now=310.0)
    assert row["status"] == "error" and "4 of 10" in row["reason"]
    row = jobrun_module.pool_row(POOL, [member("a", "error"), member("b", "lost")], now=310.0)
    assert row["status"] == "error"


def test_pool_without_workers_waits_and_stopped_pool_is_stopped():
    assert jobrun_module.pool_row(POOL, [], now=310.0)["status"] == "waiting"
    # an empty pool can be stopped, and gives up on its own when no worker joins
    row = jobrun_module.pool_row(POOL, [], now=310.0, stopped=True)
    assert row["status"] == "stopped" and row["reason"]
    later = POOL["started"] + jobrun_module.POOL_EMPTY_S + 1
    row = jobrun_module.pool_row(POOL, [], now=later)
    assert row["status"] == "cancelled" and "no workers" in row["reason"]
    assert jobrun_module.pool_row(POOL, [member("m", role="monitor")], now=later)["status"] == "waiting"
    row = jobrun_module.pool_row(POOL, [member("a", "stopped"), member("b", "done")], now=310.0, stopped=True)
    assert (row["status"], row["degraded"]) == ("stopped", None)


def test_pool_stall_follows_the_progress_source():
    source = member("monitor", role="monitor", done=5, total=10, progress_at=300.0, stalled=True, idle_s=900.0)
    assert jobrun_module.pool_row(POOL, [source, member("a")], now=310.0)["stalled"]
    busy = member("b", items=4, item_stalled=False)
    assert not jobrun_module.pool_row(POOL, [member("a", items=4, item_stalled=True), busy], now=310.0)["stalled"]


def test_pool_workers_report_items_and_the_pool_finishes(jobrun):
    jobrun("pool", "queue", "--total", "6", "--group", "batch")
    for name in ("w1", "w2"):
        jobrun("run", "--pool", "queue", "--name", name, "--",
               "for i in 1 2 3; do echo \"JOBRUN_ITEM case $i\"; sleep 0.2; done")
    wait_for(jobrun, "queue", "done")
    data = listing(jobrun)
    pool = next(r for r in data["jobs"] if r["kind"] == "pool")
    workers = [r for r in data["jobs"] if r.get("pool") == pool["id"]]
    assert (pool["done"], pool["total"], pool["incomplete"], pool["group"]) == (6, 6, False, "batch")
    assert sorted((w["name"], w["items"], w["group"]) for w in workers) == [("w1", 3, None), ("w2", 3, None)]
    assert workers[0]["item_label"] == "case 3"
    assert data["groups"][0]["jobs"] == 1  # the pool counts once in its group
    lines = jobrun("ls").stdout.splitlines()
    pool_line = next(i for i, l in enumerate(lines) if "queue" in l)
    assert f"2 workers on {socket.gethostname()}" in lines[pool_line] and "100% 6/6" in lines[pool_line] and "└ w" in lines[pool_line + 1]
    worker = lines[pool_line + 1].split("└")[1].split("done")[0].replace(socket.gethostname(), "")
    assert "6" not in worker  # workers show no progress of their own (the host name may contain digits)


def test_empty_pool_can_be_stopped_and_removed(jobrun):
    """A pool no worker joined runs nothing: stop marks it stopped, rm removes it without --force."""
    jobrun("pool", "never joined", "--group", "batch")
    jobrun("pool", "also empty")
    assert rows(jobrun)["never joined"]["status"] == "waiting"
    jobrun("stop", "never joined")
    assert rows(jobrun)["never joined"]["status"] == "stopped"
    out = jobrun("rm", "also empty")
    assert "removed" in out.stdout and "also empty" not in rows(jobrun)


def test_pool_with_open_items_warns_and_wait_fails(jobrun):
    jobrun("run", "--pool", "short", "--name", "w1", "--", "echo 'JOBRUN_PROGRESS 2/5'")
    out = jobrun("wait", "short", "--interval", "0.2", check=False)
    assert out.returncode == 1 and "2/5" in out.stdout
    pool = rows(jobrun)["short"]
    assert (pool["status"], pool["incomplete"], pool["degraded"]) == ("done", True, "warn")


def test_after_a_pool_waits_for_all_workers(jobrun, monkeypatch):
    monkeypatch.setenv("JOBRUN_WAIT_POLL_S", "0.5")
    jobrun("run", "--pool", "list", "--name", "fast", "--", "echo JOBRUN_ITEM")
    jobrun("run", "--pool", "list", "--name", "slow", "--", "sleep 2; echo JOBRUN_ITEM")
    jobrun("run", "--name", "next", "--after", "list", "--", "true")
    wait_for(jobrun, "fast", "done")
    assert rows(jobrun)["next"]["status"] == "waiting"
    nxt = wait_for(jobrun, "next", "done")
    assert nxt["started"] >= rows(jobrun)["slow"]["ended"] - 0.5


def test_stop_pool_stops_every_worker_and_wait_any_sees_a_crash(jobrun):
    jobrun("run", "--pool", "busy", "--name", "a", "--", "sleep 300")
    jobrun("run", "--pool", "busy", "--name", "b", "--", "sleep 300")
    jobrun("run", "--pool", "busy", "--name", "c", "--", "sleep 2; exit 3")
    out = jobrun("wait", "--any", "busy", "--interval", "0.2", check=False)
    assert out.returncode == 1 and "c" in out.stdout and "exit code 3" in out.stdout
    assert rows(jobrun)["busy"]["status"] == "running"
    jobrun("stop", "busy", "--grace", "2")
    found = rows(jobrun)
    assert (found["a"]["status"], found["b"]["status"], found["busy"]["status"]) == ("stopped", "stopped", "stopped")


def test_edit_moves_running_jobs_into_a_pool_that_takes_their_group(jobrun):
    jobrun("run", "--group", "old", "--name", "watch", "--", "echo 'JOBRUN_PROGRESS 3/9'; sleep 300")
    jobrun("run", "--group", "old", "--name", "w", "--", "sleep 300")
    wait_for(jobrun, "w", "running")
    jobrun("edit", "watch", "--pool", "joined", "--role", "monitor")
    jobrun("edit", "w", "--pool", "joined")
    time.sleep(0.5)
    found = rows(jobrun)
    pool = found["joined"]
    assert (pool["group"], pool["done"], pool["total"], pool["workers"]) == ("old", 3, 9, [found["w"]["id"]])
    assert found["w"]["group"] is None and found["watch"]["role"] == "monitor"
    assert "└ watch" in (line := next(l for l in jobrun("ls").stdout.splitlines() if "└ watch" in l)) and "monitor" in line
    jobrun("edit", "w", "--pool", "")
    assert rows(jobrun)["w"]["pool"] is None


def test_wait_any_on_a_pool_wakes_when_one_worker_succeeds(jobrun):
    jobrun("run", "--pool", "t pool", "--name", "w1", "--", "sleep 3")
    jobrun("run", "--pool", "t pool", "--name", "w2", "--", "sleep 300")
    pool_id = rows(jobrun)["t pool"]["id"]
    started = time.time()
    out = jobrun("wait", "--any", "t pool", "--interval", "0.2", check=False)
    assert time.time() - started < 20 and out.returncode == 0
    lines = out.stdout.splitlines()
    assert len(lines) == 3 and "w1" in lines[0] and "done" in lines[0] and f"worker of pool t pool ({pool_id})" in lines[0]
    assert lines[1] == f"still active: {pool_id} (1 workers)"
    assert lines[2].startswith(f"rearm: jobrun wait --any {pool_id} --since ")
    assert rows(jobrun)["w1"]["status"] == "done"  # the ended worker stays listed
    env = dict(os.environ, JOBRUN_HOME=str(jobrun.home))
    with pytest.raises(subprocess.TimeoutExpired):  # an end seen before the wait started does not wake it again
        subprocess.run([sys.executable, str(JOBRUN), "wait", "--any", "t pool", "--interval", "0.2"], env=env,
                       capture_output=True, timeout=2)


def test_wait_any_on_a_pool_reports_a_failed_worker_once_and_the_pool_end(jobrun):
    jobrun("run", "--pool", "f pool", "--name", "bad", "--", "sleep 3; exit 3")
    jobrun("run", "--pool", "f pool", "--name", "ok", "--", "sleep 6")
    out = jobrun("wait", "--any", "f pool", "--interval", "0.2", check=False)
    assert out.returncode == 1 and sum("bad" in line for line in out.stdout.splitlines()) == 1
    assert "exit code 3" in out.stdout
    out = jobrun("wait", "--any", "f pool", "--interval", "0.2", check=False)
    lines = out.stdout.splitlines()
    assert any("ok" in l and "worker of pool" in l for l in lines)
    assert sum(l.split()[0] == rows(jobrun)["f pool"]["id"] for l in lines) == 1  # the pool's end, once
    assert not any("still active" in l for l in lines)


def test_removals_are_recorded_with_the_command_and_caller(jobrun):
    jobrun("run", "--name", "gone", "--", "true")
    jobrun("run", "--name", "swept", "--", "true")
    wait_for(jobrun, "gone", "done")
    wait_for(jobrun, "swept", "done")
    jobrun("rm", "gone")
    jobrun("clean", "--days", "0")
    log = (jobrun.home / "removed.log").read_text().splitlines()
    entries = [json.loads(line) for line in log]
    assert [(e["name"], e["status"], e["by"]) for e in entries] == [
        ("gone", "done", "jobrun rm"), ("swept", "done", "jobrun clean --days 0")]
    assert entries[0]["ended"] and entries[0]["parent"]


def test_rearm_with_since_reports_a_worker_that_ended_between_two_waits(jobrun):
    jobrun("run", "--pool", "gap", "--name", "first", "--", "sleep 3")
    jobrun("run", "--pool", "gap", "--name", "second", "--", "sleep 6")
    jobrun("run", "--pool", "gap", "--name", "last", "--", "sleep 300")
    out = jobrun("wait", "--any", "gap", "--interval", "0.2", check=False)
    assert "first" in out.stdout.splitlines()[0]
    rearm = out.stdout.splitlines()[-1].removeprefix("rearm: jobrun ").split()
    wait_for(jobrun, "second", "done")  # ends while nobody waits
    out = jobrun(*rearm, "--interval", "0.2", check=False)
    lines = out.stdout.splitlines()
    assert out.returncode == 0 and "second" in lines[0] and not any("first" in l for l in lines)


def test_wait_any_reports_a_worker_removed_before_its_end_was_seen(jobrun):
    jobrun("run", "--pool", "del", "--name", "short", "--", "sleep 3")
    jobrun("run", "--pool", "del", "--name", "long", "--", "sleep 300")
    env = dict(os.environ, JOBRUN_HOME=str(jobrun.home))
    waiter = subprocess.Popen([sys.executable, str(JOBRUN), "wait", "--any", "del", "--interval", "30"], env=env,
                              stdout=subprocess.PIPE, text=True)
    time.sleep(2)  # the wait has taken its baseline and sleeps 30 s
    wait_for(jobrun, "short", "done")
    jobrun("rm", "short")
    out, _ = waiter.communicate(timeout=60)
    assert waiter.returncode == 0
    assert "short" in out.splitlines()[0] and "done" in out and "removed from the registry" in out


def test_a_dependency_removed_after_it_succeeded_still_counts_as_done(jobrun, monkeypatch):
    monkeypatch.setenv("JOBRUN_WAIT_POLL_S", "3")
    jobrun("run", "--name", "base", "--", "sleep 1")
    jobrun("run", "--name", "follow", "--after", "base", "--", "true")
    wait_for(jobrun, "base", "done")
    jobrun("rm", "base")  # before "follow" polls again
    assert wait_for(jobrun, "follow", "done")["exit_code"] == 0
