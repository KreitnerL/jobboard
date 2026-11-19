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
    script = ("import time\nfor i in range(30):\n print('completed ' + 'x' * 100000, flush=True); time.sleep(0.05)\n"
              "time.sleep(1.5)")
    jobrun("run", "--name", "big", "--max-log-mb", "1", "--count-regex", "^completed", "--total", "30",
           "--", sys.executable, "-c", script)
    time.sleep(0.8)
    rows(jobrun)  # count part of the log before it is trimmed
    row = wait_for(jobrun, "big", "done")
    assert os.path.getsize(row["log"]) < 2 << 20
    assert "jobrun: log exceeded" in Path(row["log"]).read_text()
    assert 20 <= row["done"] <= 30  # lines written between two refreshes may be trimmed unseen


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
    assert jobrun_module.expand_hosts("wskreitner") == ["wskreitner"]
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
