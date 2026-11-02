import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

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
    return call


def rows(jobrun):
    return {r["name"]: r for r in json.loads(jobrun("ls", "--json", "--all").stdout)}


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
    row = wait_for(jobrun, "fails", "failed")
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
    assert (row["status"], row["done"], row["total"]) == ("failed", 4, 8)


def test_stop_kills_process_tree(jobrun):
    jobrun("run", "--name", "tree", "--", "sleep 300 & sleep 300; wait")
    pid = wait_for(jobrun, "tree", "running")["pid"]
    time.sleep(0.3)
    children = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True).stdout.split()
    jobrun("stop", "tree", "--grace", "2")
    assert wait_for(jobrun, "tree", "killed")["status"] == "killed"
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
