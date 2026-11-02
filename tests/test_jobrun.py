import json
import os
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
    assert (group["status"], group["finished"]) == ("killed", 3)


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


def test_count_mode_reads_large_logs_in_bounded_chunks(jobrun, tmp_path, monkeypatch):
    log = tmp_path / "huge.log"
    log.write_text("step done\n" * 200000)
    monkeypatch.setattr(jobrun_module, "COUNT_CHUNK_BYTES", 64 * 1024)
    monkeypatch.setattr(jobrun_module, "COUNT_BUDGET_BYTES", 1 << 20)
    meta, state = {"id": "x", "log": str(log), "count_regex": "done", "total": None}, {}
    done, _, _ = jobrun_module.parse_progress(meta, state)
    assert done < 200000 and state["offset"] <= 1 << 20
    while done < 200000:
        done, _, _ = jobrun_module.parse_progress(meta, state)
    assert done == 200000


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
    assert "done" in out.stdout and "failed" in out.stdout
    assert jobrun("wait", "ok", "--interval", "0.2").returncode == 0


def test_slurm_log_pattern_expansion():
    info = {"JobIDRaw": "42", "JobID": "42", "JobName": "train", "NodeList": "atlas", "WorkDir": "/w", "StdOut": ""}
    assert jobrun_module._slurm_log("/logs/%x-%j.err", info) == "/logs/train-42.err"
    assert jobrun_module._slurm_log("", info) == "/w/slurm-42.out"
