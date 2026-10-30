"""run-all2.sh, the *_parallel2.sh wrappers and scripts/wait-for-flags.sh,
run for real against a stub `lux` (and `python`) on PATH that records its
arguments and writes the status and log files a worker would."""

import os
import re
import stat
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAIT = os.path.join(REPO, "scripts", "wait-for-flags.sh")
SCRIPTS = ["run-all2.sh", "scripts/wait-for-flags.sh", "reconcile_parallel2.sh", "merge_parallel2.sh",
           "export_parallel2.sh", "nt_parallel2.sh", "import_parallel2.sh", "harvest_parallel2.sh",
           "updateExternal_parallel2.sh"]

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="bash scripts")

FAKE_LUX = r"""#!/bin/bash
echo "lux $*" >> "$FAKE_CALLS"
cmd=$1; shift
prefix=""; worker=0
while [[ $# -gt 0 ]]; do
    case $1 in
        --prefix) prefix=$2; shift;;
        --my-worker) worker=$2; shift;;
    esac
    shift
done
if [[ "$FAKE_NOSTART" == "$cmd" ]]; then
    echo "lux: could not load the pipeline configuration: no config" >&2
    exit 1
fi
if [[ -n "$prefix" ]]; then
    if [[ "$FAKE_FAIL" == "$cmd:$worker" ]]; then
        printf 'ERROR    worker %s failed: KeyError: x\nTraceback (most recent call last):\n  ...\n' \
            $worker > "$prefix-w$worker.log"
        echo "{\"worker\": $worker, \"done\": true}" > "$prefix-w$worker.status"
        echo "lux $cmd: WorkerFailed: worker(s) $worker failed" >&2
        exit 1
    fi
    echo "INFO     fine" > "$prefix-w$worker.log"
    echo "{\"worker\": $worker, \"done\": true}" > "$prefix-w$worker.status"
fi
exit 0
"""

FAKE_PYTHON = """#!/bin/bash
echo "python $*" >> "$FAKE_CALLS"
"""


@pytest.fixture
def env(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for name, body in (("lux", FAKE_LUX), ("python", FAKE_PYTHON)):
        p = bindir / name
        p.write_text(body)
        p.chmod(p.stat().st_mode | stat.S_IEXEC)
    logs = tmp_path / "logs"
    (logs / "flags").mkdir(parents=True)
    (tmp_path / "exports").mkdir()
    e = dict(os.environ)
    e.update(PATH=f"{bindir}{os.pathsep}{e['PATH']}", FAKE_CALLS=str(tmp_path / "calls.txt"),
             LOGS=str(logs), STATUS_FILE=str(tmp_path / "status.txt"),
             EXPORTS=str(tmp_path / "exports"), WAIT_INTERVAL="0.05", WORKERS="3")
    for k in ("ENGINE", "FAKE_FAIL", "FAKE_NOSTART"):
        e.pop(k, None)
    return e


def run(script, *args, env, cwd, timeout=60):
    return subprocess.run(["bash", os.path.join(REPO, script), *args], env=env, cwd=cwd,
                          capture_output=True, text=True, timeout=timeout)


def calls(env):
    path = env["FAKE_CALLS"]
    if not os.path.exists(path):
        return []
    with open(path) as fh:
        return [line.rstrip("\n") for line in fh]


def lux_calls(env, command):
    return [c for c in calls(env) if c.startswith(f"lux {command} ")]


def worker_of(call):
    return int(re.search(r"--my-worker (\d+)", call).group(1))


def prefix_of(call):
    return re.search(r"--prefix (\S+)", call).group(1)


@pytest.mark.parametrize("script", SCRIPTS)
def test_scripts_parse(script):
    assert subprocess.run(["bash", "-n", os.path.join(REPO, script)]).returncode == 0


def test_run_all_calls_no_legacy_script():
    with open(os.path.join(REPO, "run-all2.sh")) as fh:
        code = [line for line in fh if not line.lstrip().startswith("#")]
    for line in code:
        assert not re.search(r"run-\w+\.py|manage-data\.py|merge-metatypes\.py|google-sames-diffs\.py|load-csv-map2\.py|post-build-portal\.py", line), line


# ------------------------------------------------------------- run-all2.sh


def test_run_all_null_engine(env, tmp_path):
    r = run("run-all2.sh", "--all", env=env, cwd=tmp_path)
    assert r.returncode == 0, r.stdout + r.stderr
    seq = calls(env)

    for phase in ("reconcile", "merge", "export"):
        got = lux_calls(env, phase)
        assert sorted(worker_of(c) for c in got) == [0, 1, 2]
        assert all("--max-workers 3 --no-ui" in c for c in got)
        assert len({prefix_of(c) for c in got}) == 1, "every slice shares one prefix"
        assert prefix_of(got[0]).startswith(f"{env['LOGS']}/{phase}-")
    assert all("--source all --no-finish" in c for c in lux_calls(env, "reconcile"))
    assert all("--source all" in c for c in lux_calls(env, "merge"))

    # the steps between the phases, in run-all2.sh's order, each after the
    # whole of the phase before it
    order = [c.split()[1] if c.startswith("lux") else c for c in seq]
    last_reconcile = max(i for i, c in enumerate(order) if c == "reconcile")
    assert order[last_reconcile + 1:last_reconcile + 4] == ["identify", "metatypes", "write-refs"]
    first_merge = order.index("merge")
    assert first_merge == last_reconcile + 4
    assert order[0] == "google"
    assert order.index("portal") > max(i for i, c in enumerate(order) if c == "merge")
    assert order.index("portal") < order.index("export")
    assert not any(c.startswith("python") for c in seq), "no legacy script is run"
    assert "[Success] Build was successful" in open(env["STATUS_FILE"]).read()


@pytest.mark.parametrize("arg", ["--ycba", "ycba"])
def test_run_all_source_with_or_without_dashes(env, tmp_path, arg):
    r = run("run-all2.sh", arg, env=env, cwd=tmp_path)
    assert r.returncode == 0, r.stdout + r.stderr
    assert all("--source ycba" in c for c in lux_calls(env, "reconcile") + lux_calls(env, "merge"))


def test_run_all_mp_engine(env, tmp_path):
    env["ENGINE"] = "mp"
    r = run("run-all2.sh", "--all", env=env, cwd=tmp_path)
    assert r.returncode == 0, r.stdout + r.stderr
    commands = [c.split()[1] for c in calls(env) if c.startswith("lux")]
    # reconcile's own tail runs identify, metatypes and write-refs
    assert commands == ["google", "reconcile", "merge", "portal", "export"]
    for c in calls(env):
        if c.split()[1] in ("reconcile", "merge", "export"):
            assert "--my-worker" not in c and "--max-workers 3 --no-ui --prefix" in c
    assert "--no-finish" not in lux_calls(env, "reconcile")[0]


def test_run_all_rejects_unknown_engine(env, tmp_path):
    env["ENGINE"] = "ray"
    r = run("run-all2.sh", "--all", env=env, cwd=tmp_path)
    assert r.returncode == 1
    assert calls(env) == []


def test_run_all_stops_on_a_worker_traceback(env, tmp_path):
    env["FAKE_FAIL"] = "merge:1"
    r = run("run-all2.sh", "--all", env=env, cwd=tmp_path)
    assert r.returncode == 1
    assert "Error in merge!" in r.stdout
    # whichever the loop sees first: the worker log's Traceback, or the
    # `lux merge:` line the slice then writes to its stdout file
    assert "merge failed:" in r.stdout
    assert "merge-" in r.stdout and ("-w1.log:Traceback" in r.stdout or "merge_1.txt:lux merge:" in r.stdout)
    assert lux_calls(env, "export") == []
    assert "[Error] Error in merge" in open(env["STATUS_FILE"]).read()


def test_run_all_stops_when_lux_fails_before_its_worker(env, tmp_path):
    env["FAKE_NOSTART"] = "reconcile"
    r = run("run-all2.sh", "--all", env=env, cwd=tmp_path)
    assert r.returncode == 1
    assert "Error in reconcile!" in r.stdout
    assert "lux: could not load the pipeline configuration" in r.stdout
    assert lux_calls(env, "identify") == []


@pytest.mark.parametrize("step", ["identify", "metatypes", "write-refs"])
def test_run_all_stops_when_a_step_between_phases_fails(env, tmp_path, step):
    env["FAKE_NOSTART"] = step
    r = run("run-all2.sh", "--all", env=env, cwd=tmp_path)
    assert r.returncode == 1
    assert f"Error in {step}!" in r.stdout
    assert lux_calls(env, "merge") == []


@pytest.mark.parametrize("step,not_run", [("google", "reconcile"), ("portal", "export")])
def test_run_all_stops_when_google_or_portal_fails(env, tmp_path, step, not_run):
    env["FAKE_NOSTART"] = step
    r = run("run-all2.sh", "--all", env=env, cwd=tmp_path)
    assert r.returncode == 1
    assert f"Error in {step}!" in r.stdout
    assert lux_calls(env, not_run) == []


def test_run_all_mp_failure(env, tmp_path):
    env.update(ENGINE="mp", FAKE_FAIL="reconcile:0")
    r = run("run-all2.sh", "--all", env=env, cwd=tmp_path)
    assert r.returncode == 1
    assert "Error in reconcile!" in r.stdout
    assert lux_calls(env, "merge") == []


# ------------------------------------------------------ wait-for-flags.sh


def write_status(prefix, n, done=True, log="INFO fine\n"):
    with open(f"{prefix}-w{n}.status", "w") as fh:
        fh.write(f'{{"worker": {n}, "done": {"true" if done else "false"}}}')
    with open(f"{prefix}-w{n}.log", "w") as fh:
        fh.write(log)


def wait(env, cwd, *args, timeout=10):
    return subprocess.run(["bash", WAIT, *args], env=env, cwd=cwd, capture_output=True,
                          text=True, timeout=timeout)


def test_wait_succeeds_when_every_status_is_done(env, tmp_path):
    prefix = os.path.join(env["LOGS"], "merge-x")
    for n in range(3):
        write_status(prefix, n)
    assert wait(env, tmp_path, "merge", prefix, "3").returncode == 0


def test_wait_keeps_waiting_for_an_unfinished_slice(env, tmp_path):
    prefix = os.path.join(env["LOGS"], "merge-x")
    write_status(prefix, 0)
    write_status(prefix, 1, done=False)
    with pytest.raises(subprocess.TimeoutExpired):
        wait(env, tmp_path, "merge", prefix, "2", timeout=1)


def test_wait_fails_on_a_traceback_in_a_worker_log(env, tmp_path):
    prefix = os.path.join(env["LOGS"], "merge-x")
    write_status(prefix, 0)
    write_status(prefix, 1, done=False, log="ERROR boom\nTraceback (most recent call last):\n")
    r = wait(env, tmp_path, "merge", prefix, "2")
    assert r.returncode == 1
    assert "merge failed:" in r.stdout and "merge-x-w1.log:Traceback" in r.stdout


def test_wait_fails_on_a_lux_error_line_in_the_stdout_files(env, tmp_path):
    prefix = os.path.join(env["LOGS"], "merge-x")
    write_status(prefix, 0)
    with open(os.path.join(env["LOGS"], "merge_1.txt"), "w") as fh:
        fh.write("lux merge: ValueError: Unknown source: nope\n")
    r = wait(env, tmp_path, "merge", prefix, "2", os.path.join(env["LOGS"], "merge_*.txt"))
    assert r.returncode == 1
    assert "Unknown source: nope" in r.stdout


def test_wait_ignores_a_worker_of_another_run(env, tmp_path):
    prefix = os.path.join(env["LOGS"], "merge-x")
    write_status(prefix, 0)
    write_status(os.path.join(env["LOGS"], "merge-old"), 1,
                 log="Traceback (most recent call last):\n")
    assert wait(env, tmp_path, "merge", prefix, "1").returncode == 0


def test_wait_flag_file_fallback(env, tmp_path):
    flags = os.path.join(env["LOGS"], "flags")
    outputs = os.path.join(env["LOGS"], "merge_*.txt")
    for n in range(2):
        open(os.path.join(flags, f"merge_is_done-{n}.txt"), "w").close()
    assert wait(env, tmp_path, "merge", "-", "2", outputs).returncode == 0
    with pytest.raises(subprocess.TimeoutExpired):
        wait(env, tmp_path, "merge", "-", "3", outputs, timeout=1)
    with open(os.path.join(env["LOGS"], "merge_2.txt"), "w") as fh:
        fh.write("Traceback (most recent call last):\n")
    assert wait(env, tmp_path, "merge", "-", "3", outputs).returncode == 1


def test_wait_usage(env, tmp_path):
    assert wait(env, tmp_path, "merge").returncode == 2


# ------------------------------------------------------ *_parallel2.sh


PARALLEL = [
    # script, argument, slices, expected command, stdout file of slice 0
    ("reconcile_parallel2.sh", "--all", 24, "lux reconcile --source all --no-finish", "reconcile_0.txt"),
    ("updateExternal_parallel2.sh", "--wikidata", 20, "lux reconcile --source wikidata --no-finish",
     "update_ext_0.txt"),
    ("merge_parallel2.sh", "--all", 24, "lux merge --source all", "merge_0.txt"),
    ("export_parallel2.sh", None, 24, "lux export", "export_0.txt"),
    ("nt_parallel2.sh", None, 24, "lux export --type nt", "export_nt_0.txt"),
    ("import_parallel2.sh", "--wikidata", 24, "lux load --source wikidata", "load_wikidata_0.txt"),
    ("harvest_parallel2.sh", "--ycba", 50, "lux harvest --source ycba --pages", "harvest_0_ycba.txt"),
]


@pytest.mark.parametrize("script,arg,slices,command,stdout0", PARALLEL)
def test_parallel_wrappers(env, tmp_path, script, arg, slices, command, stdout0):
    r = run(script, *([arg] if arg else []), env=env, cwd=tmp_path)
    assert r.returncode == 0, r.stdout + r.stderr
    # the wrappers start the slices and return; the printed wait command
    # waits for them
    waitline = next(line for line in r.stdout.splitlines() if line.startswith("Wait with:"))
    cmd = waitline.split(":", 1)[1].strip()
    w = subprocess.run(["bash", "-c", cmd.replace("scripts/wait-for-flags.sh", WAIT)],
                       env=env, cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert w.returncode == 0, w.stdout + w.stderr

    got = calls(env)
    assert len(got) == slices
    assert sorted(worker_of(c) for c in got) == list(range(slices))
    assert all(c.startswith(f"{command} --my-worker ") for c in got)
    assert all(f"--max-workers {slices} --no-ui --prefix" in c for c in got)
    assert len({prefix_of(c) for c in got}) == 1
    assert os.path.exists(os.path.join(env["LOGS"], stdout0))


@pytest.mark.parametrize("script", ["reconcile_parallel2.sh", "merge_parallel2.sh", "import_parallel2.sh",
                                    "harvest_parallel2.sh", "updateExternal_parallel2.sh"])
def test_parallel_wrappers_need_a_source(env, tmp_path, script):
    r = run(script, env=env, cwd=tmp_path)
    assert r.returncode == 0 and "Usage" in r.stdout
    assert calls(env) == []
