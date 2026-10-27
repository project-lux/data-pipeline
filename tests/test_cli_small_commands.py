"""`lux identify`, `lux new-token`, `lux metatypes` and `lux write-refs`:
the steps between reconcile and merge as commands. The token sequence and
`new-token` working without an idmap (the file it writes is what the idmap
needs to start), the in-progress guard all four share, the second-run
guards of `metatypes` and `write-refs`, and the order each runs its steps
in -- all against stub configurations, no database."""

import datetime
import json
import os
import sys
import time
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline import runtime
from lux_pipeline.cli import _handler, entry, identify, metatypes, new_token, write_refs
from lux_pipeline.cli._handler import refuse_while_building
from lux_pipeline.cli.help import list_commands
from lux_pipeline.process.phases import identify as identify_phase
from lux_pipeline.process.phases import reconcile as reconcile_phase
from lux_pipeline.process.phases.identify import report_lines, vacuum_idmap
from lux_pipeline.process.phases.reconcile import new_update_token, next_update_token
from lux_pipeline.process.reference_manager import ReferenceManager
from lux_pipeline.progress import unfinished_workers

TODAY = datetime.datetime(2026, 10, 27, 9, 30)


# --- stubs -------------------------------------------------------------------


class Idmap:
    def __init__(self, log, token="__20260101__", optimize=True):
        self.log = log
        self.update_token = token
        if optimize:
            self.optimize = lambda: self.log.append("optimize")


class Queue:
    """A reference queue: `queue_length` like the Postgres one, or only
    ``len()`` like the Redis one."""

    def __init__(self, log, size=3, counted=True):
        self.log = log
        self.size = size
        if counted:
            self.queue_length = lambda ceiling: min(self.size, ceiling)

    def __len__(self):
        return self.size

    def clear(self):
        self.log.append("clear")
        self.size = 0


class StubConfigs:
    def __init__(self, tmp_path, log=None, token="__20260101__", optimize=True):
        self.log = [] if log is None else log
        self.data_dir = str(tmp_path / "data")
        self.log_dir = str(tmp_path / "logs")
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        self.idmap = Idmap(self.log, token, optimize)

    def get_idmap(self):
        self.log.append("get_idmap")
        return self.idmap


class NoIdmapConfigs(StubConfigs):
    """What `lux new-token` must cope with: no idmap can be built."""

    def get_idmap(self):
        raise ValueError("update token")


def token_file(cfgs):
    return Path(cfgs.data_dir) / "idmap_update_token.txt"


def write_status(cfgs, name, done=False, age=0):
    path = Path(cfgs.log_dir) / name
    path.write_text(json.dumps({"worker": 0, "completed": 1, "total": None, "done": done,
                                "updated": time.time() - age}))
    return path


def run(handler_cls, cfgs, *argv):
    args = Namespace(command="test")
    return handler_cls(cfgs).process(args, list(argv))


@pytest.fixture
def cwd(tmp_path, monkeypatch):
    d = tmp_path / "cwd"
    d.mkdir()
    monkeypatch.chdir(d)
    return d


@pytest.fixture
def today(monkeypatch):
    class _datetime:
        @staticmethod
        def now():
            return TODAY

    monkeypatch.setattr(reconcile_phase, "datetime", Namespace(datetime=_datetime))


# --- the token ---------------------------------------------------------------


@pytest.mark.parametrize("current, expected", [
    (None, "__20261027__"),              # no token at all
    ("", "__20261027__"),
    ("__20260101__", "__20261027__"),    # first build of the day
    ("__20261027__", "__20261027a__"),   # second
    ("__20261027a__", "__20261027b__"),  # third
    ("__20261027y__", "__20261027z__"),
])
def test_next_update_token(current, expected):
    assert next_update_token(current, TODAY) == expected


def test_next_update_token_zero_pads_month_and_day():
    assert next_update_token("__20260305__", datetime.datetime(2026, 3, 5)) == "__20260305a__"
    assert next_update_token(None, datetime.datetime(2026, 3, 5)) == "__20260305__"


@pytest.mark.parametrize("content, expected", [
    ("__20261027a__\n", "__20261027b__"),
    ("__20251231__", "__20261027__"),
])
def test_new_update_token_without_an_idmap_reads_the_file(tmp_path, today, content, expected):
    cfgs = NoIdmapConfigs(tmp_path)
    token_file(cfgs).write_text(content)
    assert new_update_token(cfgs) == expected
    assert token_file(cfgs).read_text() == f"{expected}\n"


@pytest.mark.parametrize("content", [None, "garbage\n", "20261027a"])
def test_a_missing_or_malformed_token_file_gives_todays_first(tmp_path, today, caplog, content):
    cfgs = NoIdmapConfigs(tmp_path)
    if content is not None:
        token_file(cfgs).write_text(content)
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        assert new_update_token(cfgs) == "__20261027__"
    assert token_file(cfgs).read_text() == "__20261027__\n"
    assert "No usable update token" in caplog.text


def test_new_update_token_with_an_idmap_follows_and_updates_it(tmp_path, today):
    cfgs = StubConfigs(tmp_path, token="__20261027__")
    token_file(cfgs).write_text("__20200101__\n")   # the idmap's token wins
    assert new_update_token(cfgs, cfgs.idmap) == "__20261027a__"
    assert cfgs.idmap.update_token == "__20261027a__"


def test_lux_new_token_never_builds_the_idmap(tmp_path, today, caplog):
    cfgs = NoIdmapConfigs(tmp_path)
    token_file(cfgs).write_text("__20261027__\n")
    with caplog.at_level("INFO", logger="lux_pipeline"):
        assert run(new_token.CommandHandler, cfgs) == "__20261027a__"
    assert token_file(cfgs).read_text() == "__20261027a__\n"
    assert "New update token: __20261027a__" in caplog.text


def test_lux_new_token_loads_the_configuration_uninstantiated(tmp_path, today, monkeypatch):
    """Through the entry point: needs_configs is False, so entry loads
    nothing, and the handler asks for a configuration without the idmap."""
    cfgs = NoIdmapConfigs(tmp_path)
    calls = []

    def get_configs(*a, **kw):
        calls.append(kw)
        return cfgs

    monkeypatch.setattr(runtime, "get_configs", get_configs)
    assert entry.run(["new-token"]) == 0
    assert calls == [{"instantiate": False}]
    assert token_file(cfgs).read_text() == "__20261027__\n"


def test_lux_new_token_refuses_during_a_build(tmp_path, today):
    cfgs = NoIdmapConfigs(tmp_path)
    token_file(cfgs).write_text("__20261027__\n")
    write_status(cfgs, "reconcile-2026-10-27-090000-w0.status")
    with pytest.raises(RuntimeError, match="a build is running"):
        run(new_token.CommandHandler, cfgs)
    assert token_file(cfgs).read_text() == "__20261027__\n"
    assert run(new_token.CommandHandler, cfgs, "--force") == "__20261027a__"


# --- the in-progress guard ---------------------------------------------------


def test_nothing_running_is_fine(tmp_path):
    cfgs = StubConfigs(tmp_path)
    write_status(cfgs, "merge-2026-10-27-080000-w0.status", done=True)
    assert refuse_while_building(cfgs, "identify") == []


def test_no_log_dir_is_fine(tmp_path):
    cfgs = StubConfigs(tmp_path)
    cfgs.log_dir = None
    assert refuse_while_building(cfgs, "identify") == []
    cfgs.log_dir = str(tmp_path / "missing")
    assert unfinished_workers(cfgs.log_dir) == []


def test_a_recent_unfinished_worker_refuses_and_names_the_build(tmp_path):
    cfgs = StubConfigs(tmp_path)
    prefix = os.path.join(cfgs.log_dir, "reconcile-2026-10-27-090000")
    write_status(cfgs, "reconcile-2026-10-27-090000-w0.status", age=120)
    write_status(cfgs, "reconcile-2026-10-27-090000-w1.status", age=30)
    write_status(cfgs, "reconcile-2026-10-27-090000-w2.status", done=True)
    with pytest.raises(RuntimeError) as e:
        refuse_while_building(cfgs, "identify")
    msg = str(e.value)
    assert f"{prefix}: 2 worker(s) not done, last update 0 min ago" in msg
    assert "not running identify" in msg and "--force" in msg


def test_force_runs_anyway_with_a_warning(tmp_path, caplog):
    cfgs = StubConfigs(tmp_path)
    write_status(cfgs, "merge-2026-10-27-090000-w3.status")
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        got = refuse_while_building(cfgs, "write-refs", force=True)
    assert got == [os.path.join(cfgs.log_dir, "merge-2026-10-27-090000")]
    assert "running write-refs anyway (--force)" in caplog.text


def test_a_long_silent_worker_was_killed_and_only_warns(tmp_path, caplog):
    cfgs = StubConfigs(tmp_path)
    write_status(cfgs, "reconcile-2026-10-26-090000-w5.status", age=_handler.RUNNING_WINDOW + 60)
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        assert refuse_while_building(cfgs, "identify") == []
    assert "probably killed" in caplog.text
    assert "reconcile-2026-10-26-090000: 1 worker(s)" in caplog.text


def test_unreadable_status_files_are_skipped(tmp_path):
    cfgs = StubConfigs(tmp_path)
    (Path(cfgs.log_dir) / "load-2026-10-27-090000-w0.status").write_text("{not json")
    assert unfinished_workers(cfgs.log_dir) == []
    assert refuse_while_building(cfgs, "identify") == []


@pytest.mark.parametrize("handler", [identify.CommandHandler, metatypes.CommandHandler,
                                     write_refs.CommandHandler])
def test_every_command_refuses_before_touching_anything(tmp_path, cwd, handler):
    cfgs = StubConfigs(tmp_path)
    write_status(cfgs, "reconcile-2026-10-27-090000-w0.status")
    with pytest.raises(RuntimeError, match="a build is running"):
        run(handler, cfgs)
    assert cfgs.log == []


# --- identify ----------------------------------------------------------------


@pytest.fixture
def resolver(monkeypatch):
    calls = []

    class Resolver:
        def __init__(self, cfgs, idmap, my_slice=-1, max_slice=-1):
            calls.append(("IdentityResolver", idmap.update_token, my_slice))
            cfgs.log.append("IdentityResolver")

        def resolve_identity(self):
            calls.append(("resolve_identity",))
            return {"nodes": 5, "pairs": 4, "diff_pairs": 1, "clusters": 2, "conflicts": 0}

    monkeypatch.setattr(identify_phase, "IdentityResolver", Resolver)
    return calls


def test_identify_resolves_prints_and_vacuums(tmp_path, cwd, resolver, capsys):
    cfgs = StubConfigs(tmp_path, token="__20261027__")
    stats = run(identify.CommandHandler, cfgs)
    assert cfgs.log == ["get_idmap", "IdentityResolver", "optimize"]
    # the whole map, unsliced, stamped with the idmap's token
    assert resolver[0] == ("IdentityResolver", "__20261027__", -1)
    assert stats["clusters"] == 2 and "seconds" in stats
    out = capsys.readouterr().out.splitlines()
    assert out[:3] == ["nodes=5 pairs=4 diff_pairs=1 clusters=2",
                       "conflicts=0 (see identity_conflicts.jsonl)",
                       f"done in {stats['seconds']}s"]
    assert out[3:] == ["", "Vacuuming the identity map..."]


def test_identify_no_vacuum(tmp_path, cwd, resolver, capsys):
    cfgs = StubConfigs(tmp_path)
    run(identify.CommandHandler, cfgs, "--no-vacuum")
    assert "optimize" not in cfgs.log
    assert "Vacuuming" not in capsys.readouterr().out


def test_identify_through_the_entry_point(tmp_path, cwd, resolver, monkeypatch, capsys):
    cfgs = StubConfigs(tmp_path)
    monkeypatch.setattr(runtime, "get_configs", lambda *a, **kw: cfgs)
    assert entry.run(["identify", "--no-vacuum"]) == 0
    assert cfgs.log == ["get_idmap", "IdentityResolver"]


def test_vacuum_without_optimize_is_skipped():
    assert vacuum_idmap(object()) is False
    log = []
    assert vacuum_idmap(Idmap(log)) is True
    assert log == ["optimize"]


def test_report_lines_tolerate_missing_stats():
    assert report_lines({})[0] == "nodes=None pairs=None diff_pairs=None clusters=None"


# --- metatypes ---------------------------------------------------------------


@pytest.fixture
def real_merge(monkeypatch):
    """`lux metatypes` with the real `merge_metatypes`, on a reference
    manager built without its constructor (which reads replacements.json)."""
    def make(cfgs, idmap):
        cfgs.log.append("ReferenceManager")
        rm = ReferenceManager.__new__(ReferenceManager)
        rm.configs = cfgs
        return rm

    monkeypatch.setattr(metatypes, "ReferenceManager", make)


def test_metatypes_merges_the_slices_into_the_data_dir(tmp_path, cwd, real_merge):
    cfgs = StubConfigs(tmp_path)
    (cwd / "metatypes-0.json").write_text(json.dumps({"a": ["t1"]}))
    (cwd / "metatypes-1.json").write_text(json.dumps({"a": ["t2"], "b": ["t3"]}))
    got = run(metatypes.CommandHandler, cfgs)
    assert got == {"a": ["t1", "t2"], "b": ["t3"]}
    assert json.loads((Path(cfgs.data_dir) / "metatypes.json").read_text()) == got
    assert list(cwd.iterdir()) == []


def test_metatypes_refuses_to_empty_an_existing_map(tmp_path, cwd, real_merge):
    cfgs = StubConfigs(tmp_path)
    target = Path(cfgs.data_dir) / "metatypes.json"
    target.write_text(json.dumps({"a": ["t1"]}))
    with pytest.raises(RuntimeError, match="no metatypes-\\*.json"):
        run(metatypes.CommandHandler, cfgs)
    assert json.loads(target.read_text()) == {"a": ["t1"]}
    assert run(metatypes.CommandHandler, cfgs, "--force") == {}
    assert json.loads(target.read_text()) == {}


def test_metatypes_with_nothing_anywhere_writes_an_empty_map(tmp_path, cwd, real_merge):
    cfgs = StubConfigs(tmp_path)
    assert run(metatypes.CommandHandler, cfgs) == {}
    assert (Path(cfgs.data_dir) / "metatypes.json").exists()


# --- write-refs --------------------------------------------------------------


@pytest.fixture
def ref_mgr(monkeypatch):
    """A reference manager whose write_done_refs writes one line."""
    made = []

    class RefMgr:
        def __init__(self, cfgs, idmap, workers=1):
            cfgs.log.append("ReferenceManager")
            self.log = cfgs.log
            self.done_refs = made[0] if made else Queue(cfgs.log)

        def write_done_refs(self):
            self.log.append("write_done_refs")
            with open("reference_uris.txt", "w") as fh:
                fh.write("0|yuid|uri\n")

    for mod in (write_refs, reconcile_phase):
        monkeypatch.setattr(mod, "ReferenceManager", RefMgr)
    return made


def test_write_refs_writes_then_clears(tmp_path, cwd, ref_mgr, capsys):
    cfgs = StubConfigs(tmp_path)
    path = run(write_refs.CommandHandler, cfgs)
    assert cfgs.log == ["get_idmap", "ReferenceManager", "write_done_refs", "clear"]
    assert path == str(cwd / "reference_uris.txt")
    assert capsys.readouterr().out == f"wrote {path}\n"


@pytest.mark.parametrize("counted", [True, False])
def test_write_refs_refuses_to_empty_an_existing_file(tmp_path, cwd, ref_mgr, counted):
    cfgs = StubConfigs(tmp_path)
    ref_mgr.append(Queue(cfgs.log, size=0, counted=counted))
    (cwd / "reference_uris.txt").write_text("0|yuid|kept\n")
    with pytest.raises(RuntimeError, match="queue is empty"):
        run(write_refs.CommandHandler, cfgs)
    assert (cwd / "reference_uris.txt").read_text() == "0|yuid|kept\n"
    assert "write_done_refs" not in cfgs.log
    run(write_refs.CommandHandler, cfgs, "--force")
    assert cfgs.log[-2:] == ["write_done_refs", "clear"]


def test_write_refs_overwrites_an_old_file_when_there_is_a_queue(tmp_path, cwd, ref_mgr):
    cfgs = StubConfigs(tmp_path)
    (cwd / "reference_uris.txt").write_text("last build\n")
    run(write_refs.CommandHandler, cfgs)
    assert (cwd / "reference_uris.txt").read_text() == "0|yuid|uri\n"


def test_write_refs_with_an_empty_queue_and_no_file_runs(tmp_path, cwd, ref_mgr):
    cfgs = StubConfigs(tmp_path)
    ref_mgr.append(Queue(cfgs.log, size=0))
    run(write_refs.CommandHandler, cfgs)
    assert "write_done_refs" in cfgs.log


def test_the_phase_function_builds_its_own_manager(tmp_path, cwd, ref_mgr):
    cfgs = StubConfigs(tmp_path)
    path = reconcile_phase.write_refs(cfgs, cfgs.idmap)
    assert cfgs.log == ["ReferenceManager", "write_done_refs", "clear"]
    assert path == os.path.abspath("reference_uris.txt")


# --- listed ------------------------------------------------------------------


def test_the_commands_are_listed():
    listed = dict(list_commands())
    for name in ("identify", "new-token", "metatypes", "write-refs"):
        assert name in listed and listed[name]
