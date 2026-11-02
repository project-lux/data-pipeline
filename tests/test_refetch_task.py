"""`lux refetch` (RefetchManager): the records used by default and the
whole datacache with --all-records, each identifier fetched with
refetch=True and stored, the fetcher switched on in the worker, the counts,
sources that cannot fetch skipped, and --source required -- against stub
configurations whose acquirers record their calls."""

import sys
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.cli import refetch as cli
from lux_pipeline.process.tasks import _task_manager
from lux_pipeline.process.tasks.refetch_task import RefetchManager
from lux_pipeline.progress import null_progress


class Keys:
    def __init__(self, keys):
        self.keys = list(keys)

    def iter_keys(self):
        return iter(self.keys)

    def iter_keys_slice(self, mySlice=0, maxSlice=10):
        return (k for i, k in enumerate(self.keys) if i % maxSlice == mySlice)

    def len_estimate(self):
        return len(self.keys)


class Fetcher:
    enabled = False


class Acquirer:
    """do_fetch as the real one: with refetch=False a cached record comes
    back without a request, which is what the old scripts got."""

    def __init__(self, fetcher, answers=None, ignore=()):
        self.fetcher = fetcher
        self.calls = []
        self.answers = answers or {}
        self.ignore_sources = list(ignore)

    def do_fetch(self, identifier, store=True, refetch=False):
        assert self.fetcher.enabled, "fetched with the fetcher off"
        self.calls.append((identifier, store, refetch))
        ans = self.answers.get(identifier, {"identifier": identifier, "data": {}})
        if isinstance(ans, Exception):
            raise ans
        return ans


def source(name, rc_keys, dc_keys, **kw):
    f = Fetcher()
    return {"name": name, "fetcher": f, "acquirer": Acquirer(f, **kw),
            "recordcache": Keys(rc_keys), "datacache": Keys(dc_keys)}


class Configs:
    max_workers = 2

    def __init__(self):
        self.internal = {"ycba": {"name": "ycba"}}            # no fetcher
        self.external = {
            "wikidata": source("wikidata",
                               ["Q1##quaPerson", "Q1##quaGroup", "Q2##quaPlace", "Q3##quaType"],
                               ["Q1", "Q2", "Q3", "Q4", "Q5"],
                               answers={"Q2": None, "Q3": {"identifier": "Q30", "data": {}}}),
            "tgn": source("tgn", ["7000001##quaPlace"], ["7000001", "7000002"],
                          answers={"7000002": TimeoutError("read timed out")}),
            "skipme": source("skipme", ["x"], ["x"], ignore=["skipme"]),
        }
        self.results = {}

    @staticmethod
    def split_qua(key):
        base, _, qua = key.partition("##qua")
        return base, qua


def manager(cfgs, sources, workers=1, **opts):
    m = RefetchManager(cfgs, workers, Namespace(**opts))
    for s in sources:
        m.prepare_single(s)
    return m


def run_all(m, cfgs):
    m.check()
    m.results = [m._distributed(n, cfgs, null_progress) for n in range(m.max_workers)]
    return m.summary()


def calls(cfgs, name):
    return cfgs.external[name]["acquirer"].calls


def test_by_default_the_records_used_each_once(caplog):
    cfgs = Configs()
    s = run_all(manager(cfgs, ["wikidata"]), cfgs)
    # Q1 is cached as a Person and a Group: fetched once; Q4, Q5 are only in
    # the datacache: not used, not fetched
    assert calls(cfgs, "wikidata") == [("Q1", True, True), ("Q2", True, True),
                                       ("Q3", True, True)]
    assert s["wikidata"] == {"fetched": 2, "empty": 1, "redirected": 1, "failed": 0}
    assert cfgs.external["wikidata"]["fetcher"].enabled is True


def test_all_records_takes_the_whole_datacache(caplog):
    cfgs = Configs()
    s = run_all(manager(cfgs, ["tgn"], all_records=True), cfgs)
    assert [c[0] for c in calls(cfgs, "tgn")] == ["7000001", "7000002"]
    assert s["tgn"] == {"fetched": 1, "empty": 0, "redirected": 0, "failed": 1}
    assert "tgn/7000002: TimeoutError: read timed out" in caplog.text


def test_workers_share_the_keys():
    cfgs = Configs()
    m = manager(cfgs, ["wikidata"], workers=2, all_records=True)
    s = run_all(m, cfgs)
    assert sorted(c[0] for c in calls(cfgs, "wikidata")) == ["Q1", "Q2", "Q3", "Q4", "Q5"]
    assert s["wikidata"]["fetched"] + s["wikidata"]["empty"] == 5


def test_max_records_per_worker():
    cfgs = Configs()
    m = manager(cfgs, ["wikidata"], all_records=True)
    m.max_records = 2
    run_all(m, cfgs)
    assert [c[0] for c in calls(cfgs, "wikidata")] == ["Q1", "Q2"]


def test_sources_that_cannot_fetch_are_skipped(caplog):
    cfgs = Configs()
    m = manager(cfgs, ["ycba", "skipme", "tgn"])
    assert m.sources == [("external", "tgn")]
    assert "ycba: no acquirer or fetcher" in caplog.text
    assert "skipme: in ignore_sources" in caplog.text
    with pytest.raises(ValueError, match="No source to refetch"):
        manager(cfgs, ["ycba"]).check()


# --- the command ------------------------------------------------------------------


def run_command(cfgs, rest, tmp_path, monkeypatch, source):
    monkeypatch.setattr(_task_manager.runtime, "get_configs", lambda: cfgs)
    cfgs.log_dir = str(tmp_path)
    args = Namespace(command="refetch", source=source, max_workers=1, engine="null",
                     no_ui=True, prefix=str(tmp_path / "refetch"), log="INFO",
                     max_records=-1, my_worker=-1)
    return cli.CommandHandler(cfgs).process(args, list(rest))


def test_command_needs_a_source(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="needs --source"):
        run_command(Configs(), [], tmp_path, monkeypatch, None)


def test_command_all_is_the_external_sources(tmp_path, monkeypatch, capsys):
    cfgs = Configs()
    m = run_command(cfgs, ["--all-records"], tmp_path, monkeypatch, "all")
    assert m.all_records is True
    assert [n for _, n in m.sources] == ["wikidata", "tgn"]
    out = capsys.readouterr().out
    assert "source" in out and "fetched" in out and "redirected" in out
    assert any(l.startswith("wikidata") and l.split()[1:] == ["4", "1", "1", "0"]
               for l in out.splitlines())


def test_listed():
    from lux_pipeline.cli.help import list_commands
    assert dict(list_commands())["refetch"].startswith("Fetch records again from their source")
