"""`ExportManager` and `lux export`: which phase function each type calls
and with what (its own slice, the directories, the compression and chunk
options, the source list), the external export sharing sources out between
workers, the selection rules, and the command end to end on the null
engine, with the phase functions as recorders. `export_external` itself
against in-memory caches: one line per identifier, in the form
`Loader.load_export2` reads."""

import gzip
import json
import os
import sys
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.cli.export import CommandHandler
from lux_pipeline.process.phases import export as phase
from lux_pipeline.process.tasks import _task_manager, export_task
from lux_pipeline.process.tasks._task_manager import WorkerFailed
from lux_pipeline.process.tasks.export_task import ExportManager
from lux_pipeline.progress import StatusWriter, null_progress

PHASES = ("export_slice", "export_nt_slice", "export_sources_slice", "export_external")


# --- stubs -------------------------------------------------------------------


class StubConfigs:
    """Two internal sources, five external, the merged results, and the
    directories."""

    max_workers = 4

    def __init__(self, tmp_path, nt_dir=True):
        self.data_dir = str(tmp_path / "data")
        self.log_dir = str(tmp_path / "logs")
        self.exports_dir = str(tmp_path / "exports")
        if nt_dir:
            self.nt_dir = str(tmp_path / "nt")
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        self.internal = {n: {"name": n} for n in ("ypm", "ycba")}
        self.external = {n: {"name": n} for n in ("aat", "ulan", "lcsh", "tgn", "wikidata")}
        self.results = {"merged": {"name": "merged"}, "marklogic": {"name": "marklogic"}}


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """A configuration, the phase functions replaced by recorders, and
    `runtime.get_configs` pointed at the configuration so the null engine's
    worker finds it."""
    monkeypatch.delenv("LUX_EXPORT_CHUNK", raising=False)
    monkeypatch.delenv("LUX_EXPORT_GZIP", raising=False)

    class Harness:
        def __init__(self):
            self.cfgs = None
            self.calls = []
            self.fail = False

        def configure(self, **kw):
            self.cfgs = StubConfigs(tmp_path, **kw)
            monkeypatch.setattr(_task_manager.runtime, "get_configs", lambda: self.cfgs)
            return self.cfgs

        def patch(self):
            def recorder(fname):
                def fn(cfgs, *args, **kw):
                    self.calls.append((fname, cfgs, args, kw))
                    if self.fail:
                        raise RuntimeError(f"{fname} raised")
                    return {"fn": fname, "args": args}
                return fn
            for fname in PHASES:
                monkeypatch.setattr(export_task, fname, recorder(fname))

        def manager(self, sources=None, workers=4, **options):
            if self.cfgs is None:
                self.configure()
            self.patch()
            m = ExportManager(self.cfgs, workers, Namespace(**options))
            if sources is None:
                sources = export_task.default_sources(m.export_type) or []
            for s in sources:
                m.prepare_single(s)
            return m

        def run_all(self, m):
            m.check()
            for n in range(m.max_workers):
                m._distributed(n, self.cfgs, null_progress)

    return Harness()


# --- dispatch ----------------------------------------------------------------


def test_marklogic_is_the_default_and_calls_export_slice(harness):
    h = harness
    m = h.manager(workers=1)
    assert m.export_type == "marklogic"
    result = m.run_single()
    fname, cfgs, args, kw = h.calls[0]
    assert fname == "export_slice" and cfgs is h.cfgs
    assert args == (0, 1)
    assert kw == dict(no_ml=False, gz=False, chunk_size=1000, gzip_level=9, out_dir=None,
                      progress=null_progress)
    assert result == {"fn": "export_slice", "args": (0, 1)}


def test_every_worker_gets_its_own_slice(harness):
    h = harness
    m = h.manager(workers=4)
    h.run_all(m)
    assert [c[2] for c in h.calls] == [(0, 4), (1, 4), (2, 4), (3, 4)]


def test_merged_is_export_slice_without_marklogic(harness):
    h = harness
    m = h.manager(workers=1, export_type="merged", gz=True, chunk=50, gzip_level=3,
                  out_dir="/x")
    m.run_single()
    _, _, _, kw = h.calls[0]
    assert (kw["no_ml"], kw["gz"], kw["chunk_size"], kw["gzip_level"], kw["out_dir"]) == \
        (True, True, 50, 3, "/x")


def test_the_environment_supplies_chunk_and_gzip_level(harness, monkeypatch):
    h = harness
    monkeypatch.setenv("LUX_EXPORT_CHUNK", "250")
    monkeypatch.setenv("LUX_EXPORT_GZIP", "4")
    m = h.manager(workers=1, gz=True)
    m.run_single()
    assert (h.calls[0][3]["chunk_size"], h.calls[0][3]["gzip_level"]) == (250, 4)


def test_nt_goes_to_the_nt_dir(harness):
    h = harness
    m = h.manager(workers=2, export_type="nt")
    h.run_all(m)
    assert [(c[0], c[2]) for c in h.calls] == [("export_nt_slice", (0, 2)),
                                                ("export_nt_slice", (1, 2))]
    assert h.calls[0][3] == dict(out_dir=h.cfgs.nt_dir, progress=null_progress)


def test_nt_falls_back_to_the_phase_default_without_an_nt_dir(harness):
    h = harness
    h.configure(nt_dir=False)
    m = h.manager(workers=1, export_type="nt")
    m.run_single()
    assert h.calls[0][3]["out_dir"] is None


def test_out_dir_overrides_the_nt_dir(harness):
    h = harness
    m = h.manager(workers=1, export_type="nt", out_dir="/x", gzip_level=5)
    m.run_single()
    assert h.calls[0][3] == dict(out_dir="/x", gzip_level=5, progress=null_progress)


def test_sources_by_default_are_the_four_in_order(harness):
    h = harness
    m = h.manager(workers=2, export_type="sources")
    h.run_all(m)
    fname, _, args, kw = h.calls[1]
    assert (fname, args) == ("export_sources_slice", (1, 2))
    assert kw == dict(sources=["aat", "ulan", "lcsh", "tgn"], out_dir=None,
                      nt_dir=h.cfgs.nt_dir, progress=null_progress)


def test_sources_named_and_out_dir(harness):
    h = harness
    m = h.manager(sources=["tgn", "aat"], workers=1, export_type="sources", out_dir="/x")
    m.run_single()
    kw = h.calls[0][3]
    assert (kw["sources"], kw["out_dir"], kw["nt_dir"]) == (["tgn", "aat"], "/x", "/x")


def test_external_shares_sources_out_and_caps_the_workers(harness):
    h = harness
    m = h.manager(sources=["aat", "ulan", "lcsh", "tgn", "wikidata"], workers=2,
                  export_type="external")
    assert m.hide_idle_workers
    h.run_all(m)
    assert [c[2][0] for c in h.calls] == ["aat", "lcsh", "wikidata", "ulan", "tgn"]
    assert h.calls[0][3] == dict(out_dir=None, chunk_size=1000, progress=null_progress)

    h.calls.clear()
    m = h.manager(sources=["aat", "tgn"], workers=8, export_type="external-zip")
    assert m.export_type == "external"
    h.run_all(m)
    assert m.max_workers == 2
    assert [c[2][0] for c in h.calls] == ["aat", "tgn"]


def test_an_idle_external_worker_returns_nothing(harness):
    h = harness
    m = h.manager(sources=["aat"], workers=4, export_type="external")
    assert m._distributed(3, h.cfgs, null_progress) == []
    assert h.calls == []


def test_process_on_the_null_engine(harness):
    h = harness
    m = h.manager(workers=4, my_worker=2)
    results = m.process("null")
    assert results == [{"fn": "export_slice", "args": (2, 4)}]


def test_a_worker_failure_is_raised(harness):
    h = harness
    m = h.manager(workers=1)
    h.fail = True
    with pytest.raises(WorkerFailed):
        m.process("null")


# --- selection ---------------------------------------------------------------


def test_unknown_type_is_rejected(harness):
    with pytest.raises(ValueError, match="Unknown export type: qlever"):
        harness.manager(export_type="qlever")


@pytest.mark.parametrize("export_type, source, match", [
    ("marklogic", "ycba", "exports the merged records, not ycba"),
    ("nt", "aat", "exports the merged records, not aat"),
    ("sources", "ycba", "applies to external sources, not ycba"),
    ("external", "merged", "applies to external sources, not merged"),
])
def test_wrong_sources_are_rejected(harness, export_type, source, match):
    h = harness
    m = h.manager(sources=[], export_type=export_type)
    with pytest.raises(ValueError, match=match):
        m.prepare_single(source)


def test_external_with_no_source_is_rejected_before_any_worker(harness):
    h = harness
    m = h.manager(sources=[], workers=1, export_type="external")
    with pytest.raises(ValueError, match="No source selected"):
        m.process("null")
    assert h.calls == []


def test_a_source_is_added_once(harness):
    m = harness.manager(sources=["aat", "aat"], export_type="sources")
    assert m.sources == [("external", "aat")]


# --- export_external ---------------------------------------------------------


class MemCache:
    def __init__(self, rows, multi=True):
        self.rows = rows
        if not multi:
            self.get_multi = None
        self.queries = 0

    def iter_keys(self):
        return iter(self.rows)

    def get_multi(self, keys, raw=False):
        self.queries += 1
        return {k: {"identifier": k, "data": json.dumps(self.rows[k]) if raw else self.rows[k]}
                for k in keys if k in self.rows}

    def __getitem__(self, k):
        return {"identifier": k, "data": self.rows[k]} if k in self.rows else None

    def len_estimate(self):
        return len(self.rows)


class ExtConfigs:
    def __init__(self, tmp_path, keys, data, multi=True):
        self.exports_dir = str(tmp_path / "exports")
        self.external = {"aat": {"name": "aat",
                                 "recordcache": MemCache(dict.fromkeys(keys, {})),
                                 "datacache": MemCache(data, multi)}}

    def split_qua(self, recid):
        return recid.split("##qua")


@pytest.fixture
def no_pool(monkeypatch):
    class Pool:
        @classmethod
        def get_instance(cls):
            return cls()

        def put_all(self, name):
            pass
    monkeypatch.setattr(phase, "PoolManager", Pool)


def read_lines(fn):
    with gzip.open(fn, "rt") as fh:
        return [line.rstrip("\n").split("\t", 1) for line in fh]


@pytest.mark.parametrize("multi", [True, False])
def test_export_external_writes_one_line_per_identifier(tmp_path, no_pool, caplog, multi):
    keys = ["300001##quaType", "300001##quaMaterial", "300002##quaType", "300003##quaType"]
    data = {"300001": {"id": "aat:300001"}, "300002": {"id": "aat:300002"}}
    cfgs = ExtConfigs(tmp_path, keys, data, multi)
    seen = []
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        result = phase.export_external(cfgs, "aat", chunk_size=2,
                                       progress=lambda **kw: seen.append(kw))
    fn = os.path.join(cfgs.exports_dir, "export_aat.jsonl.gz")
    assert result == {"source": "aat", "written": 2, "missing": 1, "path": fn}
    lines = read_lines(fn)
    assert [ident for ident, _ in lines] == ["300001", "300002"]
    assert [json.loads(js) for _, js in lines] == [{"data": data["300001"]},
                                                   {"data": data["300002"]}]
    assert "1 identifiers in the recordcache have no datacache record" in caplog.text
    assert seen[0] == {"description": "aat", "total": 4}
    assert sum(kw.get("advance", 0) for kw in seen) == 4
    if multi:
        assert cfgs.external["aat"]["datacache"].queries == 2


def test_export_external_lines_load_with_load_export2(tmp_path, no_pool):
    """The lines go through the loader's own parse into what the datacache's
    __setitem__ takes as a full record."""
    cfgs = ExtConfigs(tmp_path, ["300001##quaType"], {"300001": {"id": "aat:300001"}})
    result = phase.export_external(cfgs, "aat")
    with gzip.open(result["path"], "rt") as fh:
        for line in fh:
            ident, json_data = line.strip().split("\t", 1)
            js = json.loads(json_data)
    assert ident == "300001" and js == {"data": {"id": "aat:300001"}}


def test_export_external_leaves_no_file_when_empty(tmp_path, no_pool):
    cfgs = ExtConfigs(tmp_path, [], {})
    result = phase.export_external(cfgs, "aat", out_dir=str(tmp_path / "o"))
    assert result == {"source": "aat", "written": 0, "missing": 0, "path": None}
    assert os.listdir(tmp_path / "o") == []


# --- the command -------------------------------------------------------------


def run_command(cfgs, rest, tmp_path, source=None, workers=1, my_worker=-1):
    args = Namespace(command="export", source=source, max_workers=workers,
                     engine="null", no_ui=True, prefix=str(tmp_path / "export"),
                     log="INFO", max_records=-1, my_worker=my_worker)
    return CommandHandler(cfgs).process(args, list(rest))


def resolve(cfgs, export_type, source):
    return CommandHandler(cfgs).resolve_sources(Namespace(export_type=export_type, source=source))


def test_resolve_sources(tmp_path):
    cfgs = StubConfigs(tmp_path)
    for t in ("marklogic", "merged", "nt"):
        for s in (None, "all", "merged"):
            assert resolve(cfgs, t, s) == ["merged"]
    with pytest.raises(ValueError, match="--source ycba does not apply"):
        resolve(cfgs, "marklogic", "ycba")
    assert resolve(cfgs, "sources", None) == ["aat", "ulan", "lcsh", "tgn"]
    assert resolve(cfgs, "sources", "all") == ["aat", "ulan", "lcsh", "tgn"]
    assert resolve(cfgs, "sources", "tgn") == ["tgn"]
    assert resolve(cfgs, "external-zip", "all") == ["aat", "ulan", "lcsh", "tgn", "wikidata"]
    assert resolve(cfgs, "external", "wikidata,aat") == ["wikidata", "aat"]
    with pytest.raises(ValueError, match="needs --source"):
        resolve(cfgs, "external", None)
    with pytest.raises(ValueError, match="Unknown source: nope"):
        resolve(cfgs, "external", "nope")


def test_command_full_run(harness, tmp_path, capsys):
    h = harness
    h.configure()
    h.patch()
    m = run_command(h.cfgs, [], tmp_path, workers=1)
    assert isinstance(m, ExportManager)
    assert m.sources == [("results", "merged")]
    fname, _, args, kw = h.calls[0]
    assert (fname, args) == ("export_slice", (0, 1))
    assert isinstance(kw["progress"], StatusWriter)
    assert "status/log prefix:" in capsys.readouterr().out
    assert (tmp_path / "export-w0.status").exists()


def test_command_options(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    m = run_command(h.cfgs, ["--type", "merged", "--gz", "--chunk", "10", "--gzip-level", "2",
                             "--out-dir", "/x"], tmp_path, workers=1)
    assert (m.export_type, m.gz, m.chunk_size, m.gzip_level, m.out_dir) == \
        ("merged", True, 10, 2, "/x")
    assert h.calls[0][3]["no_ml"] is True


def test_command_external(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    m = run_command(h.cfgs, ["--type", "external-zip"], tmp_path, source="aat,tgn", workers=4)
    assert m.max_workers == 2
    assert [c[2][0] for c in h.calls] == ["aat"]          # the null engine runs worker 0


def test_command_my_worker(harness, tmp_path):
    h = harness
    h.configure()
    h.patch()
    run_command(h.cfgs, ["--type", "nt"], tmp_path, workers=24, my_worker=3)
    assert [c[2] for c in h.calls] == [(3, 24)]


def test_command_rejects_an_unknown_type(harness, tmp_path):
    h = harness
    h.configure()
    with pytest.raises(SystemExit):
        run_command(h.cfgs, ["--type", "qlever"], tmp_path)


def test_command_is_listed_by_help():
    from lux_pipeline.cli.help import list_commands
    assert ("export", "Export the build's records: MarkLogic JSONL, merged JSONL, N-Triples, "
                      "sources.") in list_commands()


# --- what an earlier run left ------------------------------------------------


def earlier_export(cfgs, out_dir=None):
    out = Path(out_dir or cfgs.exports_dir)
    out.mkdir(parents=True, exist_ok=True)
    for fn in ("export_full_0.jsonl", "export_full_30.jsonl.gz", "export_merged_0.jsonl",
               "export_aat.jsonl.gz", "notes.txt"):
        (out / fn).write_text("")
    flags = Path(cfgs.log_dir) / "flags"
    flags.mkdir(exist_ok=True)
    (flags / "export_is_done-30.txt").write_text("1")
    return out


@pytest.mark.parametrize("export_type,left", [
    ("marklogic", ["export_aat.jsonl.gz", "export_merged_0.jsonl", "notes.txt"]),
    ("merged", ["export_aat.jsonl.gz", "export_full_0.jsonl", "export_full_30.jsonl.gz",
                "notes.txt"]),
])
def test_a_full_run_clears_its_type_earlier_slices(harness, export_type, left):
    h = harness
    cfgs = h.configure()
    out = earlier_export(cfgs)
    m = h.manager(workers=2, export_type=export_type)
    m.process("null")
    assert sorted(p.name for p in out.iterdir()) == left
    assert not (Path(cfgs.log_dir) / "flags" / "export_is_done-30.txt").exists()


def test_out_dir_is_where_it_clears(harness, tmp_path):
    h = harness
    cfgs = h.configure()
    exports = earlier_export(cfgs)
    other = earlier_export(cfgs, tmp_path / "elsewhere")
    h.manager(workers=2, out_dir=str(other)).clear_stale()
    assert (exports / "export_full_0.jsonl").exists()
    assert not (other / "export_full_0.jsonl").exists()


@pytest.mark.parametrize("options", [dict(my_worker=1), dict(export_type="nt")])
def test_one_slice_or_another_type_leaves_them(harness, options):
    h = harness
    cfgs = h.configure()
    out = earlier_export(cfgs)
    assert h.manager(workers=2, **options).clear_stale() == 0
    assert len(list(out.iterdir())) == 5
