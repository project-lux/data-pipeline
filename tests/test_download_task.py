"""`DownloadManager` and `lux download`: files flattened from the
downloaders, spread over the workers, failures counted rather than
raised, idle workers, and the command end to end on the null engine."""

import logging
import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli.download import CommandHandler
from lux_pipeline.process.tasks import _task_manager
from lux_pipeline.process.tasks.download_task import DownloadManager
from lux_pipeline.progress import null_progress

logger = logging.getLogger("lux_pipeline")


# --- stubs -------------------------------------------------------------------


class FakeDownloader:
    """get_urls answers from `files` ({type: [names]}); process records
    the call, raises for names in `fail`, returns 0 for names in `kept`."""

    def __init__(self, name, files=None, fail=(), kept=()):
        self.name = name
        self.files = files or {}
        self.fail = set(fail)
        self.kept = set(kept)
        self.calls = []

    def get_urls(self, type="all"):
        out = []
        for t, names in self.files.items():
            if type in ("all", t):
                out += [{"url": f"https://x.org/{n}", "path": f"/dumps/{self.name}/{n}"} for n in names]
        return out

    def prepare(self, my_slice=-1, max_slice=-1, progress=null_progress):
        self.calls.append(("prepare", my_slice, max_slice, progress))
        return self

    def process(self, download, overwrite=True):
        name = download["path"].rsplit("/", 1)[-1]
        self.calls.append(("process", name, overwrite))
        if name in self.fail:
            raise IOError(f"{name}: 503")
        return 0 if name in self.kept else 1

    def processed(self):
        return [c[1] for c in self.calls if c[0] == "process"]


class StubConfigs:
    max_workers = 4
    log_dir = None

    def __init__(self, **downloaders):
        """``name=downloader``; names starting with ``y`` are internal;
        ``None`` means no downloader."""
        self.internal, self.external, self.results = {}, {}, {}
        for name, dl in downloaders.items():
            cfg = {"name": name, "downloader": dl}
            (self.internal if name.startswith("y") else self.external)[name] = cfg

    def dl(self, name):
        for group in (self.internal, self.external):
            if name in group:
                return group[name]["downloader"]
        raise KeyError(name)


def manager(cfgs, sources, workers=2, **options):
    m = DownloadManager(cfgs, workers, Namespace(**options))
    for s in sources:
        m.prepare_single(s)
    return m


@pytest.fixture
def stub_runtime(monkeypatch):
    def use(cfgs):
        monkeypatch.setattr(_task_manager.runtime, "get_configs", lambda: cfgs)
        return cfgs
    return use


# --- which files -------------------------------------------------------------


def test_files_are_flattened_from_the_downloaders_in_source_order():
    cfgs = StubConfigs(ror=FakeDownloader("ror", {"records": ["a.zip"]}),
                       bne=FakeDownloader("bne", {"records": ["b1.zip", "b2.zip"], "export": ["bne.zip"]}),
                       aat=None, tgn=FakeDownloader("tgn"))
    m = manager(cfgs, ["ror", "bne", "aat", "tgn"])
    assert [(w, s, d["path"]) for w, s, d in m.sources] == [
        ("external", "ror", "/dumps/ror/a.zip"),
        ("external", "bne", "/dumps/bne/b1.zip"),
        ("external", "bne", "/dumps/bne/b2.zip"),
        ("external", "bne", "/dumps/bne/bne.zip"),
    ]


def test_type_is_passed_to_get_urls():
    cfgs = StubConfigs(bne=FakeDownloader("bne", {"records": ["b.zip"], "export": ["bne.zip"]}))
    assert [d["path"] for _, _, d in manager(cfgs, ["bne"], type="export").sources] == ["/dumps/bne/bne.zip"]
    assert [d["path"] for _, _, d in manager(cfgs, ["bne"], type="links").sources] == []


def test_a_source_added_twice_adds_its_files_once():
    cfgs = StubConfigs(ror=FakeDownloader("ror", {"records": ["a.zip"]}))
    m = manager(cfgs, ["ror", "ror"])
    assert len(m.sources) == 1


# --- distribution ------------------------------------------------------------


def test_each_worker_takes_every_max_workers_th_file():
    cfgs = StubConfigs(bne=FakeDownloader("bne", {"records": ["0.zip", "1.zip", "2.zip"]}))
    m = manager(cfgs, ["bne"], workers=2)
    assert m.run_single(0) == {"downloaded": 2, "kept": 0, "failed": 0}
    assert cfgs.dl("bne").processed() == ["0.zip", "2.zip"]
    assert m.run_single(1) == {"downloaded": 1, "kept": 0, "failed": 0}
    assert cfgs.dl("bne").processed() == ["0.zip", "2.zip", "1.zip"]


def test_prepare_gets_the_slice_and_progress_and_overwrite_is_passed():
    seen = []

    def progress(**kw):
        seen.append(kw)

    cfgs = StubConfigs(ror=FakeDownloader("ror", {"records": ["a.zip"]}))
    m = manager(cfgs, ["ror"], workers=3, overwrite=False)
    m._distributed(0, cfgs, progress)
    assert cfgs.dl("ror").calls == [("prepare", 0, 3, progress), ("process", "a.zip", False)]
    assert seen[0] == {"phase": "download", "total": 1, "completed": 0}


def test_kept_files_are_counted_apart():
    cfgs = StubConfigs(ror=FakeDownloader("ror", {"records": ["a.zip", "b.zip"]}, kept=["b.zip"]))
    m = manager(cfgs, ["ror"], workers=1, overwrite=False)
    assert m.run_single(0) == {"downloaded": 1, "kept": 1, "failed": 0}


def test_a_failing_file_is_logged_and_counted_and_the_rest_still_download(caplog):
    cfgs = StubConfigs(bne=FakeDownloader("bne", {"records": ["0.zip", "1.zip", "2.zip"]}, fail=["0.zip"]))
    m = manager(cfgs, ["bne"], workers=1)
    with caplog.at_level("ERROR", logger="lux_pipeline"):
        assert m.run_single(0) == {"downloaded": 2, "kept": 0, "failed": 1}
    assert cfgs.dl("bne").processed() == ["0.zip", "1.zip", "2.zip"]
    assert "bne: failed to download https://x.org/0.zip to /dumps/bne/0.zip: OSError: 0.zip: 503" in caplog.text


def test_an_idle_worker_reports_zero_counts():
    cfgs = StubConfigs(ror=FakeDownloader("ror", {"records": ["a.zip"]}))
    m = manager(cfgs, ["ror"], workers=4)
    assert m.run_single(3) == {"downloaded": 0, "kept": 0, "failed": 0}
    assert list(m.idle_workers()) == [1, 2, 3]
    assert DownloadManager.hide_idle_workers is True


# --- the driver --------------------------------------------------------------


def test_process_refuses_with_nothing_to_download():
    cfgs = StubConfigs(ror=FakeDownloader("ror"))
    with pytest.raises(RuntimeError, match="nothing to download \\(type 'records'\\)"):
        manager(cfgs, ["ror"], type="records").process("null")


def test_process_uses_no_more_workers_than_files_and_keeps_the_results(stub_runtime, caplog):
    cfgs = stub_runtime(StubConfigs(ror=FakeDownloader("ror", {"records": ["a.zip"]})))
    m = manager(cfgs, ["ror"], workers=4)
    with caplog.at_level("INFO", logger="lux_pipeline"):
        results = m.process("null")
    assert m.max_workers == 1
    assert results == m.results == [{"downloaded": 1, "kept": 0, "failed": 0}]
    assert m.summary() == {"downloaded": 1, "kept": 0, "failed": 0}
    assert "1 file(s) to download: using 1 of 4 workers" in caplog.text


def test_summary_sums_over_workers():
    m = DownloadManager(StubConfigs(), 2)
    m.results = [{"downloaded": 2, "kept": 1, "failed": 0}, None, {"downloaded": 0, "kept": 0, "failed": 3}]
    assert m.summary() == {"downloaded": 2, "kept": 1, "failed": 3}


def test_defaults_without_args():
    m = DownloadManager(StubConfigs(), 2)
    assert (m.download_type, m.overwrite, m.results) == ("all", True, None)


# --- the command -------------------------------------------------------------


def run_command(cfgs, rest, tmp_path, source="all", workers=2):
    args = Namespace(command="download", source=source, max_workers=workers,
                     engine="null", no_ui=True, prefix=str(tmp_path / "download"),
                     log="INFO", max_records=-1, my_worker=-1)
    return CommandHandler(cfgs).process(args, list(rest))


def test_command_downloads_and_logs_a_summary(tmp_path, stub_runtime, caplog):
    cfgs = stub_runtime(StubConfigs(ror=FakeDownloader("ror", {"records": ["a.zip"]}),
                                    ycba=FakeDownloader("ycba", {"records": ["y.tgz"]}, kept=["y.tgz"])))
    with caplog.at_level("INFO", logger="lux_pipeline"):
        # one worker: the null engine runs worker 0 only
        m = run_command(cfgs, ["--no-overwrite"], tmp_path, workers=1)
    assert isinstance(m, DownloadManager)
    assert (m.download_type, m.overwrite) == ("all", False)
    assert m.summary() == {"downloaded": 1, "kept": 1, "failed": 0}
    assert "downloaded 1, kept 1, failed 0" in caplog.text
    assert (tmp_path / "download-w0.status").exists()


def test_command_type_option(tmp_path, stub_runtime):
    cfgs = stub_runtime(StubConfigs(bne=FakeDownloader("bne", {"records": ["b.zip"], "export": ["bne.zip"]})))
    m = run_command(cfgs, ["--type", "export"], tmp_path, source="bne")
    assert m.download_type == "export"
    assert cfgs.dl("bne").processed() == ["bne.zip"]


def test_command_fails_at_the_end_when_a_file_failed(tmp_path, stub_runtime):
    cfgs = stub_runtime(StubConfigs(bne=FakeDownloader("bne", {"records": ["0.zip", "1.zip"]}, fail=["1.zip"])))
    with pytest.raises(RuntimeError, match="1 download\\(s\\) failed"):
        run_command(cfgs, [], tmp_path, workers=1)
    assert cfgs.dl("bne").processed() == ["0.zip", "1.zip"]


def test_command_with_nothing_to_download_is_an_error(tmp_path):
    with pytest.raises(RuntimeError, match="nothing to download"):
        run_command(StubConfigs(ror=FakeDownloader("ror")), [], tmp_path)


def test_command_is_listed_by_help():
    from lux_pipeline.cli.help import list_commands
    assert ("download", "Download the files each source's loader reads.") in list_commands()
