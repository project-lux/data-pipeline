"""Downloads with rsync: a source's ``rsync`` entries become downloads of
the base downloader, run as ``rsync -rt --partial --delay-updates`` with
the bar's total from a dry run and its steps from ``--progress``. The
transfers here are real, between local directories (skipped where rsync
is not installed); the parsing is checked against both rsync 3 and
openrsync / 2.6.9 output."""

import shutil
import sys
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.base import rsync
from lux_pipeline.process.base.downloader import BaseDownloader
from lux_pipeline.progress import read_status

from test_download_task import StubConfigs, run_command, stub_runtime  # noqa: F401 (fixture)

needs_rsync = pytest.mark.skipif(shutil.which("rsync") is None, reason="rsync is not installed")


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)

    def values(self, key):
        return [c[key] for c in self.calls if key in c]


def source_tree(root):
    src = root / "src"
    (src / "sub").mkdir(parents=True)
    (src / "a-pages-articles1.xml.bz2").write_bytes(b"a" * 300_000)
    (src / "notes.txt").write_bytes(b"n" * 1000)
    (src / "sub" / "c-pages-articles2.xml.bz2").write_bytes(b"c" * 50_000)
    return src


def downloader(root, **spec):
    cfg = {"name": "wikimedia", "all_configs": SimpleNamespace(dumps_dir=str(root / "dumps")),
           "dumps_dir": "wikimedia", "rsync": spec}
    return BaseDownloader(cfg)


# --- configuration ---------------------------------------------------------------

def test_entries_from_the_config():
    assert rsync.rsync_specs({"rsync": "rsync://m.org/d/"}) == [{"source": "rsync://m.org/d/"}]
    assert rsync.rsync_specs({}) == []
    two = rsync.rsync_specs({"rsync": [{"source": "a::x/"}, "b::y/"]})
    assert [s["source"] for s in two] == ["a::x/", "b::y/"]
    for bad, msg in (({"path": "x"}, "needs a source"), ({"source": "a", "dest": "x"}, "unknown rsync keys"),
                     ({"source": "a", "include": "*.bz2"}, "include should be a list")):
        with pytest.raises(ValueError, match=msg):
            rsync.rsync_specs({"name": "s", "rsync": bad})


def test_the_command_line():
    spec = {"source": "rsync://m.org/dumps/x/", "include": ["*.bz2"], "exclude": ["*-meta*"],
            "options": ["--bwlimit=20m"], "delete": True}
    assert rsync.command(spec, "/dumps/x") == [
        "rsync", "-rtL", "--partial", "--delay-updates", "--progress", "--out-format=%n", "--delete",
        "--include=*/", "--include=*.bz2", "--exclude=*-meta*", "--exclude=*", "--prune-empty-dirs",
        "--bwlimit=20m", "rsync://m.org/dumps/x/", "/dumps/x/"]
    dry = rsync.command({"source": "h::m/"}, "/d/", overwrite=False, dry_run=True)
    assert dry == ["rsync", "-rtL", "--partial", "--delay-updates", "--dry-run", "--out-format=%l %n",
                   "--ignore-existing", "h::m/", "/d/"]


def test_downloads_from_the_config(tmp_path):
    dl = downloader(tmp_path, source="rsync://m.org/c/", path="commonswiki")
    assert dl.get_urls() == dl.get_urls("records") == [
        {"url": "rsync://m.org/c/", "path": str(tmp_path / "dumps" / "wikimedia" / "commonswiki"),
         "rsync": {"source": "rsync://m.org/c/", "path": "commonswiki"}}]
    assert dl.get_urls("export") == []
    exp = downloader(tmp_path, source="h::e/", type="export")
    assert [u["url"] for u in exp.get_urls("export")] == ["h::e/"] and exp.get_urls("records") == []
    # no path: the source's dumps_dir itself
    assert downloader(tmp_path, source="h::e/").get_urls()[0]["path"] == str(tmp_path / "dumps" / "wikimedia") + "/"


# --- reading the output ----------------------------------------------------------

def test_progress_from_openrsync_and_rsync_2():
    names = []
    r = rsync.ProgressReader(on_name=names.append)
    out = ["./", "a-pages-articles1.xml.bz2", "       10289152  34%    9.78MB/s   00:00:01",
           "       20512768  68%    9.75MB/s   00:00:00",
           "       30000000 100%   13.86MB/s   00:00:02 (xfer#1, to-check=1/5)",
           "sub/", "sub/c.xml.bz2", "         753664  15%  784.57KB/s   00:00:05",
           "        5000000 100%    4.99MB/s   00:00:00 (xfer#2, to-check=4/5)"]
    steps = [r.feed(line) for line in out]
    assert steps == [0, 0, 10289152, 10223616, 9487232, 0, 0, 753664, 4246336]
    assert (r.files, r.bytes, names) == (2, 35_000_000, ["a-pages-articles1.xml.bz2", "sub/c.xml.bz2"])


def test_progress_from_rsync_3():
    r = rsync.ProgressReader()
    # rsync 3 groups digits and may print the name after the file is done
    out = ["         32,768   0%    0.00kB/s    0:00:00", "    268,435,456  50%  120.00MB/s    0:00:02",
           "    536,870,912 100%  119.21MB/s    0:00:04 (xfr#1, to-chk=1/2)", "big.bz2",
           "      1,000,000  40%    1.00MB/s    0:00:01",
           "      2,500,000 100%    1.00MB/s    0:00:02 (xfr#2, to-chk=0/2)", "small.bz2"]
    assert sum(r.feed(line) for line in out) == 536_870_912 + 2_500_000
    assert r.files == 2


def test_a_new_file_without_a_name_line_starts_again():
    r = rsync.ProgressReader()
    assert [r.feed("  900 90%  1kB/s  0:00:01"), r.feed("  100 10%  1kB/s  0:00:01")] == [900, 100]


def test_lines_split_at_carriage_returns_and_newlines():
    import io
    data = io.BytesIO(b"name\n   10  1%\r   20  2%\r   30 100% (xfr#1)\nlast")
    assert list(rsync.lines(data, size=7)) == ["name", "   10  1%", "   20  2%", "   30 100% (xfr#1)", "last"]


# --- real transfers ----------------------------------------------------------------

@needs_rsync
def test_a_transfer_fills_the_bar_and_a_second_finds_nothing(tmp_path):
    src = source_tree(tmp_path)
    dl = downloader(tmp_path, source=f"{src}/", path="commonswiki", include=["*-pages-articles*.xml*.bz2"])
    [u] = dl.get_urls()
    bar = Recorder()
    dl.prepare(0, 1, progress=bar)
    assert dl.process(u) == 1
    dest = tmp_path / "dumps" / "wikimedia" / "commonswiki"
    assert sorted(str(p.relative_to(dest)) for p in dest.rglob("*") if p.is_file()) == [
        "a-pages-articles1.xml.bz2", "sub/c-pages-articles2.xml.bz2"]
    assert bar.values("total") == [350_000] and sum(bar.values("advance")) == 350_000
    assert "wikimedia/commonswiki: sub/c-pages-articles2.xml.bz2" in bar.values("description")
    # times are kept, so nothing is sent again
    assert dl.process(u) == 0
    assert bar.values("description")[-1] == "wikimedia/commonswiki (up to date)"


@needs_rsync
def test_a_changed_file_is_fetched_again_unless_not_overwriting(tmp_path):
    src = source_tree(tmp_path)
    dl = downloader(tmp_path, source=f"{src}/")
    [u] = dl.get_urls()
    dl.process(u)
    (src / "notes.txt").write_bytes(b"changed")
    assert dl.process(u, overwrite=False) == 0
    assert (tmp_path / "dumps" / "wikimedia" / "notes.txt").read_bytes() == b"n" * 1000
    assert dl.process(u) == 1
    assert (tmp_path / "dumps" / "wikimedia" / "notes.txt").read_bytes() == b"changed"


@needs_rsync
def test_a_failed_transfer_raises_with_rsyncs_message(tmp_path):
    dl = downloader(tmp_path, source=f"{tmp_path}/missing/")
    with pytest.raises(RuntimeError, match=r"wikimedia/wikimedia: rsync exited \d+: .*missing"):
        dl.process(dl.get_urls()[0])


def test_no_rsync_installed(tmp_path):
    with pytest.raises(RuntimeError, match="no-such-rsync is not installed"):
        rsync.transfer({"source": "x/"}, str(tmp_path / "d"), Recorder(), "s", binary="no-such-rsync")


@needs_rsync
def test_lux_download_reports_the_transfer_in_the_status_file(tmp_path, stub_runtime):
    src = source_tree(tmp_path)
    dl = downloader(tmp_path, source=f"{src}/", path="commonswiki")
    cfgs = stub_runtime(StubConfigs(wikimedia=dl))
    m = run_command(cfgs, [], tmp_path, source="wikimedia", workers=1)
    assert m.summary() == {"downloaded": 1, "kept": 0, "failed": 0}
    status = read_status(str(tmp_path / "download" / "w0.status"))
    assert (status["completed"], status["total"]) == (351_000, 351_000)


@needs_rsync
def test_a_latest_directory_of_links_arrives_as_files(tmp_path):
    src = source_tree(tmp_path)
    latest = tmp_path / "latest"
    latest.mkdir()
    (latest / "dump-latest.xml.bz2").symlink_to(src / "a-pages-articles1.xml.bz2")
    dl = downloader(tmp_path, source=f"{latest}/")
    assert dl.process(dl.get_urls()[0]) == 1
    got = tmp_path / "dumps" / "wikimedia" / "dump-latest.xml.bz2"
    assert not got.is_symlink() and got.stat().st_size == 300_000
