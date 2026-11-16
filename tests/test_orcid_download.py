"""The orcid downloader: the summaries file of the year's figshare article
(not the eleven activities files), then split into JSON Lines files the
loader reads. Discovery is against canned JSON and the download is a
small tar.gz written in place; no network."""

import gzip
import io
import json
import os
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.config import importObject
from lux_pipeline.process.base.downloader import BaseDownloader
from lux_pipeline.sources.orcid import downloader as od
from lux_pipeline.sources.orcid.downloader import OrcidDownloader, figshare_api_url, summaries_file
from lux_pipeline.sources.orcid.loader import OrcidLoader

API = "https://api.figshare.com/v2/articles/34697766"
PAGE = "https://orcid.figshare.com/articles/dataset/ORCID_Public_Data_File_2026/34697766"
SUMMARIES_URL = "https://ndownloader.figshare.com/files/70342863"
ARTICLE = {"title": "ORCID Public Data File 2026", "files": [
    {"name": "ORCID_2026_10_activities_0.tar.gz", "download_url": "https://ndownloader.figshare.com/files/70335732",
     "computed_md5": "b4408eaed823afbd42d6a800b01c821c"},
    {"name": "ORCID_2026_10_summaries.tar.gz", "download_url": SUMMARIES_URL,
     "computed_md5": "a55a16fe0b292bda03cc6016546c8984"},
    {"name": "ORCID_2026_10_activities_X.tar.gz", "download_url": "https://ndownloader.figshare.com/files/70335672",
     "computed_md5": "ba665d7d3471ac873d1d7b4ed93d3a1f"},
]}

# a member with only a name, which should_load drops, and one with data
EMPTY = """<record:record path="/{id}"><person:person path="/{id}/person">
<researcher-url:researcher-urls path="/{id}/researcher-urls"/><email:emails path="/{id}/email"/>
<keyword:keywords path="/{id}/keywords"/><external-identifier:external-identifiers path="/{id}/external-identifiers"/>
</person:person><activities:activities-summary><activities:employments path="/{id}/employments"/>
<activities:fundings path="/{id}/fundings"/><activities:memberships path="/{id}/memberships"/>
</activities:activities-summary></record:record>"""
FULL = EMPTY.replace('<email:emails path="/{id}/email"/>', '<email:emails path="/{id}/email"><email/></email:emails>')
IDS = [f"0000-0001-5000-{n:04d}" for n in range(7)]


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)


def write_archive(path):
    """A summaries archive of IDS; the even ones have data."""
    with tarfile.open(path, "w:gz") as tf:
        for n, ident in enumerate(IDS):
            data = (FULL if n % 2 == 0 else EMPTY).format(id=ident).encode()
            info = tarfile.TarInfo(f"ORCID_2026_10_summaries/{ident[-3:]}/{ident}.xml")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))


def make(tmp_path, **keys):
    cfg = {"name": "orcid", "all_configs": SimpleNamespace(dumps_dir=str(tmp_path)),
           "dumps_dir": "orcid", "datacache": None, "dumpFilePath": str(tmp_path / "orcid" / "orcid.tgz"),
           "downloadMdUrl": API, **keys}
    cfg["loader"] = OrcidLoader(cfg)
    return OrcidDownloader(cfg)


def canned(monkeypatch, payload=ARTICLE):
    seen = []
    monkeypatch.setattr(OrcidDownloader, "fetch_webpage",
                        lambda self, url: seen.append(url) or json.dumps(payload))
    return seen


def fake_fetch(monkeypatch, calls):
    """BaseDownloader.process writes the archive, or keeps an existing one
    when not overwriting, as the real one does."""
    def process(self, download, overwrite=True):
        calls.append(download["url"])
        if not overwrite and os.path.exists(download["path"]):
            return 0
        os.makedirs(os.path.dirname(download["path"]), exist_ok=True)
        write_archive(download["path"])
        return 1
    monkeypatch.setattr(BaseDownloader, "process", process)


def read_shard(path):
    with gzip.open(path, "rt") as fh:
        return [line.split("\t", 1)[0] for line in fh]


def test_the_article_from_its_page_or_api_url():
    assert figshare_api_url(PAGE) == API
    assert figshare_api_url(API) == API
    assert figshare_api_url(PAGE + "/?file=70342863") == API
    with pytest.raises(ValueError):
        figshare_api_url("https://orcid.figshare.com/articles/dataset/ORCID")


def test_only_the_summaries_file_is_downloaded(tmp_path, monkeypatch):
    seen = canned(monkeypatch)
    dl = make(tmp_path, remoteDumpFile="https://orcid.figshare.com/ndownloader/files/49560102")
    urls = dl.get_urls()
    assert seen == [API]
    assert urls == [{"url": SUMMARIES_URL, "path": str(tmp_path / "orcid" / "orcid.tgz"), "shards": 24,
                     "checksum": "md5:a55a16fe0b292bda03cc6016546c8984"}]
    assert summaries_file({"files": ARTICLE["files"][:1]}) is None


def test_the_file_keeps_its_own_name_without_a_dump_path_and_other_types_skip_figshare(tmp_path, monkeypatch):
    seen = canned(monkeypatch)
    dl = make(tmp_path, dumpFilePath=None, shards=6)
    [u] = dl.get_urls("records")
    assert u["path"] == str(tmp_path / "orcid" / "ORCID_2026_10_summaries.tar.gz") and u["shards"] == 6
    assert dl.get_urls("export") == [] and seen == [API]


def test_an_article_with_no_summaries_downloads_nothing(tmp_path, monkeypatch, caplog):
    canned(monkeypatch, {"files": ARTICLE["files"][:1]})
    assert make(tmp_path).get_urls() == []
    assert "no *_summaries.tar.gz file" in caplog.text


def test_the_download_is_split_into_shards_of_the_members_with_data(tmp_path, monkeypatch):
    canned(monkeypatch)
    calls = []
    fake_fetch(monkeypatch, calls)
    dl = make(tmp_path, shards=3)
    base = tmp_path / "orcid" / "orcid"
    base.parent.mkdir()
    # last year's: one more shard than now, and a split that died
    for name in ("orcid_000.jsonl.gz", "orcid_003.jsonl.gz", "orcid_001.jsonl.gz.tmp"):
        (base.parent / name).write_bytes(b"")
    dl.prepare(0, 1, progress=Recorder())
    [u] = dl.get_urls()
    assert dl.process(u) == 1 and calls == [SUMMARIES_URL]
    assert sorted(os.listdir(base.parent)) == ["orcid.tgz", "orcid_000.jsonl.gz", "orcid_001.jsonl.gz",
                                               "orcid_002.jsonl.gz"]
    # member n goes to shard n % 3; only the even members have data
    assert [read_shard(f"{base}_{n:03d}.jsonl.gz") for n in range(3)] == \
        [[IDS[0], IDS[6]], [IDS[4]], [IDS[2]]]
    assert sum(c.get("advance", 0) for c in dl.progress.calls) == len(IDS)
    ldr = dl.config["loader"]
    assert ldr.find_shards() == [f"{base}_{n:03d}.jsonl.gz" for n in range(3)]


def test_a_kept_archive_with_its_shards_is_not_split_again(tmp_path, monkeypatch):
    canned(monkeypatch)
    fake_fetch(monkeypatch, [])
    dl = make(tmp_path, shards=3)
    [u] = dl.get_urls()
    dl.process(u)
    monkeypatch.setattr(OrcidLoader, "split", lambda *a: pytest.fail("split again"))
    assert dl.process(u, overwrite=False) == 0
    # one shard gone: split again from the kept archive
    os.remove(tmp_path / "orcid" / "orcid_001.jsonl.gz")
    split = []
    monkeypatch.setattr(OrcidLoader, "split", lambda self, s, n: split.append(n))
    assert dl.process(u, overwrite=False) == 0 and split == [3]


def test_the_class_resolves():
    assert importObject("sources.orcid.downloader.OrcidDownloader") is OrcidDownloader
    assert od.DEFAULT_SHARDS == 24
