"""The per-source downloaders (item 30): ror (Zenodo), viaf (monthly dump
API), pleiades (base behaviour over input_files) and the ILS S3 stub; all
discovery is against canned JSON, no network."""

import json
import sys
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.config import importObject
from lux_pipeline.process.tasks.download_task import DownloadManager
from lux_pipeline.sources.oclc.downloader import ViafDownloader
from lux_pipeline.sources.pleiades.downloader import PleiadesDownloader
from lux_pipeline.sources.ror.downloader import RorDownloader, zenodo_filename
from lux_pipeline.sources.yale.library.downloader import S3Downloader

ROOT = Path(__file__).parent.parent
ZENODO = "https://zenodo.org/api/records/20512981/files/v2.8-2026-06-02-ror-data.zip/content"
ZENODO_MD = {"hits": {"hits": [{"files": [{"links": {"self": ZENODO}}]}]}}
VIAF_MD = {"queryResult": {"Monthly": {"clusterRdfXml": "viaf-20260401-clusters-rdf.xml.gz"}}}


def canned(monkeypatch, cls, payload):
    """fetch_webpage answers with `payload` (JSON) and records the URL."""
    seen = []

    def fetch(self, url):
        seen.append(url)
        return json.dumps(payload) if payload is not None else None

    monkeypatch.setattr(cls, "fetch_webpage", fetch)
    return seen


def cfg(name, base_dumps="/dumps", **keys):
    """A source config under a base configuration whose dumps_dir is
    `base_dumps`; `keys` may include the per-source ``dumps_dir``."""
    c = {"name": name, "all_configs": SimpleNamespace(dumps_dir=base_dumps)}
    c.update(keys)
    return c


ROR_KEYS = {"downloadMdUrl": "https://zenodo.org/api/records/14429114/versions?size=5",
            "downloadXPath": "hits/hits[0]/files[0]/links/self", "dumps_dir": "ror"}


# --- ror ---------------------------------------------------------------------


def test_zenodo_filename():
    assert zenodo_filename(ZENODO) == "v2.8-2026-06-02-ror-data.zip"
    assert zenodo_filename("https://x.org/files/a.zip") == "a.zip"
    assert zenodo_filename("https://x.org/files/a.zip/") == "a.zip"


def test_ror_discovers_the_release_and_saves_it_under_the_zenodo_name(monkeypatch):
    seen = canned(monkeypatch, RorDownloader, ZENODO_MD)
    dl = RorDownloader(cfg("ror", **ROR_KEYS))
    assert dl.get_urls() == [{"url": ZENODO, "path": "/dumps/ror/v2.8-2026-06-02-ror-data.zip"}]
    assert seen == [ROR_KEYS["downloadMdUrl"]]
    assert dl.get_urls("records") == dl.get_urls("all")


def test_ror_uses_dumpFilePath_only_when_it_names_a_zip(monkeypatch):
    canned(monkeypatch, RorDownloader, ZENODO_MD)
    dl = RorDownloader(cfg("ror", dumpFilePath="/dumps/ror/ror-data.zip", **ROR_KEYS))
    assert dl.get_urls()[0]["path"] == "/dumps/ror/ror-data.zip"
    # the live configuration names the extracted JSON: never overwritten
    dl = RorDownloader(cfg("ror", dumpFilePath="/dumps/ror/ror-data.json", **ROR_KEYS))
    assert dl.get_urls()[0]["path"] == "/dumps/ror/v2.8-2026-06-02-ror-data.zip"


def test_ror_export_type_skips_discovery_and_keeps_input_files(monkeypatch):
    seen = canned(monkeypatch, RorDownloader, ZENODO_MD)
    export = {"export": [{"url": "https://yale-lux-data.s3.amazonaws.com/sources/ror.zip", "path": "ror.zip"}]}
    dl = RorDownloader(cfg("ror", input_files=export, **ROR_KEYS))
    assert dl.get_urls("export") == [{"url": export["export"][0]["url"], "path": "/dumps/ror/ror.zip"}]
    assert seen == []
    all_urls = dl.get_urls("all")
    assert [u["path"] for u in all_urls] == ["/dumps/ror/v2.8-2026-06-02-ror-data.zip", "/dumps/ror/ror.zip"]


def test_ror_discovery_failure_is_logged_not_raised(monkeypatch, caplog):
    canned(monkeypatch, RorDownloader, None)
    dl = RorDownloader(cfg("ror", **ROR_KEYS))
    with caplog.at_level("ERROR", logger="lux_pipeline"):
        assert dl.get_urls() == []
    assert "ror: could not discover the current data release" in caplog.text


def test_ror_without_discovery_keys_is_the_base_downloader(monkeypatch):
    seen = canned(monkeypatch, RorDownloader, ZENODO_MD)
    assert RorDownloader(cfg("ror")).get_urls() == []
    assert seen == []


# --- viaf --------------------------------------------------------------------

VIAF_KEYS = {"downloadMdUrl": "https://viaf.org/api/download/files",
             "downloadXPath": "queryResult/Monthly/clusterRdfXml",
             "downloadUrlBase": "https://downloads.viaf.org/viaf/data/monthly/", "dumps_dir": "viaf"}


def test_viaf_joins_the_base_and_the_monthly_name(monkeypatch):
    seen = canned(monkeypatch, ViafDownloader, VIAF_MD)
    dl = ViafDownloader(cfg("viaf", **VIAF_KEYS))
    assert dl.get_urls() == [{"url": "https://downloads.viaf.org/viaf/data/monthly/viaf-20260401-clusters-rdf.xml.gz",
                              "path": "/dumps/viaf/viaf-20260401-clusters-rdf.xml.gz"}]
    assert seen == ["https://viaf.org/api/download/files"]
    assert dl.get_urls("export") == []


def test_viaf_accepts_an_absolute_url_from_the_api(monkeypatch):
    canned(monkeypatch, ViafDownloader, {"queryResult": {"Monthly": {"clusterRdfXml": "https://cdn.viaf.org/m/x.xml.gz"}}})
    assert ViafDownloader(cfg("viaf", **VIAF_KEYS)).get_urls()[0] == {
        "url": "https://cdn.viaf.org/m/x.xml.gz", "path": "/dumps/viaf/x.xml.gz"}


def test_viaf_discovery_failure_is_logged_not_raised(monkeypatch, caplog):
    canned(monkeypatch, ViafDownloader, {"queryResult": {}})
    with caplog.at_level("ERROR", logger="lux_pipeline"):
        assert ViafDownloader(cfg("viaf", **VIAF_KEYS)).get_urls() == []
    assert "viaf: could not discover the current monthly dump" in caplog.text


# --- pleiades and the ILS stub -----------------------------------------------


def test_pleiades_downloads_every_input_file_into_dumps_dir():
    sample = json.load(open(ROOT / "docs" / "sample_config" / "pleiades.json"))
    dl = PleiadesDownloader(cfg("pleiades", **{k: sample[k] for k in ("dumps_dir", "input_files")}))
    assert dl.get_urls() == [
        {"url": "https://atlantides.org/downloads/pleiades/rdf/place-types.ttl",
         "path": "/dumps/pleiades/place-types.ttl"},
        {"url": "https://atlantides.org/downloads/pleiades/json/pleiades-places-latest.json.gz",
         "path": "/dumps/pleiades/pleiades-places-latest.json.gz"},
    ]
    assert dl.get_urls("records") == dl.get_urls("all")
    assert dl.get_urls("export") == []


def test_s3_stub_has_nothing_to_download():
    assert S3Downloader(cfg("ils", dumpFilePath="/dumps/yul", remoteDumpFile="s3://bucket/yul")).get_urls() == []


# --- wiring ------------------------------------------------------------------


def test_download_manager_lists_the_discovered_file(monkeypatch):
    canned(monkeypatch, RorDownloader, ZENODO_MD)
    dl = RorDownloader(cfg("ror", **ROR_KEYS))
    configs = SimpleNamespace(internal={}, results={}, max_workers=2,
                              external={"ror": {"name": "ror", "downloader": dl}})
    m = DownloadManager(configs, 2, Namespace(type="records"))
    m.prepare_single("ror")
    assert [(w, s, d["path"]) for w, s, d in m.sources] == [
        ("external", "ror", "/dumps/ror/v2.8-2026-06-02-ror-data.zip")]


@pytest.mark.parametrize("sample", ["ror.json", "viaf.json", "pleiades.json"])
def test_sample_configs_parse_and_their_classes_resolve(sample):
    d = json.load(open(ROOT / "docs" / "sample_config" / sample))
    for k, v in d.items():
        if k.endswith("Class") and v:
            assert importObject(v) is not None
    assert d["downloaderClass"].endswith("Downloader")
