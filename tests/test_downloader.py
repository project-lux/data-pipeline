"""Tests for the base downloader (refactor item 14)."""

import http.server
import importlib
import json
import logging
import os
import threading
from functools import partial
from types import SimpleNamespace

import pytest
import requests

from lux_pipeline.process.base import downloader as dl_mod
from lux_pipeline.process.base.downloader import BaseDownloader


class Recorder:
    """A progress callable that remembers every call."""

    def __init__(self):
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)

    def totals(self):
        return [c["total"] for c in self.calls if c.get("total") is not None]

    def advanced(self):
        return sum(c.get("advance", 0) for c in self.calls)

    def descriptions(self):
        return [c["description"] for c in self.calls if c.get("description")]


def make_config(dumps_dir=None, **keys):
    cfg = {"name": "src", "all_configs": SimpleNamespace(dumps_dir=dumps_dir)}
    cfg.update(keys)
    return cfg


def make_downloader(dumps_dir=None, **keys):
    return BaseDownloader(make_config(dumps_dir, **keys))


# --- get_urls -----------------------------------------------------------


def test_no_download_keys_gives_empty_list():
    assert make_downloader("/dumps").get_urls() == []
    assert make_downloader("/dumps").get_urls("records") == []


def test_remote_dump_file_pair():
    dl = make_downloader("/dumps", remoteDumpFile="https://x.org/a.gz", dumpFilePath="/abs/a.gz")
    assert dl.get_urls() == [{"url": "https://x.org/a.gz", "path": "/abs/a.gz"}]


def test_remote_dump_file_needs_both_keys():
    assert make_downloader("/dumps", remoteDumpFile="https://x.org/a.gz").get_urls() == []
    assert make_downloader("/dumps", dumpFilePath="/abs/a.gz").get_urls() == []


def test_relative_dump_file_path_resolves_against_dumps_dir():
    dl = make_downloader("/dumps", remoteDumpFile="https://x.org/a.gz", dumpFilePath="src/a.gz")
    assert dl.get_urls()[0]["path"] == "/dumps/src/a.gz"


def test_remote_dump_file_counts_as_records():
    dl = make_downloader("/dumps", remoteDumpFile="https://x.org/a.gz", dumpFilePath="/abs/a.gz")
    assert len(dl.get_urls("records")) == 1
    assert dl.get_urls("export") == []


def test_input_files_entries_by_type():
    dl = make_downloader(
        "/dumps",
        input_files={
            "records": [
                {"url": "https://x.org/r1.zip", "path": "r1.zip", "type": "zip/json"},
                {"url": "https://x.org/dir/r2.json.gz"},
                {"path": "local-only.json"},
            ],
            "export": [{"url": "https://x.org/ex.tgz", "path": "/elsewhere/ex.tgz"}],
        },
    )
    assert dl.get_urls("records") == [
        {"url": "https://x.org/r1.zip", "path": "/dumps/r1.zip"},
        {"url": "https://x.org/dir/r2.json.gz", "path": "/dumps/r2.json.gz"},
    ]
    assert dl.get_urls("export") == [{"url": "https://x.org/ex.tgz", "path": "/elsewhere/ex.tgz"}]
    assert len(dl.get_urls("all")) == 3
    assert dl.get_urls("other") == []


def test_both_shapes_combine():
    dl = make_downloader(
        "/dumps",
        remoteDumpFile="https://x.org/a.gz",
        dumpFilePath="a.gz",
        input_files={"records": [{"url": "https://x.org/b.gz"}]},
    )
    assert [u["path"] for u in dl.get_urls()] == ["/dumps/a.gz", "/dumps/b.gz"]


def test_per_source_dumps_dir_is_a_subdirectory():
    cfg = make_config("/dumps", input_files={"records": [{"url": "https://x.org/b.gz"}]})
    cfg["dumps_dir"] = "src"
    dl = BaseDownloader(cfg)
    assert dl.dumps_dir == "/dumps/src"
    assert dl.get_urls()[0]["path"] == "/dumps/src/b.gz"


def test_relative_path_without_dumps_dir_is_an_error():
    dl = make_downloader(None, input_files={"records": [{"url": "https://x.org/b.gz"}]})
    with pytest.raises(ValueError, match="dumps_dir"):
        dl.get_urls()


def test_works_without_base_init_fields():
    # A config with no all_configs or name, as a bare stub would be
    dl = BaseDownloader({"input_files": {"records": [{"url": "https://x.org/b.gz", "path": "/abs/b.gz"}]}})
    assert dl.get_urls() == [{"url": "https://x.org/b.gz", "path": "/abs/b.gz"}]


# --- verify_ssl ---------------------------------------------------------


def test_verify_ssl_defaults_true_and_reads_source_then_base():
    assert make_downloader("/d").verify_ssl() is True
    assert make_downloader("/d", download_verify_ssl=False).verify_ssl() is False
    cfg = make_config("/d")
    cfg["all_configs"].download_verify_ssl = False
    assert BaseDownloader(cfg).verify_ssl() is False
    cfg["download_verify_ssl"] = True
    assert BaseDownloader(cfg).verify_ssl() is True


# --- splash-page helpers --------------------------------------------------


ZENODO = {
    "hits": {
        "hits": [
            {"files": [{"links": {"self": "https://zenodo.org/api/records/1/files/v2.zip/content"}}]},
            {"files": [{"links": {"self": "https://zenodo.org/api/records/0/files/v1.zip/content"}}]},
        ]
    },
    "count": 2,
}


@pytest.fixture
def canned(monkeypatch):
    dl = make_downloader("/dumps")
    monkeypatch.setattr(dl, "fetch_webpage", lambda url: json.dumps(ZENODO))
    return dl


def test_get_value_from_json_dotted_and_slashed(canned):
    expect = "https://zenodo.org/api/records/1/files/v2.zip/content"
    assert canned.get_value_from_json("https://zenodo.org/api/x", "hits.hits[0].files[0].links.self") == expect
    assert canned.get_value_from_json("https://zenodo.org/api/x", "hits/hits[0]/files[0]/links/self") == expect
    assert canned.get_value_from_json("https://zenodo.org/api/x", "hits.hits[1].files[0].links.self").endswith("v1.zip/content")


def test_get_value_from_json_non_string_or_missing_is_none(canned, caplog):
    assert canned.get_value_from_json("u", "count") is None
    assert canned.get_value_from_json("u", "hits.hits[5].files") is None
    assert canned.get_value_from_json("u", "hits.nope.x") is None
    assert "cannot follow JSON path" in caplog.text


def test_get_value_from_json_bad_responses(monkeypatch):
    dl = make_downloader("/dumps")
    monkeypatch.setattr(dl, "fetch_webpage", lambda url: None)
    assert dl.get_value_from_json("u", "a") is None
    monkeypatch.setattr(dl, "fetch_webpage", lambda url: "not json")
    with pytest.raises(ValueError, match="parse"):
        dl.get_value_from_json("u", "a")
    monkeypatch.setattr(dl, "fetch_webpage", lambda url: "{}")
    with pytest.raises(ValueError, match="empty"):
        dl.get_value_from_json("u", "a")


def test_html_and_xml_helpers_are_explicit_stubs():
    dl = make_downloader("/dumps")
    with pytest.raises(NotImplementedError):
        dl.get_value_from_html("u", "//a")
    with pytest.raises(NotImplementedError):
        dl.get_value_from_xml("u", "//a")


def test_fetch_webpage_passes_verify_and_timeout(monkeypatch):
    seen = {}

    class Resp:
        text = "hello"

        def raise_for_status(self):
            pass

    def fake_get(url, **kw):
        seen.update(kw, url=url)
        return Resp()

    monkeypatch.setattr(dl_mod.requests, "get", fake_get)
    dl = make_downloader("/dumps", download_verify_ssl=False)
    assert dl.fetch_webpage("https://x.org/page") == "hello"
    assert seen["verify"] is False
    assert seen["timeout"] == BaseDownloader.timeout
    assert seen["headers"] == {"User-Agent": dl_mod.USER_AGENT}
    assert "lux-pipeline" in dl_mod.USER_AGENT


def test_fetch_webpage_failure_is_none(monkeypatch, caplog):
    def fake_get(url, **kw):
        raise requests.ConnectionError("no route")

    monkeypatch.setattr(dl_mod.requests, "get", fake_get)
    assert make_downloader("/dumps").fetch_webpage("https://x.org/page") is None
    assert "no route" in caplog.text


# --- process against a local HTTP server --------------------------------


class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args):
        pass


class ShortHandler(QuietHandler):
    """Promise more bytes than are sent, then hang up."""

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", "4096")
        self.end_headers()
        self.wfile.write(b"x" * 1024)
        self.wfile.flush()
        self.close_connection = True


SIZE = 1024 * 1024


@pytest.fixture
def served(tmp_path):
    """Serve ``tmp_path/srv`` over HTTP on a background thread; yield (base_url, srv_dir)."""
    srv_dir = tmp_path / "srv"
    srv_dir.mkdir()
    (srv_dir / "big.bin").write_bytes(os.urandom(SIZE))
    handler = partial(QuietHandler, directory=str(srv_dir))
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", srv_dir
    finally:
        server.shutdown()
        server.server_close()


def test_process_streams_to_part_then_renames(served, tmp_path):
    base_url, srv_dir = served
    target = tmp_path / "dumps" / "src" / "big.bin"
    rec = Recorder()
    dl = make_downloader(str(tmp_path / "dumps")).prepare(0, 1, progress=rec)
    assert dl.process({"url": f"{base_url}/big.bin", "path": str(target)}) == 1
    assert target.read_bytes() == (srv_dir / "big.bin").read_bytes()
    assert not target.with_name("big.bin.part").exists()
    assert rec.totals() == [SIZE]
    assert rec.advanced() == SIZE
    assert rec.descriptions()[0] == "src/big.bin"
    assert rec.calls[-1] == {"completed": SIZE}


def test_process_verifies_a_checksum(served, tmp_path):
    import hashlib
    base_url, srv_dir = served
    data = (srv_dir / "big.bin").read_bytes()
    dl = make_downloader(str(tmp_path))
    good = {"url": f"{base_url}/big.bin", "path": str(tmp_path / "a.bin"),
            "checksum": "md5:" + hashlib.md5(data).hexdigest()}
    assert dl.process(good) == 1
    bare = dict(good, path=str(tmp_path / "b.bin"), checksum=hashlib.md5(data).hexdigest().upper())
    assert dl.process(bare) == 1                       # bare hex is MD5, case-insensitive
    sha = dict(good, path=str(tmp_path / "c.bin"), checksum="sha256:" + hashlib.sha256(data).hexdigest())
    assert dl.process(sha) == 1
    bad = dict(good, path=str(tmp_path / "d.bin"), checksum="md5:" + "0" * 32)
    with pytest.raises(IOError, match="md5 checksum mismatch"):
        dl.process(bad)
    assert not (tmp_path / "d.bin").exists() and not (tmp_path / "d.bin.part").exists()


def test_parse_checksum(caplog):
    assert BaseDownloader.parse_checksum("md5:ABC") == ("md5", "abc")
    assert BaseDownloader.parse_checksum("abc") == ("md5", "abc")
    assert BaseDownloader.parse_checksum("SHA256:ff") == ("sha256", "ff")
    assert BaseDownloader.parse_checksum("") is None and BaseDownloader.parse_checksum(None) is None
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        assert BaseDownloader.parse_checksum("nope:ff") is None
    assert "Unknown checksum algorithm 'nope'" in caplog.text


def test_process_overwrite_false_keeps_existing(served, tmp_path, caplog):
    caplog.set_level(logging.INFO, logger="lux_pipeline")
    base_url, _ = served
    target = tmp_path / "big.bin"
    target.write_bytes(b"old")
    rec = Recorder()
    dl = make_downloader(str(tmp_path)).prepare(progress=rec)
    assert dl.process({"url": f"{base_url}/big.bin", "path": str(target)}, overwrite=False) == 0
    assert target.read_bytes() == b"old"
    assert rec.advanced() == 0
    assert "not overwriting" in caplog.text


def test_process_overwrite_true_replaces_existing(served, tmp_path):
    base_url, srv_dir = served
    target = tmp_path / "big.bin"
    target.write_bytes(b"old")
    dl = make_downloader(str(tmp_path))
    assert dl.process({"url": f"{base_url}/big.bin", "path": str(target)}, overwrite=True) == 1
    assert target.read_bytes() == (srv_dir / "big.bin").read_bytes()


def test_process_http_error_raises_and_leaves_nothing(served, tmp_path):
    base_url, _ = served
    target = tmp_path / "missing.bin"
    dl = make_downloader(str(tmp_path))
    with pytest.raises(requests.HTTPError):
        dl.process({"url": f"{base_url}/missing.bin", "path": str(target)})
    assert not target.exists()
    assert not target.with_name("missing.bin.part").exists()


def test_process_short_transfer_raises_and_removes_part(tmp_path):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), ShortHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        target = tmp_path / "short.bin"
        dl = make_downloader(str(tmp_path))
        with pytest.raises(Exception):
            dl.process({"url": f"http://127.0.0.1:{server.server_address[1]}/short.bin", "path": str(target)})
    finally:
        server.shutdown()
        server.server_close()
    assert not target.exists()
    assert not target.with_name("short.bin.part").exists()


def test_process_passes_verify_and_timeout_to_requests(monkeypatch, tmp_path):
    seen = {}

    class Resp:
        headers = {"content-length": "6"}

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            yield b"abc"
            yield b"def"

    def fake_get(url, **kw):
        seen.update(kw)
        return Resp()

    monkeypatch.setattr(dl_mod.requests, "get", fake_get)
    target = tmp_path / "x.bin"
    dl = make_downloader(str(tmp_path), download_verify_ssl=False)
    assert dl.process({"url": "https://x.org/x.bin", "path": str(target)}) == 1
    assert target.read_bytes() == b"abcdef"
    assert seen == {"stream": True, "timeout": BaseDownloader.timeout, "verify": False,
                    "headers": {"User-Agent": dl_mod.USER_AGENT}}


# --- acceptance: the pleiades downloader imports --------------------------


def test_pleiades_downloader_imports():
    mod = importlib.import_module("lux_pipeline.sources.pleiades.downloader")
    assert issubclass(mod.PleiadesDownloader, BaseDownloader)
