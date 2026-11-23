"""Artifacts (docs/artifacts.md): configuration records of type "artifact",
`ArtifactCache` (the files in artifacts_dir and the manifest.json it keeps
as it writes them), staleness, `Config.artifact_path` and its fallback to
data_dir, the Getty replaced-ids generator and reader, the record checks,
and `lux artifact`. On the W00 installation; no database, no network (the
Getty endpoint is stubbed)."""

import datetime
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import config_schema, config_store
from lux_pipeline.artifacts import cache as artifact_cache
from lux_pipeline.artifacts import base as artifact_base
from lux_pipeline.artifacts import getty
from lux_pipeline.artifacts.base import ArtifactError, json_rows, line_rows
from lux_pipeline.artifacts.cache import (code_version, config_version, file_problem,
                                          record_sha256)
from lux_pipeline.cli import entry
from lux_pipeline.process.reference_manager import load_redirects
from webui_fixtures import add_subconfig_store, make_configs, make_installation, use_installation

LOGGER = logging.getLogger("lux_pipeline")
LINES = "artifact_fixtures.Lines"
GETTY = "lux_pipeline.artifacts.getty.ReplacementUris"
ROWS = [("http://vocab.getty.edu/aat/300281837", "http://vocab.getty.edu/aat/300140808"),
        ("http://vocab.getty.edu/ulan/500396271", "http://vocab.getty.edu/ulan/500353823")]


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    """Each test starts with no warning already given and the logger as
    it was, and a SPARQL endpoint asked again is asked at once."""
    monkeypatch.setattr(artifact_cache, "_warned", set())
    monkeypatch.delenv(artifact_cache.WARNINGS_ENV, raising=False)
    monkeypatch.setattr(artifact_base.time, "sleep", lambda s: None)
    saved = (list(LOGGER.handlers), LOGGER.level)
    yield
    LOGGER.handlers[:], LOGGER.level = saved


def lines_record(name="lines", files=("lines.txt",), rows=10, **extra):
    return {"name": name, "type": "artifact", "generatorClass": LINES, "files": list(files),
            "rows": rows, "maxAgeDays": 30, **extra}


def install(tmp_path, *records, store=False):
    """The W00 installation with these artifact records, in the
    configuration directory or (`store`) an ``artifacts`` sub-config store
    as pipeline-configs has them. Returns the root."""
    root = make_installation(tmp_path)
    for rec in records:
        (root / "config_cache" / f"{rec['name']}.json").write_text(json.dumps(rec, indent=2))
    if store:
        add_subconfig_store(root, name="artifacts", move=[r["name"] for r in records])
    return root


def sparql(rows=ROWS):
    return json.dumps({"head": {"vars": ["from", "to"]},
                       "results": {"bindings": [{"from": {"type": "uri", "value": f},
                                                 "to": {"type": "uri", "value": t}}
                                                for f, t in rows]}})


class Answer:
    def __init__(self, text, status=200):
        self.text, self.status_code = text, status


def getty_answers(monkeypatch, text, status=200):
    seen = []

    def get(url, params=None, timeout=None, headers=None):
        seen.append((url, params))
        return Answer(text, status)
    monkeypatch.setattr(artifact_base.requests, "get", get)
    return seen


# --- configuration -------------------------------------------------------------------------


def test_artifacts_dir_defaults_under_data_dir_and_resolves_relative(tmp_path):
    cfgs = make_configs(tmp_path)
    assert cfgs.artifacts_dir == os.path.join(cfgs.data_dir, "artifacts")
    base = tmp_path / "config_cache" / "base.json"
    rec = json.loads(base.read_text())
    rec["artifacts_dir"] = "data/artifacts"
    base.write_text(json.dumps(rec))
    assert make_configs(tmp_path).artifacts_dir == str(tmp_path / "data" / "artifacts")


@pytest.mark.parametrize("store", [False, True])
def test_artifact_records_are_filed_and_never_instantiated(tmp_path, store):
    install(tmp_path, lines_record(), store=store)
    cfgs = make_configs(tmp_path, instantiate=True)
    assert set(cfgs.artifacts) == {"lines"}
    assert cfgs.artifacts["lines"]["generatorClass"] == LINES
    origin = cfgs.record_origins["lines"]
    assert origin["store"] == ("artifacts" if store else None)
    assert origin["path"].endswith("lines.json")
    # reading the configuration made nothing
    assert not os.path.exists(cfgs.artifacts_dir)


def test_the_catalogue_knows_artifact_records():
    assert "artifact" in config_schema.RECORD_TYPES
    assert config_schema.keys_for("artifact")["generatorClass"].required
    assert config_schema.keys_for("base")["artifacts_dir"].type == "path"
    rec = config_schema.template("artifact", "x")
    assert rec["type"] == "artifact" and rec["files"] == []


# --- the cache: making, the manifest, the checks ------------------------------------------


def test_make_writes_the_files_and_the_manifest_entry(tmp_path):
    install(tmp_path, lines_record(files=["lines.txt", "sub/more.csv"]))
    cfgs = make_configs(tmp_path)
    cache = cfgs.artifacts_cache
    entry = cache.make("lines")
    d = Path(cfgs.artifacts_dir)
    assert (d / "lines.txt").read_text().startswith("lines.txt 0\n")
    assert (d / "sub" / "more.csv").exists()
    manifest = json.loads((d / "manifest.json").read_text())
    assert manifest == {"lines": entry}
    assert entry["generator"] == LINES
    assert [f["path"] for f in entry["files"]] == ["lines.txt", "sub/more.csv"]
    f = entry["files"][0]
    assert f["rows"] == 10 and f["size"] == (d / "lines.txt").stat().st_size
    assert f["md5"] == artifact_cache.md5_file(d / "lines.txt")
    assert set(entry["code"]) == {"commit", "dirty", "version"}
    assert entry["config"]["path"] == cfgs.record_origins["lines"]["path"]
    assert entry["config"]["record_sha256"] == record_sha256(cfgs.artifacts["lines"])
    assert datetime.datetime.fromisoformat(entry["generated_at"]).tzinfo is not None
    # no temporary directory left behind
    assert [p.name for p in d.iterdir() if p.name.startswith(".tmp-")] == []
    assert cache.path("lines", "sub/more.csv") == str(d / "sub" / "more.csv")
    with pytest.raises(ArtifactError, match="say which"):
        cache.path("lines")


def test_two_artifacts_keep_both_entries(tmp_path):
    install(tmp_path, lines_record("a", ["a.txt"]), lines_record("b", ["b.txt"]))
    cache = make_configs(tmp_path).artifacts_cache
    cache.make("a")
    cache.make("b")
    cache.make("a")
    assert sorted(cache.entries()) == ["a", "b"]


def test_a_failed_generator_leaves_the_previous_files_and_entry(tmp_path):
    install(tmp_path, lines_record())
    cfgs = make_configs(tmp_path)
    cache = cfgs.artifacts_cache
    before = cache.make("lines")
    text = Path(cache.path("lines")).read_text()
    cfgs.artifacts["lines"].update(rows=20, fail=True)
    with pytest.raises(RuntimeError, match="the generator failed"):
        cache.make("lines")
    assert Path(cache.path("lines")).read_text() == text
    assert cache.entry("lines") == before
    assert [p for p in Path(cfgs.artifacts_dir).iterdir() if p.name.startswith(".tmp-")] == []


def test_a_file_that_shrinks_too_far_is_refused_unless_forced(tmp_path):
    install(tmp_path, lines_record(rows=100))
    cfgs = make_configs(tmp_path)
    cache = cfgs.artifacts_cache
    before = cache.make("lines")
    cfgs.artifacts["lines"]["rows"] = 60          # 40% fewer: within the default 0.5
    cache.make("lines")
    cfgs.artifacts["lines"]["rows"] = 20          # 67% fewer
    with pytest.raises(ArtifactError, match="down from 60, more than its maxShrink of 50%"):
        cache.make("lines")
    assert cache.entry("lines")["files"][0]["rows"] == 60
    assert cache.make("lines", force=True)["files"][0]["rows"] == 20
    cfgs.artifacts["lines"].update(rows=1, maxShrink=1)
    assert cache.make("lines")["files"][0]["rows"] == 1
    assert before["files"][0]["rows"] == 100


def test_set_wants_exactly_the_records_files(tmp_path):
    install(tmp_path, lines_record(files=["a.txt", "b.txt"]))
    cache = make_configs(tmp_path).artifacts_cache
    written = tmp_path / "a.txt"
    written.write_text("x\n")
    with pytest.raises(ArtifactError, match="has the files a.txt, b.txt, not a.txt"):
        cache.set("lines", {"a.txt": (str(written), 1)})
    with pytest.raises(ArtifactError, match="b.txt was not written"):
        cache.set("lines", {"a.txt": (str(written), 1), "b.txt": (str(tmp_path / "nope"), 1)})


def test_set_publishes_what_a_command_wrote(tmp_path):
    """A command that is a generator in its own right (lux sheets, lux
    build-differents) hands its files to set and gets the same entry."""
    install(tmp_path, lines_record())
    cfgs = make_configs(tmp_path)
    written = tmp_path / "elsewhere.txt"
    written.write_text("one\ntwo\n")
    entry = cfgs.artifacts_cache.set("lines", {"lines.txt": (str(written), 2)}, seconds=1.25)
    assert entry["seconds"] == 1.2 and entry["generator"] == LINES
    assert not written.exists()
    assert Path(cfgs.artifacts_dir, "lines.txt").read_text() == "one\ntwo\n"


def test_needs_sources_refuses_without_the_source(tmp_path):
    install(tmp_path, lines_record(generatorClass="artifact_fixtures.FromWikidata"))
    cache = make_configs(tmp_path).artifacts_cache
    with pytest.raises(ArtifactError, match="made from the source wikidata, which this "
                                            "configuration does not have"):
        cache.make("lines")
    assert cache.entries() == {}


def test_an_unknown_artifact_is_an_error(tmp_path):
    cache = make_configs(tmp_path).artifacts_cache
    with pytest.raises(ArtifactError, match="no artifact record 'nope'"):
        cache.make("nope")


# --- staleness and checking ------------------------------------------------------------------


def test_stale_reasons(tmp_path):
    install(tmp_path, lines_record())
    cfgs = make_configs(tmp_path)
    cache = cfgs.artifacts_cache
    assert cache.stale("lines") == "never made"
    cache.make("lines")
    assert cache.stale("lines") is None
    later = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=31)
    assert cache.stale("lines", now=later) == "made 31 days ago, more than its maxAgeDays of 30"
    cfgs.artifacts["lines"]["maxAgeDays"] = None
    assert cache.stale("lines", now=later) == "its configuration record changed since it was made"
    cache.make("lines")
    assert cache.stale("lines", now=later) is None        # no maxAgeDays: never stale by age
    cfgs.artifacts["lines"]["generatorClass"] = "artifact_fixtures.FromWikidata"
    assert cache.stale("lines").startswith("its generator is now artifact_fixtures.FromWikidata")
    cfgs.artifacts["lines"]["generatorClass"] = LINES
    os.remove(cache.path("lines"))
    assert cache.stale("lines") == "lines.txt missing"


def test_a_new_code_commit_alone_is_not_stale(tmp_path):
    install(tmp_path, lines_record())
    cache = make_configs(tmp_path).artifacts_cache
    cache.make("lines")
    manifest = Path(cache.manifest_path)
    js = json.loads(manifest.read_text())
    js["lines"]["code"]["commit"] = "0" * 40
    manifest.write_text(json.dumps(js))
    assert cache.stale("lines") is None


def test_status_lists_configured_artifacts_and_entries_without_a_record(tmp_path):
    install(tmp_path, lines_record("a", ["a.txt"]), lines_record("b", ["b.txt"]))
    cfgs = make_configs(tmp_path)
    cache = cfgs.artifacts_cache
    cache.make("a")
    del cfgs.artifacts["a"]
    rows = {r["name"]: r for r in cache.status()}
    assert rows["a"]["configured"] is False and rows["a"]["stale"] == "no configuration record"
    assert rows["a"]["rows"] == 10 and rows["a"]["files"] == ["a.txt"]
    assert rows["b"]["stale"] == "never made" and rows["b"]["generated_at"] is None
    assert rows["b"]["max_age_days"] == 30


def test_check_compares_sizes_and_md5s(tmp_path):
    install(tmp_path, lines_record("a", ["a.txt"]), lines_record("b", ["b.txt"]))
    cache = make_configs(tmp_path).artifacts_cache
    cache.make("a")
    cache.make("b")
    assert cache.check() == []
    path = Path(cache.path("a"))
    text = path.read_text()
    path.write_text(text.replace("0", "9"))                   # same size, other bytes
    os.remove(cache.path("b"))
    assert cache.check() == [{"artifact": "a", "path": "a.txt",
                              "problem": "md5 differs from the manifest's"},
                             {"artifact": "b", "path": "b.txt", "problem": "missing"}]
    path.write_text(text + "more\n")
    assert "the manifest says" in cache.check("a")[0]["problem"]


def test_a_manifest_that_is_not_json(tmp_path, caplog):
    install(tmp_path, lines_record())
    cache = make_configs(tmp_path).artifacts_cache
    os.makedirs(cache.directory)
    Path(cache.manifest_path).write_text("{not json")
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert cache.entries() == {}
    assert "cannot be read" in caplog.text
    with pytest.raises(ArtifactError, match="cannot be read"):
        cache.make("lines")


# --- the versions in the manifest ------------------------------------------------------------


def git(directory, *args):
    subprocess.run(["git", "-C", str(directory), *args], check=True, capture_output=True,
                   env={**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.org",
                        "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.org"})


def test_config_version_reads_the_records_checkout(tmp_path):
    rec = lines_record()
    path = tmp_path / "lines.json"
    path.write_text(json.dumps(rec))
    outside = config_version(str(path), rec)
    assert (outside["commit"], outside["dirty"]) == (None, None)
    git(tmp_path, "init", "-q")
    git(tmp_path, "add", "lines.json")
    git(tmp_path, "commit", "-q", "-m", "x")
    clean = config_version(str(path), rec)
    assert len(clean["commit"]) == 40 and clean["dirty"] is False
    (tmp_path / "other.json").write_text("{}")             # another file: still clean
    assert config_version(str(path), rec)["dirty"] is False
    path.write_text(json.dumps({**rec, "rows": 3}))
    assert config_version(str(path), rec)["dirty"] is True
    assert clean["record_sha256"] == record_sha256(rec) != record_sha256({**rec, "rows": 3})


def test_code_version_is_this_checkout():
    v = code_version()
    head = subprocess.run(["git", "-C", artifact_cache.CODE_ROOT, "rev-parse", "HEAD"],
                          capture_output=True, text=True)
    if head.returncode == 0:
        assert v["commit"] == head.stdout.strip() and isinstance(v["dirty"], bool)
    assert v["version"]


# --- reading an artifact ---------------------------------------------------------------------


def test_artifact_path_reads_the_made_artifact(tmp_path, caplog):
    install(tmp_path, lines_record())
    cfgs = make_configs(tmp_path)
    Path(cfgs.data_dir, "lines.txt").write_text("old\n")
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        # not made yet: data_dir's file, with a warning
        assert cfgs.artifact_path("lines") == os.path.join(cfgs.data_dir, "lines.txt")
        cfgs.artifact_path("lines")
    assert caplog.text.count("artifact lines has not been made: reading") == 1
    cfgs.artifacts_cache.make("lines")
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert cfgs.artifact_path("lines") == os.path.join(cfgs.artifacts_dir, "lines.txt")
    assert caplog.text == ""
    cfgs.artifacts["lines"]["rows"] = 11
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        cfgs.artifact_path("lines")
        cfgs.artifact_path("lines")
    assert caplog.text.count("artifact lines is stale (its configuration record changed") == 1


@pytest.mark.parametrize("value", ["0", "false", "No", "off"])
def test_catch_up_warnings_can_be_turned_off(tmp_path, caplog, monkeypatch, value):
    monkeypatch.setenv("LUX_ARTIFACT_WARNINGS", value)
    install(tmp_path, lines_record(), getty_record())
    cfgs = make_configs(tmp_path)
    Path(cfgs.data_dir, "lines.txt").write_text("old\n")
    Path(cfgs.data_dir, "other.txt").write_text("old\n")
    Path(cfgs.data_dir, "replacements.json").write_text(sparql(ROWS[1:]))
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        # not made, no record, the Getty file under its old name
        assert cfgs.artifact_path("lines") == os.path.join(cfgs.data_dir, "lines.txt")
        assert cfgs.artifact_path("other", "other.txt") == os.path.join(cfgs.data_dir, "other.txt")
        assert load_redirects(cfgs) == dict(ROWS[1:])
        cfgs.artifacts_cache.make("lines")
        cfgs.artifacts["lines"]["rows"] = 11
        cfgs.artifact_path("lines")  # stale
    assert caplog.text == ""
    with caplog.at_level(logging.DEBUG, logger="lux_pipeline"):
        cfgs.artifact_path("other", "other.txt")
    assert "artifact lines has not been made" not in caplog.text  # still once per process
    monkeypatch.setattr(artifact_cache, "_warned", set())
    with caplog.at_level(logging.DEBUG, logger="lux_pipeline"):
        cfgs.artifact_path("other", "other.txt")
    assert "the configuration has no artifact record other" in caplog.text


def test_a_broken_artifact_record_still_warns_with_catch_up_warnings_off(tmp_path, caplog, monkeypatch):
    monkeypatch.setenv("LUX_ARTIFACT_WARNINGS", "0")
    install(tmp_path, lines_record(files=("a.txt", "b.txt")))
    cfgs = make_configs(tmp_path)
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert cfgs.artifact_path("lines", "c.txt") is None
    assert caplog.text != ""


def test_artifact_path_without_a_record_reads_data_dir(tmp_path, caplog):
    cfgs = make_configs(tmp_path)
    assert cfgs.artifact_path("lines", "lines.txt") is None
    assert cfgs.artifact_path("lines") is None
    Path(cfgs.data_dir, "lines.txt").write_text("old\n")
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert cfgs.artifact_path("lines", "lines.txt") == os.path.join(cfgs.data_dir, "lines.txt")
    assert "the configuration has no artifact record lines" in caplog.text


def test_artifact_path_with_one_file_ignores_the_old_name(tmp_path):
    """`file` is the name the file had in data_dir; the record's own name
    is what artifacts_dir has."""
    install(tmp_path, lines_record(files=["getty/replaced.json"]))
    cfgs = make_configs(tmp_path)
    cfgs.artifacts_cache.make("lines")
    assert cfgs.artifact_path("lines", "old_name.json") == \
        os.path.join(cfgs.artifacts_dir, "getty", "replaced.json")


# --- the Getty replaced ids --------------------------------------------------------------------


def getty_record(**extra):
    return {"name": "getty_replacement_uris", "type": "artifact", "generatorClass": GETTY,
            "files": ["getty_replacement_uris.json"], "maxAgeDays": 90, **extra}


def test_the_getty_generator_writes_the_sparql_result(tmp_path, monkeypatch):
    install(tmp_path, getty_record())
    cfgs = make_configs(tmp_path)
    seen = getty_answers(monkeypatch, sparql())
    entry = cfgs.artifacts_cache.make("getty_replacement_uris")
    assert seen == [(getty.ENDPOINT, {"query": getty.QUERY})]
    assert entry["files"][0]["rows"] == 2
    assert Path(cfgs.artifacts_dir, "getty_replacement_uris.json").read_text() == sparql()
    # what the reference manager and the fetcher read
    assert load_redirects(cfgs) == dict(ROWS)


def test_the_getty_generator_uses_the_records_endpoint_and_query(tmp_path, monkeypatch):
    install(tmp_path, getty_record(endpoint="https://example.org/sparql", query="SELECT 1"))
    seen = getty_answers(monkeypatch, sparql())
    make_configs(tmp_path).artifacts_cache.make("getty_replacement_uris")
    assert seen == [("https://example.org/sparql", {"query": "SELECT 1"})]


def test_a_bad_answer_is_asked_again(tmp_path, monkeypatch):
    install(tmp_path, getty_record())
    answers = [Answer(""), Answer("busy", 429), Answer(sparql())]
    monkeypatch.setattr(artifact_base.requests, "get", lambda *a, **kw: answers.pop(0))
    entry = make_configs(tmp_path).artifacts_cache.make("getty_replacement_uris")
    assert entry["files"][0]["rows"] == 2 and answers == []


@pytest.mark.parametrize("text,status,message", [
    ("<html>busy</html>", 200, "did not answer with a SPARQL JSON result"),
    ("PKIX path building failed", 200, "did not answer with a SPARQL JSON result"),
    ("oops", 503, "answered 503"),
    (json.dumps({"results": {"bindings": [{"from": {"value": "x"}}]}}), 200, "without from and to"),
])
def test_a_bad_answer_from_getty_makes_nothing(tmp_path, monkeypatch, text, status, message):
    install(tmp_path, getty_record())
    cache = make_configs(tmp_path).artifacts_cache
    getty_answers(monkeypatch, text, status)
    with pytest.raises(ArtifactError, match=message):
        cache.make("getty_replacement_uris")
    assert cache.entries() == {}


def test_load_redirects_falls_back_to_data_dir_until_the_artifact_is_made(tmp_path, caplog):
    install(tmp_path, getty_record())
    cfgs = make_configs(tmp_path)
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert load_redirects(cfgs) == {}
    assert "lux artifact --type getty_replacement_uris makes it" in caplog.text
    Path(cfgs.data_dir, "getty_replacement_uris.json").write_text(sparql(ROWS[:1]))
    assert load_redirects(cfgs) == dict(ROWS[:1])
    Path(cfgs.data_dir, "getty_replacement_uris.json").unlink()
    Path(cfgs.data_dir, "replacements.json").write_text(sparql(ROWS[1:]))
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert load_redirects(cfgs) == dict(ROWS[1:])
    assert "replacements.json: rename it to" in caplog.text


# --- the record checks -------------------------------------------------------------------------


def problems(tmp_path, key, data, others=()):
    install(tmp_path, *others)
    return config_store.artifact_problems(key, data, make_configs(tmp_path))


def test_a_good_record_has_no_problems(tmp_path):
    assert problems(tmp_path, "lines", lines_record()) == []
    assert config_store.artifact_problems("getty_replacement_uris", getty_record()) == []


def test_record_problems(tmp_path):
    rec = lines_record(name="other", files=["../up.txt", "/abs.txt", "a.txt", "a.txt", "taken.txt"],
                       maxShrink=1.5, maxAgeDays=-1, rows=None, colour="blue")
    found = problems(tmp_path, "lines", rec, others=[lines_record("theirs", ["taken.txt"])])
    assert found == [
        "name is 'other': an artifact's name must be its key 'lines'",
        "files: ../up.txt is not a file inside artifacts_dir",
        "files: /abs.txt is absolute; files are relative to artifacts_dir",
        "files: a.txt listed more than once",
        "files: taken.txt also written by artifact theirs",
        "maxShrink should be a fraction from 0 to 1, not 1.5",
        "maxAgeDays should not be negative, not -1",
        "rows is required by Lines",
        "warning: colour is not a key of Lines or of every artifact: a typo is ignored",
    ]


def test_a_needed_source_missing_is_a_warning(tmp_path):
    rec = lines_record(generatorClass="artifact_fixtures.FromWikidata")
    assert problems(tmp_path, "lines", rec) == [
        "warning: FromWikidata is made from wikidata, which this configuration does not have: "
        "lux artifact will refuse to make it"]


def test_file_problem():
    assert file_problem("a/b.csv") is None
    assert file_problem("manifest.json") is not None
    assert file_problem("") is not None and file_problem("a/../..") is not None


def test_lux_config_check_reads_artifact_records(tmp_path, monkeypatch, capsys):
    use_installation(monkeypatch, install(tmp_path, lines_record(colour="blue"), getty_record(),
                                          store=True))
    assert entry.run(["config", "check"]) == 0
    out = capsys.readouterr().out
    assert "lines: warning: colour is not a key of Lines" in out
    assert "0 error(s), 1 warning(s)" in out


def test_row_counts(tmp_path):
    p = tmp_path / "x"
    p.write_text("a\nb\nc\n")
    assert line_rows(p) == 3
    p.write_text(sparql())
    assert json_rows(p) == 2
    p.write_text(json.dumps({"a": 1, "b": 2, "c": 3}))
    assert json_rows(p) == 3
    p.write_text("[1, 2]")
    assert json_rows(p) == 2
    p.write_text("7")
    assert json_rows(p) is None


# --- lux artifact --------------------------------------------------------------------------------


def run(*argv):
    return entry.run(["artifact", "--no-ui", *argv])


def test_lux_artifact_lists_makes_and_checks(tmp_path, monkeypatch, capsys):
    root = use_installation(monkeypatch, install(tmp_path, lines_record(), getty_record()))
    getty_answers(monkeypatch, sparql())
    assert run() == 0
    out = capsys.readouterr().out
    assert "lines" in out and "stale: never made" in out and "getty_replacement_uris" in out

    assert run("--stale") == 0
    capsys.readouterr()
    assert run("--list", "--json") == 0
    rows = {r["name"]: r for r in json.loads(capsys.readouterr().out)}
    assert rows["lines"]["stale"] is None and rows["getty_replacement_uris"]["rows"] == 2

    assert run("--check") == 0
    assert "Every file matches" in capsys.readouterr().out
    Path(root, "data/files/artifacts/lines.txt").write_text("changed\n")
    with pytest.raises(SystemExit) as ex:
        run("--check")
    assert ex.value.code == 1
    assert "lines: lines.txt size" in capsys.readouterr().out

    assert run("--type", "lines") == 0
    assert run("--check") == 0


def test_lux_artifact_stale_with_nothing_stale_makes_nothing(tmp_path, monkeypatch, capsys):
    use_installation(monkeypatch, install(tmp_path, lines_record()))
    assert run("--type", "lines") == 0
    made = json.loads(Path(tmp_path, "data/files/artifacts/manifest.json").read_text())
    assert run("--stale") == 0
    assert json.loads(Path(tmp_path, "data/files/artifacts/manifest.json").read_text()) == made


def test_lux_artifact_unknown_name_and_failures(tmp_path, monkeypatch, capsys):
    use_installation(monkeypatch, install(tmp_path, lines_record("good", ["g.txt"]),
                                          lines_record("bad", ["b.txt"], fail=True)))
    assert run("--type", "nope") == 1
    assert "no artifact record nope; configured: bad, good" in capsys.readouterr().err
    # one failing does not stop the others
    assert run("--all") == 1
    assert "not made: bad" in capsys.readouterr().err
    assert Path(tmp_path, "data/files/artifacts/g.txt").exists()


def test_lux_list_artifacts(tmp_path, monkeypatch, capsys):
    use_installation(monkeypatch, install(tmp_path, lines_record()))
    assert entry.run(["list", "--artifacts", "--no-ui"]) == 0
    out = capsys.readouterr().out
    assert "Artifacts" in out and "lines" in out and "never made" in out
