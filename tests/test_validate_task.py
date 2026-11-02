"""`lux validate` (ValidateManager): records validated slice by slice, the
error report per worker, errors grouped into kinds, anyOf failures
explained by their best sub-error, --ignore, --max-records, records with
no schema, the checks before any worker, and the command's summary. A
small real JSON schema stands in for Linked Art's, so the errors are real
jsonschema errors."""

import json
import sys
from argparse import Namespace
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.cli import validate as cli
from lux_pipeline.process.tasks import _task_manager
from lux_pipeline.process.tasks.validate_task import ValidateManager, explain
from lux_pipeline.progress import null_progress

NAME = {"type": "object", "required": ["type", "content"], "additionalProperties": False,
        "properties": {"type": {"const": "Name"}, "content": {"type": "string"}}}
IDENTIFIER = {"type": "object", "required": ["type", "content"], "additionalProperties": False,
              "properties": {"type": {"const": "Identifier"}, "content": {"type": "string"}}}
PERSON = {"type": "object", "required": ["type", "_label"],
          "properties": {"type": {"const": "Person"}, "_label": {"type": "string"},
                         "identified_by": {"type": "array",
                                           "items": {"anyOf": [NAME, IDENTIFIER]}}}}


class StubValidator:
    """Validator's interface over the small schema: Person only."""

    def __init__(self, missing=()):
        self._missing = list(missing)
        self.schema_dir = "/schemas"
        self.v = Draft202012Validator(PERSON)

    def missing(self):
        return self._missing

    def validate(self, rec):
        js = rec["data"]
        if js.get("type") != "Person":
            raise ValueError(f"Cannot validate record with class: {js.get('type')}")
        return list(self.v.iter_errors(js))


class Cache:
    def __init__(self, rows, key="identifier"):
        self.rows, self.key = rows, key

    def _rec(self, k):
        return {self.key: k, "data": self.rows[k]}

    def iter_records(self):
        return (self._rec(k) for k in sorted(self.rows))

    def iter_records_slice(self, mySlice=0, maxSlice=10):
        return (self._rec(k) for i, k in enumerate(sorted(self.rows)) if i % maxSlice == mySlice)

    def len_estimate(self):
        return len(self.rows)


def person(label=True, names=(), extra=None):
    rec = {"type": "Person", "identified_by": list(names), **(extra or {})}
    if label:
        rec["_label"] = "x"
    return rec


GOOD = {"type": "Name", "content": "Smith"}
BAD_NAME = {"type": "Name", "content": "Smith", "language": "en"}


class Configs:
    max_workers = 2

    def __init__(self, tmp_path, missing=()):
        self.log_dir = str(tmp_path / "logs")
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        self.validator = StubValidator(missing)
        self.internal = {
            "ycba": {"name": "ycba", "recordcache": Cache({
                "a": person(), "b": person(label=False), "c": person(names=[BAD_NAME, BAD_NAME]),
                "d": person(names=[GOOD]), "e": {"type": "Place"}})},
            "ypm": {"name": "ypm", "recordcache": Cache({"p": person(names=[BAD_NAME])}),
                    "datacache": Cache({"q": person(label=False)})},
        }
        self.external = {"aat": {"name": "aat", "recordcache": None}}
        self.results = {"merged": {"name": "merged", "recordcache": Cache(
            {"0b5c": person(label=False)}, key="yuid")}}


def manager(cfgs, sources=("ycba", "ypm"), workers=2, **opts):
    m = ValidateManager(cfgs, workers, Namespace(**opts))
    for s in sources:
        m.prepare_single(s)
    return m


def run_all(m, cfgs, tmp_path):
    m.check()
    m.report_dir = str(tmp_path / "report")
    Path(m.report_dir).mkdir(exist_ok=True)
    m.results = [m._distributed(n, cfgs, null_progress) for n in range(m.max_workers)]
    return m.summary()


def report(tmp_path):
    lines = []
    for fn in sorted((tmp_path / "report").glob("validate-w*.jsonl")):
        lines += [json.loads(l) for l in fn.read_text().splitlines()]
    return lines


# --- the work --------------------------------------------------------------------


def test_every_record_once_across_the_workers(tmp_path):
    cfgs = Configs(tmp_path)
    s = run_all(manager(cfgs), cfgs, tmp_path)
    assert s["sources"]["ycba/recordcache"] == {"records": 5, "invalid": 2, "errors": 4,
                                                "unvalidatable": 1}
    assert s["sources"]["ypm/recordcache"] == {"records": 1, "invalid": 1, "errors": 1,
                                               "unvalidatable": 0}
    idents = {(e["source"], e["identifier"]) for e in report(tmp_path)}
    assert idents == {("ycba", "b"), ("ycba", "c"), ("ycba", "e"), ("ypm", "p")}


def test_anyof_is_explained_by_its_best_sub_error(tmp_path):
    cfgs = Configs(tmp_path)
    s = run_all(manager(cfgs), cfgs, tmp_path)
    kinds = {(cls, rule, path): (n, msg) for n, cls, rule, path, msg in s["kinds"]}
    # three bad Names in two records, at different positions: one kind
    n, msg = kinds[("Person", "anyOf>additionalProperties", "/identified_by/*")]
    assert n == 3 and "'language' was unexpected" in msg
    assert kinds[("Person", "required", "/")] == (1, "'_label' is a required property")
    assert kinds[("Place", "class", "/")][1] == "Cannot validate record with class: Place"
    # most common first
    assert s["kinds"][0][0] == 3
    errs = [e for e in report(tmp_path) if e["identifier"] == "c"]
    assert sorted(e["path"] for e in errs) == ["/identified_by/0", "/identified_by/1"]
    assert all(e["rule"] == "anyOf>additionalProperties" and e["class"] == "Person" for e in errs)


def test_explain_leaves_other_errors_alone():
    err = next(Draft202012Validator(PERSON).iter_errors({"type": "Person"}))
    assert explain(err) == ("required", err)


def test_ignore_drops_matching_errors(tmp_path):
    cfgs = Configs(tmp_path)
    s = run_all(manager(cfgs, ignore=["'language' was unexpected"]), cfgs, tmp_path)
    assert s["sources"]["ycba/recordcache"]["invalid"] == 1          # only b
    assert s["sources"]["ypm/recordcache"] == {"records": 1, "invalid": 0, "errors": 0,
                                               "unvalidatable": 0}


def test_max_records_caps_each_worker(tmp_path):
    cfgs = Configs(tmp_path)
    m = manager(cfgs, sources=("ycba",), workers=2)
    m.max_records = 1
    s = run_all(m, cfgs, tmp_path)
    assert s["sources"]["ycba/recordcache"]["records"] == 2


def test_one_worker_reads_unsliced_and_other_caches(tmp_path):
    cfgs = Configs(tmp_path)
    s = run_all(manager(cfgs, sources=("ypm",), workers=1, cache="recordcache,datacache"),
                cfgs, tmp_path)
    assert list(s["sources"]) == ["ypm/recordcache", "ypm/datacache"]
    assert s["sources"]["ypm/datacache"]["invalid"] == 1


def test_merged_records_are_identified_by_yuid(tmp_path):
    cfgs = Configs(tmp_path)
    run_all(manager(cfgs, sources=("merged",), workers=1), cfgs, tmp_path)
    assert [e["identifier"] for e in report(tmp_path)] == ["0b5c"]


# --- checks before any worker ----------------------------------------------------


def test_missing_schemas_stop_it_first(tmp_path):
    cfgs = Configs(tmp_path, missing=["core.json"])
    with pytest.raises(FileNotFoundError, match="core.json.*lux initialize"):
        manager(cfgs).check()


def test_nothing_to_validate(tmp_path):
    cfgs = Configs(tmp_path)
    with pytest.raises(ValueError, match="No source selected"):
        manager(cfgs, sources=()).check()
    with pytest.raises(ValueError, match="has a recordcache"):
        manager(cfgs, sources=("aat",)).check()


def test_process_writes_beside_the_logs_and_clears_old_reports(tmp_path, monkeypatch):
    cfgs = Configs(tmp_path)
    monkeypatch.setattr(_task_manager.runtime, "get_configs", lambda: cfgs)
    old = Path(f"{tmp_path}/run-errors")
    old.mkdir()
    (old / "validate-w0.jsonl").write_text('{"old": true}\n')
    m = manager(cfgs, sources=("ypm",), workers=1)
    m.process("null", prefix=str(tmp_path / "run"))
    assert m.report_dir == f"{tmp_path}/run-errors"
    lines = (old / "validate-w0.jsonl").read_text().splitlines()
    assert len(lines) == 1 and json.loads(lines[0])["identifier"] == "p"


# --- the command ------------------------------------------------------------------


def run_command(cfgs, rest, tmp_path, monkeypatch, source=None):
    monkeypatch.setattr(_task_manager.runtime, "get_configs", lambda: cfgs)
    args = Namespace(command="validate", source=source, max_workers=1, engine="null",
                     no_ui=True, prefix=str(tmp_path / "validate"), log="INFO",
                     max_records=-1, my_worker=-1)
    return cli.CommandHandler(cfgs).process(args, list(rest))


def test_command_defaults_to_internal_and_prints_a_summary(tmp_path, monkeypatch, capsys):
    cfgs = Configs(tmp_path)
    m = run_command(cfgs, [], tmp_path, monkeypatch)
    assert m.sources == [("internal", "ycba"), ("internal", "ypm")]
    out = capsys.readouterr().out
    assert "ycba/recordcache" in out and "most common errors (of 3 kinds):" in out
    assert "Person /identified_by/* [anyOf>additionalProperties]: Additional properties" in out
    assert f"{tmp_path}/validate-errors/validate-w*.jsonl" in out


def test_command_strict_fails_on_invalid(tmp_path, monkeypatch):
    cfgs = Configs(tmp_path)
    with pytest.raises(RuntimeError, match="invalid record"):
        run_command(cfgs, ["--strict"], tmp_path, monkeypatch, source="ypm")
    run_command(cfgs, ["--strict", "--ignore", "'language' was unexpected"], tmp_path,
                monkeypatch, source="ypm")


def test_summary_without_errors():
    lines = cli.summary_lines({"sources": {"ycba/recordcache": {
        "records": 3, "invalid": 0, "errors": 0, "unvalidatable": 0}}, "kinds": [],
        "report_dir": "/r"})
    assert lines[-1] == "no errors"


def test_listed():
    from lux_pipeline.cli.help import list_commands
    assert dict(list_commands())["validate"].startswith("Validate records against the Linked Art")
