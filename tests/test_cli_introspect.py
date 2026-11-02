"""Item W02: every lux command and its options as data, read from the real
parsers (lux_pipeline.cli.introspect), and a form's values turned back
into a command line the parsers accept. No configuration is loaded."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline import runtime
from lux_pipeline.cli import entry
from lux_pipeline.cli.help import list_commands
from lux_pipeline.cli.introspect import (
    CONFIRM, GROUPS, NOT_RUNNABLE, all_command_specs, argv_from_values, coerce, command_parser,
    command_spec, confirm_needed, confirm_reason, option_name,
)

COMMANDS = [name for name, _ in list_commands()]
GLOBAL_DESTS = ["log", "debug", "env", "source", "max_records", "max_workers", "my_worker",
                "engine", "no_ui", "prefix"]


@pytest.fixture(autouse=True)
def _no_real_config(monkeypatch):
    """No installation, as on a fresh machine: nothing here may load one."""
    def boom(*a, **kw):
        raise RuntimeError("LUX_BASEPATH is not set (no .env found)")
    monkeypatch.setattr(runtime, "get_configs", boom)


def by_dest(spec):
    return {o["dest"]: o for o in spec["options"]}


def parse(name, argv):
    """`argv` (as argv_from_values returns it) through the global parser
    and then the command's own, as `lux` parses it."""
    args, rest = entry.build_parser().parse_known_args(argv)
    assert args.command == name
    _, ap = command_parser(name)
    return ap.parse_args(rest, namespace=args)


# --- the specs -------------------------------------------------------------------------


def test_every_listed_command_has_a_spec_that_json_can_carry():
    specs = all_command_specs()
    assert [s["name"] for s in specs] == COMMANDS
    json.dumps(specs)
    for s in specs:
        assert s["summary"] and s["module"] == f"lux_pipeline.cli.{s['name'].replace('-', '_')}"


@pytest.mark.parametrize("name", COMMANDS)
def test_every_global_option_appears_once_unless_the_command_reuses_it(name):
    spec = command_spec(name)
    dests = [o["dest"] for o in spec["options"]]
    assert len(dests) == len(set(dests))
    globs = [o["dest"] for o in spec["options"] if o["global"]]
    own = {o["dest"] for o in spec["options"] if not o["global"]}
    assert globs == [d for d in GLOBAL_DESTS if d not in own]


def test_the_command_wins_a_shared_destination():
    spec = command_spec("watch")
    prefix = by_dest(spec)["prefix"]
    assert prefix["positional"] and not prefix["global"]


def test_what_the_specs_say_about_some_options():
    rec = command_spec("reconcile")
    o = by_dest(rec)
    assert rec["group"] == "phases" and rec["runs_workers"] and rec["needs_configs"]
    assert o["recid"]["kind"] == "str" and o["recid"]["flags"] == ["--recid"]
    assert o["no_refs"]["kind"] == "flag" and o["no_refs"]["flag_value"] is True
    assert o["no_refs"]["flags"] == ["--no-refs", "--norefs"]
    assert o["baseline"]["help"]
    assert o["max_workers"]["global"] and o["max_workers"]["kind"] == "int"
    assert o["engine"]["choices"] == ["mp", "ray", "null"]
    assert by_dest(command_spec("export"))["format"]["choices"] == ["jsonl", "lmdb"]
    tr = by_dest(command_spec("timing-report"))
    assert tr["compare"]["nargs"] == 2 and tr["profile"]["nargs"] == "?" and tr["profile"]["const"] == 20
    assert by_dest(command_spec("harvest"))["overwrite"]["flag_value"] is False
    assert by_dest(command_spec("validate"))["ignore"]["repeat"] is True
    assert by_dest(command_spec("load"))["file"]["kind"] == "path"
    assert by_dest(command_spec("harvest"))["until"]["type_name"] == "iso8601"


def test_handlers_and_needs():
    specs = {s["name"]: s for s in all_command_specs()}
    assert specs["counts"]["runs_workers"] is False
    assert specs["watch"]["needs_configs"] is False and specs["build"]["needs_configs"] is False
    assert all(not specs[n]["runnable"] for n in NOT_RUNNABLE if n in specs)
    assert specs["reconcile"]["runnable"] and specs["clear"]["runnable"]


def test_groups_cover_every_command_once():
    seen = [n for names in GROUPS.values() for n in names]
    assert len(seen) == len(set(seen))
    assert set(COMMANDS) <= set(seen)
    assert set(CONFIRM) <= set(COMMANDS)


# --- values to a command line ------------------------------------------------------------


def test_the_documents_example():
    spec = command_spec("reconcile")
    values = {"source": "ycba", "recid": "obj/1", "no_refs": True, "max_workers": 4}
    argv = argv_from_values(spec, values)
    assert argv == ["reconcile", "--source", "ycba", "--max-workers", "4", "--recid", "obj/1",
                    "--no-refs"]
    ns = parse("reconcile", argv)
    assert (ns.source, ns.max_workers, ns.recid, ns.no_refs) == ("ycba", 4, "obj/1", True)


def sample(o, tmp_path):
    """A value for option `o` that is not its default."""
    if o["kind"] == "flag":
        return o["flag_value"]
    if o["choices"]:
        return next(c for c in reversed(o["choices"]) if c != o["default"])
    one = {"iso8601": "2026-01-02T03:04:05", "feed_url": "https://example.org/feed.json"}.get(
        o["type_name"]) or {"int": 7, "float": 2.5}.get(o["kind"])
    if o["kind"] == "path":
        f = tmp_path / "dump.json"
        f.write_text("{}")
        one = str(f)
    if one is None:
        one = "x1"
    if o["repeat"] or isinstance(o["nargs"], int) or o["nargs"] in ("*", "+"):
        return [one, one.replace("1", "2") if isinstance(one, str) else one + 1]
    return one


def expected(name, o, value):
    """What argparse stores for `value`: the option's type applied, the
    const for an optional value given bare."""
    _, ap = command_parser(name)
    actions = {a.dest: a for a in [*ap._actions, *entry.build_parser()._actions]}
    a = actions[o["dest"]]
    if value is True and o["nargs"] == "?":
        return a.const
    conv = a.type or (lambda v: v)
    if isinstance(value, list):
        return [conv(v) for v in value]
    return conv(value) if o["kind"] != "flag" else value


CASES = [(name, o["dest"]) for name in COMMANDS for o in command_spec(name)["options"]
         if not o["global"] or name == "reconcile"]


@pytest.mark.parametrize("name,dest", CASES)
def test_every_option_round_trips_through_the_real_parsers(name, dest, tmp_path):
    spec = command_spec(name)
    o = by_dest(spec)[dest]
    if dest == "my_worker":
        pytest.skip("never on a command line built from a form")
    values = [sample(o, tmp_path)]
    if o["nargs"] == "?" and o["const"] is not None:
        values.append(True)                           # the bare flag
    # a required option (restore's backup directory) has to be there too
    required = {r["dest"]: sample(r, tmp_path) for r in spec["options"]
                if r["required"] and r["dest"] != dest}
    for v in values:
        argv = argv_from_values(spec, {**required, dest: v})
        assert argv[0] == name and len(argv) > 1, argv
        ns = parse(name, argv)
        assert getattr(ns, dest) == expected(name, o, v), (argv, v)


def test_defaults_and_empty_values_are_left_out():
    spec = command_spec("export")
    assert argv_from_values(spec, {}) == ["export"]
    assert argv_from_values(spec, {"format": "jsonl", "out_dir": None, "source": "",
                                   "gz": False, "max_workers": 0}) == ["export"]
    assert argv_from_values(command_spec("harvest"), {"overwrite": True}) == ["harvest"]
    assert argv_from_values(command_spec("harvest"), {"overwrite": False}) == ["harvest",
                                                                             "--no-overwrite"]


def test_form_strings_are_coerced():
    spec = command_spec("reconcile")
    assert argv_from_values(spec, {"max_workers": "4", "no_refs": "on", "baseline": "off"}) == \
        ["reconcile", "--max-workers", "4", "--no-refs"]
    assert coerce(by_dest(spec)["max_workers"], "12") == 12
    with pytest.raises(ValueError, match="--max-workers: 'four' is not an integer"):
        argv_from_values(spec, {"max_workers": "four"})
    with pytest.raises(ValueError, match="--no-refs: 'maybe' is not true or false"):
        argv_from_values(spec, {"no_refs": "maybe"})


def test_bad_values_are_refused():
    spec = command_spec("reconcile")
    with pytest.raises(ValueError, match="no option 'nonsense'"):
        argv_from_values(spec, {"nonsense": 1})
    with pytest.raises(ValueError, match="--engine: 'spark' is not one of mp, ray, null"):
        argv_from_values(spec, {"engine": "spark"})
    with pytest.raises(ValueError, match="--my-worker runs one slice"):
        argv_from_values(spec, {"my_worker": 3})
    assert argv_from_values(spec, {"my_worker": -1}) == ["reconcile"]     # its default
    with pytest.raises(ValueError, match="--compare takes 2 values, not 1"):
        argv_from_values(command_spec("timing-report"), {"compare": ["a"]})


def test_a_required_option_must_be_given():
    spec = command_spec("restore")
    assert by_dest(spec)["backup"]["required"] and by_dest(spec)["backup"]["positional"]
    with pytest.raises(ValueError, match="lux restore needs backup"):
        argv_from_values(spec, {"list": True})
    assert argv_from_values(spec, {"list": True, "backup": "/b/1"}) == ["restore", "/b/1", "--list"]


def test_positionals_come_before_options_that_take_an_optional_value():
    argv = argv_from_values(command_spec("initialize"), {"clone": True, "directory": "/srv/lux"})
    assert argv == ["initialize", "/srv/lux", "--clone"]
    ns = parse("initialize", argv)
    assert ns.directory == "/srv/lux"
    assert ns.clone == by_dest(command_spec("initialize"))["clone"]["const"]


def test_option_names_prefer_the_long_dashed_spelling():
    o = by_dest(command_spec("harvest"))["two_phase"]
    assert o["flags"] == ["--two-phase", "--two_phase"] and option_name(o) == "--two-phase"


# --- confirmation ---------------------------------------------------------------------------


@pytest.mark.parametrize("name,values,needed", [
    ("clear", {"source": "aat"}, True),
    ("restore", {}, True),
    ("new-token", {}, True),
    ("identify", {}, True),
    ("write-refs", {}, True),
    ("build", {}, False),
    ("build", {"clear": True}, True),
    ("build", {"clear": "on"}, True),
    ("pg-maintain", {"bloat": True}, False),
    ("pg-maintain", {"bloat": True, "vacuum": True}, True),
    ("pg-maintain", {"rewrite": True}, True),
    ("counts", {}, False),
    ("reconcile", {}, False),
    ("merge", {}, False),
    ("export", {}, False),
    ("backup", {}, False),
])
def test_confirm_needed(name, values, needed):
    assert confirm_needed(command_spec(name), values) is needed


def test_the_reason_is_said_in_words():
    assert "deletes" in confirm_reason(command_spec("clear"), {})
    assert confirm_reason(command_spec("load"), {}).startswith("writes records")
    assert confirm_reason(command_spec("load"), {"clear": True}).startswith("empties the datacache")
    assert command_spec("reconcile")["confirms"] is None


# --- lux help --json ---------------------------------------------------------------------------


def test_lux_help_json(capsys):
    assert entry.run(["help", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert [s["name"] for s in data] == COMMANDS
    assert entry.run(["help", "merge", "--json"]) == 0
    one = json.loads(capsys.readouterr().out)
    assert one["name"] == "merge" and "only_refs" in by_dest(one)
