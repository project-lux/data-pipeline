# W02. Command introspection (`cli/introspect.py`)

**Size:** S · **Depends on:** — · **Unblocks:** W10 · **Done:** 2026-11-02
**Read:** `lux_pipeline/cli/entry.py` (`build_parser`, `resolve_command`,
`command_help`), `lux_pipeline/cli/help.py` (`list_commands`,
`GLOBAL_OPTIONS`), `lux_pipeline/cli/_handler.py` (`BaseHandler`,
`CommandHandler`), three or four command modules' `add_args`
(`reconcile.py`, `export.py`, `clear.py`, `backup.py`), `tests/test_cli_entry.py`

## Intent

Describe every `lux` command and its options **as data**, read from the
real `ArgumentParser`s, so the web can build a form for a command and turn
the filled form back into an `argv` — and never keep a hand-written copy of
the options that drifts from the code.

```python
from lux_pipeline.cli.introspect import command_spec, all_command_specs, argv_from_values
spec = command_spec("reconcile")
spec["options"][0]  # {"dest": "recid", "flags": ["--recid"], "kind": "str", "help": "...", "default": None, "choices": None, "required": False, "global": False}
argv_from_values(spec, {"source": "ycba", "recid": "obj/1", "no_refs": True, "max_workers": 4})
# -> ["reconcile", "--source", "ycba", "--max-workers", "4", "--recid", "obj/1", "--no-refs"]
```

## Files

| Action | Path | Notes |
|---|---|---|
| Create | `lux_pipeline/cli/introspect.py` | the module |
| Create | `tests/test_cli_introspect.py` | |
| Edit | `lux_pipeline/cli/help.py` | optional: `lux help --json` prints `all_command_specs()`; useful for debugging the web forms |

## Design

```python
GROUPS = {
  "phases": ("download", "harvest", "load", "load-index", "reconcile", "merge", "export", "portal", "sheets", "build"),
  "steps":  ("identify", "metatypes", "new-token", "write-refs", "build-differents", "refetch", "validate"),
  "stores": ("counts", "clear", "backup", "restore", "pg-maintain"),
  "info":   ("list", "timing-report", "pg-tune", "testinstall", "guess-dump-type", "viz", "help", "watch", "interactive", "initialize", "serve", "config", "show"),
}
# anything list_commands() returns that is not named above goes in "info" and is logged once at DEBUG

DESTRUCTIVE = {"clear", "restore", "new-token", "identify", "metatypes", "write-refs", "pg-maintain", "build", "refetch", "load", "load-index", "harvest", "download"}
# the rule, in the docstring: a command that deletes or rewrites a store, or starts a build, confirms; one that only reads or writes files under exports/logs does not. `build` is destructive only with --clear (see `confirm_needed`).

def command_spec(name) -> dict
def all_command_specs() -> list[dict]
def argv_from_values(spec, values: dict) -> list[str]
def confirm_needed(spec, values) -> bool
```

`command_spec(name)`:

```python
{"name": "reconcile", "module": "lux_pipeline.cli.reconcile", "doc": <full docstring>, "summary": <first line>,
 "group": "phases", "runs_workers": True,          # CommandHandler subclass of _handler.CommandHandler (has make_manager)
 "needs_configs": True, "destructive": False,
 "options": [ ...global options..., ...own options... ]}
```

Build the parsers exactly as `entry.command_help` does — `entry.build_parser()`
for the globals, `mod.CommandHandler(None).add_args(ap)` on a fresh
`ArgumentParser(add_help=False)` for the command's own — and walk
`ap._actions`. Skip `help` actions and the positional `command`. For each
action:

| from the action | to the spec |
|---|---|
| `option_strings` | `flags` (positionals: `[]`, `positional: True`) |
| `dest` | `dest` |
| `help` | `help` (`%(default)s` expanded with `ap._get_formatter()._expand_help(action)` guarded by try) |
| `default` | `default` (`argparse.SUPPRESS` → `None`) |
| `choices` | `choices` as a list or `None` |
| `required` | `required` |
| `nargs` | `nargs` (`None`, `"?"`, `"*"`, `"+"`, int) |
| `type` / action class | `kind`: `"flag"` for `store_true` / `store_false` (with `"flag_value": True/False`), `"int"` for `type is int`, `"float"`, `"path"` for `load.existing_path`, `"str"` otherwise; `"count"` for `_CountAction` |
| from the global parser | `global: True` |

`argv_from_values(spec, values)`: `[spec["name"]]`, then for each option
in spec order whose `dest` is in `values` with a value that is not `None`,
`""`, `False` or equal to its default: flags → the first long flag when
the value equals `flag_value`; others → `flag, str(value)` (lists for
`nargs` `*`/`+` → one flag then each value; `nargs == 2` as `--compare`
→ flag then both). Positionals are appended in order after options.
`--my-worker` is never emitted (the web runs whole commands). A `dest`
not in the spec raises `ValueError` naming it.

`confirm_needed(spec, values)`: `spec["destructive"]`, except `build`
without `values.get("clear")` and `pg-maintain` with only `bloat` set,
which do not.

## Known hazards

* `add_args` of some handlers sets `self.extra_args`; the handler is
  constructed with `cfgs=None` — fine, `command_help` already does this.
  `export.py`'s `__init__` takes `cfgs`; still `None`-safe (check).
* The global parser has `--max-records` with two spellings and `--no-ui`
  with three; `flags` keeps them all, forms show the first.
* `watch` uses the global `--prefix` as a positional; `help` has a `topic`
  positional. Positionals come back with `positional: True` and the form
  shows a text box.
* `ap._actions` is private API but stable across every Python the
  package supports (3.10–3.14); say so in a comment.

## Steps

1. Write the module; `python -c "from lux_pipeline.cli.introspect import *; import json; print(json.dumps(command_spec('export'), indent=1))"` and read it against `lux export --help`.
2. Tests.
3. (Optional) `lux help --json`.

## As built

`lux_pipeline/cli/introspect.py` as designed above, plus:

* **Spec fields** beyond the design: `runnable` (False for `NOT_RUNNABLE`:
  help, watch, interactive, initialize, serve), `confirms` (the reason
  text, or None). Each option also has `type_name` (`iso8601`,
  `feed_url`, `existing_path`: a hint for the form), `const` (the value an
  optional-value option takes when given bare: `timing-report --profile`
  20, `initialize --clone` the pipeline-configs URL) and `repeat` (an
  append option: `validate --ignore`).
* **A destination the command reuses** drops the global option: only
  `watch` does (its positional `prefix` is the global `--prefix`).
* **Confirmation** is `confirm_reason(spec, values)`: one sentence per
  command in `CONFIRM`, None when no confirmation is needed (`build`
  without `--clear`; `pg-maintain` with nothing but `--bloat`); `load
  --clear` says it empties the datacache first. `confirm_needed` is
  `confirm_reason(...) is not None`. W10 shows this sentence and keeps no
  table of its own.
* **`argv_from_values`**: `coerce` turns form strings into what the option
  takes (`"4"` to 4; `"on"`/`"off"` for flags; a list for repeated and
  two-valued options) and names the option in its `ValueError`;
  positionals go straight after the command (`initialize --clone DIR`
  would otherwise take DIR as the URL); a store-false flag
  (`--no-overwrite`) is written when its value is False; a bare optional
  value (`--profile` with True) is written as the flag alone; a required
  option (restore's `backup` directory) must be given; `my_worker` raises
  rather than being dropped silently.
* `lux help --json` and `lux help <command> --json` print the specs.
* The module keeps the plan's name `introspect.py`, not `_introspect.py`:
  like `entry.py`, it is a public module of `lux_pipeline.cli` that is not
  a command, and `lux help` (which looks for a `CommandHandler`) does not
  list it.

## Tests

`tests/test_cli_introspect.py`, no configuration needed (`entry.command_help`
does not load one either):

* every name from `list_commands()` has a spec; every spec has every
  global option exactly once; `reconcile`'s own options include `recid`,
  `no_refs` (kind flag), `baseline`; `export`'s `--format` has choices
  `["jsonl", "lmdb"]`; `timing-report`'s `--compare` has `nargs == 2`.
* round trip: for `reconcile`, `argv_from_values` with the example above
  gives the argv shown; parsing that argv with `entry.build_parser().parse_known_args`
  then the command's parser gives back the same values (this is the test
  that matters: the form must produce what the CLI accepts).
* `argv_from_values` omits defaults and `None`, never emits `--my-worker`,
  raises on an unknown dest.
* `confirm_needed`: `clear` yes, `build` no, `build` with `clear=True`
  yes, `pg-maintain` with `bloat=True` only no, with `vacuum=True` yes,
  `counts` no.
* groups cover every listed command; nothing is in two groups.

## Acceptance criteria

* Tests pass; `lux help` and `lux <cmd> --help` output unchanged.
* `all_command_specs()` runs with no `.env` present (the test harness in
  `test_cli_entry.py` makes `get_configs` raise; reuse that fixture).
