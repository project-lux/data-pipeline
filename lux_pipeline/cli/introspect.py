"""Every ``lux`` command and its options as data, read from the real parsers.

A form for a command (the web UI's command launcher, item W10) is built
from `command_spec` and turned back into a command line by
`argv_from_values`, so the options are never copied by hand and cannot
drift from the code: the parsers are built exactly as ``lux <command>
--help`` builds them (`entry.command_help`), the global options from
`entry.build_parser` and the command's own from its handler's ``add_args``,
without a configuration.

    >>> spec = command_spec("reconcile")
    >>> argv_from_values(spec, {"source": "ycba", "recid": "obj/1", "no_refs": True})
    ['reconcile', '--source', 'ycba', '--recid', 'obj/1', '--no-refs']

Each command also says which group it belongs to (`GROUPS`), whether it
can be run from a form (`NOT_RUNNABLE`), and whether a run of it needs a
typed confirmation (`confirm_reason`): the commands that delete, replace
or rewrite something a build depends on, or start a whole build from
scratch. The phases that produce a build's data (reconcile, merge,
export, sheets, portal) do not ask; the steps that rewrite what they
leave behind do.

Reads ``ArgumentParser._actions``, which is private but has kept its shape
across every Python the package supports.
"""

import argparse
import json
import logging

from lux_pipeline.cli import _handler, entry
from lux_pipeline.cli.help import list_commands

logger = logging.getLogger("lux_pipeline")

GROUPS = {
    "phases": ("download", "harvest", "load", "load-index", "reconcile", "merge", "export",
               "portal", "sheets", "build"),
    "steps": ("identify", "metatypes", "new-token", "write-refs", "build-differents", "refetch",
              "validate"),
    "stores": ("counts", "clear", "backup", "restore", "pg-maintain"),
    "info": ("list", "timing-report", "pg-tune", "testinstall", "guess-dump-type", "viz", "help",
             "watch", "interactive", "initialize", "serve", "config", "show"),
}

# Interactive, long-lived, or about the terminal: described, never started
# from a form
# (config: the web UI has its own configuration pages, and edit opens an editor)
NOT_RUNNABLE = frozenset({"help", "watch", "interactive", "initialize", "serve", "config"})

# Why a run needs a typed confirmation. A command named here always asks,
# except as `_unless` says.
CONFIRM = {
    "clear": "deletes the selected caches, maps, indexes or reference queues",
    "restore": "replaces stores with the contents of a backup",
    "new-token": "starts a new build generation in the identity map; a new token in the "
                 "middle of a build splits it in two",
    "identify": "rewrites the identity map from the assertions reconcile logged",
    "metatypes": "replaces <data_dir>/metatypes.json with the reconcile slices' metatypes",
    "write-refs": "replaces reference_uris.txt and empties the done-refs queue",
    "pg-maintain": "rewrites, re-indexes or vacuums Postgres tables, locking them while it runs",
    "build": "first clears every source's computed caches and the reference queues, and "
             "mints a new update token",
    "refetch": "fetches records again over the network and replaces the cached copies",
    "load": "writes records from the dump files over the datacache's copies",
    "load-index": "rebuilds the reconcile indexes",
    "harvest": "writes harvested records over the datacache's copies",
    "download": "replaces dump files under dumps_dir",
}


def _unless(name, values):
    """True when this run of a `CONFIRM` command does not need confirming."""
    if name == "build":
        return not _truthy(values.get("clear"))
    if name == "pg-maintain":
        changes = ("vacuum", "rewrite", "drop_time_indexes", "idmap_indexes")
        return not any(_truthy(values.get(k)) for k in changes)
    return False


# --- reading the parsers ------------------------------------------------------------


def _group(name):
    for group, names in GROUPS.items():
        if name in names:
            return group
    logger.debug(f"command {name} is in no group; listed under info")
    return "info"


def _jsonable(value):
    """`value` as JSON can carry it: primitives, lists and dicts as they
    are, anything else as its string."""
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        if isinstance(value, (list, tuple)):
            return [_jsonable(v) for v in value]
        return str(value)


def _kind(action):
    if isinstance(action, (argparse._StoreTrueAction, argparse._StoreFalseAction)):
        return "flag"
    if isinstance(action, argparse._CountAction):
        return "count"
    t = action.type
    if t is int:
        return "int"
    if t is float:
        return "float"
    if getattr(t, "__name__", "") == "existing_path":
        return "path"
    return "str"


def _option(action, ap, is_global):
    flags = list(action.option_strings)
    try:
        help_text = ap._get_formatter()._expand_help(action) if action.help else None
    except (KeyError, TypeError, ValueError):
        help_text = action.help
    default = None if action.default is argparse.SUPPRESS else action.default
    spec = {
        "dest": action.dest,
        "flags": flags,
        "positional": not flags,
        "kind": _kind(action),
        "type_name": getattr(action.type, "__name__", None) if action.type else None,
        "help": help_text,
        "default": _jsonable(default),
        "choices": list(action.choices) if action.choices is not None else None,
        "required": bool(action.required),
        "nargs": action.nargs,
        "const": _jsonable(action.const),
        "repeat": isinstance(action, argparse._AppendAction),
        "global": is_global,
    }
    if spec["kind"] == "flag":
        spec["flag_value"] = isinstance(action, argparse._StoreTrueAction)
    return spec


def _actions(ap):
    """The parser's actions a form shows: not ``--help``, not the global
    parser's ``command`` positional."""
    for a in ap._actions:
        if isinstance(a, argparse._HelpAction) or a.dest in ("help", "command"):
            continue
        yield a


def command_parser(name):
    """``(module, ArgumentParser)`` with command `name`'s own options, built
    as ``lux <name> --help`` builds them; ``ValueError`` for an unknown
    command."""
    mod, err = entry.resolve_command(name)
    if mod is None:
        raise ValueError(err)
    ap = argparse.ArgumentParser(prog=f"lux {name}", add_help=False)
    mod.CommandHandler(None).add_args(ap)
    return mod, ap


def command_spec(name):
    """Command `name` as data::

        {"name", "module", "doc", "summary", "group", "runs_workers",
         "needs_configs", "runnable", "confirms", "options": [...]}

    `options` holds the global options, then the command's own, in parser
    order; each is ``{"dest", "flags", "positional", "kind" (flag, int,
    float, path, str, count), "type_name", "help", "default", "choices",
    "required", "nargs", "const", "repeat", "global"}`` and, for a flag,
    ``"flag_value"`` (the value giving the flag stores). A global option
    whose destination the command reuses is left out: the command's own
    wins (``lux watch``'s positional prefix is the global ``--prefix``).
    `confirms` is the reason a run needs confirming, or None when no run
    of it does (`confirm_reason` decides for given values)."""
    mod, ap = command_parser(name)
    gap = entry.build_parser()
    own = [_option(a, ap, False) for a in _actions(ap)]
    taken = {o["dest"] for o in own}
    glob = [_option(a, gap, True) for a in _actions(gap) if a.dest not in taken]
    handler = mod.CommandHandler
    doc = (mod.__doc__ or "").strip()
    return {
        "name": name,
        "module": mod.__name__,
        "doc": doc,
        "summary": doc.split("\n", 1)[0],
        "group": _group(name),
        "runs_workers": issubclass(handler, _handler.CommandHandler),
        "needs_configs": bool(getattr(handler, "needs_configs", True)),
        "runnable": name not in NOT_RUNNABLE,
        "confirms": CONFIRM.get(name),
        "options": glob + own,
    }


def all_command_specs():
    """`command_spec` for every command `lux help` lists, in its order.
    Needs no configuration."""
    return [command_spec(name) for name, _ in list_commands()]


# --- values to a command line ----------------------------------------------------------

_TRUE = {"1", "true", "yes", "on", "y", "t"}
_FALSE = {"0", "false", "no", "off", "n", "f", ""}


def _truthy(value):
    if isinstance(value, str):
        return value.strip().lower() in _TRUE
    return bool(value)


def _coerce_one(option, value):
    kind = option["kind"]
    try:
        if kind == "int" and not isinstance(value, bool):
            return int(value)
        if kind == "float" and not isinstance(value, bool):
            return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{option_name(option)}: {value!r} is not a{'n' if kind == 'int' else ''} "
                         f"{'integer' if kind == 'int' else 'number'}") from None
    return value


def coerce(option, value):
    """`value` (from a form: strings, ``"on"`` for a ticked box, lists for
    repeated or two-valued options) as the option takes it; ``ValueError``
    naming the option when it cannot be."""
    if option["kind"] == "flag":
        if isinstance(value, str):
            low = value.strip().lower()
            if low not in _TRUE | _FALSE:
                raise ValueError(f"{option_name(option)}: {value!r} is not true or false")
            return low in _TRUE
        return bool(value)
    if option["nargs"] == "?" and option["const"] is not None and value is True:
        return True       # the bare flag: argparse stores the const
    if option["repeat"] or option["nargs"] in ("*", "+") or isinstance(option["nargs"], int):
        items = value if isinstance(value, (list, tuple)) else [value]
        items = [_coerce_one(option, v) for v in items if v not in (None, "")]
        if isinstance(option["nargs"], int) and items and len(items) != option["nargs"]:
            raise ValueError(f"{option_name(option)} takes {option['nargs']} values, "
                             f"not {len(items)}")
        return items
    return _coerce_one(option, value)


def option_name(option):
    """The spelling a command line uses: the first ``--long`` flag, else
    the first flag, else (a positional) the destination."""
    for f in option["flags"]:
        if f.startswith("--"):
            return f
    return option["flags"][0] if option["flags"] else option["dest"]


def _is_unset(option, value):
    if value is None or value == "" or value == [] or value == ():
        return True
    if option["kind"] == "flag":
        return value != option["flag_value"]
    return value == option["default"]


def argv_from_values(spec, values):
    """The command line, without ``lux``, that sets `values` (``{dest:
    value}``) for the command `spec` describes.

    A value that is None, empty, or the option's default is left out, as
    is a flag not set to its ``flag_value``. Positionals come straight
    after the command (an option with an optional value would otherwise
    swallow them), then the options in spec order: the global ones first.
    ``ValueError`` for a destination the command does not have, a value
    the option cannot take, a value outside its choices, a missing
    required option, or ``my_worker``: one slice of a run is for a shell
    loop, not a form."""
    by_dest = {o["dest"]: o for o in spec["options"]}
    for dest in values:
        if dest not in by_dest:
            raise ValueError(f"lux {spec['name']} has no option {dest!r}")
    positionals, options = [], []
    for o in spec["options"]:
        raw = values.get(o["dest"])
        value = coerce(o, raw) if raw is not None else None
        if _is_unset(o, value):
            if o["required"]:
                raise ValueError(f"lux {spec['name']} needs {option_name(o)}")
            continue
        if o["dest"] == "my_worker":
            raise ValueError("--my-worker runs one slice of a larger run; run the whole command")
        items = value if isinstance(value, list) else [value]
        if o["choices"] is not None:
            for v in items:
                if v is not True and v not in o["choices"]:
                    raise ValueError(f"{option_name(o)}: {v!r} is not one of "
                                     f"{', '.join(map(str, o['choices']))}")
        if o["positional"]:
            positionals += [str(v) for v in items]
        elif o["kind"] == "flag":
            options.append(option_name(o))
        elif value is True:                      # an optional value given bare
            options.append(option_name(o))
        elif o["repeat"]:
            for v in items:
                options += [option_name(o), str(v)]
        elif isinstance(value, list):
            options += [option_name(o), *(str(v) for v in items)]
        else:
            options += [option_name(o), str(value)]
    return [spec["name"], *positionals, *options]


def confirm_reason(spec, values):
    """Why this run of the command needs a typed confirmation, or None."""
    reason = spec.get("confirms")
    if reason is None or _unless(spec["name"], values):
        return None
    if spec["name"] == "load" and _truthy(values.get("clear")):
        return "empties the datacache, then " + reason.replace("writes records", "loads records")
    return reason


def confirm_needed(spec, values):
    """Whether this run of the command needs a typed confirmation."""
    return confirm_reason(spec, values) is not None
