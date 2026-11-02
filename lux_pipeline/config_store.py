"""Read, check and write configuration records in the files they came from.

`Config` keeps, for every record, the file it was read from
(``Config.record_origins``: the configuration directory or a sub-config
store). This module edits those files:

    read_record(cfgs, "aat")                      # the file's JSON, not the live dict
    validate_record(cfgs, "aat", data)            # problems; "warning: ..." ones do not block
    diff_record(cfgs, "aat", data)                # what write_record would change, as a diff
    write_record(cfgs, "aat", data)               # -> (path, warnings)
    write_text(cfgs, "aat", text)                 # an editor's text, verbatim, once it validates
    new_record(cfgs, "wd2", data, store="sources")
    delete_record(cfgs, "wd2")

**Never the live dict.** ``cfgs.external["aat"]`` holds resolved absolute
paths, built components and ``all_configs``; the file holds what a person
wrote. Everything here reads and writes the file.

**Minimal diffs.** The configuration files are kept in git
(pipeline-configs) and written by hand in four styles: 4-space, 2-space
and tab indents, and hand-grouped with blank lines. `write_record`
changes only the top-level members whose value changed, in place, in the
file's own indentation; a new member goes after the last one, a removed
one goes with its line. Everything else stays byte for byte. If the
result would not read back as the new data (a file with a key twice, or
written on one line), the record is written whole in the file's
indentation instead.

**Reload after writing.** Nothing here changes `cfgs`: a process that
goes on using the configuration must build it again
(``runtime.reset_configs()``). Writing while a build runs is the caller's
decision (`lux config` refuses without ``--force``): its workers read the
configuration when they start.
"""

import copy
import difflib
import importlib
import json
import os
import re

from lux_pipeline import config as _config
from lux_pipeline import config_schema as schema

KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
# path_types may name these even when base does not set them: Config gives
# them defaults
DEFAULTED_DIRS = ("temp_dir", "indexes_dir", "nt_dir", "distribution_dir", "backup_dir")
_JSON_TYPES = {"str": (str,), "path": (str,), "class": (str,), "url": (str,), "int": (int,),
               "bool": (bool,), "list": (list,), "dict": (dict,)}


class ConfigError(ValueError):
    """A record that cannot be read or written; `problems` lists why."""

    def __init__(self, message, problems=()):
        self.problems = list(problems)
        if self.problems:
            message = message + ": " + "; ".join(self.problems)
        super().__init__(message)


# --- where records are -------------------------------------------------------------------


def _unknown(cfgs, key):
    near = difflib.get_close_matches(key, list(cfgs.record_origins), n=3)
    hint = f" (did you mean {', '.join(near)}?)" if near else ""
    return ConfigError(f"no configuration record {key!r}{hint}")


def record_path(cfgs, key):
    """The file record `key` was read from."""
    origin = cfgs.record_origins.get(key)
    if origin is None:
        raise _unknown(cfgs, key)
    if not origin.get("path"):
        raise ConfigError(f"record {key!r} is not in a file (store {origin.get('store')!r})")
    return origin["path"]


def read_text(cfgs, key):
    with open(record_path(cfgs, key), encoding="utf-8") as fh:
        return fh.read()


def read_record(cfgs, key):
    """Record `key` as its file holds it."""
    text = read_text(cfgs, key)
    try:
        data = json.loads(text)
    except ValueError as e:
        raise ConfigError(f"{record_path(cfgs, key)} is not JSON: {e}") from None
    return data


def all_records(cfgs):
    """``[{"key", "type", "name", "store", "path", "error"}]`` for every
    record, in the order Config read them (a later one with the same key
    replaced an earlier one, and only it is listed)."""
    out = []
    for key, origin in cfgs.record_origins.items():
        row = {"key": key, "type": None, "name": None, "store": origin.get("store"),
               "path": origin.get("path"), "error": None}
        try:
            data = read_record(cfgs, key)
            row["type"], row["name"] = data.get("type"), data.get("name")
        except (OSError, ConfigError) as e:
            row["error"] = str(e)
        out.append(row)
    return out


# --- checking --------------------------------------------------------------------------------


def resolve_class(path):
    """``(class or None, error or None, current path or None)`` for a
    ``*Class`` value, resolved as `config.importObject` resolves it
    (as given, then under ``lux_pipeline.``), without its logging. The
    third item is the path to write instead when `path` uses the layout
    from before the package rename (`importObject` accepts it with a
    warning)."""
    modern = path
    if modern.startswith(_config._LEGACY_PACKAGE_PREFIX):
        modern = "lux_pipeline." + modern[len(_config._LEGACY_PACKAGE_PREFIX):]
    m = _config._LEGACY_SOURCE_CATEGORY.match(modern)
    if m:
        modern = m.group(1) + m.group(2)
    legacy = modern if modern != path else None
    if "." not in modern:
        return None, f"{path!r} is not a module.Class path", legacy
    candidates = [modern] if modern.startswith("lux_pipeline.") else [modern, f"lux_pipeline.{modern}"]
    last = None
    for cand in candidates:
        modname, _, clsname = cand.rpartition(".")
        try:
            module = importlib.import_module(modname)
        except ModuleNotFoundError as e:
            if e.name and (modname == e.name or modname.startswith(e.name + ".")):
                last = f"no module {modname}"
                continue
            return None, f"importing {modname}: {e}", legacy
        except Exception as e:
            return None, f"importing {modname}: {type(e).__name__}: {e}", legacy
        obj = getattr(module, clsname, None)
        if obj is None:
            return None, f"{modname} has no {clsname}", legacy
        return obj, None, legacy
    return None, last or f"cannot import {path}", legacy


def _type_ok(spec, value):
    if value is None:
        return spec.nullable or spec.type == "any"
    if spec.type == "any":
        return True
    types = _JSON_TYPES[spec.type]
    if bool in types:
        return isinstance(value, bool)
    if int in types:
        return isinstance(value, int) and not isinstance(value, bool)
    return isinstance(value, types)


def _same_name(cfgs, key, data):
    """Other records of the same type with the same name (maps and
    MarkLogic stores are looked up by name, not key)."""
    clash = []
    for row in all_records(cfgs):
        if row["key"] != key and row["type"] == data.get("type") and row["name"] == data.get("name"):
            clash.append(row["key"])
    return clash


def validate_record(cfgs, key, data, *, new=False):
    """Problems with writing `data` as record `key`, as sentences; ``[]``
    when there are none. One starting ``warning:`` does not stop a write.

    Checks: a JSON object whose ``type`` Config accepts; the keys its type
    needs, filled in; ``name`` equal to the key for a source (the caches
    are named after it) and unique among its type's records for a map or a
    MarkLogic store (they are looked up by name); every ``*Class`` value
    importable, with a warning for a pre-rename path; the type of every key
    the catalogue knows; ``indexes`` resolving; for base / system, the
    directories ``path_types`` names and a positive ``max_workers``; for a
    new record, a key free in every store and on disk. A key the catalogue
    does not know is never a problem."""
    problems = []
    if not KEY_RE.match(key or ""):
        problems.append(f"{key!r} is not a usable record key (letters, digits, _ . -)")
    if not isinstance(data, dict):
        return problems + [f"a record is a JSON object, not {type(data).__name__}"]
    typ = data.get("type")
    if typ not in schema.RECORD_TYPES:
        return problems + [f"type {typ!r} is not one of {', '.join(schema.RECORD_TYPES)}"]
    keys = schema.keys_for(data)

    for k, spec in keys.items():
        if k == "*" or not spec.required:
            continue
        if k not in data:
            problems.append(f"{k} is missing ({spec.help})")
        elif data[k] in ("", None, {}, []) and k not in ("name", "type"):
            problems.append(f"{k} is empty ({spec.help})")
    if typ in schema.SOURCE_TYPES and data.get("name") != key:
        problems.append(f"name is {data.get('name')!r}: a source's name must be its key {key!r}")
    if typ in ("map", "marklogic", "config") and data.get("name"):
        clash = _same_name(cfgs, key, data)
        if clash:
            problems.append(f"name {data['name']!r} is also the name of {', '.join(clash)}")
    if typ == "default" and data.get("name") not in schema.DEFAULT_KINDS:
        problems.append(f"a default record's name is one of {', '.join(schema.DEFAULT_KINDS)}, "
                        f"not {data.get('name')!r}")

    for k, value in data.items():
        spec = keys.get(k) or keys.get("*")
        if spec is not None and not _type_ok(spec, value):
            problems.append(f"{k} should be {'null or ' if spec.nullable else ''}"
                            f"{spec.type}, not {type(value).__name__} ({value!r})")
        # globals' values are terms (it has a taxonomic rank called Class)
        if typ != "globals" and k.endswith("Class") and isinstance(value, str):
            if not value:
                problems.append(f"{k} is empty (null switches the component off)")
                continue
            _, error, modern = resolve_class(value)
            if error:
                problems.append(f"{k}: {error}")
            elif modern:
                problems.append(f"warning: {k} uses the old path {value}; write {modern}")

    if isinstance(data.get("indexes"), dict):
        try:
            cfgs.resolve_indexes(copy.deepcopy(data))
        except Exception as e:
            problems.append(f"indexes: {type(e).__name__}: {e}")

    if typ in ("base", "system"):
        mw = data.get("max_workers")
        # (a value that is not an integer at all is the type check's)
        if isinstance(mw, int) and not isinstance(mw, bool) and mw < 1:
            problems.append(f"max_workers should be at least 1, not {mw}")
        for pkey, dir_key in (data.get("path_types") or {}).items():
            if dir_key not in data and dir_key not in DEFAULTED_DIRS:
                problems.append(f"path_types maps {pkey} to {dir_key}, which this record "
                                f"does not set")

    if new:
        if key in cfgs.record_origins:
            origin = cfgs.record_origins[key]
            where = f"store {origin['store']}" if origin.get("store") else "the configuration directory"
            problems.append(f"record {key!r} already exists in {where}")
    return problems


def file_problems(text):
    """Problems a record's text has that its data cannot show: a key
    written twice at the top level (JSON keeps the last value, so the
    first is silently ignored). Warnings."""
    try:
        members, _ = _members(text)
    except ValueError as e:
        return [f"not a JSON object: {e}"]
    seen, out = set(), []
    for key, *_ in members:
        if key in seen:
            out.append(f"warning: {key} appears twice in the file; only the last value counts")
        seen.add(key)
    return out


def check_record(cfgs, key):
    """Every problem with record `key` as it is on disk: its text
    (`file_problems`) and its data (`validate_record`)."""
    try:
        text = read_text(cfgs, key)
    except OSError as e:
        return [f"cannot read {record_path(cfgs, key)}: {e}"]
    problems = file_problems(text)
    try:
        data = json.loads(text)
    except ValueError as e:
        return problems + [f"not JSON: {e}"]
    return problems + validate_record(cfgs, key, data)


def errors_only(problems):
    return [p for p in problems if not p.startswith("warning:")]


def warnings_only(problems):
    return [p for p in problems if p.startswith("warning:")]


# --- the file's style and in-place edits --------------------------------------------------


_WS = " \t\r\n"


def _skip(text, i):
    while i < len(text) and text[i] in _WS:
        i += 1
    return i


def _members(text):
    """``([(key, key_start, value_start, value_end)], closing_brace)`` of
    the top-level object in `text`; ValueError if it is not one."""
    dec = json.JSONDecoder()
    i = _skip(text, 0)
    if i >= len(text) or text[i] != "{":
        raise ValueError("not a JSON object")
    i = _skip(text, i + 1)
    out = []
    if text[i] == "}":
        return out, i
    while True:
        key, kend = dec.raw_decode(text, i)
        if not isinstance(key, str):
            raise ValueError("a key is not a string")
        j = _skip(text, kend)
        if text[j] != ":":
            raise ValueError("expected ':'")
        vstart = _skip(text, j + 1)
        _, vend = dec.raw_decode(text, vstart)
        out.append((key, i, vstart, vend))
        k = _skip(text, vend)
        if text[k] == ",":
            i = _skip(text, k + 1)
        elif text[k] == "}":
            return out, k
        else:
            raise ValueError("expected ',' or '}'")


def _line_start(text, pos):
    """Start of `pos`'s line when only whitespace comes before it there,
    else `pos`."""
    start = text.rfind("\n", 0, pos) + 1
    return start if not text[start:pos].strip() else pos


def file_style(text):
    """``{"indent": str, "newline": bool}`` of a record's text: the indent
    of its top-level members (4 spaces when it cannot be told) and whether
    it ends with a newline."""
    indent = "    "
    try:
        members, _ = _members(text)
    except ValueError:
        members = []
    if members:
        start = _line_start(text, members[0][1])
        if start != members[0][1]:
            indent = text[start:members[0][1]] or indent
    return {"indent": indent, "newline": text.endswith("\n")}


def dumps(data, style=None):
    """`data` as a whole file in `style` (default 4 spaces, trailing
    newline)."""
    style = style or {"indent": "    ", "newline": True}
    indent = style["indent"]
    text = json.dumps(data, ensure_ascii=False,
                      indent=indent if indent.strip(" ") else len(indent))
    return text + ("\n" if style["newline"] else "")


def _value_text(value, indent):
    """`value` as the text of a top-level member's value: nested lines
    one `indent` deeper than the member."""
    text = json.dumps(value, ensure_ascii=False, indent=indent if indent.strip(" ") else len(indent))
    return text.replace("\n", "\n" + indent)


def _canon(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def edit_text(text, new):
    """The file text `text` changed to hold `new` (a dict), touching only
    the top-level members whose value changed, were added or were removed.
    Falls back to the whole record in the file's style when the text
    cannot be edited in place (or the edit would not read back as `new`)."""
    style = file_style(text)
    try:
        if _canon(json.loads(text)) == _canon(new):
            return text            # nothing changed: never touch the file
    except ValueError:
        pass
    try:
        members, close = _members(text)
        keys = [m[0] for m in members]
        if len(set(keys)) != len(keys):
            raise ValueError("a key appears twice")
        old = json.loads(text)
        indent = style["indent"]
        sep = ": "
        if members:
            key, kstart, vstart, _ = members[0]
            kend = json.JSONDecoder().raw_decode(text, kstart)[1]
            sep = text[kend:vstart] or ": "
        edits = []         # (start, end, replacement)
        for n, (key, kstart, vstart, vend) in enumerate(members):
            if key not in new:
                if n + 1 < len(members):
                    # this member's own line, up to and with its comma; a blank
                    # line after it (a hand-made group break) stays
                    start = _line_start(text, kstart)
                    comma = _skip(text, vend)
                    end = comma + 1
                    if start != kstart:
                        nl = text.find("\n", end)
                        if nl != -1 and not text[end:nl].strip():
                            end = nl + 1
                    else:
                        end = _skip(text, end)
                    edits.append((start, end, ""))
                elif n > 0:
                    edits.append((members[n - 1][3], vend, ""))
                else:
                    edits.append((_line_start(text, kstart), vend, ""))
            elif _canon(new[key]) != _canon(old[key]):
                edits.append((vstart, vend, _value_text(new[key], indent)))
        added = [k for k in new if k not in old]
        if added:
            tail = "".join(f",\n{indent}{json.dumps(k, ensure_ascii=False)}{sep}"
                           f"{_value_text(new[k], indent)}" for k in added)
            survivors = [m for m in members if m[0] in new]
            if survivors:
                edits.append((survivors[-1][3], survivors[-1][3], tail))
            else:
                start = text.index("{") + 1
                edits.append((start, start, "\n" + tail[2:] + "\n"))
        out = text
        for start, end, rep in sorted(edits, key=lambda e: e[0], reverse=True):
            out = out[:start] + rep + out[end:]
        if _canon(json.loads(out)) != _canon(new):
            raise ValueError("the edit does not read back")
        return out
    except ValueError:
        return dumps(new, style)


def _write_atomic(path, text):
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
    os.replace(tmp, path)


# --- writing --------------------------------------------------------------------------------


def _check(cfgs, key, data, new=False):
    problems = validate_record(cfgs, key, data, new=new)
    errors = errors_only(problems)
    if errors:
        raise ConfigError(f"not writing {key}", errors)
    return warnings_only(problems)


def diff_record(cfgs, key, data):
    """What `write_record` would change, as a unified diff (empty when
    nothing would)."""
    path = record_path(cfgs, key)
    text = read_text(cfgs, key)
    return "".join(difflib.unified_diff(text.splitlines(True), edit_text(text, data).splitlines(True),
                                        fromfile=path, tofile=path))


def write_record(cfgs, key, data):
    """Write `data` as record `key`, changing only the members that
    changed. ConfigError listing the problems if it does not validate.
    Returns ``(path, warnings)``."""
    warnings = _check(cfgs, key, data)
    path = record_path(cfgs, key)
    text = read_text(cfgs, key)
    new_text = edit_text(text, data)
    if new_text != text:
        _write_atomic(path, new_text)
    return path, warnings


def write_text(cfgs, key, text):
    """Write `text` verbatim as record `key` (an editor's buffer: its
    layout is the person's), once it parses and validates. Returns
    ``(path, warnings)``."""
    try:
        data = json.loads(text)
    except ValueError as e:
        raise ConfigError(f"not writing {key}: not JSON: {e}") from None
    warnings = _check(cfgs, key, data)
    path = record_path(cfgs, key)
    _write_atomic(path, text if text.endswith("\n") else text + "\n")
    return path, warnings


def new_record_path(cfgs, key, store=None):
    """Where a new record `key` goes: the configuration directory, or the
    directory of sub-config store `store`."""
    if store is None:
        cache = getattr(cfgs, "configcache", None)
    else:
        if store not in cfgs.subconfig_stores:
            raise ConfigError(f"no sub-config store {store!r} "
                              f"(stores: {', '.join(cfgs.subconfig_stores) or 'none'})")
        cache = cfgs.instantiate_config(store)["datacache"]
    path_of = getattr(cache, "_path", None)
    if path_of is None:
        raise ConfigError(f"{'store ' + store if store else 'the configuration'} is not a "
                          f"directory of files")
    return os.path.abspath(path_of(key))


def _sibling_style(directory):
    """The style of the first record in `directory`, so a new one matches."""
    try:
        names = sorted(n for n in os.listdir(directory) if n.endswith(".json"))
    except OSError:
        names = []
    for n in names:
        try:
            with open(os.path.join(directory, n), encoding="utf-8") as fh:
                return file_style(fh.read())
        except OSError:
            continue
    return None


def new_record(cfgs, key, data, store=None):
    """Create record `key` in the configuration directory or sub-config
    store `store`, indented as the records already there. Returns
    ``(path, warnings)``."""
    warnings = _check(cfgs, key, data, new=True)
    path = new_record_path(cfgs, key, store)
    if os.path.exists(path):
        raise ConfigError(f"not writing {key}: {path} exists")
    _write_atomic(path, dumps(data, _sibling_style(os.path.dirname(path))))
    return path, warnings


def delete_record(cfgs, key):
    """Delete record `key`'s file. Returns its path."""
    path = record_path(cfgs, key)
    os.remove(path)
    return path


# --- editing values ---------------------------------------------------------------------------


def parse_value(text):
    """A value typed at a prompt: JSON when it parses (``12``, ``true``,
    ``null``, ``["a"]``, ``"12"``), else the text as a string."""
    try:
        return json.loads(text)
    except ValueError:
        return text


def set_key(data, dotted, value):
    """Set ``a.b.c`` in `data` (dicts made on the way). Returns `data`."""
    parts = dotted.split(".")
    node = data
    for p in parts[:-1]:
        nxt = node.get(p)
        if nxt is None:
            nxt = node[p] = {}
        elif not isinstance(nxt, dict):
            raise ConfigError(f"{p} in {dotted} is a {type(nxt).__name__}, not an object")
        node = nxt
    node[parts[-1]] = value
    return data


def unset_key(data, dotted):
    """Remove ``a.b.c`` from `data`; ConfigError if it is not there."""
    parts = dotted.split(".")
    node = data
    for p in parts[:-1]:
        node = node.get(p) if isinstance(node, dict) else None
        if not isinstance(node, dict):
            raise ConfigError(f"no {dotted} in the record")
    if parts[-1] not in node:
        raise ConfigError(f"no {dotted} in the record")
    del node[parts[-1]]
    return data
