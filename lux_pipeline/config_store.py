"""Read, check and write configuration records in the stores they came from.

`Config` reads its records from caches: the configuration directory
(``Config.configcache``) and then each sub-config store (a ``"config"``
record), a later store's record replacing an earlier one with the same key;
``Config.record_origins`` remembers where each came from. A `ConfigStore`
(``cfgs.config_store``) works on those same caches:

    store = cfgs.config_store
    store.read("aat")                         # the stored JSON, not the live dict
    store.validate("aat", data)               # problems; "warning: ..." ones do not block
    store.diff("aat", data)                   # what write() would change, as a diff
    store.write("aat", data)                  # -> (path, warnings)
    store.write_text("aat", text)             # an editor's text, once it parses and validates
    store.create("wd2", data, store="sources")
    store.delete("wd2")
    store.locations("aat")                    # every store holding "aat", the winner last

**Never the live dict.** ``cfgs.external["aat"]`` holds resolved absolute
paths, built components and ``all_configs``; the store holds what a person
wrote. Everything here reads and writes the store.

**Normalized files.** The caches write records as their ``json_format``
says; configuration stores default to ``normalized`` (2-space indent, "/"
and non-ASCII as written), so a record written here is laid out like every
other and a diff in git shows what changed. A hand-formatted file is
reformatted the first time it is written.

**Checked twice.** Each configuration cache carries a JSON Schema generated
from `lux_pipeline.config_schema` (``json_schema``; a store record may name
its own) and logs what a record written to it breaks. `validate` phrases
those problems for people and adds what a schema cannot say: classes that
import, a source named as its key, map names unique, indexes that resolve.

**Reload after writing.** Nothing here changes `cfgs`' records: a process
that goes on using the configuration must build it again
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


class ConfigError(ValueError):
    """A record that cannot be read or written; `problems` lists why."""

    def __init__(self, message, problems=()):
        self.problems = list(problems)
        if self.problems:
            message = message + ": " + "; ".join(self.problems)
        super().__init__(message)


def errors_only(problems):
    return [p for p in problems if not p.startswith("warning:")]


def warnings_only(problems):
    return [p for p in problems if p.startswith("warning:")]


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


def text_problems(text):
    """Problems a record's text has that its data cannot show: not JSON,
    or a key written twice in one object (JSON keeps the last value, so the
    first is silently ignored; a warning)."""
    twice = []

    def pairs(items):
        seen = set()
        for k, _ in items:
            if k in seen:
                twice.append(k)
            seen.add(k)
        return dict(items)

    try:
        json.loads(text, object_pairs_hook=pairs)
    except ValueError as e:
        return [f"not JSON: {e}"]
    return [f"warning: {k} appears twice in the file; only the last value counts"
            for k in dict.fromkeys(twice)]


def schema_problem(error, data):
    """A jsonschema error from the catalogue's schema as sentences."""
    keys = schema.keys_for(data)
    path = list(error.path)
    if error.validator == "required":
        have = error.instance if isinstance(error.instance, dict) else {}
        return [f"{k} is missing ({keys[k].help})" if k in keys else f"{k} is missing"
                for k in error.validator_value if k not in have]
    if len(path) == 1:
        k = path[0]
        spec = keys.get(k) or keys.get("*")
        if spec is not None and error.validator == "not":
            return [f"{k} is empty ({spec.help})"]
        if spec is not None and error.validator == "type":
            v = error.instance
            return [f"{k} should be {'null or ' if spec.nullable else ''}{spec.type}, "
                    f"not {type(v).__name__} ({v!r})"]
        if k == "name" and data.get("type") == "default" and error.validator == "enum":
            return [f"a default record's name is one of {', '.join(schema.DEFAULT_KINDS)}, "
                    f"not {data.get('name')!r}"]
    return [f"{'.'.join(str(p) for p in path) or 'record'}: {error.message}"]


_catalogue_validator = None


def catalogue_validator():
    """The validator for `config_schema.json_schema`."""
    global _catalogue_validator
    if _catalogue_validator is None:
        from jsonschema import Draft202012Validator
        _catalogue_validator = Draft202012Validator(schema.json_schema())
    return _catalogue_validator


class ConfigStore:
    """The configuration records of `cfgs`, in the caches they came from."""

    def __init__(self, cfgs):
        self.cfgs = cfgs

    # --- the stores, and finding a record in them ----------------------------------------

    def cache(self, store=None):
        """The cache of sub-config store `store`, or of the configuration
        directory (None)."""
        if store is None:
            cache = getattr(self.cfgs, "configcache", None)
            if cache is None:
                raise ConfigError("the configuration has no store of its own")
            return cache
        if store not in self.cfgs.subconfig_stores:
            raise ConfigError(f"no sub-config store {store!r} "
                              f"(stores: {', '.join(self.cfgs.subconfig_stores) or 'none'})")
        try:
            return self.cfgs.instantiate_config(store)["datacache"]
        except ValueError as e:
            raise ConfigError(f"sub-config store {store!r}: {e}") from None

    def stores(self):
        """None (the configuration directory), then every sub-config store,
        in the order Config reads them: a later one's record replaces an
        earlier one's with the same key."""
        order = list(getattr(self.cfgs, "subconfig_order", []))
        return [None] + order + [k for k in sorted(self.cfgs.subconfig_stores) if k not in order]

    @staticmethod
    def where(store):
        return f"store {store}" if store else "the configuration directory"

    def locations(self, key):
        """Every store holding a record `key`, in reading order: the last
        is the one Config uses, any before it are replaced by it."""
        out = []
        for store in self.stores():
            try:
                cache = self.cache(store)
            except ConfigError:
                continue
            if key in cache:
                out.append(store)
        return out

    def unknown(self, key):
        near = difflib.get_close_matches(key, list(self.cfgs.record_origins), n=3)
        hint = f" (did you mean {', '.join(near)}?)" if near else ""
        return ConfigError(f"no configuration record {key!r}{hint}")

    def find(self, key):
        """The store record `key` was read from (None: the configuration
        directory). ConfigError for a key Config did not read."""
        origin = self.cfgs.record_origins.get(key)
        if origin is None:
            raise self.unknown(key)
        return origin.get("store")

    def path(self, key):
        """The file record `key` is kept in."""
        store = self.find(key)
        path_of = getattr(self.cache(store), "path", None)
        if path_of is None:
            raise ConfigError(f"record {key!r} is not in a file ({self.where(store)})")
        return os.path.abspath(path_of(key))

    # --- reading ---------------------------------------------------------------------------

    def text(self, key):
        """Record `key` as stored: its file's text, or for a store that is
        not a directory of files the record as that store would write it."""
        cache = self.cache(self.find(key))
        read_text = getattr(cache, "read_text", None)
        if read_text is not None:
            text = read_text(key)
        else:
            row = cache.get(key)
            text = None if row is None else self._dumps(cache, row["data"])
        if text is None:
            raise ConfigError(f"record {key!r} is no longer in {self.where(self.find(key))}")
        return text

    def read(self, key):
        """Record `key` as stored (a fresh dict, not the live one)."""
        try:
            return json.loads(self.text(key))
        except ValueError as e:
            raise ConfigError(f"record {key!r} is not JSON: {e}") from None

    def records(self):
        """``[{"key", "type", "name", "store", "path", "error"}]`` for every
        record, in the order Config read them (a later one with the same
        key replaced an earlier one, and only it is listed)."""
        out = []
        for key, origin in self.cfgs.record_origins.items():
            row = {"key": key, "type": None, "name": None, "store": origin.get("store"),
                   "path": origin.get("path"), "error": None}
            try:
                data = self.read(key)
                row["type"], row["name"] = data.get("type"), data.get("name")
            except (OSError, ConfigError) as e:
                row["error"] = str(e)
            out.append(row)
        return out

    def dumps(self, data, store=None):
        """`data` as `store` writes it."""
        return self._dumps(self.cache(store), data)

    @staticmethod
    def _dumps(cache, data):
        dumps = getattr(cache, "dumps", None)
        return dumps(data) if dumps else json.dumps(data, indent=2, ensure_ascii=False) + "\n"

    # --- checking --------------------------------------------------------------------------

    def _same_name(self, key, data):
        """Other records of the same type with the same name (maps and
        MarkLogic stores are looked up by name, not key)."""
        return [row["key"] for row in self.records()
                if row["key"] != key and row["type"] == data.get("type")
                and row["name"] == data.get("name")]

    def validate(self, key, data, *, new=False, store=None):
        """Problems with writing `data` as record `key` (a new one in
        `store` when `new`), as sentences; ``[]`` when there are none. One
        starting ``warning:`` does not stop a write.

        The catalogue's JSON Schema checks the record's type, the keys its
        type needs, filled in, and the type of every key it knows; a store
        that names a schema of its own (``json_schema``) adds that. Then: ``name`` equal to the key
        for a source (the caches are named after it) and unique among its
        type's records for a map or a MarkLogic store (they are looked up
        by name); every ``*Class`` value importable, with a warning for a
        pre-rename path; ``indexes`` resolving; for base / system, the
        directories ``path_types`` names and a positive ``max_workers``; for
        a new record, a key free in every store. A key the catalogue does
        not know is never a problem."""
        problems = []
        if not KEY_RE.match(key or ""):
            problems.append(f"{key!r} is not a usable record key (letters, digits, _ . -)")
        if not isinstance(data, dict):
            return problems + [f"a record is a JSON object, not {type(data).__name__}"]
        typ = data.get("type")
        if typ not in schema.RECORD_TYPES:
            return problems + [f"type {typ!r} is not one of {', '.join(schema.RECORD_TYPES)}"]

        # the catalogue's schema always; a store that names a schema of its
        # own adds it
        validators = [catalogue_validator()]
        if not new and key in self.cfgs.record_origins:
            store = self.cfgs.record_origins[key].get("store")
        try:
            cache = self.cache(store)
            own = cache.schema_validator() if hasattr(cache, "schema_validator") else None
        except (ConfigError, ValueError) as e:
            problems.append(f"the store's schema: {e}")
            own = None
        if own is not None and own.schema is not schema.json_schema():
            validators.append(own)
        for validator in validators:
            for error in sorted(validator.iter_errors(data), key=lambda e: [str(p) for p in e.path]):
                problems.extend(p for p in schema_problem(error, data) if p not in problems)

        if typ in schema.SOURCE_TYPES and data.get("name") != key:
            problems.append(f"name is {data.get('name')!r}: a source's name must be its key {key!r}")
        if typ in (*schema.SOURCE_TYPES, "map"):
            # Config refuses to load a record with one of these (store_name_problem)
            found = []
            if typ != "map" or data.get("name"):
                found.append(_config.store_name_problem(data.get("name")))
            if typ == "map" and "tableName" in data:
                found.append(_config.store_name_problem(data["tableName"], "tableName"))
            problems.extend(p for p in found if p)
        if typ in ("map", "marklogic", "config") and data.get("name"):
            clash = self._same_name(key, data)
            if clash:
                problems.append(f"name {data['name']!r} is also the name of {', '.join(clash)}")

        # globals' values are terms (it has a taxonomic rank called Class)
        if typ != "globals":
            for k, value in data.items():
                if not k.endswith("Class") or not isinstance(value, str):
                    continue
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
                self.cfgs.resolve_indexes(copy.deepcopy(data))
            except Exception as e:
                problems.append(f"indexes: {type(e).__name__}: {e}")

        if typ in ("base", "system"):
            mw = data.get("max_workers")
            # (a value that is not an integer at all is the schema's)
            if isinstance(mw, int) and not isinstance(mw, bool) and mw < 1:
                problems.append(f"max_workers should be at least 1, not {mw}")
            for pkey, dir_key in (data.get("path_types") or {}).items():
                if dir_key not in data and dir_key not in DEFAULTED_DIRS:
                    problems.append(f"path_types maps {pkey} to {dir_key}, which this record "
                                    f"does not set")

        if new and key in self.cfgs.record_origins:
            problems.append(f"record {key!r} already exists in "
                            f"{self.where(self.cfgs.record_origins[key].get('store'))}")
        return problems

    def check(self, key):
        """Every problem with record `key` as it is stored: its text
        (`text_problems`), its data (`validate`), and a warning when it
        replaces a record of the same key in an earlier store."""
        try:
            text = self.text(key)
        except OSError as e:
            return [f"cannot read record {key!r}: {e}"]
        problems = text_problems(text)
        if problems and not problems[0].startswith("warning:"):
            return problems
        problems += self.validate(key, json.loads(text))
        stores = self.locations(key)
        if len(stores) > 1:
            replaced = ", ".join(self.where(s) for s in stores[:-1])
            problems.append(f"warning: {key} in {self.where(stores[-1])} replaces the record "
                            f"of the same key in {replaced}")
        return problems

    # --- writing ---------------------------------------------------------------------------

    def _check(self, key, data, new=False, store=None):
        problems = self.validate(key, data, new=new, store=store)
        errors = errors_only(problems)
        if errors:
            raise ConfigError(f"not writing {key}", errors)
        return warnings_only(problems)

    def _location(self, cache, key):
        path_of = getattr(cache, "path", None)
        return os.path.abspath(path_of(key)) if path_of else None

    def diff(self, key, data):
        """What `write` would change, as a unified diff (empty when nothing
        would)."""
        name = self.path(key) if self._location(self.cache(self.find(key)), key) else key
        old = self.text(key)
        new = self.dumps(data, self.find(key))
        return "".join(difflib.unified_diff(old.splitlines(True), new.splitlines(True),
                                            fromfile=name, tofile=name))

    def write(self, key, data):
        """Write `data` as record `key` in the store it came from, as that
        store writes JSON. ConfigError listing the problems if it does not
        validate; nothing is written when the stored text would not change.
        Returns ``(path, warnings)`` (path None for a store that is not a
        directory of files)."""
        warnings = self._check(key, data)
        cache = self.cache(self.find(key))
        if self._dumps(cache, data) != self.text(key):
            cache.set(data, identifier=key)
        return self._location(cache, key), warnings

    def write_text(self, key, text):
        """Write an editor's `text` as record `key`, once it parses and
        validates; stored as the store writes JSON. Returns ``(path,
        warnings)``, a key the text has twice among the warnings."""
        problems = text_problems(text)
        if errors_only(problems):
            raise ConfigError(f"not writing {key}: {problems[0]}")
        path, warnings = self.write(key, json.loads(text))
        return path, problems + warnings

    def create(self, key, data, store=None):
        """Create record `key` in the configuration directory or sub-config
        store `store`. Returns ``(path, warnings)``."""
        warnings = self._check(key, data, new=True, store=store)
        cache = self.cache(store)
        if key in cache:
            where = self._location(cache, key) or self.where(store)
            raise ConfigError(f"not writing {key}: {where} exists")
        cache.set(data, identifier=key)
        return self._location(cache, key), warnings

    def delete(self, key):
        """Delete record `key` from its store. Returns its path."""
        cache = self.cache(self.find(key))
        where = self._location(cache, key)
        cache.delete(key)
        return where


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
