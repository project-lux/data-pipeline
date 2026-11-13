"""Configuration records, read, checked and written in the stores they came from.

`Config` reads its records from caches: the configuration directory
(``Config.configcache``) and then each sub-config store (a ``"config"``
record), a later store's record replacing an earlier one with the same key;
``Config.record_origins`` remembers where each came from. Each of those
caches is a `ConfigCache`: an `FsCache`, which already reads, checks and
writes JSON records as files (`FsCache.read`, `validate`, `check`, `diff`,
`write`, `write_text`, `create`, `delete`), with the configuration's own
checks added. The functions here find the cache a record is in:

    from lux_pipeline import config_store as store
    cache = store.find(cfgs, "aat")           # the store the record Config uses is in
    cache.read("aat")                         # the file's JSON, not the live dict
    cache.validate("aat", data)               # problems; "warning: ..." ones do not block
    cache.diff("aat", data)                   # what write() would change, as a diff
    cache.write("aat", data)                  # -> (path, warnings)
    cache.write_text("aat", text)             # an editor's text, once it parses and validates
    store.store_cache(cfgs, "sources").create("wd2", data)
    cache.delete("aat")
    store.locations(cfgs, "aat")              # every store holding "aat", the winner last
    store.records(cfgs)                       # every record: key, type, name, store, file

**Never the live dict.** ``cfgs.external["aat"]`` holds resolved absolute
paths, built components and ``all_configs``; the file holds what a person
wrote. Everything here reads and writes the file.

**Normalized files.** A `ConfigCache` writes ``json_format`` ``normalized``
(2-space indent, "/" and non-ASCII as written) unless its store record
says otherwise, so a record written here is laid out like every other and
a diff in git shows what changed. A hand-formatted file is reformatted the
first time it is written.

**Checked.** Each `ConfigCache` checks against the JSON Schema generated
from `lux_pipeline.config_schema` (a store record may add its own with
``json_schema``), worded from the catalogue (`schema_problem`), then what a
schema cannot say: classes that import, a source named as its key, map
names unique, indexes that resolve, a source's ``matches`` ending where its
namespace does.

**Reload after writing.** Nothing here changes `cfgs`' records: a process
that goes on using the configuration must build it again
(``runtime.reset_configs()``). Writing while a build runs is the caller's
decision (`lux config` refuses without ``--force``): its workers read the
configuration when they start.
"""

import copy
import difflib
import json
import re

from lux_pipeline import config as _config
from lux_pipeline import config_schema as schema
from lux_pipeline.storage.cache.filesystem import (FsCache, RecordError, errors_only,  # noqa: F401
                                                   text_problems, warnings_only)

KEY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
# path_types may name these even when base does not set them: Config gives
# them defaults
DEFAULTED_DIRS = ("temp_dir", "indexes_dir", "nt_dir", "distribution_dir", "backup_dir")


class ConfigError(RecordError):
    """A configuration record that cannot be read or written; `problems`
    lists why."""


# ``(class or None, error or None, modern path or None)`` for a ``*Class``
# value, resolved exactly as Config resolves it (`config.importObject`),
# without logging or raising
resolve_class = _config.resolve_object


def matches_problems(data):
    """A source's first ``matches`` entry found in a URI of its own
    namespace has to end where the namespace does.

    `Config.split_uri` takes the identifier as what follows the entry and
    `Config.canonicalize` puts the namespace back in front of it, so an
    entry that stops short (``data.example.org/source-records`` for the
    namespace ``https://data.example.org/source-records/``) keeps the rest
    of the namespace in the identifier: every reference becomes
    ``.../source-records//x``, which no record is registered under, and
    the identifier ``/x`` is no key in the source's caches. An error,
    unless the source's mapper has its own ``fix_identifier`` (which may
    put it right): then a warning."""
    ns, ms = data.get("namespace"), data.get("matches")
    if isinstance(ms, str):
        ms = [ms]
    if not isinstance(ns, str) or not ns or not isinstance(ms, list):
        return []
    uri = ns + "ID"
    # split_uri: the first entry found in the URI, the identifier after its last occurrence
    m = next((m for m in ms if isinstance(m, str) and m and m in uri), None)
    if m is None:
        return []
    ident = uri.rsplit(m, 1)[1]
    if ident == "ID":
        return []
    rest = ident[: -len("ID")]
    problem = (f"matches entry {m!r} stops short of the namespace {ns!r}: "
               f"{uri} splits to the identifier {ident!r} and canonicalizes to "
               f"{ns + ident}, so references never reach the records; write {m + rest!r}")
    if _overrides_fix_identifier(data.get("mapperClass")):
        return [f"warning: {problem} (unless {data['mapperClass']}.fix_identifier removes "
                f"{rest!r})"]
    return [problem]


def _overrides_fix_identifier(path):
    if not isinstance(path, str) or not path:
        return False
    cls, error, _ = resolve_class(path)
    if error or cls is None:
        return False
    from lux_pipeline.process.base.mapper import Mapper
    return getattr(cls, "fix_identifier", Mapper.fix_identifier) is not Mapper.fix_identifier


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


class ConfigCache(FsCache):
    """A directory of configuration records: the configuration directory
    (``Config.configcache``) or a sub-config store. An `FsCache` (which
    reads, checks and writes JSON records as files) that writes normalized
    JSON and checks against the catalogue's JSON Schema by default
    (``json_format`` / ``json_schema`` in its configuration override
    either), words schema errors from the catalogue, and adds the checks
    that need the configuration: classes that import, a source named as its
    key, unique map names, indexes that resolve, a new key free in every
    store, a record that shadows one in an earlier store.

    ``all_configs`` in its configuration is the `Config` it belongs to;
    ``store_key`` is the sub-config store's key (None: the configuration
    directory)."""

    error_class = ConfigError

    def __init__(self, config):
        config = dict(config)
        config.setdefault("json_format", "normalized")
        if "json_schema" not in config:
            config["json_schema"] = schema.json_schema()
        super().__init__(config)
        self.configs = config.get("all_configs")
        self.store_key = config.get("store_key")

    @property
    def where(self):
        return store_label(self.store_key)

    def get(self, key, _key_type=None, raw=False):
        """`FsCache.get`, but a file that is not JSON is a `ConfigError`
        naming the file and where the JSON breaks: FsCache would hand it
        back as ``{"data": <text>}``, which Config can only report as a
        record with no type."""
        text = self.read_text(key)
        if text is not None:
            broken = errors_only(text_problems(text))
            if broken:
                raise ConfigError(f"configuration record {key} ({self.path(key)}) is {broken[0]}")
        return super().get(key, _key_type, raw)

    def schema_validators(self):
        """The catalogue's schema always; a store that names its own adds it."""
        own = super().schema_validators()
        return [catalogue_validator()] + [v for v in own if v.schema is not schema.json_schema()]

    def describe_error(self, error, data):
        return schema_problem(error, data)

    def validate(self, key, data, *, new=False):
        """Problems with writing `data` as record `key` here (a new record
        when `new`), as sentences; ``[]`` when there are none. One starting
        ``warning:`` does not stop a write.

        The schemas (`schema_validators`) check the record's type, the keys
        its type needs, filled in, and the type of every key the catalogue
        knows. Then:
        ``name`` equal to the key for a source (the caches are named after
        it) and unique among its type's records for a map or a MarkLogic
        store (they are looked up by name); every ``*Class`` value
        importable, with a warning for a pre-rename path; ``indexes``
        resolving; a source's ``matches`` ending where its namespace
        does (`matches_problems`); for base / system, the directories ``path_types`` names
        and a positive ``max_workers``; for a new record, a key free in
        every store. A key the catalogue does not know is never a
        problem."""
        problems = []
        if not KEY_RE.match(key or ""):
            problems.append(f"{key!r} is not a usable record key (letters, digits, _ . -)")
        if not isinstance(data, dict):
            return problems + [f"a record is a JSON object, not {type(data).__name__}"]
        typ = data.get("type")
        if typ not in schema.RECORD_TYPES:
            return problems + [f"type {typ!r} is not one of {', '.join(schema.RECORD_TYPES)}"]

        # the schemas (FsCache.validate: the catalogue's and the store's
        # own); whether a new key is free is decided across every store below
        problems.extend(p for p in super().validate(key, data) if p not in problems)

        if typ in schema.SOURCE_TYPES and data.get("name") != key:
            problems.append(f"name is {data.get('name')!r}: a source's name must be its key {key!r}")
        if typ in schema.SOURCE_TYPES:
            problems.extend(matches_problems(data))
        if typ in (*schema.SOURCE_TYPES, "map"):
            # Config refuses to load a record with one of these (store_name_problem)
            found = []
            if typ != "map" or data.get("name"):
                found.append(_config.store_name_problem(data.get("name")))
            if typ == "map" and "tableName" in data:
                found.append(_config.store_name_problem(data["tableName"], "tableName"))
            problems.extend(p for p in found if p)
        if typ in ("map", "marklogic", "config") and data.get("name") and self.configs is not None:
            clash = [row["key"] for row in records(self.configs)
                     if row["key"] != key and row["type"] == typ and row["name"] == data["name"]]
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

        if isinstance(data.get("indexes"), dict) and self.configs is not None:
            try:
                self.configs.resolve_indexes(copy.deepcopy(data))
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

        if new:
            origins = self.configs.record_origins if self.configs is not None else {}
            if key in origins:
                problems.append(f"record {key!r} already exists in "
                                f"{store_label(origins[key].get('store'))}")
            elif key in self:
                problems.append(f"record {key!r} already exists in {self.where}")
        return problems

    def check(self, key):
        """`FsCache.check`, and a warning when the record replaces one of
        the same key in an earlier store."""
        problems = super().check(key)
        if errors_only(problems) or self.configs is None:
            return problems
        stores = locations(self.configs, key)
        if len(stores) > 1:
            replaced = ", ".join(store_label(s) for s in stores[:-1])
            problems.append(f"warning: {key} in {store_label(stores[-1])} replaces the "
                            f"record of the same key in {replaced}")
        return problems


# --- the stores of a configuration, and finding a record in them -----------------------------


def store_label(store):
    return f"store {store}" if store else "the configuration directory"


def stores(cfgs):
    """None (the configuration directory), then every sub-config store, in
    the order Config reads them: a later one's record replaces an earlier
    one's with the same key."""
    order = list(getattr(cfgs, "subconfig_order", []))
    return [None] + order + [k for k in sorted(cfgs.subconfig_stores) if k not in order]


def store_cache(cfgs, store=None):
    """The `ConfigCache` of sub-config store `store`, or of the
    configuration directory (None). ConfigError when there is no such
    store, or its class is not a ConfigCache (it can be read, not
    written here)."""
    if store is None:
        cache = getattr(cfgs, "configcache", None)
        if cache is None:
            raise ConfigError("the configuration has no store of its own")
    else:
        if store not in cfgs.subconfig_stores:
            raise ConfigError(f"no sub-config store {store!r} "
                              f"(stores: {', '.join(cfgs.subconfig_stores) or 'none'})")
        try:
            cache = cfgs.instantiate_config(store)["datacache"]
        except ValueError as e:
            raise ConfigError(f"sub-config store {store!r}: {e}") from None
    if not isinstance(cache, ConfigCache):
        raise ConfigError(f"{store_label(store)} is a {type(cache).__name__}, not a ConfigCache: "
                          f"its records cannot be checked or written here")
    return cache


def locations(cfgs, key):
    """Every store holding a record `key`, in reading order: the last is
    the one Config uses, any before it are replaced by it."""
    out = []
    for store in stores(cfgs):
        try:
            cache = store_cache(cfgs, store)
        except ConfigError:
            continue
        if key in cache:
            out.append(store)
    return out


def unknown(cfgs, key):
    near = difflib.get_close_matches(key, list(cfgs.record_origins), n=3)
    hint = f" (did you mean {', '.join(near)}?)" if near else ""
    return ConfigError(f"no configuration record {key!r}{hint}")


def find(cfgs, key):
    """The `ConfigCache` holding the record `key` Config uses.
    ConfigError for a key Config did not read."""
    origin = cfgs.record_origins.get(key)
    if origin is None:
        raise unknown(cfgs, key)
    return store_cache(cfgs, origin.get("store"))


def records(cfgs):
    """``[{"key", "type", "name", "store", "path", "error"}]`` for every
    record, in the order Config read them (a later one with the same key
    replaced an earlier one, and only it is listed)."""
    out = []
    for key, origin in cfgs.record_origins.items():
        row = {"key": key, "type": None, "name": None, "store": origin.get("store"),
               "path": origin.get("path"), "error": None}
        try:
            data = find(cfgs, key).read(key)
            row["type"], row["name"] = data.get("type"), data.get("name")
        except (OSError, ConfigError) as e:
            row["error"] = str(e)
        out.append(row)
    return out


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
