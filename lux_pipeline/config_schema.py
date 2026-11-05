"""The keys each kind of configuration record is known to carry.

A catalogue for people and forms: each key's type, what reads it, and the
default a missing class key falls back to. It describes; it never
forbids. A key not listed here is legal (a source's mapper may read keys
only it knows), and `lux_pipeline.config_store.validate_record` only
checks the type of a key that is listed.

Written from the configurations in use (the build machine's copies and
the pipeline-configs repository) and the code that reads them. A key no
code reads is marked ``unused``; one the code will read once a planned
item lands is marked ``planned``.
"""

from dataclasses import dataclass, field

# What Config.handle_config_record accepts. ("build", refactor_docs/42, is
# not accepted until that item lands.)
RECORD_TYPES = ("base", "system", "caches", "default", "globals", "internal", "external",
                "results", "map", "marklogic", "config")
SOURCE_TYPES = ("internal", "external", "results")
# default records: name -> the kind of store they hold defaults for
DEFAULT_KINDS = {"cache": "caches", "map": "map", "marklogic": "marklogic", "ml": "marklogic"}

# JSON types: str, int, bool, list, dict, any; path and class are strings
# with a meaning; class keys may be null (switches the component off)
TYPES = ("str", "path", "class", "url", "int", "bool", "list", "dict", "any")


@dataclass(frozen=True)
class Key:
    type: str
    help: str
    nullable: bool = False
    required: bool = False
    default: object = None      # a value, or {record type: value}
    unused: bool = False
    planned: bool = False
    roles: tuple = field(default=())   # for source keys: the source types that use it; () = all

    def default_for(self, record_type):
        if isinstance(self.default, dict) and set(self.default) <= set(SOURCE_TYPES):
            return self.default.get(record_type)
        return self.default


def _k(type_, help_, **kw):
    if type_ == "class":
        kw.setdefault("nullable", True)
    return Key(type_, help_, **kw)


_NAME = _k("str", "The record's name, which the code looks it up by.", required=True)
_TYPE = _k("str", "What the record is; decides where Config files it.", required=True)

BASE = {
    "type": _TYPE,
    "base_dir": _k("path", "Root of the installation; relative *_dir values resolve under it.",
                   required=True),
    "data_dir": _k("path", "Data files: the idmap update token, metatypes.json, the validator's "
                           "schemas, the same/different CSVs.", required=True),
    "exports_dir": _k("path", "Where lux export writes its JSONL files.", required=True),
    "log_dir": _k("path", "Every run's status, log, record and timing files."),
    "tests_dir": _k("path", "Test data."),
    "temp_dir": _k("path", "Scratch files, and the files the phases hand each other "
                           "(metatypes slices, reference_uris.txt). Default <base_dir>/tmp."),
    "indexes_dir": _k("path", "Reconcile index files; relative index paths resolve here. "
                              "Default data_dir."),
    "dumps_dir": _k("path", "Downloaded dump files; relative dump paths resolve here."),
    "nt_dir": _k("path", "Where N-Triples exports go. Default exports_dir."),
    "distribution_dir": _k("path", "Where lux export --format lmdb builds distribution stores. "
                                   "Default exports_dir."),
    "backup_dir": _k("path", "Where lux backup writes. Default <data_dir>/backups."),
    "max_workers": _k("int", "Workers a parallel phase starts by default. Default: the cores "
                             "less three."),
    "idmap_name": _k("str", "The name of the map record that is the identity map.",
                     required=True),
    "internal_uri": _k("url", "The namespace of LUX's own records; YUIDs are minted under it."),
    "validatorClass": _k("class", "The record validator (reads <data_dir>/schema)."),
    "validate_allow_underscore_properties": _k("bool", "Whether properties starting with _ pass "
                                                       "validation."),
    "allow_network": _k("bool", "Whether fetchers and harvesters may go to the network."),
    "debug_reconciliation": _k("bool", "Record how reconciliation connected records (lux viz)."),
    "max_distance": _k("int", "How many reference hops from a build's records are built too."),
    "reconcile_min_equivs": _k("int", "Read by the reconciler."),
    "ok_record_types": _k("dict", "Linked Art class -> the kind of record it is stored as."),
    "parent_record_types": _k("dict", "Concept subclass -> the class its identity is kept "
                                      "under."),
    "reconcile_record_types": _k("list", "The classes the reconcilers look at."),
    "do_not_reidentify": _k("list", "URI fragments the reidentifier leaves alone."),
    "other_external_matches": _k("list", "URI fragments of known external namespaces with no "
                                         "configured source; kept rather than dropped."),
    "external_uri_rewrites": _k("dict", "URI prefix -> its canonical form, applied first."),
    "path_types": _k("dict", "Source key holding a path -> the *_dir key it is relative to."),
    "activitystreams_cache_source": _k("str", "The source whose datacache holds harvested "
                                              "activity stream pages."),
    "subconfigs": _k("list", "Not read: sub-config stores are 'config' records.", unused=True),
    "web": _k("dict", "lux serve settings (host, port): web UI item W06.", planned=True),
}

CACHES = {
    "name": _k("str", "A label; the caches record is found by its type."),
    "type": _TYPE,
    "host": _k("str", "Postgres host; empty for the local socket."),
    "port": _k("int", "Postgres port."),
    "user": _k("str", "Postgres user; empty for the operating-system user."),
    "password": _k("str", "Postgres password."),
    "dbname": _k("str", "Postgres database; empty for the operating-system user's."),
    "connect": _k("bool", "Whether the caches connect when built."),
    "overwrite": _k("bool", "Whether a write replaces a record already there."),
    "change_types": _k("bool", "Read by the harvester."),
    "refresh_delta": _k("any", "Not read.", nullable=True, unused=True),
    "cursor_size": _k("int", "Rows a server-side cursor fetches at a time."),
    "export_path": _k("path", "Where a loader writes an export dump."),
    "base_dir": _k("path", "For filesystem caches: the directory they live under."),
}

MAP = {
    "name": _NAME,
    "type": _TYPE,
    "storeClass": _k("class", "The store.", required=True,
                     default="storage.idmap.redis.IdMap"),
    "tableName": _k("str", "Postgres maps: the table (default the map's name)."),
    "masterMap": _k("str", "A subsidiary idmap: the map it is a partial view of."),
    "prefix_map_out": _k("dict", "Short prefix -> namespace for the keys the map stores."),
    "base_dir": _k("path", "Filesystem maps: the directory the map's file is in."),
    "host": _k("str", "Redis maps: the host."),
    "port": _k("int", "Redis maps: the port."),
    "db": _k("int", "Redis maps: the database number."),
    "maxItems": _k("int", "Redis maps: read by the Redis map."),
}

MARKLOGIC = {
    "name": _NAME,
    "type": _TYPE,
    "storeClass": _k("class", "The store.", default="storage.marklogic.rest.MarkLogicStore"),
    "rest": _k("url", "The REST endpoint.", required=True),
    "search": _k("url", "The search endpoint.", required=True),
    "sparql": _k("url", "The SPARQL endpoint.", required=True),
    "xquery": _k("url", "The XQuery eval endpoint.", required=True),
    "auth": _k("dict", "How to authenticate: type and credentials.", required=True),
    "database": _k("str", "The content database (default lux-content)."),
    "verify_ssl": _k("bool", "Whether to verify TLS certificates (default true)."),
    "apps": _k("str", "Read by the MarkLogic store."),
}

CONFIG = {
    "name": _NAME,
    "type": _TYPE,
    "base_dir": _k("path", "The directory the store's <name>_<tabletype> directory is in; "
                           "relative to the configuration directory.", required=True),
    "tabletype": _k("str", "The directory suffix (default cache: <name>_cache)."),
    "datacacheClass": _k("class", "The store's class.",
                         default="storage.cache.filesystem.FsCache"),
}

GLOBALS = {
    "type": _TYPE,
    "*": _k("str", "A name the code uses -> an AAT id (3...), a Wikidata Q-id, or a URI; looked "
                   "up in the idmap when the configuration is built."),
}

SOURCE = {
    "name": _NAME,
    "type": _TYPE,
    "namespace": _k("url", "The namespace of the source's URIs; also its idmap prefix.",
                    required=True),
    "matches": _k("list", "URI fragments that identify the source's URIs."),
    "fetch": _k("url", "URI template for fetching one record ({identifier}); the acquirer "
                       "needs a fetcher, which only this key gives."),
    "merge_order": _k("int", "Precedence when records are merged; lower wins."),
    "activitystreams": _k("list", "Activity stream collections the harvester reads."),
    "activitystreams_cache_source": _k("str", "The source whose datacache holds the harvested "
                                              "pages."),
    "wikidata_property": _k("list", "Wikidata properties holding this source's identifiers."),
    "harvesterClass": _k("class", "The harvester.",
                         default={"internal": "process.base.harvester.ASHarvester"}),
    "harvestProtocolClass": _k("class", "The harvester's protocol (how pages are read)."),
    "mapperClass": _k("class", "The mapper (raw data to Linked Art).",
                      default="process.base.mapper.Mapper"),
    "loaderClass": _k("class", "The loader (dump files to the datacache).",
                      default="process.base.loader.Loader"),
    "downloaderClass": _k("class", "The downloader (dump files from the web).",
                          default="process.base.downloader.BaseDownloader"),
    "fetcherClass": _k("class", "The fetcher (one record from the web), when fetch is set.",
                       default="process.base.fetcher.Fetcher"),
    "acquirerClass": _k("class", "The acquirer (a record from the caches, fetching and "
                                 "mapping as needed).",
                        default="process.base.acquirer.Acquirer"),
    "reconcilerClass": _k("class", "The source's reconciler."),
    "indexLoaderClass": _k("class", "Builds the source's reconcile indexes."),
    "testerClass": _k("class", "A tester."),
    "datacacheClass": _k("class", "The raw-record cache.",
                         default="storage.cache.postgres.DataCache"),
    "recordcacheClass": _k("class", "The mapped-record cache.",
                           default={"internal": "storage.cache.postgres.InternalRecordCache",
                                    "external": "storage.cache.postgres.ExternalRecordCache",
                                    "results": "storage.cache.postgres.InternalRecordCache"}),
    "recordcacheReconciledClass": _k("class", "The reconciled-record cache (external sources).",
                                     default={"external": "storage.cache.postgres."
                                                          "ExternalReconciledRecordCache"},
                                     roles=("external",)),
    "recordcache2Class": _k("class", "The rewritten-record cache.",
                            default="storage.cache.postgres.RecordCache"),
    "indexes": _k("dict", "Named reconcile indexes: name -> {path, indexClass, mapExponent}."),
    "reconcileDbPath": _k("path", "The name index (legacy form of indexes.reconcile_label)."),
    "inverseEquivDbPath": _k("path", "The identifier index (legacy form of "
                                     "indexes.reconcile_id)."),
    "differentDbPath": _k("path", "The different-from index (merged)."),
    "mapSizeExponent": _k("int", "LMDB map size of the source's indexes, as a power of two "
                                 "(default 30)."),
    "cycleBreakPath": _k("path", "Broader-term cycles to break (relative to data_dir)."),
    "reconcileDumpPath": _k("path", "The dump the index loader reads."),
    "dumpFilePath": _k("path", "The dump file the loader reads (relative to dumps_dir)."),
    "dumpFileType": _k("str", "The dump's format, e.g. tar.gz/json."),
    "remoteDumpFile": _k("url", "Where the dump is downloaded from."),
    "input_files": _k("dict", "Dump files by load type: [{url, path, type}] (the loader DSL)."),
    "dumps_dir": _k("path", "The source's directory under dumps_dir (lux guess-dump-type)."),
    "totalRecords": _k("int", "How many records the dump holds (for progress)."),
    "downloadMdUrl": _k("url", "Read by the ror and oclc downloaders."),
    "downloadUrlBase": _k("url", "Read by the oclc downloader."),
    "downloadXPath": _k("str", "Read by the ror and oclc downloaders."),
    "externalLinksPath": _k("path", "Read by the lc loader."),
    "headingsPath": _k("path", "Read by the Yale library index loader."),
    "networkmap_name": _k("str", "The map that records failed fetches (default networkmap)."),
    "validate_on_acquire": _k("bool", "Validate each record as it is mapped."),
    "raise_on_validation_error": _k("bool", "With validate_on_acquire: fail on an invalid "
                                            "record."),
    "ignore_sources_for_records": _k("list", "Read by the acquirer."),
    "debug": _k("any", "The acquirer's debug flag."),
    "idmap_name": _k("str", "Results: the identity map the records are keyed by."),
    "trash_non_yale_images": _k("bool", "merged: drop images that are not Yale's."),
    "portal": _k("any", "lux portal tags the merged records this source's records lead to: "
                        "true tags them with the source's name, a string with that tag "
                        "(e.g. \"ypm\" for a renamed source).", roles=("internal",)),
}

KEYS = {"base": BASE, "system": BASE, "caches": CACHES, "map": MAP, "marklogic": MARKLOGIC,
        "config": CONFIG, "globals": GLOBALS, "internal": SOURCE, "external": SOURCE,
        "results": SOURCE}


def keys_for(record):
    """The catalogue for a record (a dict) or a record type: ``{key: Key}``.
    A default record gets the keys of the kind it holds defaults for."""
    if isinstance(record, dict):
        typ = record.get("type")
        if typ == "default":
            kind = DEFAULT_KINDS.get(record.get("name"))
            keys = dict(KEYS.get(kind, {})) if kind else {}
            keys.update({"name": _k("str", "The kind of store these defaults are for: cache, "
                                           "map or marklogic.", required=True),
                         "type": _TYPE})
            for k in keys:
                if keys[k].required and k not in ("name", "type"):
                    keys[k] = Key(**{**keys[k].__dict__, "required": False})
            return keys
        return KEYS.get(typ, {})
    return KEYS.get(record, {})


def known(record, key):
    """The `Key` for `key` in `record`'s catalogue, or None."""
    keys = keys_for(record)
    return keys.get(key) or keys.get("*")


def class_defaults(record_type):
    """``{class key: the class used when the key is absent}`` for a record
    type (from Config.instantiate and friends)."""
    out = {}
    for k, spec in keys_for(record_type).items():
        if spec.type == "class":
            d = spec.default_for(record_type)
            if d:
                out[k] = d
    return out


TEMPLATES = {
    "internal": lambda n: {"name": n, "type": "internal", "namespace": "", "matches": [],
                           "fetch": "", "merge_order": 100},
    "external": lambda n: {"name": n, "type": "external", "namespace": "", "matches": [],
                           "fetch": "", "merge_order": 100},
    "results": lambda n: {"name": n, "type": "results", "namespace": "", "acquirerClass": None},
    "map": lambda n: {"name": n, "type": "map", "storeClass": ""},
    "marklogic": lambda n: {"name": n, "type": "marklogic", "rest": "", "search": "",
                            "sparql": "", "xquery": "", "auth": {}},
    "config": lambda n: {"name": n, "type": "config", "base_dir": ""},
    "default": lambda n: {"name": n, "type": "default"},
    "globals": lambda n: {"type": "globals"},
    "caches": lambda n: {"name": "_caches", "type": "caches"},
}


def template(record_type, name):
    """A starting point for a new record: its name and type and the keys
    the record must have, empty where only a person can fill them in
    (`config_store.validate_record` says which). ValueError for base and
    system (one per installation) and unknown types."""
    if record_type not in TEMPLATES:
        raise ValueError(f"no template for a {record_type!r} record")
    return TEMPLATES[record_type](name)
