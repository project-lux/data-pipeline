"""Helpers the web UI's tests share (webui_docs/). Not a test module.

`write_fake_run` (item W01) leaves what a run leaves under ``log_dir``: one
status and one log file per worker and, unless asked not to, the driver's
``<prefix>.run`` record.

`make_installation` (item W00) writes a small, complete installation into a
directory -- configuration records, data directories, the idmap's update
token, a ``.env`` -- whose stores are all on the filesystem, so a test can
build a real `Config` and call ``instantiate_all()`` with no Postgres, no
Redis and no network. `make_configs` builds the `Config`, `seed_records`
puts a few records in its caches and idmap, and `use_installation` points
`lux_pipeline.runtime` at it for code that calls ``get_configs()``.

The records are written here, in code. Never copy them from
``docs/sample_config`` (out of date: classes that no longer exist, no
``idmap_name``) or from ``config_cache/`` (the build machine's files,
untracked).
"""

import json
import os
import socket
import time
from pathlib import Path

from lux_pipeline.progress import StatusWriter, log_path, run_path, status_path

# --- the installation (W00) ------------------------------------------------------

FS = "lux_pipeline.storage.cache.filesystem."
FS_MAP = "lux_pipeline.storage.idmap.filesystem."
INTERNAL_URI = "https://lux.collections.yale.edu/data/"
TOKEN = "__20260101__"
# relative to base_dir, as the build machine's base.json has some of them
DIRS = {"data_dir": "data/files", "log_dir": "data/logs", "exports_dir": "data/exports",
        "indexes_dir": "data/indexes", "dumps_dir": "data/dumps", "temp_dir": "data/tmp",
        "tests_dir": "data/tests"}
MAPS = ("idmap", "networkmap", "all_refs", "done_refs", "redirects")

# The sources make_installation can write: (group, namespace, fetch, extra keys).
# Each must build with instantiate_all() and nothing but these records.
SOURCES = {
    "ycba": ("internal", "https://ycba-lux.s3.amazonaws.com/v3/",
             "https://ycba-lux.s3.amazonaws.com/v3/{identifier}",
             {"merge_order": 1, "mapperClass": "lux_pipeline.sources.yale.ycba.mapper.YcbaMapper"}),
    "yuag": ("internal", "https://media.art.yale.edu/content/lux/",
             "https://media.art.yale.edu/content/lux/{identifier}.json",
             {"merge_order": 2, "mapperClass": "lux_pipeline.sources.yale.yuag.mapper.YuagMapper"}),
    "aat": ("external", "http://vocab.getty.edu/aat/",
            "https://vocab.getty.edu/aat/{identifier}.jsonld",
            {"merge_order": 10, "mapperClass": "lux_pipeline.sources.getty.mapper.AatMapper"}),
    "ulan": ("external", "http://vocab.getty.edu/ulan/",
             "https://vocab.getty.edu/ulan/{identifier}.jsonld",
             {"merge_order": 11, "mapperClass": "lux_pipeline.sources.getty.mapper.UlanMapper"}),
}
DEFAULT_SOURCES = ("ycba", "aat")


def installation_records(root, sources=DEFAULT_SOURCES):
    """``{record key: record}`` for an installation at `root` with
    `sources` (names from `SOURCES`) plus ``merged`` and the maps."""
    root = str(root)
    files = os.path.join(root, DIRS["data_dir"])
    recs = {
        "base": {
            "type": "base", "base_dir": root, "max_workers": 2, "idmap_name": "idmap",
            **DIRS,
            "internal_uri": INTERNAL_URI,
            "validatorClass": "lux_pipeline.process.validator.Validator",
            "validate_allow_underscore_properties": True,
            "allow_network": False, "debug_reconciliation": False,
            "max_distance": 3, "reconcile_min_equivs": 3,
            "ok_record_types": {"HumanMadeObject": "object", "Type": "concept",
                                "Material": "concept", "Language": "concept",
                                "Person": "person", "Group": "group", "Place": "place",
                                "Set": "set", "VisualItem": "visual",
                                "LinguisticObject": "text", "DigitalObject": "digital",
                                "Activity": "activity", "Event": "activity", "Period": "activity"},
            "parent_record_types": {"Material": "Type", "Language": "Type"},
            "reconcile_record_types": ["Person", "Group", "Place", "Type", "Language", "Material"],
            "path_types": {"reconcileDbPath": "indexes_dir", "inverseEquivDbPath": "indexes_dir",
                           "dumpFilePath": "dumps_dir", "exportPath": "exports_dir",
                           "cycleBreakPath": "data_dir"},
            "other_external_matches": [], "external_uri_rewrites": {}, "do_not_reidentify": [],
        },
        # merged under every cache's configuration: the filesystem caches
        # need base_dir, and no connection settings are wanted
        "caches": {"name": "_caches", "type": "caches", "base_dir": files, "overwrite": True},
        # nothing for cache_globals to look up
        "globals": {"type": "globals"},
        "merged": {"name": "merged", "type": "results", "namespace": INTERNAL_URI,
                   "datacacheClass": FS + "DataCache", "recordcacheClass": FS + "MergedRecordCache",
                   "recordcache2Class": FS + "RecordCache", "acquirerClass": None},
    }
    for name in sources:
        group, ns, fetch, extra = SOURCES[name]
        rec = {"name": name, "type": group, "namespace": ns,
               "matches": [ns.split("://", 1)[1]], "fetch": fetch,
               "datacacheClass": FS + "DataCache", "recordcache2Class": FS + "RecordCache"}
        if group == "internal":
            rec["recordcacheClass"] = FS + "InternalRecordCache"
        else:
            rec["recordcacheClass"] = FS + "ExternalRecordCache"
            rec["recordcacheReconciledClass"] = FS + "ReconciledRecordCache"
        rec.update(extra)
        recs[name] = rec
    for m in MAPS:
        recs[f"map_{m}"] = {"name": m, "type": "map", "base_dir": files,
                            "storeClass": FS_MAP + ("IdMap" if m == "idmap" else "FsMap")}
    return recs


def make_installation(tmp_path, sources=DEFAULT_SOURCES):
    """Write an installation into `tmp_path` and return it as a `Path`:
    ``config_cache/<key>.json`` for every record of
    `installation_records`, the data directories, the idmap's update
    token (the filesystem `IdMap` refuses to start without a well-formed
    one) and a ``.env`` setting ``LUX_BASEPATH``."""
    root = Path(tmp_path)
    cc = root / "config_cache"
    cc.mkdir(parents=True, exist_ok=True)
    for rel in DIRS.values():
        (root / rel).mkdir(parents=True, exist_ok=True)
    (root / DIRS["data_dir"] / "idmap_update_token.txt").write_text(f"{TOKEN}\n")
    for key, rec in installation_records(root, sources).items():
        (cc / f"{key}.json").write_text(json.dumps(rec, indent=4) + "\n")
    (root / ".env").write_text(f"LUX_BASEPATH={root}\n")
    return root


def add_subconfig_store(root, name="sources", move=("aat",), indent=2):
    """Give the installation at `root` a sub-config store (item 35), as
    pipeline-configs has: ``<root>/subs/<name>_cache/``, the records named
    in `move` moved into it from the configuration directory and written
    with `indent` (pipeline-configs uses 2), and the ``config`` record
    ``<name>.json`` that points at it. Returns the store's directory."""
    root = Path(root)
    cc = root / "config_cache"
    directory = root / "subs" / f"{name}_cache"
    directory.mkdir(parents=True, exist_ok=True)
    for key in move:
        rec = json.loads((cc / f"{key}.json").read_text())
        (directory / f"{key}.json").write_text(json.dumps(rec, indent=indent) + "\n")
        (cc / f"{key}.json").unlink()
    (cc / f"{name}.json").write_text(json.dumps({"type": "config", "name": name,
                                                 "base_dir": "subs"}, indent=4) + "\n")
    return directory


def make_configs(tmp_path, instantiate=False, sources=DEFAULT_SOURCES):
    """`make_installation` (unless `tmp_path` already holds one) and its
    `Config`; with `instantiate`, also ``get_idmap()``,
    ``cache_globals()`` and ``instantiate_all()``, as
    ``runtime.get_configs()`` does."""
    from lux_pipeline.config import Config
    root = Path(tmp_path)
    if not (root / "config_cache" / "base.json").exists():
        make_installation(root, sources)
    cfgs = Config(basepath=str(root))
    if instantiate:
        cfgs.get_idmap()
        cfgs.cache_globals()
        cfgs.instantiate_all()
    return cfgs


def use_installation(monkeypatch, tmp_path, sources=DEFAULT_SOURCES):
    """Point `lux_pipeline.runtime` at an installation in `tmp_path`
    (written if missing): ``LUX_BASEPATH`` set, the cached configuration
    forgotten now and again when the test ends. Returns the root."""
    from lux_pipeline import runtime
    root = Path(tmp_path)
    if not (root / "config_cache" / "base.json").exists():
        make_installation(root, sources)
    monkeypatch.setenv("LUX_BASEPATH", str(root))
    monkeypatch.delenv("LUX_ENV_FILE", raising=False)
    runtime.reset_configs()
    monkeypatch.setattr(runtime, "_cfgs", None)   # monkeypatch restores it afterwards
    return root


def seed_records(cfgs):
    """A few records, stored as the pipeline stores them, in an
    instantiated `Config` from `make_configs`: for each of ycba and aat
    (when configured) a datacache row, its record in the recordcache (bare
    key for an internal source, ``<identifier>##qua<Type>`` for an external
    one), a YUID minted in the idmap, and the merged record under the
    YUID's UUID, as merge writes it. Returns ``{source: {"identifier",
    "uri", "key", "type", "yuid", "uuid"}}``."""
    idmap = cfgs.get_idmap()
    merged = cfgs.results["merged"]["recordcache"]
    wanted = {
        "ycba": ("object/1", "HumanMadeObject", "object", "Portrait of a Lady"),
        "aat": ("300033618", "Type", "concept", "paintings (visual works)"),
    }
    out = {}
    for name, (ident, typ, slug, label) in wanted.items():
        cfg = cfgs.internal.get(name) or cfgs.external.get(name)
        if cfg is None:
            continue
        uri = f"{cfg['namespace']}{ident}"
        record = {"@context": "https://linked.art/ns/v1/linked-art.json", "id": uri,
                  "type": typ, "_label": label}
        cfg["datacache"][ident] = dict(record)
        key = ident if cfg["type"] == "internal" else cfgs.make_qua(ident, typ)
        cfg["recordcache"][key] = dict(record)
        yuid = idmap.mint(uri, slug, typ)
        uu = yuid.rsplit("/", 1)[1]
        merged[uu] = dict(record, id=yuid)
        out[name] = {"identifier": ident, "uri": uri, "key": key, "type": typ,
                     "yuid": yuid, "uuid": uu}
    return out


# --- runs (W01) ---------------------------------------------------------------------


def write_fake_run(log_dir, command, workers, *, done=False, record=True,
                   ts="2026-11-02-141500", totals=100, completed=40, pid=None,
                   exit=None, started=None, updated=None, extra=None, visible=None):
    """Write a run's files under `log_dir` and return its prefix.

    `done` is one flag for every worker or a sequence with one per worker;
    `totals` likewise (``None`` for a worker that has not reported one).
    The record gets ``pid`` (default: this process, so the run reads as
    running until `exit` is given), ``exit`` and ``started``. `updated`
    back-dates every status file's ``updated`` time. `record=False` leaves
    only the workers' files, as a shell-started slice does."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    prefix = os.path.join(str(log_dir), f"{command}-{ts}")
    dones = list(done) if isinstance(done, (list, tuple)) else [done] * workers
    tots = list(totals) if isinstance(totals, (list, tuple)) else [totals] * workers
    for n in range(workers):
        w = StatusWriter(status_path(prefix, n), n, throttle=0)
        w(phase=command, description=f"source{n}", completed=completed,
          **({"total": tots[n]} if tots[n] is not None else {}), done=dones[n])
        if updated is not None:
            st = json.loads(Path(status_path(prefix, n)).read_text())
            st["updated"] = updated
            Path(status_path(prefix, n)).write_text(json.dumps(st))
        Path(log_path(prefix, n)).write_text(f"14:15:0{n} INFO     worker {n} started\n")
    if record:
        rec = {"prefix": prefix, "command": command, "argv": ["lux", command],
               "host": socket.gethostname(), "pid": os.getpid() if pid is None else pid,
               "engine": "mp", "workers": workers,
               "visible_workers": list(range(workers)) if visible is None else list(visible),
               "sources": [], "parent": None,
               "started": started if started is not None else time.time(),
               "finished": None if exit is None else time.time(), "exit": exit,
               "error": None if exit in (None, 0) else "failed"}
        rec.update(extra or {})
        Path(run_path(prefix)).write_text(json.dumps(rec))
    return prefix


def dead_pid():
    """The pid of a process that has exited (and been reaped)."""
    import subprocess
    import sys
    p = subprocess.Popen([sys.executable, "-c", "pass"])
    p.wait()
    return p.pid
