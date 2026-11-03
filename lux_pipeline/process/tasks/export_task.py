"""`ExportManager`: write the build's output files, one per worker. The
manager behind `lux export`; replaces `run-export.py`, `run-source-build.py`
and `manage-data.py --nt` run from `export_parallel.sh` / `nt_parallel.sh`.

``export_type`` picks the work each worker does, all of it in
`lux_pipeline.process.phases.export`:

* ``marklogic`` (the default): `export_slice`, the MarkLogic documents,
  ``export_full_<n>.jsonl``. ``merged`` is the same with ``no_ml``: the
  merged records as they are, ``export_merged_<n>.jsonl``.
* ``nt``: `export_nt_slice`, the merged records as N-Triples,
  ``lux_<n>.nt.gz``.
* ``sources``: `export_sources_slice`, the aat / ulan / lcsh / tgn records
  mapped into their recordcaches and written as JSONL and N-Triples.
* ``external`` (``external-zip``): `export_external`, per external source
  the datacache records its recordcache used, in the form ``lux load
  --type export`` reads.

``caches`` (``--cache datacache[,recordcache...]``) is its own kind of
export: every record of those caches of the selected sources, internal,
external or results, by `export_cache_slice` -- each source's cache sliced
across all the workers like the merged records, one
``export_<source>_<cache>_<n>.jsonl.gz`` per worker (no ``_<n>`` with one
worker), in ``--type external``'s line format. ``--type`` is left unset (or
``external``) with it.

The first four types slice their caches by worker number and worker count, so
every record is written by exactly one worker. ``external`` writes one file
per source and so cannot share a source between workers: worker ``n`` takes
every ``max_workers``-th selected source starting at the ``n``-th, and
there are never more workers than sources.

JSONL goes to ``exports_dir``, N-Triples to ``nt_dir`` (which falls back to
``exports_dir``); ``out_dir`` sends everything a run writes to one place.

``format = "lmdb"`` (``--format lmdb``) builds LMDB distribution stores
(`lux_pipeline.storage.distribution`) instead of files, for ``merged``,
``external`` and ``--cache``: ``merged.lmdb`` (UUID keys), ``external.lmdb``
(``<source>:<id>`` keys) and, for ``--cache``, one store per cache type,
``<source>_<cache>.lmdb`` for one source or ``<cache>.lmdb`` for several
(``<source>:<id>`` keys); ``lmdb_keys`` overrides the key form. They go to
``distribution_dir`` (falling back to ``exports_dir``), or ``out_dir``. The
workers write sorted runs into ``<store>.runs/``; the driver tail merges
them into the store in key order once every worker is done. A
``--my-worker`` slice writes its runs only, and ``merge_only`` runs just
the tail, over the runs every slice left. That tail is the only one.
"""

import shutil

import logging
import os

from lux_pipeline.process.phases.export import (CHUNK, GZIP_LEVEL, SOURCES, _estimate,
                                                cache_export_path, cache_records,
                                                export_cache_slice, export_external,
                                                export_lmdb_runs, export_nt_slice,
                                                export_slice, export_sources_slice,
                                                external_records, merged_records)
from lux_pipeline.storage.distribution import KEY_FORMATS, build_store, missing_workers
from ._task_manager import TaskManager, flags_pattern, remove_stale

logger = logging.getLogger("lux_pipeline")

EXPORT_TYPES = ("marklogic", "merged", "nt", "sources", "external")
ALIASES = {"external-zip": "external", "external_zip": "external"}

# The types that export the merged records; --source does not apply
MERGED_TYPES = ("marklogic", "merged", "nt")


def normalize_type(export_type):
    """The canonical name of `export_type`; ``ValueError`` for an unknown one."""
    t = ALIASES.get(export_type, export_type)
    if t not in EXPORT_TYPES:
        raise ValueError(f"Unknown export type: {export_type} "
                         f"(one of {', '.join(EXPORT_TYPES)}, external-zip)")
    return t


# the export_type of a --cache export
CACHE_EXPORT = "cache"

FORMATS = ("jsonl", "lmdb")
# what --format lmdb can build a store from
LMDB_TYPES = ("merged", "external", CACHE_EXPORT)


def parse_caches(value, export_type=None):
    """The cache types ``--cache`` names, or ``[]``. ``--type`` may be
    left unset or be ``external`` alongside it; anything else is a
    ``ValueError``."""
    caches = [c.strip() for c in (value or "").split(",") if c.strip()]
    if caches and export_type not in (None, "external", *[a for a, t in ALIASES.items()
                                                          if t == "external"]):
        raise ValueError(f"--cache exports whole caches; it does not combine with "
                         f"--type {export_type}")
    return caches


class ExportManager(TaskManager):
    """Options, read from `args` in ``__init__``: ``export_type``, ``caches``
    (``--cache``), ``gz``,
    ``chunk`` (records per cache query; ``LUX_EXPORT_CHUNK``, else 1000),
    ``gzip_level`` (``LUX_EXPORT_GZIP``, else 9, for the JSONL exports; the
    phase's own default for the others) and ``out_dir``."""

    def __init__(self, configs, max_workers=0, args=None):
        super().__init__(configs, max_workers, args)
        self.export_type = "marklogic"
        self.gz = False
        self.chunk_size = None
        self.gzip_level = None
        self.out_dir = None
        self.caches = []
        self.format = "jsonl"
        self.lmdb_keys = None
        self.merge_only = False
        self.stores = []
        self.store_stats = []
        if args is not None:
            self.format = getattr(args, "format", None) or "jsonl"
            self.lmdb_keys = getattr(args, "lmdb_keys", None)
            self.merge_only = bool(getattr(args, "merge_only", False))
            self.caches = parse_caches(getattr(args, "cache", None),
                                       getattr(args, "export_type", None))
            self.export_type = (CACHE_EXPORT if self.caches else
                                normalize_type(getattr(args, "export_type", None) or "marklogic"))
            self.gz = bool(getattr(args, "gz", False))
            self.chunk_size = getattr(args, "chunk", None)
            self.gzip_level = getattr(args, "gzip_level", None)
            self.out_dir = getattr(args, "out_dir", None)
        if self.chunk_size is None:
            self.chunk_size = int(os.getenv("LUX_EXPORT_CHUNK", str(CHUNK)))
        if self.gzip_level is None and self.export_type in ("marklogic", "merged"):
            self.gzip_level = int(os.getenv("LUX_EXPORT_GZIP", str(GZIP_LEVEL)))
        if self.format not in FORMATS:
            raise ValueError(f"Unknown format: {self.format} (one of {', '.join(FORMATS)})")
        if self.format == "lmdb" and self.export_type not in LMDB_TYPES:
            raise ValueError(f"--format lmdb builds a store from --type merged, --type external "
                             f"or --cache, not --type {self.export_type}")
        if self.lmdb_keys is not None and self.lmdb_keys not in KEY_FORMATS:
            raise ValueError(f"Unknown key form: {self.lmdb_keys} (one of {', '.join(KEY_FORMATS)})")
        if (self.lmdb_keys or self.merge_only) and self.format != "lmdb":
            raise ValueError("--lmdb-keys and --merge-only apply to --format lmdb")

    @property
    def lmdb(self):
        return self.format == "lmdb"

    @property
    def hide_idle_workers(self):
        return self.export_type == "external"

    # ----------------------------------------------------------- sources

    def maybe_add(self, which, cfg):
        """The merged results for the merged-record types, an external
        source for ``sources`` and ``external``."""
        name = cfg["name"]
        if self.export_type == CACHE_EXPORT:
            pass        # any source: what it has is checked in check()
        elif self.export_type in MERGED_TYPES:
            if (which, name) != ("results", "merged"):
                raise ValueError(f"--type {self.export_type} exports the merged records, "
                                 f"not {name}")
        elif which != "external":
            raise ValueError(f"--type {self.export_type} applies to external sources, "
                             f"not {name}")
        if (which, name) not in self.sources:
            self.sources.append((which, name))

    @property
    def names(self):
        return [name for _, name in self.sources]

    def check(self):
        """Raised in the driver before any worker starts: nothing selected.
        For ``external``, no more workers than sources."""
        if self.export_type == CACHE_EXPORT:
            if not self.sources:
                raise ValueError("No source selected for --cache")
            missing = [f"{name} has no {c}" for which, name in self.sources for c in self.caches
                       if getattr(self.configs, which)[name].get(c) is None]
            if missing:
                raise ValueError("--cache: " + "; ".join(missing))
        elif self.export_type not in MERGED_TYPES and not self.sources:
            raise ValueError(f"No source selected for --type {self.export_type}")
        if self.gz and self.export_type not in ("marklogic", "merged"):
            logger.info(f"--gz has no effect on --type {self.export_type}: its files are "
                        f"always gzipped")
        if self.export_type == "external" and self.max_workers > len(self.sources):
            self.max_workers = len(self.sources)
        if self.lmdb:
            self.stores = self.plan_stores()

    def plan_stores(self):
        """The stores this run builds: ``{"name", "path", "runs", "keys",
        "kind", "targets"}``, with plain-string paths so they travel to the
        workers. Read in the driver, where the configuration is."""
        out_dir = self.out_dir or getattr(self.configs, "distribution_dir", None) \
            or self.configs.exports_dir
        plans = []
        if self.export_type == "merged":
            plans.append(("merged", "merged", None))
        elif self.export_type == "external":
            plans.append(("external", "external", list(self.names)))
        else:
            for c in self.caches:
                name = f"{self.sources[0][1]}_{c}" if len(self.sources) == 1 else c
                plans.append((name, CACHE_EXPORT, [(w, n, c) for w, n in self.sources]))
        stores = []
        for name, kind, targets in plans:
            path = os.path.join(out_dir, f"{name}.lmdb")
            stores.append({"name": name, "path": path, "runs": f"{path}.runs", "kind": kind,
                           "targets": targets,
                           "keys": self.lmdb_keys or ("uuid" if kind == "merged" else "text")})
        return stores

    # ----------------------------------------------------------- workers

    def sources_for(self, n):
        """The external sources worker `n` exports."""
        return self.names[n::self.max_workers]

    @property
    def targets(self):
        """``[(group, source name, cache type)]`` for a ``--cache`` export."""
        return [(which, name, c) for which, name in self.sources for c in self.caches]

    def _gzip_kw(self):
        return {} if self.gzip_level is None else {"gzip_level": self.gzip_level}

    def _distributed(self, n, cfgs, progress):
        """Worker `n`'s share of the export; returns what the phase
        function returns (a list of them for ``external``)."""
        t = self.export_type
        mx = self.max_workers
        if self.lmdb:
            return self._lmdb_runs(n, cfgs, progress)
        if t == CACHE_EXPORT:
            # one worker writes the whole cache, unsliced, as export-source.py did
            sliced = mx > 1
            return export_cache_slice(cfgs, self.targets, n if sliced else -1, mx if sliced else -1,
                                      out_dir=self.out_dir, progress=progress, **self._gzip_kw())
        if t in ("marklogic", "merged"):
            return export_slice(cfgs, n, mx, no_ml=(t == "merged"), gz=self.gz,
                                chunk_size=self.chunk_size, out_dir=self.out_dir,
                                progress=progress, **self._gzip_kw())
        if t == "nt":
            out_dir = self.out_dir or getattr(cfgs, "nt_dir", None)
            return export_nt_slice(cfgs, n, mx, out_dir=out_dir, progress=progress,
                                   **self._gzip_kw())
        if t == "sources":
            nt_dir = self.out_dir or getattr(cfgs, "nt_dir", None)
            return export_sources_slice(cfgs, n, mx, sources=self.names, out_dir=self.out_dir,
                                        nt_dir=nt_dir, progress=progress, **self._gzip_kw())
        mine = self.sources_for(n)
        if not mine:
            progress(done=True)
            return []
        return [export_external(cfgs, name, out_dir=self.out_dir, chunk_size=self.chunk_size,
                                progress=progress, **self._gzip_kw())
                for name in mine]

    def _lmdb_runs(self, n, cfgs, progress):
        """Worker `n`'s sorted runs for every store this run builds."""
        mx = self.max_workers
        sliced = mx > 1
        sl, ms = (n, mx) if sliced else (-1, -1)
        out = []
        for st in self.stores:
            if st["kind"] == "merged":
                rc = cfgs.results["merged"]["recordcache"]
                recs, total = merged_records(cfgs, sl, ms), _estimate(rc) // max(mx, 1)
            elif st["kind"] == "external":
                names = self.sources_for(n)
                if not names:
                    continue
                recs = external_records(cfgs, names, self.chunk_size)
                total = sum(_estimate(cfgs.external[nm]["recordcache"]) for nm in names)
            else:
                recs = cache_records(cfgs, [tuple(x) for x in st["targets"]], sl, ms)
                total = sum(_estimate(getattr(cfgs, g)[nm][c]) for g, nm, c in st["targets"]) \
                    // max(mx, 1)
            out.append(export_lmdb_runs(cfgs, recs, st["runs"], n, st["keys"], total=total,
                                        progress=progress, description=st["name"]))
        if not out:
            progress(done=True)
        return out

    def build_stores(self):
        """The driver tail: merge each store's runs into it, in key order."""
        self.store_stats = []
        for st in self.stores:
            if not os.path.isdir(st["runs"]):
                raise RuntimeError(f"no runs to merge in {st['runs']}: run the export's "
                                   f"workers first")
            missing = missing_workers(st["runs"], self.max_workers)
            if missing:
                shown = ", ".join(map(str, missing[:10])) + (" ..." if len(missing) > 10 else "")
                raise RuntimeError(f"{st['name']}: worker(s) {shown} of {self.max_workers} have "
                                   f"not finished their runs in {st['runs']}; run those "
                                   f"slices (same --max-workers), then --merge-only")
            logger.info(f"Merging the runs into {st['path']}")
            sources = sorted({x[1] for x in st["targets"]} if st["kind"] == CACHE_EXPORT
                             else (st["targets"] or ["merged"]))
            meta = {"export": st["kind"], "sources": sources}
            if st["kind"] == CACHE_EXPORT:
                meta["cache"] = st["targets"][0][2]
            self.store_stats.append(build_store(st["path"], st["runs"], st["keys"], meta=meta))
        return self.store_stats

    # ----------------------------------------------------------- driver

    def clear_stale(self):
        """Before a marklogic / merged (or ``--cache``) run over every slice: that type's
        ``export_full_*`` / ``export_merged_*`` files in the directory this
        run writes to (an earlier run with more workers leaves its higher
        slices behind, and the load would read them) and the done flags
        (run-all.sh's ``rm``). A single slice leaves them."""
        if self.my_slice > -1:
            return 0
        if self.lmdb:
            # an earlier attempt's runs would be merged in with this one's
            removed = 0
            for st in self.stores:
                if os.path.isdir(st["runs"]):
                    shutil.rmtree(st["runs"])
                    removed += 1
            if removed:
                logger.info(f"removed the runs of {removed} earlier, unmerged store build(s)")
            return removed
        out_dir = self.out_dir or self.configs.exports_dir
        if self.export_type == CACHE_EXPORT:
            # this run's files, and the slices or the unsliced file of an
            # earlier one, which a load of the directory would read too
            pats = []
            for _, name, c in self.targets:
                pats.append(cache_export_path(out_dir, name, c))
                pats.append(cache_export_path(out_dir, name, c, 0).replace("_0.jsonl", "_*.jsonl"))
            return remove_stale(*pats)
        if self.export_type not in ("marklogic", "merged"):
            return 0
        stem = "export_merged" if self.export_type == "merged" else "export_full"
        return remove_stale(os.path.join(out_dir, f"{stem}_*.jsonl"),
                            os.path.join(out_dir, f"{stem}_*.jsonl.gz"),
                            flags_pattern(self.configs, "export"))

    def process(self, engine="mp", prefix=None, view=None):
        """Check the run, clear what an earlier run left, then run every
        worker. Returns the workers' results."""
        self.check()
        if self.lmdb and self.merge_only:
            return self.build_stores()
        self.clear_stale()
        results = super().process(engine, prefix, view)
        if self.lmdb:
            # the null engine runs one worker: --my-worker i, or worker 0
            if self.my_slice > -1 or (engine == "null" and self.max_workers > 1):
                logger.info(f"worker {max(self.my_slice, 0)} of {self.max_workers}: runs "
                            f"written; when every slice is done, merge them with the same "
                            f"command and --merge-only")
            else:
                self.build_stores()
        return results


def default_sources(export_type):
    """What an export of `export_type` covers when ``--source`` is not
    given: the merged records, or run-source-build.py's four sources;
    ``None`` for ``external``, which has to be told."""
    if export_type in MERGED_TYPES:
        return ["merged"]
    if export_type == "sources":
        return list(SOURCES)
    return None
