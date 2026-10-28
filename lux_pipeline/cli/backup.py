"""Back up caches, maps and reconcile indexes, whatever backend they are on.

    lux backup --map idmap                        # before `lux clear --map idmap --force`
    lux backup --source aat --cache datacache     # what backup_tables.sh does
    lux backup --index all                        # the lmdb reconcile indexes
    lux backup --all                              # every cache, map and index (not MarkLogic)
    lux backup --all --portable                   # backend-independent, for moving stores
    lux backup --map all_refs,done_refs --verify  # re-read the files, sample the store

Each run writes ``<dest>/<label>/``: one file (or two, or an LMDB
directory) per store and ``manifest.json`` last, so a backup that did not
finish has no manifest and `lux restore` refuses it. ``--dest`` defaults to
``backup_dir`` (``<data_dir>/backups`` when unset), ``--label`` to the time.

``--source`` without ``--cache`` backs up every cache the source has.
Postgres stores are copied with COPY, one snapshot per store; LMDB with its
own hot copy; everything else, and everything with ``--portable``, as
gzipped JSON Lines (lux_pipeline.storage.backup). Take a backup when no
phase is running: Redis and filesystem stores are not snapshots, so this
refuses while a ``lux`` build is (``--force`` runs anyway). The space the
stores need is checked against what ``--dest`` has free before anything is
written. A store that cannot be backed up (the subsidiary idmap) is
reported and skipped, and the command then fails.
"""

import logging
import os
import shutil
from datetime import datetime

from lux_pipeline.storage import backup as bk

from . import _targets as targets
from ._handler import BaseHandler, refuse_while_building

logger = logging.getLogger("lux_pipeline")

CACHES = ("datacache", "recordcache", "reconciledRecordcache", "recordcache2")


class Target:
    """One store to back up or restore: what the manifest calls it, and
    how to get the store."""

    def __init__(self, kind, name, open_store, **labels):
        self.kind = kind
        self.name = name
        self.labels = labels
        self._open = open_store

    @property
    def stem(self):
        extra = self.labels.get("cache") or self.labels.get("index")
        return f"{self.name}.{extra}" if extra else self.name

    def describe(self):
        extra = self.labels.get("cache") or self.labels.get("index")
        return f"{self.kind} {self.name}" + (f" {extra}" if extra else "")

    def key(self):
        return (self.kind, self.name, self.labels.get("cache") or self.labels.get("index"))

    def open(self):
        return self._open()


def open_index(path, flag="r"):
    from lux_pipeline.storage.idmap.lmdb import TabLmdb
    if flag != "r":
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        return TabLmdb.open(path, flag, map_size=2**24)
    return TabLmdb.open(path, "r", readahead=False)


def cache_targets(cfgs, which, caches):
    out = []
    for group, cfg in targets.sources(cfgs, which):
        for c in caches:
            if cfg.get(c) is not None:
                out.append(Target("cache", cfg["name"], (lambda cfg=cfg, c=c: cfg[c]),
                                  group=group, cache=c))
    return out


def map_targets(cfgs, which):
    return [Target("map", m, (lambda m=m: targets.map_store(cfgs, m)))
            for m in targets.maps(cfgs, which)]


def index_targets(cfgs, which, flag="r"):
    return [Target("index", source, (lambda path=path: open_index(path, flag)),
                   group=group, index=name, path=path)
            for group, source, name, path in targets.indexes(cfgs, which)]


def add_target_args(ap, verb):
    ap.add_argument("--cache", type=str,
                    help=f"Cache types, separated by commas (default: every cache the source has)")
    ap.add_argument("--map", type=str, help=f"Maps to {verb}, separated by commas, or 'all'")
    ap.add_argument("--index", type=str,
                    help=f"Sources whose reconcile indexes to {verb}, separated by commas, or 'all'")


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        add_target_args(ap, "back up")
        ap.add_argument("--all", action="store_true",
                        help="Every cache of every source, every map and every index")
        ap.add_argument("--dest", type=str, help="Directory to write under (default: backup_dir)")
        ap.add_argument("--label", type=str, help="Name of this backup (default: the time)")
        ap.add_argument("--portable", action="store_true",
                        help="JSON Lines for every store, restorable into any backend")
        ap.add_argument("--verify", action="store_true",
                        help="Re-read every file against the manifest and sample the stores")
        ap.add_argument("--force", action="store_true",
                        help="Run even though a build looks to be running")

    def resolve_targets(self, args):
        """Every store asked for, resolved before anything is written so a
        typo in one name backs up nothing."""
        cfgs = self.configs
        caches = targets.split(args.cache) if getattr(args, "cache", None) else list(CACHES)
        source = "all" if args.all else getattr(args, "source", None)
        maps = "all" if args.all else getattr(args, "map", None)
        indexes = "all" if args.all else getattr(args, "index", None)
        out = []
        if source:
            out.extend(cache_targets(cfgs, source, caches))
        if maps:
            out.extend(map_targets(cfgs, maps))
        if indexes:
            out.extend(index_targets(cfgs, indexes))
        if not (source or maps or indexes):
            raise ValueError("Nothing to back up: give --source, --map, --index or --all")
        return out

    def destination(self, args):
        dest = args.dest or getattr(self.configs, "backup_dir", None) \
            or os.path.join(self.configs.data_dir, "backups")
        label = args.label or datetime.now().strftime("%Y-%m-%d-%H%M%S")
        out = os.path.join(dest, label)
        if os.path.exists(os.path.join(out, bk.MANIFEST)):
            raise ValueError(f"{out} already holds a backup; give another --label")
        return out, label

    @staticmethod
    def check_space(out, sized):
        """Refuse before writing when the stores that can say how big they
        are need more than the destination has free."""
        need = sum(n for n in sized if n)
        probe = out
        while not os.path.exists(probe):
            probe = os.path.dirname(probe) or "."
        free = shutil.disk_usage(probe).free
        if need > free:
            raise RuntimeError(f"the stores need about {need / 1e9:.1f} GB and {probe} has "
                               f"{free / 1e9:.1f} GB free")
        return need, free

    def process(self, args, rest):
        super().process(args, rest)
        refuse_while_building(self.configs, "backup", args.force)
        todo = self.resolve_targets(args)
        out, label = self.destination(args)

        opened, failed, skipped = [], [], []
        for t in todo:
            if t.kind == "index" and not os.path.exists(t.labels["path"]):
                skipped.append(t)
                continue
            try:
                store = t.open()
            except Exception as e:
                failed.append((t, f"could not open: {e}"))
                continue
            why = bk.refused(store)
            if why:
                failed.append((t, why))
                continue
            opened.append((t, store))
        self.check_space(out, [s.backup_size() for _, s in opened if hasattr(s, "backup_size")])

        os.makedirs(out, exist_ok=True)
        manifest = bk.new_manifest(label)
        for t, store in opened:
            try:
                entry = bk.backup(store, t.kind, t.name, out, stem=t.stem,
                                  portable=args.portable, **t.labels)
            except Exception as e:
                logger.exception(f"backup of {t.describe()} failed")
                failed.append((t, f"{type(e).__name__}: {e}"))
                continue
            manifest["entries"].append(entry)
            print(f"{t.describe()}: {entry['rows']:,} rows, {entry['format']}, "
                  f"{entry['bytes'] / 1e6:,.1f} MB in {entry['seconds']:.1f}s")
            if args.verify:
                problems = bk.verify(store, out, entry)
                for p in problems:
                    print(f"  verify: {p}")
                if problems:
                    failed.append((t, f"verify found {len(problems)} problem(s)"))
            close = getattr(store, "close", None) if t.kind == "index" else None
            if close:
                close()
        if manifest["entries"]:
            bk.write_manifest(out, manifest)
            print(f"backup in {out}")
        for t in skipped:
            print(f"{t.describe()}: not built ({t.labels['path']}), nothing to back up")
        for t, why in failed:
            print(f"{t.describe()}: not backed up: {why}")
        if failed:
            raise RuntimeError(f"{len(failed)} of {len(todo)} stores not backed up")
        return manifest
