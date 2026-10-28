"""Restore caches, maps and reconcile indexes from a `lux backup`.

    lux restore <dir> --list                       # show the manifest, restore nothing
    lux restore <dir> --map idmap --force          # the idmap always needs --force, as in clear
    lux restore <dir> --source aat --cache datacache
    lux restore <dir> --index aat
    lux restore <dir> --force                      # everything in the backup

``<dir>`` is one backup: the directory holding ``manifest.json``. With no
``--source``, ``--map`` or ``--index`` every entry is restored. Each store
is *replaced*: a Postgres one in a single transaction (a failure leaves
what was there), the others emptied and loaded. Everything is resolved and
checked before anything is touched: a store that is not empty, and the
idmap whether it is or not, needs ``--force``; an entry copied natively
restores only into the class that wrote it (back up with ``--portable`` to
move a store between backends); a subsidiary idmap is refused. Refuses
while a ``lux`` build is running (``--force``). Restoring the idmap does
not touch ``idmap_update_token.txt``.
"""

import os

from lux_pipeline.config import source_indexes
from lux_pipeline.storage import backup as bk

from . import _targets as targets
from ._handler import BaseHandler, refuse_while_building
from .backup import add_target_args, open_index


def is_empty(store):
    """Whether `store` holds nothing, without counting it: LMDB answers
    ``len()`` from its header, anything else is asked for a first key."""
    if hasattr(store, "env"):
        return len(store) == 0
    keys = getattr(store, "iter_keys", None)
    if keys is None:
        return len(store) == 0
    it = keys()
    try:
        next(it)
        return False
    except StopIteration:
        return True
    finally:
        close = getattr(it, "close", None)
        if close is not None:
            close()


def describe(entry):
    extra = entry.get("cache") or entry.get("index")
    return f"{entry['kind']} {entry['name']}" + (f" {extra}" if extra else "")


def render_manifest(m, src):
    lines = [f"backup {m.get('label')} in {src}",
             f"  created {m.get('created')} on {m.get('host')}, git {(m.get('git') or '?')[:10]}"]
    for e in m["entries"]:
        lines.append(f"  {describe(e):40} {e['rows']:>14,} rows  {e['format']:12} "
                     f"{e.get('bytes', 0) / 1e6:>10,.1f} MB  {e.get('store_class', '')}")
    return "\n".join(lines)


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("backup", help="The backup directory (the one holding manifest.json)")
        ap.add_argument("--list", action="store_true", help="Show the manifest and stop")
        add_target_args(ap, "restore")
        ap.add_argument("--force", action="store_true",
                        help="Replace stores that are not empty; required for the idmap")

    def select(self, args, entries):
        """The entries the options name (all of them when none is given);
        naming something the backup does not hold is an error."""
        cfgs = self.configs
        source = getattr(args, "source", None)
        maps = getattr(args, "map", None)
        indexes = getattr(args, "index", None)
        if not (source or maps or indexes):
            return list(entries)
        chosen = []

        def take(pred, asked):
            got = [e for e in entries if pred(e)]
            if not got:
                raise ValueError(f"the backup has no {asked}")
            chosen.extend(e for e in got if e not in chosen)

        if source:
            caches = set(targets.split(args.cache)) if getattr(args, "cache", None) else None
            names = {cfg["name"] for _, cfg in targets.sources(cfgs, source)}
            take(lambda e: e["kind"] == "cache" and e["name"] in names
                 and (caches is None or e.get("cache") in caches),
                 f"caches for --source {source}" + (f" --cache {args.cache}" if caches else ""))
        if maps:
            if maps == "all":
                take(lambda e: e["kind"] == "map", "maps")
            else:
                for m in targets.maps(cfgs, maps, allow_all=False):
                    take(lambda e, m=m: e["kind"] == "map" and e["name"] == m, f"map {m}")
        if indexes:
            names = {cfg["name"] for _, cfg in targets.sources(cfgs, indexes)}
            take(lambda e: e["kind"] == "index" and e["name"] in names,
                 f"indexes for --index {indexes}")
        return chosen

    def open_target(self, entry):
        """The configured store an entry restores into."""
        cfgs = self.configs
        kind, name = entry["kind"], entry["name"]
        if kind == "cache":
            _, cfg = targets.lookup(cfgs, name)
            store = cfg.get(entry["cache"])
            if store is None:
                raise ValueError(f"{name} has no {entry['cache']} configured")
            return store
        if kind == "map":
            if name not in cfgs.map_stores:
                raise ValueError(f"Unknown map: {name}")
            return targets.map_store(cfgs, name)
        _, cfg = targets.lookup(cfgs, name)
        path = source_indexes(cfg).get(entry["index"])
        if not path:
            raise ValueError(f"{name} has no {entry['index']} index configured")
        return open_index(path, "c")

    def process(self, args, rest):
        super().process(args, rest)
        src = os.path.abspath(args.backup)
        manifest = bk.read_manifest(src)
        if args.list:
            print(render_manifest(manifest, src))
            return manifest

        refuse_while_building(self.configs, "restore", args.force)
        chosen = self.select(args, manifest["entries"])
        if not chosen:
            raise ValueError(f"{src} holds no entries")

        # everything resolved and checked before anything is touched
        plan, problems = [], []
        for entry in chosen:
            try:
                store = self.open_target(entry)
                bk.check_restorable(store, entry)
            except (ValueError, bk.BackupRefused) as e:
                problems.append(f"{describe(entry)}: {e}")
                continue
            if entry["kind"] == "map" and entry["name"] == "idmap" and not args.force:
                problems.append("idmap: refusing to replace the idmap without --force")
            elif not args.force and not is_empty(store):
                problems.append(f"{describe(entry)}: not empty; --force replaces it")
            plan.append((entry, store))
        if problems:
            raise ValueError("nothing restored:\n  " + "\n  ".join(problems))

        for entry, store in plan:
            n = bk.restore(store, src, entry)
            print(f"{describe(entry)}: {n:,} rows restored ({entry['format']})")
        return plan
