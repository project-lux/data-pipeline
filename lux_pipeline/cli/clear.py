"""Clear caches, maps and reconcile indexes.

    lux clear --source ycba                    # recordcache, reconciledRecordcache, recordcache2 (NOT datacache)
    lux clear --source ycba --cache datacache  # a cache has to be named to clear harvested data
    lux clear --source all --force             # every computed cache of every source
    lux clear --map all_refs,done_refs         # the reference queues ...
    lux clear --refs                           # ... which is what this means
    lux clear --map idmap --force              # the identity map; refuses without --force
    lux clear --index lcnaf                    # delete the lmdb index files; the next load recreates them

Equivalents of the `manage-data.py` flags:

    --clear <src>_<cache>   lux clear --source <src> --cache <cache>
    --clear-internal        lux clear --source internal
    --clear-external        lux clear --source external
    --clear-results         lux clear --source results
    --clear-refs            lux clear --refs
    --clear-all             lux clear --source all --refs
    --clear-idmap           lux clear --map idmap --force

Clearing more than one source asks for confirmation when stdin is a
terminal; ``--force`` skips the question (shell scripts pass it). Clearing
the idmap never touches ``idmap_update_token.txt``: the token marks the
current update run, and a cleared map is still part of that run.
"""

import logging
import sys

from lux_pipeline.cli._handler import BaseHandler

logger = logging.getLogger("lux_pipeline")

GROUPS = ("internal", "external", "results")
# the computed caches; datacache holds harvested data and must be named
DEFAULT_CACHES = ("recordcache", "reconciledRecordcache", "recordcache2")
REFS_MAPS = ("all_refs", "done_refs")
INDEX_PATHS = ("reconcileDbPath", "inverseEquivDbPath")


def _split(value):
    return [s.strip() for s in value.split(",") if s.strip()]


class CommandHandler(BaseHandler):
    def add_args(self, ap):
        ap.add_argument("--cache", type=str,
                        help="Cache types separated by commas "
                             f"(default: {','.join(DEFAULT_CACHES)})")
        ap.add_argument("--map", type=str,
                        help="Maps to clear, separated by commas; idmap needs --force")
        ap.add_argument("--refs", action="store_true",
                        help=f"Clear the reference queues (--map {','.join(REFS_MAPS)})")
        ap.add_argument("--index", type=str,
                        help="Sources whose reconcile indexes to delete, "
                             "separated by commas, or 'all'")
        ap.add_argument("--force", action="store_true",
                        help="Do not ask for confirmation; required for --map idmap")

    # --- lookups -----------------------------------------------------------

    def _lookup(self, name):
        for group in GROUPS:
            cfg = getattr(self.configs, group)
            if name in cfg:
                return cfg[name]
        raise ValueError(f"Unknown source: {name}")

    def _all_sources(self):
        return [c for g in GROUPS for c in getattr(self.configs, g).values()]

    def _sources_for(self, which):
        if which == "all":
            return self._all_sources()
        if which in GROUPS:
            return list(getattr(self.configs, which).values())
        return [self._lookup(s) for s in _split(which)]

    def resolve_sources(self, args):
        """``(configs, wide)``: the sources whose caches to clear, and
        whether the request was wider than one named source."""
        which = getattr(args, "source", None)
        if not which:
            return [], False
        cfgs = self._sources_for(which)
        return cfgs, which in ("all", *GROUPS) or len(cfgs) > 1

    def resolve_caches(self, args):
        which = getattr(args, "cache", None)
        return _split(which) if which else list(DEFAULT_CACHES)

    def resolve_maps(self, args):
        names = _split(args.map) if getattr(args, "map", None) else []
        if getattr(args, "refs", False):
            names.extend(m for m in REFS_MAPS if m not in names)
        for n in names:
            if n not in self.configs.map_stores:
                raise ValueError(f"Unknown map: {n}")
        if "idmap" in names and not getattr(args, "force", False):
            raise ValueError("Refusing to clear idmap without --force")
        return names

    def resolve_indexes(self, args):
        """``[(source name, path)]`` for every configured index file."""
        which = getattr(args, "index", None)
        if not which:
            return []
        out = []
        for cfg in self._sources_for(which):
            for key in INDEX_PATHS:
                if cfg.get(key):
                    out.append((cfg["name"], cfg[key]))
        return out

    # --- confirmation ------------------------------------------------------

    def confirm(self, what):
        """Ask before a wide clear when a person is at the keyboard."""
        if not sys.stdin.isatty():
            return True
        answer = input(f"Clear {what}? [y/N] ")
        return answer.strip().lower() in ("y", "yes")

    # --- clearing ----------------------------------------------------------

    def clear_caches(self, sources, caches):
        for cfg in sources:
            for c in caches:
                cache = cfg.get(c)
                if cache is None or not hasattr(cache, "clear"):
                    continue
                print(f"Clearing {cfg['name']} {c}")
                cache.clear()

    def clear_maps(self, maps):
        for m in maps:
            cfg = self.configs.map_stores[m]
            store = cfg.get("store")
            if store is None:
                store = self.configs.instantiate_map(m)["store"]
            print(f"Clearing map {m}")
            store.clear()

    def clear_indexes(self, indexes):
        from lux_pipeline.storage.idmap.lmdb import remove_lmdb
        for source, path in indexes:
            print(f"Removing index {path} for {source}")
            remove_lmdb(path, missing_ok=True)

    # --- the command -------------------------------------------------------

    def process(self, args, rest):
        super().process(args, rest)
        # resolve everything before clearing anything, so a typo in one
        # name clears nothing
        sources, wide = self.resolve_sources(args)
        caches = self.resolve_caches(args)
        maps = self.resolve_maps(args)
        indexes = self.resolve_indexes(args)
        if not (sources or maps or indexes):
            raise ValueError("Nothing to clear: give --source, --map, --refs or --index")

        if wide and not getattr(args, "force", False):
            names = ", ".join(c["name"] for c in sources)
            if not self.confirm(f"{', '.join(caches)} for {len(sources)} sources ({names})"):
                print("Nothing cleared")
                return False

        self.clear_caches(sources, caches)
        self.clear_maps(maps)
        self.clear_indexes(indexes)
        return True
