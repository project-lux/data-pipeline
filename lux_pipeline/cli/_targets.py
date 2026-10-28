"""What ``--source``, ``--cache``, ``--map`` and ``--index`` name: the
resolution `lux counts`, `lux clear` and `lux backup` / `lux restore`
share. Each command keeps its own defaults (which caches when none is
named, what no ``--source`` means); the lookups and their errors are here,
so a typo is the same ``ValueError`` from every command.

Sources come back as ``(group, config)`` pairs, in configuration order;
indexes as ``(group, source name, index name, path)``.
"""

from lux_pipeline.config import source_indexes

GROUPS = ("internal", "external", "results")


def split(value):
    """``"a, b,,c"`` -> ``["a", "b", "c"]``."""
    return [s.strip() for s in value.split(",") if s.strip()]


def lookup(cfgs, name):
    """``(group, config)`` for a source name, or ``ValueError``."""
    for group in GROUPS:
        cfg = getattr(cfgs, group)
        if name in cfg:
            return group, cfg[name]
    raise ValueError(f"Unknown source: {name}")


def all_sources(cfgs):
    return [(g, c) for g in GROUPS for c in getattr(cfgs, g).values()]


def sources(cfgs, which):
    """``[(group, config)]`` for ``all``, a group name, or a
    comma-separated list of source names."""
    if which == "all":
        return all_sources(cfgs)
    if which in GROUPS:
        return [(which, c) for c in getattr(cfgs, which).values()]
    return [lookup(cfgs, s) for s in split(which)]


def maps(cfgs, which, allow_all=True):
    """Map names for ``all`` (when `allow_all`) or a comma-separated list;
    an unknown name is a ``ValueError``."""
    stores = cfgs.map_stores
    if allow_all and which == "all":
        return list(stores.keys())
    names = split(which)
    for n in names:
        if n not in stores:
            raise ValueError(f"Unknown map: {n}")
    return names


def indexes(cfgs, which):
    """``[(group, source name, index name, path)]`` for every configured
    index of the sources `which` names (as for `sources`)."""
    out = []
    for group, cfg in sources(cfgs, which):
        for name, path in source_indexes(cfg).items():
            out.append((group, cfg["name"], name, path))
    return out


def map_store(cfgs, name):
    """The store object for map `name`, instantiating it if the
    configuration has not yet."""
    store = cfgs.map_stores[name].get("store")
    if store is None:
        store = cfgs.instantiate_map(name)["store"]
    return store
