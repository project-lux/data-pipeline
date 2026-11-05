"""The post-build portal step: post-build-portal.py's body as functions.

Which sources get a portal is configuration: an internal source whose
record has ``"portal": true`` is one, tagged with its own name, and
``"portal": "<tag>"`` names the tag instead (so a renamed source can keep
the tag the exports already know, e.g. ``"ypm"``). Every merged record a
portal source's records lead to, within a few hops, gets the tag in the
merged recordcache's ``change`` metadata, so the export can mark the
records the portal needs. A record within reach of several portals gets
their tags joined with ``|``, which is how the MarkLogic and QLever
mappers read ``change``. Runs after merge, before export.

A portal source's records (the keys of its ``recordcache2``) are distance
0. Their merged records are walked in parallel for the records they
reference (distance 1); from those the walk carries on in this process,
to distance 5. ``equivalent``, ``access_point`` and ``conforms_to`` are
not followed. Finally the metadata is set, in parallel again.

Called by ``lux portal`` (and so ``lux build``'s ``portal`` phase).
"""

import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed

from lux_pipeline import runtime

logger = logging.getLogger("lux_pipeline")

PORTAL_KEY = "portal"  # the source configuration key
MAX_DISTANCE = 4  # a reference found from a record at this distance is the last one walked
SKIP_KEYS = ("equivalent", "access_point", "conforms_to")


def portal_sources(cfgs):
    """``{source name: tag}`` for every source configured with ``portal``.

    ``true`` tags with the source's name, a string with that string. Only
    internal sources have the ``recordcache2`` the walk starts from; the
    key on any other source is reported and ignored."""
    out = {}
    for kind in ("internal", "external", "results"):
        for name, src in (getattr(cfgs, kind, None) or {}).items():
            value = src.get(PORTAL_KEY) if hasattr(src, "get") else None
            if not value:
                continue
            if kind != "internal":
                logger.warning(f"{name}: '{PORTAL_KEY}' is only read on internal sources; "
                               f"this one is {kind}")
                continue
            if isinstance(value, str):
                out[name] = value
            else:
                if value is not True:
                    logger.warning(f"{name}: '{PORTAL_KEY}' should be true or a tag, "
                                   f"not {value!r}; using the source's name")
                out[name] = name
    return out


def portal_tags(cfgs):
    """The tags the portal step can write, for the exporters' allowed lists."""
    return set(portal_sources(cfgs).values())


def default_workers():
    return max(1, multiprocessing.cpu_count() * 2 // 3)


def walk_for_refs(node, distance, distances, added, top=False):
    """Record every referenced yuid in `node` at `distance` in `distances`
    (keeping the shortest), and those not seen before in `added`."""
    if not top and "id" in node and node["id"] and not node["id"].startswith("_"):
        yuid = node["id"].rsplit("/", 1)[-1]
        dist = distances.get(yuid, 100)
        if distance < dist:
            distances[yuid] = distance
        if dist == 100:
            added[yuid] = distance

    for k, v in node.items():
        if k in SKIP_KEYS:
            continue
        if type(v) == list:
            for vi in v:
                if type(vi) == dict:
                    walk_for_refs(vi, distance, distances, added)
        elif type(v) == dict:
            walk_for_refs(v, distance, distances, added)


def _merged():
    return runtime.get_configs().results["merged"]["recordcache"]


def process_recids(recids):
    """Worker: the references (distance 1) of these distance-0 records.
    The script's KeyError handler referred to names only its main process
    had; a missing record is logged here instead."""
    merged = _merged()
    local_dists, local_added = {}, {}
    for recid in recids:
        try:
            rec = merged[recid]
        except KeyError:
            logger.warning(f"missing: {recid}")
            continue
        if not rec:
            continue
        walk_for_refs(rec["data"], 1, local_dists, local_added, top=True)
    return local_dists, local_added


def set_portal(tagged):
    """Worker: set ``change`` on these ``(recid, tags)`` merged records."""
    merged = _merged()
    done = 0
    for recid, tags in tagged:
        try:
            merged.set_metadata(recid, "change", tags)
            done += 1
        except Exception:
            logger.warning(f"Failed to set metadata on {recid}")
    return done


def chunks(items, n):
    """`items` in `n` slices, the last taking the remainder."""
    size = len(items) // n
    return [items[x * size:(x + 1) * size if x < n - 1 else len(items)] for x in range(n)]


def _parallel(fn, slices, workers, *extra):
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as executor:
        futures = [executor.submit(fn, s, *extra) for s in slices if s]
        for future in as_completed(futures):
            yield future.result()


def portal_distances(cfgs, source, workers=None):
    """``{yuid: distance}`` for every record within reach of `source`'s."""
    workers = workers or default_workers()
    rc = cfgs.internal[source]["recordcache2"]
    all_distances = {k: 0 for k in rc.iter_keys()}
    recids = list(all_distances.keys())
    logger.info(f"{source}: dist=0, {workers} processes, {len(recids)} keys")

    added_refs = {}
    for local_dists, local_added in _parallel(process_recids, chunks(recids, workers), workers):
        all_distances.update(local_dists)
        added_refs.update(local_added)

    logger.info(f"{source}: added refs: {len(added_refs)} base")
    merged = cfgs.results["merged"]["recordcache"]
    while added_refs:
        (k, d) = added_refs.popitem()
        if d > MAX_DISTANCE:
            continue
        rec = merged[k]
        if not rec:
            continue
        walk_for_refs(rec["data"], d + 1, all_distances, added_refs, top=True)
    return all_distances


def tag_portal(cfgs, workers=None, sources=None):
    """The whole step, for `sources` (``{source: tag}``; default: every
    source configured with ``portal``). Returns ``(records in reach,
    records tagged)``; ``(0, 0)`` when no source is a portal."""
    workers = workers or default_workers()
    sources = portal_sources(cfgs) if sources is None else sources
    if not sources:
        logger.warning(f"No source has '{PORTAL_KEY}' set; nothing to tag")
        return 0, 0
    tags = {}
    for source, tag in sorted(sources.items()):
        logger.info(f"Portal {tag!r} from source {source}")
        for yuid in portal_distances(cfgs, source, workers):
            tags.setdefault(yuid, set()).add(tag)
    tagged = [(yuid, "|".join(sorted(t))) for yuid, t in tags.items()]
    logger.info(f"Setting metadata: {workers} processes, {len(tagged)}")
    done = sum(_parallel(set_portal, chunks(tagged, workers), workers))
    return len(tagged), done
