"""The identify step: run-identify.py's body as functions.

Runs once, in one process, after every reconcile slice has finished:
`resolve_identity` aggregates the ``assertions-*.tsv`` files the slices
wrote, clusters them under the differentFrom constraints and rewrites the
identity map (see `IdentityResolver.resolve_identity`); `vacuum_idmap`
then clears the litter that left in the map's tables. It is a streaming
unix ``sort`` pipeline and is never sliced.

Called by run-identify.py, `ReconcileManager.finish` (item 21) and
``lux identify`` (item 22).
"""

import logging
import time

from lux_pipeline.process.identity_resolver import IdentityResolver

logger = logging.getLogger("lux_pipeline")


def resolve_identity(cfgs, idmap):
    """Resolve the identity map from the reconcile slices' assertions.
    Returns the resolver's stats (``nodes``, ``pairs``, ``diff_pairs``,
    ``clusters``, ``conflicts``) plus ``seconds``. Refused (conflicting)
    assertions go to ``identity_conflicts.jsonl`` in the working
    directory."""
    resolver = IdentityResolver(cfgs, idmap)
    start = time.time()
    stats = resolver.resolve_identity()
    stats["seconds"] = round(time.time() - start, 1)
    return stats


def report_lines(stats):
    """The three lines run-identify.py prints for `stats`."""
    return [f"nodes={stats.get('nodes')} pairs={stats.get('pairs')} "
            f"diff_pairs={stats.get('diff_pairs')} clusters={stats.get('clusters')}",
            f"conflicts={stats.get('conflicts')} (see identity_conflicts.jsonl)",
            f"done in {stats.get('seconds')}s"]


def vacuum_idmap(idmap):
    """Vacuum the identity map, when it can be. Returns False for a map
    without ``optimize()``.

    The map has just been rewritten member by member, and nothing else in
    the pipeline vacuums it -- autovacuum's default 20% scale factor never
    fires on a table this size, so it had never run at all (see
    IdMap.optimize()). Identify is the only step that writes identity, and
    merge is the phase that pays for the litter, so it is cleared here
    rather than leaving 9% dead tuples in the two indexes merge probes tens
    of millions of times.
    """
    optimize = getattr(idmap, "optimize", None)
    if optimize is None:
        return False
    optimize()
    return True
