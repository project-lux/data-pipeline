"""Resolve the identity map deterministically after reconciliation.

Runs once, after all run-reconcile.py slices have finished. Aggregates the
assertions-*.tsv files the slices wrote, clusters them under the
differentFrom constraints, assigns YUIDs (reusing prior YUIDs by member
vote, minting uuid5 for genuinely new clusters), and bulk-loads the result
into the redis idmap for the merge phase.

Given the same assertions, diffs and prior idmap, the output is identical
regardless of how many reconcile workers ran or in what order they
processed records. Refused (conflicting) assertions are written to
identity_conflicts.jsonl for review.
"""

import sys
import time

from lux_pipeline.runtime import configure_logging, get_configs
from lux_pipeline.process.identity_resolver import IdentityResolver

configure_logging("INFO")
cfgs = get_configs(instantiate=False)
idmap = cfgs.get_idmap()
cfgs.cache_globals()

resolver = IdentityResolver(cfgs, idmap)

start = time.time()
stats = resolver.resolve_identity()
stats["seconds"] = round(time.time() - start, 1)

print(f"nodes={stats['nodes']} pairs={stats['pairs']} "
      f"diff_pairs={stats['diff_pairs']} clusters={stats['clusters']}")
print(f"conflicts={stats['conflicts']} (see identity_conflicts.jsonl)")
print(f"done in {stats['seconds']}s")

# The map has just been rewritten member by member, and nothing else in the
# pipeline vacuums it -- autovacuum's default 20% scale factor never fires on
# a table this size, so it had never run at all (see IdMap.optimize()). This
# is the only phase that writes identity, and merge is the phase that pays for
# the litter, so clear it here rather than leaving 9% dead tuples in the two
# indexes merge probes tens of millions of times.
if "--no-vacuum" not in sys.argv:
    print("\nVacuuming the identity map...")
    idmap.optimize()
