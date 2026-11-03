"""Resolve the identity map deterministically after reconciliation.

Runs once, after all run-reconcile.py slices have finished. Aggregates the
assertions-*.tsv files the slices wrote, clusters them under the
differentFrom constraints, assigns YUIDs (reusing prior YUIDs by member
vote, minting uuid5 for genuinely new clusters), and bulk-loads the result
into the idmap for the merge phase.

Given the same assertions, diffs and prior idmap, the output is identical
regardless of how many reconcile workers ran or in what order they
processed records. Refused (conflicting) assertions are written to
identity_conflicts.jsonl for review.

The body is lux_pipeline.process.phases.identify, which `lux identify`
also runs.
"""

import sys

from lux_pipeline.runtime import configure_logging, get_configs
from lux_pipeline.process.phases.identify import report_lines, resolve_identity, vacuum_idmap

configure_logging("INFO")
cfgs = get_configs(instantiate=False)
idmap = cfgs.get_idmap()
cfgs.cache_globals()

stats = resolve_identity(cfgs, idmap)
for line in report_lines(stats):
    print(line)

# Vacuum: see vacuum_idmap for why this is the step that does it
if "--no-vacuum" not in sys.argv:
    print("\nVacuuming the identity map...")
    vacuum_idmap(idmap)
