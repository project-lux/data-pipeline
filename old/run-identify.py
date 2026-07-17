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

import glob
import os
import sys
import time

from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.identity import resolve_identity
from pipeline.storage.idmap.lmdb import TabLmdb

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()

files = sorted(glob.glob("assertions-*.tsv"))
if not files:
    print("No assertions-*.tsv files found; did reconcile run?")
    sys.exit(1)
print(f"Resolving identity from {len(files)} assertion files")

diff_index = None
fn = cfgs.results["merged"].get("differentDbPath", "")
if fn:
    try:
        diff_index = TabLmdb.open(fn, "r", readahead=False, writemap=True)
    except Exception as e:
        print(f"Could not open differents index {fn}: {e}")
else:
    print("No differentDbPath configured; clustering without diff constraints")

start = time.time()
stats = resolve_identity(cfgs, idmap, files, diff_index=diff_index)
stats["seconds"] = round(time.time() - start, 1)

print(f"nodes={stats['nodes']} pairs={stats['pairs']} "
      f"diff_pairs={stats['diff_pairs']} clusters={stats['clusters']}")
print(f"reused={stats['reused']} minted={stats['minted']} "
      f"moved={stats['moved']} deleted_yuids={stats['deleted_yuids']}")
print(f"conflicts={stats['conflicts']} (see identity_conflicts.jsonl)")
print(f"done in {stats['seconds']}s")
