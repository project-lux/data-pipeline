# What the identity map backends cost in the two places the pipeline actually
# hammers them: reconciliation, and the per-record identity assignment in
# ReferenceManager.manage_identifiers().
#
#   python bench-idmap-workload.py --source ulan --records 2000
#   python bench-idmap-workload.py --source ulan --backends redis,postgres
#
# Records are acquired once, up front, through the real acquirer, then each
# backend runs over identical deep copies of them. So the only difference
# between the runs is the identity map: same records, same mapping, same
# reconcilers.
#
# Every idmap call is counted and timed through a wrapper, so the report
# separates "how much slower is the whole stage" from "how much of that is the
# identity map" -- which are very different numbers when mapping dominates.

import argparse
import copy
import os
import sys
import time
from collections import Counter

from dotenv import load_dotenv

from pipeline.config import Config
from pipeline.process.reconciler import Reconciler
from pipeline.process.reference_manager import ReferenceManager
from pipeline.process.reidentifier import Reidentifier
from pipeline.storage.idmap import postgres as pg_idmap
from pipeline.storage.idmap import redis as redis_idmap

# Stages that only read the map. The others write identity.
READ_ONLY_STAGES = ("reidentify", "merge")

# Calls worth attributing. Anything else on the idmap passes through untimed.
TIMED = ("get", "get_multi", "mint", "set", "delete", "delete_yuid",
         "has_update_token", "add_update_token", "has_item", "count")


class CountingIdMap:
    """Wraps an idmap and records how long the pipeline spends in it.

    Attribute access falls through, so the components that reach for
    prefix_map_in, update_token or conn still work. Dunders have to be
    declared: implicit `idmap[key]` never goes through __getattr__."""

    def __init__(self, inner):
        self._inner = inner
        self.stats = {}

    def _tally(self, name, fn):
        def call(*a, **kw):
            t0 = time.perf_counter()
            try:
                return fn(*a, **kw)
            finally:
                s = self.stats.setdefault(name, [0, 0.0])
                s[0] += 1
                s[1] += time.perf_counter() - t0
        return call

    def __getattr__(self, name):
        attr = getattr(self._inner, name)
        if name in TIMED and callable(attr):
            return self._tally(name, attr)
        return attr

    def __getitem__(self, k):
        return self._tally("__getitem__", self._inner.__getitem__)(k)

    def __setitem__(self, k, v):
        return self._tally("__setitem__", self._inner.__setitem__)(k, v)

    def __delitem__(self, k):
        return self._tally("__delitem__", self._inner.__delitem__)(k)

    def __contains__(self, k):
        return self._tally("__contains__", self._inner.__contains__)(k)

    def totals(self):
        calls = sum(c for c, _ in self.stats.values())
        secs = sum(s for _, s in self.stats.values())
        return calls, secs

    def reset(self):
        self.stats = {}


def make_backend(cfgs, which, args):
    if which == "redis":
        mcfg = dict(cfgs.map_stores[cfgs.idmap_name])
        mcfg["all_configs"] = cfgs
        mcfg.pop("store", None)
        return redis_idmap.IdMap(mcfg), "redis"

    pcfg = dict(cfgs.caches)
    pcfg["all_configs"] = cfgs
    pcfg["tableName"] = args.table
    return pg_idmap.IdMap(pcfg), "postgres"


def load_records(cfgs, source, n, want_types=None):
    """Acquire real records once, through the real acquirer, so every backend
    runs over the same inputs and nothing is fetched over the network mid-run
    (the identifiers come out of the source's own datacache)."""
    cfg = cfgs.internal.get(source) or cfgs.external.get(source)
    if cfg is None:
        raise SystemExit(f"no such source: {source}")
    acquirer = cfg["acquirer"]
    datacache = cfg["datacache"]
    print(f"--- acquiring {n:,} records from {source}")
    records = []
    types = Counter()
    start = time.time()
    for recid in datacache.iter_keys():
        try:
            rec = acquirer.acquire(recid)
        except Exception as e:
            continue
        if not rec or "data" not in rec or "type" not in rec.get("data", {}):
            continue
        if want_types and rec["data"]["type"] not in want_types:
            continue
        records.append(rec)
        types[rec["data"]["type"]] += 1
        if len(records) >= n:
            break
    print(f"    {len(records):,} records in {time.time() - start:.0f}s")
    print(f"    types: {', '.join(f'{t} {c}' for t, c in types.most_common(8))}")
    recon = [t for t in types if t in cfgs.reconcile_record_types]
    print(f"    {sum(types[t] for t in recon):,} are reconcile types "
          f"({', '.join(sorted(recon)) or 'none'})")
    return records, types


def run_reconcile(cfgs, idmap, records, networkmap):
    """The per-record body of run-reconcile.py, minus the assertion log (file
    IO, identical across backends)."""
    reconciler = Reconciler(cfgs, idmap, networkmap)
    ref_mgr = ReferenceManager(cfgs, idmap)
    t0 = time.perf_counter()
    done = errs = 0
    for rec in records:
        try:
            rec2 = reconciler.reconcile(rec)
            ref_mgr.walk_top_for_refs(rec2["data"], 0)
            done += 1
        except Exception as e:
            errs += 1
            if errs <= 2:
                print(f"      reconcile error: {type(e).__name__}: {str(e)[:90]}")
    return time.perf_counter() - t0, done, errs


def run_identify(cfgs, idmap, records):
    """ReferenceManager.manage_identifiers over the same records: the
    per-record identity assignment, which is where a read tier can pay."""
    ref_mgr = ReferenceManager(cfgs, idmap)
    t0 = time.perf_counter()
    done = errs = 0
    for rec in records:
        try:
            ref_mgr.manage_identifiers(rec)
            done += 1
        except Exception as e:
            errs += 1
            if errs <= 2:
                print(f"      identify error: {type(e).__name__}: {str(e)[:90]}")
    return time.perf_counter() - t0, done, errs


def run_reidentify(cfgs, idmap, records):
    """Reidentifier.reidentify(): what merge does to every record it builds.

    Read-only by contract -- the class says so itself -- and it prefetches
    through get_multi, so it is the heaviest idmap consumer in the build."""
    reider = Reidentifier(cfgs, idmap)
    t0 = time.perf_counter()
    done = errs = 0
    for rec in records:
        try:
            reider.reidentify(rec)
            done += 1
        except Exception as e:
            errs += 1
            if errs <= 2:
                print(f"      reidentify error: {type(e).__name__}: {str(e)[:90]}")
    return time.perf_counter() - t0, done, errs


def run_merge(cfgs, idmap, records):
    """run-merge.py's own idmap pattern, which Reidentifier does not cover:
    the record's own YUID and then its cluster members.

        full_yuid = idmap[qrecid]        # run-merge.py:184
        cluster   = idmap[full_yuid]     # run-merge.py:199

    Both keys are the record's own, whereas reidentify's keys are the entities
    the record points AT -- people, places and concepts whatever the record
    is. The two stages therefore have very different key mixes."""
    t0 = time.perf_counter()
    done = errs = 0
    for rec in records:
        try:
            qrecid = cfgs.make_qua(rec["data"]["id"], rec["data"]["type"])
            full_yuid = idmap[qrecid]
            if full_yuid is not None:
                cluster = idmap[full_yuid] or set()
                done += 1
        except Exception as e:
            errs += 1
            if errs <= 2:
                print(f"      merge error: {type(e).__name__}: {str(e)[:90]}")
    return time.perf_counter() - t0, done, errs


def report(label, stage, elapsed, done, errs, counted, baseline):
    calls, idmap_secs = counted.totals()
    per_rec = elapsed / max(done, 1) * 1000
    share = idmap_secs / elapsed * 100 if elapsed else 0
    line = (f"  {label:<22} {elapsed:>7.2f}s total  {per_rec:>7.2f} ms/rec  "
            f"idmap {idmap_secs:>6.2f}s ({share:>4.1f}%)  {calls:>8,} calls")
    if baseline is not None and baseline > 0:
        line += f"  {elapsed / baseline:>5.2f}x"
    print(line)
    if errs:
        print(f"  {'':22} {errs} records errored")
    ops = sorted(counted.stats.items(), key=lambda kv: -kv[1][1])
    for name, (c, s) in ops[:6]:
        print(f"  {'':22}   {name:<18} {c:>8,} calls  {s:>6.2f}s  "
              f"{s / max(c, 1) * 1e6:>7.1f} us/call")
    return elapsed


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", default="ulan", help="source to pull records from")
    ap.add_argument("--records", type=int, default=2000)
    ap.add_argument("--types", default="",
                    help="only records of these types, eg HumanMadeObject "
                         "-- a source's keys are not evenly mixed, and which "
                         "types you get decides which store answers")
    ap.add_argument("--backends", default="redis,postgres")
    ap.add_argument("--table", default="idmap")
    ap.add_argument("--stages", default="reconcile,identify,reidentify,merge")
    ap.add_argument("--repeat", type=int, default=2,
                    help="passes per backend; the last is reported, so a cold "
                         "cache in the first backend doesn't flatter the last")
    args = ap.parse_args()

    load_dotenv()
    cfgs = Config(basepath=os.getenv("LUX_BASEPATH", ""))
    print("# instantiating pipeline")
    cfgs.cache_globals()
    cfgs.instantiate_all()
    networkmap = cfgs.instantiate_map("networkmap")["store"]

    want = {t.strip() for t in args.types.split(",") if t.strip()} or None
    records, types = load_records(cfgs, args.source, args.records, want)
    if not records:
        raise SystemExit("no records acquired")

    stages = [s.strip() for s in args.stages.split(",") if s.strip()]
    backends = [b.strip() for b in args.backends.split(",") if b.strip()]
    baseline = {}

    for stage in stages:
        print(f"\n=== {stage} over {len(records):,} {args.source} records")
        for which in backends:
            inner, label = make_backend(cfgs, which, args)
            counted = CountingIdMap(inner)
            # Anything that resolves the idmap lazily gets this one too
            cfgs.map_stores[cfgs.idmap_name]["store"] = counted
            # reconcile mutates record["data"]["equivalent"], so every backend
            # gets its own copy of identical input
            batch = copy.deepcopy(records)
            if stage in READ_ONLY_STAGES and args.repeat > 1:
                # read-only, so it can simply be run again; the first pass
                # warms this backend's caches the same way the previous
                # backend warmed its own
                for _ in range(args.repeat - 1):
                    if stage == "reidentify":
                        run_reidentify(cfgs, counted, copy.deepcopy(records))
                    else:
                        run_merge(cfgs, counted, copy.deepcopy(records))
                counted.reset()
            if stage == "reconcile":
                el, done, errs = run_reconcile(cfgs, counted, batch, networkmap)
            elif stage == "reidentify":
                el, done, errs = run_reidentify(cfgs, counted, batch)
            elif stage == "merge":
                el, done, errs = run_merge(cfgs, counted, batch)
            else:
                el, done, errs = run_identify(cfgs, counted, batch)
            report(label, stage, el, done, errs, counted, baseline.get(stage))
            if which == "redis":
                baseline[stage] = el
            if hasattr(inner, "shutdown") and which != "redis":
                inner.shutdown()

    print("\n# x-values are against redis on the same records; 'idmap' is the "
          "share of\n# stage time spent inside the identity map, which is what "
          "changing backend moves.")


if __name__ == "__main__":
    main()
