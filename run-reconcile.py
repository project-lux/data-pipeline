import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.timing import PhaseTimer
from pipeline.process.reconciler import Reconciler
from pipeline.process.reference_manager import ReferenceManager
from pipeline.process.identity_resolver import IdentityResolver
from pipeline.storage.cache.postgres import PoolManager

import io
import cProfile
import pstats
from pstats import SortKey

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
networkmap = cfgs.instantiate_map("networkmap")["store"]
cfgs.cache_globals()
cfgs.instantiate_all()

# --- process command line arguments ---

my_slice = -1
max_slice = -1

if "--profile" in sys.argv:
    sys.argv.remove("--profile")
    profiling = True
else:
    profiling = False
if "--norefs" in sys.argv:
    sys.argv.remove("--norefs")
    DO_REFERENCES = False
else:
    DO_REFERENCES = True

# NOTE: This implies the existence and use of AAT
if "--baseline" in sys.argv:
    # Only do global terms. Only needed with a new idmap
    recids = [x for x in list(cfgs.globals_cfg.values()) if x.startswith("3")]
    lngs = cfgs.external["aat"]["mapper"].process_langs.values()
    recids.extend([l.id.replace("http://vocab.getty.edu/aat/", "") for l in lngs])
    to_do = [["aat", cfgs.external["aat"], recids]]

    recids = [x for x in list(cfgs.globals_cfg.values()) if x.startswith("Q")]
    if recids:
        to_do.append(["wikidata", cfgs.external['wikidata'], recids])


else:
    recids = []
    if "--all" in sys.argv:
        to_do = list([x,y,[]] for (x,y) in cfgs.internal.items())
    else:
        to_do = []
        for src, cfg in cfgs.internal.items():
            if f"--{src}" in sys.argv:
                to_do.append([src, cfg, []])
        for src, cfg in cfgs.external.items():
            if f"--{src}" in sys.argv:
                to_do.append([src, cfg, []])

    while "--recid" in sys.argv:
        idx = sys.argv.index("--recid")
        recid = sys.argv[idx + 1]
        recids.append(recid)
        sys.argv.pop(idx)
        sys.argv.pop(idx)

    if recids and len(to_do) > 1:
        print("Can only build individual records from a single source")
        sys.exit(0)
    to_do[0][2] = recids

    if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
        my_slice = int(sys.argv[1])
        max_slice = int(sys.argv[2])

    # order to_do from smallest to biggest datacache
    s_to_do = [(x, x[1]["datacache"].len_estimate()) for x in to_do]
    s_to_do.sort(key=lambda x: x[1])
    to_do = [x[0] for x in s_to_do]


# --- set up environment ---
reconciler = Reconciler(cfgs, idmap, networkmap)
# workers: how many processes share the reference queue. Lets pop_ref() shrink
# its claim as the queue drains, so the last references spread over all the
# workers instead of one worker taking the final batch and expanding it alone.
ref_mgr = ReferenceManager(cfgs, idmap, workers=max_slice if max_slice > 0 else 1)
assertion_log = IdentityResolver(cfgs, idmap, my_slice)
debug = cfgs.debug_reconciliation

if my_slice > -1:
    # Running in parallel, will cause cross-process errors
    idmap.disable_memory_cache()
else:
    # Running single, memory cache will remain accurate
    idmap.enable_memory_cache()

# DO NOT defer commits here, however tempting the fsync saving looks.
#
# Deferral is safe in merge and export because each worker owns a disjoint
# set of keys -- its slice of YUIDs -- so two workers never hold locks on the
# same row. Reconcile is the opposite: collect() acquires and stores shared
# external authority records, so every worker upserts into the same bnf/ulan/
# aat rows. Holding those row locks open across a batch of writes lets two
# workers each wait on a row the other has already written, and postgres
# kills one with `deadlock detected`.
#
# Committing per write keeps each lock held for microseconds, so contention
# degrades to a brief wait instead of a deadlock.

print("Starting...")
print(f"Update token is: {idmap.update_token}")

sys.stdout.flush()

# Where the time goes, and whether the phase is cpu bound or waiting. Writes
# timing-reconcile-<slice>.json next to the logs so a 24-way run can be added
# up rather than read across 24 files.
timer = PhaseTimer("reconcile", slice_n=my_slice, max_slice=max_slice,
                   out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir)

if profiling:
    pr = cProfile.Profile()
    pr.enable()

for name, cfg, recids in to_do:
    print(f" *** {name} ***")
    timer.mark(name)
    sys.stdout.flush()
    in_db = cfg["datacache"]
    mapper = cfg["mapper"]
    acquirer = cfg["acquirer"]

    # Iterate whole rows rather than keys-then-fetch-each-key: the datacache
    # row comes back in the same server-side cursor that found it, so the
    # acquirer no longer SELECTs the same row straight back. That is one round
    # trip and one large jsonb parse per record removed, across the whole
    # corpus -- the change run-merge already made (iter_records_slice there).
    #
    # The partition is unchanged: iter_records_slice hashes the same key
    # column iter_keys_slice did, so a worker sees exactly the records it saw
    # before.
    #
    # An explicit --recid list has no cursor to stream, so those still let
    # acquire() do the fetch.
    source_name = in_db.config["name"]
    if recids:
        todo = ((r, None) for r in recids)
    elif my_slice > -1:
        todo = ((r[in_db.key], r) for r in in_db.iter_records_slice(my_slice, max_slice))
    else:
        todo = ((r[in_db.key], r) for r in in_db.iter_records())

    for (recid, row) in todo:
        if row is not None:
            # get() stamps this on every row it returns and the mappers and
            # reconciler read it; the iterators don't, so set it here. Same
            # reason run-merge sets it after switching to an iterator.
            row["source"] = source_name
        # Acquire the record from cache or network
        # XXX acquire_all() to get multiple records from a single one?
        with timer.stage("acquire"):
            if acquirer.returns_multiple():
                recs = acquirer.acquire_all(recid, data=row)
            else:
                rec = acquirer.acquire(recid, data=row)
                if rec is not None:
                    recs = [rec]
                else:
                    recs = []
        if not recs:
            print(f" *** Failed to acquire any record for {name}/{recid} ***")
            timer.skip()
        for rec in recs:
            # Reconcile it
            with timer.stage("reconcile"):
                rec2 = reconciler.reconcile(rec)
            # Do any post-reconciliation clean up
            with timer.stage("post_reconcile"):
                mapper.post_reconcile(rec2)
            # XXX Shouldn't this be stored somewhere after reconciliation?

            # Find references from the record
            with timer.stage("walk_refs"):
                ref_mgr.walk_top_for_refs(rec2["data"], 0)
            # Log equivalence assertions; identity is resolved after all
            # slices complete (run-identify.py)
            with timer.stage("assertions"):
                assertion_log.write_record(rec2)
            timer.step()
    recids = []

if profiling:
    pr.disable()
    s = io.StringIO()
    sortby = SortKey.CUMULATIVE
    # sortby = SortKey.TIME
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())
    raise ValueError()

# now do references

if DO_REFERENCES:
    print("\nProcessing References...")
    records = timer.finish()["records"]
    # A second phase, not more of the first: it claims from a shared queue
    # rather than walking a slice, so its rate means something different.
    timer = PhaseTimer("reconcile-refs", slice_n=my_slice, max_slice=max_slice,
                       out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir)
    item = 1
    while item:
        # Item is uri, {dist, type} or None. None means the shared queue has
        # been empty for the whole idle timeout, not merely empty right now:
        # processing a reference enqueues the references IT finds, so an
        # empty read while other workers are still going is transient. See
        # ReferenceManager._wait_for_refs.
        with timer.stage("claim"):
            item = ref_mgr.pop_ref()
        try:
            (uri, dct) = item
            distance = dct["dist"]
        except:
            continue
        try:
            maptype = dct["type"]
        except:
            continue
        if distance > cfgs.max_distance:
            continue

        # NB: did_ref (marking the reference done) now happens only after a
        # successful acquire below. Marking it done up-front meant a transient
        # fetch failure silently dropped the record AND every concept
        # reachable only through it, for the whole build.
        if cfgs.is_qua(uri):
            quri = uri
            uri, rectype = cfgs.split_qua(uri)
        else:
            raise ValueError(f"No qua in referenced {uri} and needed")
        try:
            (source, recid) = cfgs.split_uri(uri)
        except:
            if debug:
                print(f"Not processing: {uri}")
            # permanently unusable URI: mark done so it isn't re-queued
            ref_mgr.did_ref(quri, distance)
            continue
        if not source["type"] == "external":
            # Don't process internal or results
            print(f"Got internal reference! {uri}")
            raise ValueError(uri)

        # put back the qua to the id after splitting/canonicalizing in split_uri
        mapper = source["mapper"]
        acquirer = source["acquirer"]

        # Acquire the record from cache or network
        with timer.stage("acquire"):
            rec = acquirer.acquire(recid, rectype=rectype)
        if rec is not None:
            with timer.stage("did_ref"):
                ref_mgr.did_ref(quri, distance)
            # Reconcile it
            with timer.stage("reconcile"):
                rec2 = reconciler.reconcile(rec)
            # Do any post-reconciliation clean up
            with timer.stage("post_reconcile"):
                mapper.post_reconcile(rec2)
            # XXX Shouldn't this be stored somewhere after reconciliation?

            # Find references from this record
            with timer.stage("walk_refs"):
                ref_mgr.walk_top_for_refs(rec2["data"], distance)
            # Log equivalence assertions; identity is resolved after all
            # slices complete (run-identify.py)
            with timer.stage("assertions"):
                assertion_log.write_record(rec2)
            timer.step()
        else:
            print(f"Failed to acquire {rectype} reference: {source['name']}:{recid}")
            timer.skip()

timer.finish()

# final tidy up
assertion_log.close()
ref_mgr.write_metatypes(my_slice)
# force all postgres connections to close
poolman = PoolManager.get_instance()
poolman.put_all("localsocket")

# Report Status for orchestration
if my_slice > -1:
    fn = os.path.join(cfgs.log_dir, "flags", f"reconcile_is_done-{my_slice}.txt")
    with open(fn, "w") as fh:
        fh.write("1")
