import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.reconciler import Reconciler
from pipeline.process.reference_manager import ReferenceManager
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
        to_do = list([x,y,[]] for x in cfgs.internal.items())
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

    if len(to_do) > 1:
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
ref_mgr = ReferenceManager(cfgs, idmap)
debug = cfgs.debug_reconciliation

if my_slice > -1:
    # Running in parallel, will cause cross-process errors
    idmap.disable_memory_cache()
else:
    # Running single, memory cache will remain accurate
    idmap.enable_memory_cache()

print("Starting...")
print(f"Update token is: {idmap.update_token}")

sys.stdout.flush()

if profiling:
    pr = cProfile.Profile()
    pr.enable()

for name, cfg, recids in to_do:
    print(f" *** {name} ***")
    sys.stdout.flush()
    in_db = cfg["datacache"]
    mapper = cfg["mapper"]
    acquirer = cfg["acquirer"]

    if not recids:
        if my_slice > -1:
            recids = in_db.iter_keys_slice(my_slice, max_slice)
        else:
            recids = in_db.iter_keys()

    for recid in recids:
        sys.stdout.write(".")
        sys.stdout.flush()
        # Acquire the record from cache or network
        # XXX acquire_all() to get multiple records from a single one?
        if acquirer.returns_multiple():
            recs = acquirer.acquire_all(recid)
        else:
            rec = acquirer.acquire(recid)
            if rec is not None:
                recs = [rec]
            else:
                recs = []
        if not recs:
            print(f" *** Failed to acquire any record for {name}/{recid} ***")
        for rec in recs:
            # Reconcile it
            rec2 = reconciler.reconcile(rec)
            # Do any post-reconciliation clean up
            mapper.post_reconcile(rec2)
            # XXX Shouldn't this be stored somewhere after reconciliation?

            # Find references from the record
            ref_mgr.walk_top_for_refs(rec2["data"], 0)
            # Manage identifiers for rec now we've reconciled and collected
            ref_mgr.manage_identifiers(rec2)
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
    item = 1
    while item:
        # Item is uri, {dist, type} or None
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

        ref_mgr.did_ref(uri, distance)

        if cfgs.is_qua(uri):
            uri, rectype = cfgs.split_qua(uri)
        else:
            raise ValueError(f"No qua in referenced {uri} and needed")
        try:
            (source, recid) = cfgs.split_uri(uri)
        except:
            if debug:
                print(f"Not processing: {uri}")
            continue
        if not source["type"] == "external":
            # Don't process internal or results
            print(f"Got internal reference! {uri}")
            raise ValueError(uri)

        # put back the qua to the id after splitting/canonicalizing in split_uri
        mapper = source["mapper"]
        acquirer = source["acquirer"]

        # Acquire the record from cache or network
        rec = acquirer.acquire(recid, rectype=rectype)
        if rec is not None:
            sys.stdout.write(".")
            sys.stdout.flush()
            # Reconcile it
            rec2 = reconciler.reconcile(rec)
            # Do any post-reconciliation clean up
            mapper.post_reconcile(rec2)
            # XXX Shouldn't this be stored somewhere after reconciliation?

            # Find references from this record
            ref_mgr.walk_top_for_refs(rec2["data"], distance)
            # Manage identifiers for rec now we've reconciled and collected

            # rebuild should be False if this is an equivalent of an internal rec
            # as we've already seen it, so don't remove URIs (e.g. internal uris)
            ref_mgr.manage_identifiers(rec2)
        else:
            print(f"Failed to acquire {rectype} reference: {source['name']}:{recid}")

# final tidy up
ref_mgr.write_metatypes(my_slice)
# force all postgres connections to close
poolman = PoolManager.get_instance()
poolman.put_all("localsocket")

# Report Status for orchestration
if my_slice > -1:
    fn = os.path.join(cfgs.log_dir, "flags", f"reconcile_is_done-{my_slice}.txt")
    with open(fn, "w") as fh:
        fh.write("1")
