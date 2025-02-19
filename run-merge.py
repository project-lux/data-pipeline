import os
import sys
import json
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.reidentifier import Reidentifier
from pipeline.process.merger import MergeHandler
from pipeline.process.reference_manager import ReferenceManager
from pipeline.storage.cache.postgres import PoolManager

import datetime
import io
import cProfile
import pstats
from pstats import SortKey

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

with open(os.path.join(cfgs.data_dir, "idmap_update_token.txt")) as fh:
    token = fh.read()
token = token.strip()
if not token.startswith("__") or not token.endswith("__"):
    print("Idmap Update Token is badly formed, should be 8 character date with leading/trailing __")
    raise ValueError("update token")
    sys.exit(0)
else:
    idmap.update_token = token

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

max_slice = -1
my_slice = -1
recids = []
if "--all" in sys.argv:
    to_do = list(cfgs.internal.items())
elif "--onlyrefs" in sys.argv:
    to_do = []
else:
    to_do = []
    for src, scfg in cfgs.internal.items():
        if f"--{src}" in sys.argv:
            to_do.append((src, scfg))
    for src, scfg in cfgs.external.items():
        if f"--{src}" in sys.argv:
            to_do.append((src, scfg))

while "--recid" in sys.argv:
    idx = sys.argv.index("--recid")
    recid = sys.argv[idx + 1]
    recids.append(recid)
    sys.argv.pop(idx)
    sys.argv.pop(idx)

if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])


MAX_DISTANCE = cfgs.max_distance
order = sorted([(x["namespace"], x.get("merge_order", -1)) for x in cfgs.external.values()], key=lambda x: x[1])
PREF_ORDER = [x[0] for x in order if x[1] >= 0]

reider = Reidentifier(cfgs, idmap)
ref_mgr = ReferenceManager(cfgs, idmap)
merger = MergeHandler(cfgs, idmap, ref_mgr)

merged_cache = cfgs.results["merged"]["recordcache"]
merged_cache.config["overwrite"] = True
final = cfgs.results["merged"]["mapper"]

# if merged is not empty, then only want to write to it if the record
# hasn't already been written this build
# OTOH, if merged starts off empty, it must have been this build
start_time = datetime.datetime.now()

# merge only reads, so enable AAT memory cache
idmap.enable_memory_cache()

# -------------------------------------------------
if profiling:
    pr = cProfile.Profile()
    pr.enable()


for src_name, src in to_do:
    rcache = src["recordcache"]

    if not recids:
        if my_slice > -1:
            print(f"*** {src['name']}: slice {my_slice} ***")
            recids = rcache.iter_keys_slice(my_slice, max_slice)
        else:
            print(f"*** {src['name']} ***")
            recids = rcache.iter_keys()

    for recid in recids:
        distance = 0
        rec = rcache[recid]
        if not rec:
            print(f"Couldn't find {src['name']} / {recid}")
            continue
        recuri = f"{src['namespace']}{recid}"
        qrecid = cfgs.make_qua(recuri, rec["data"]["type"])
        yuid = idmap[qrecid]
        if not yuid:
            print(f" !!! Couldn't find YUID for internal record: {qrecid}")
            continue
        yuid = yuid.rsplit("/", 1)[1]
        ins_time = merged_cache.metadata(yuid, "insert_time")
        if ins_time is not None and ins_time["insert_time"] > start_time:
            # Already processed this record this build
            continue
        else:
            # Always reidentify in case the UUIDs have changed during
            # subsequent reconcile phases
            rec2 = reider.reidentify(rec)
            src["recordcache2"][rec2["yuid"]] = rec2["data"]

        equivs = idmap[rec2["data"]["id"]]
        if equivs:
            if qrecid in equivs:
                equivs.remove(qrecid)
            if recuri in equivs:
                equivs.remove(recuri)
            if idmap.update_token in equivs:
                equivs.remove(idmap.update_token)
        else:
            equivs = []
        sys.stdout.write(".")
        sys.stdout.flush()

        rec3 = merger.merge(rec2, equivs)
        # Final tidy up after merges
        try:
            rec3 = final.transform(rec3, rec3["data"]["type"])
        except:
            print(f"*** Final transform raised exception for {rec2['identifier']}")
            raise
        # Store it
        if rec3 is not None:
            try:
                del rec3["identifier"]
            except:
                pass
            merged_cache[rec3["yuid"]] = rec3
        else:
            print(f"*** Final transform returned None")
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

if DO_REFERENCES:
    item = 1
    for dist, ext_uri in ref_mgr.iter_done_refs(my_slice, max_slice):
        uri = idmap[ext_uri]
        if not uri:
            print(f" *** No YUID for reference {ext_uri} from done_refs")
            continue
        yuid = uri.rsplit("/", 1)[-1]
        ins_time = merged_cache.metadata(yuid, "insert_time")
        if ins_time is not None and ins_time["insert_time"] > start_time:
            # Already processed this record this build
            continue

        equivs = idmap[uri]
        if not equivs:
            print(f"FAILED TO BUILD: {uri}")
            continue
        # get a base record
        rec2 = None
        stop = False
        for pref in PREF_ORDER:
            for eq in equivs:
                if pref in eq:
                    baseUri = eq
                    (src, recid) = cfgs.split_uri(baseUri)
                    if recid in src["recordcache"]:
                        rec = src["recordcache"][recid]
                        if rec is not None:
                            rec2 = reider.reidentify(rec)
                            if rec2:
                                equivs.remove(baseUri)
                                del rec2["identifier"]
                                src["recordcache2"][rec2["yuid"]] = rec2
                                stop = True
                                break
                            else:
                                print(f" *** Could not reidentify {src['name']} {recid}")
            if stop:
                break

        if rec2 is None:
            print(f" *** Could not find ANY record for {uri} in {equivs}")
            # raise ValueError()
        else:
            # print(f" ... Processing equivs for {recid}")
            sys.stdout.write("+")
            sys.stdout.flush()
            rec3 = merger.merge(rec2, equivs)
            # Final tidy up
            try:
                rec3 = final.transform(rec3, rec3["data"]["type"])
            except:
                print(f"*** Final transform raised exception for {rec2['identifier']}")
            # Store it
            if rec3 is not None:
                try:
                    del rec3["identifier"]
                except:
                    pass
                merged_cache[rec3["yuid"]] = rec3
            else:
                print(f"*** Final transform returned None")

# force all postgres connections to close
poolman = PoolManager.get_instance()
poolman.put_all("localsocket")

with open(os.path.join(cfgs.log_dir, "flags", f"merge_is_done-{my_slice}.txt"), "w") as fh:
    fh.write("1\n")
