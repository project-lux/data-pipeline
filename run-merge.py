import cProfile
import datetime
import io
import json
import os
import pstats
import sys
from pstats import SortKey

from dotenv import load_dotenv

from pipeline.config import Config
from pipeline.process.merger import MergeHandler
from pipeline.process.reference_manager import ReferenceManager
from pipeline.process.reidentifier import Reidentifier
from pipeline.process.timing import PhaseTimer
from pipeline.storage.cache.postgres import PoolManager

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

if "--resume" in sys.argv:
    RESUME = True
else:
    RESUME = False

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

# Which worker builds which merged record is decided by claim_member() and
# the slice predicate, not by what has already been written -- see the
# reference loop below, which used to compare insert_time against this.
start_time = datetime.datetime.now()

# merge only reads, so enable AAT memory cache
idmap.enable_memory_cache()

# Committing inside every set() cost an fsync per write -- roughly five per
# merged record, times however many workers. Batch instead: all caches in the
# process share one write connection, so one commit covers the merged row and
# its recordcache2 rows together and no other worker ever sees a partially
# written record. checkpoint() below marks the record boundary the commit is
# allowed to land on; the cache does the counting. Anything not yet committed
# when a worker dies is simply redone (--resume skips on
# merged_cache.metadata, which only sees committed rows).
merged_cache.defer_commits(every=500)


def fetch_records(rcache, ids, name):
    # explicit --recid list: still one fetch per id
    for r in ids:
        rec = rcache[r]
        if rec is None:
            print(f"Couldn't find {name} / {r}")
            continue
        yield rec


# -------------------------------------------------
if profiling:
    pr = cProfile.Profile()
    pr.enable()


# namespaces of internal sources, for the cross-slice claim check below
internal_namespaces = tuple(c["namespace"] for c in cfgs.internal.values())
# the internal sources this run is actually merging
todo_names = {s["name"] for (n, s) in to_do}


def claim_member(cluster, present=()):
    """Which internal record builds this cluster's merged record, if any.

    Every write a merged record makes -- the merged row and the rewritten
    row in each contributing source's cache -- is keyed by the cluster's
    YUID, so two workers may only build the same cluster if they are
    prepared to deadlock over those rows. This is the single rule that
    decides which one does it: the lexicographically smallest internal
    member that still exists in its recordcache. Members in `present` are
    known to exist and skip the lookup.

    Returns (member_uri, source_config), or (None, None) when the cluster
    has no internal member -- those clusters belong to the reference pass.
    """
    for cand in sorted(m for m in cluster
                       if not m.startswith("__") and m.startswith(internal_namespaces)):
        try:
            (csrc, crecid) = cfgs.split_uri(cfgs.split_qua(cand)[0])
        except Exception:
            continue
        if cand in present or crecid in csrc["recordcache"]:
            return (cand, csrc)
    return (None, None)


print(start_time)

# Where the time goes, and whether this is cpu bound or waiting on postgres.
# Writes timing-merge-<slice>.json next to the logs.
timer = PhaseTimer("merge", slice_n=my_slice, max_slice=max_slice,
                   out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir)
t_done = 0
for src_name, src in to_do:
    timer.mark(src["name"])
    rcache = src["recordcache"]

    # Iterate whole records rather than keys-then-fetch-each-key: the rows
    # come back in the same server-side cursor that found them, which drops
    # one round trip per record.
    if recids:
        records = fetch_records(rcache, recids, src["name"])
    elif my_slice > -1:
        print(f"*** {src['name']}: slice {my_slice} ***")
        records = rcache.iter_records_slice(my_slice, max_slice)
    else:
        print(f"*** {src['name']} ***")
        records = rcache.iter_records()

    for rec in records:
        t_done += 1
        timer.step()

        distance = 0
        recid = rec["identifier"]
        # get() stamps this on every row it returns and merger.merge() needs
        # it; the iterators don't, so set it here for all three paths
        rec["source"] = src["name"]
        recuri = f"{src['namespace']}{recid}"
        qrecid = cfgs.make_qua(recuri, rec["data"]["type"])
        with timer.stage("idmap_forward"):
            full_yuid = idmap[qrecid]
        if not full_yuid:
            print(f" !!! Couldn't find YUID for internal record: {qrecid}")
            timer.skip()
            continue
        yuid = full_yuid.rsplit("/", 1)[1]
        if RESUME:
            with timer.stage("resume_check"):
                ins_time = merged_cache.metadata(yuid, "insert_time")
            if ins_time is not None: # and (RESUME or ins_time["insert_time"] > start_time):
                timer.skip()
                continue

        # Deterministic cross-slice claim: when several internal records
        # share this YUID, the insert_time guard above is a check-then-act
        # race between slices (whichever wrote first used to win). Instead,
        # only the lexicographically-smallest internal member that still
        # exists in its recordcache builds the merged record.
        with timer.stage("idmap_cluster"):
            cluster = idmap[full_yuid] or set()
        other_internals = [e for e in cluster
                           if e != qrecid and not e.startswith("__")
                           and e.startswith(internal_namespaces)]
        if other_internals:
            # our own record is in hand, so it doesn't need a cache lookup
            with timer.stage("claim_member"):
                (claimed, _) = claim_member(set(other_internals) | {qrecid}, present=(qrecid,))
            if claimed != qrecid:
                # a smaller, still-present member owns this YUID
                timer.skip()
                continue

        with timer.stage("reidentify"):
            rec2 = reider.reidentify(rec)
        if rec2 is None:
            # reidentify already reported why; indexing it would just turn
            # that into a TypeError mid-build
            print(f" *** Could not reidentify {src['name']}/{recid}")
            continue
        if rec2["yuid"] != yuid:
            # This worker owns `yuid`, not whatever the reidentifier came
            # back with; writing there would collide with the worker that
            # does own it. Means the idmap disagrees with itself about this
            # record, so leave it for identify to fix rather than guessing.
            print(f"CLUSTER-ESCAPE: {src['name']}/{recid} is in {yuid} but "
                  f"reidentifies to {rec2['yuid']}; skipping")
            continue
        with timer.stage("write_rewritten"):
            src["recordcache2"][rec2["yuid"]] = rec2["data"]

        with timer.stage("idmap_equivs"):
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

        with timer.stage("merge"):
            rec3 = merger.merge(rec2, equivs)
        # Final tidy up after merges
        try:
            with timer.stage("final_transform"):
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
            with timer.stage("write_merged"):
                merged_cache[rec3["yuid"]] = rec3
        else:
            print(f"*** Final transform returned None")

        # record complete: a safe point for the cache to commit its batch
        with timer.stage("checkpoint"):
            merged_cache.checkpoint()
    merged_cache.flush()
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
    timer.finish()
    # A different shape of work: rebuilding merged records for references,
    # driven by the done_refs file rather than a slice of the record cache.
    timer = PhaseTimer("merge-refs", slice_n=my_slice, max_slice=max_slice,
                       out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir)
    item = 1
    # the YUID comes from the file: write_done_refs resolved it once, which
    # is also what guarantees one line -- and so one worker -- per YUID
    for dist, uri, ext_uri in ref_mgr.iter_done_refs(my_slice, max_slice):
        if not uri:
            print(f" *** No YUID for reference {ext_uri} from done_refs")
            continue
        yuid = uri.rsplit("/", 1)[-1]
        timer.step()
        if RESUME:
            with timer.stage("resume_check"):
                ins_time = merged_cache.metadata(yuid, "insert_time")
            if ins_time is not None:
                timer.skip()
                continue

        with timer.stage("idmap_equivs"):
            equivs = idmap[uri]
        if not equivs:
            print(f"FAILED TO BUILD: {uri}")
            timer.skip()
            continue

        # Don't rebuild what the loop above owns. This used to be an
        # insert_time > start_time check, which is a check-then-act race: the
        # worker that owns this YUID may not have committed it yet (or may
        # still be running), so both built it and both upserted the same rows
        # -- nondeterministic as to which version survived, and the source of
        # the cross-worker `deadlock detected` once commits were deferred.
        # claim_member() gives the same answer in every worker without
        # looking at what has been written so far.
        with timer.stage("claim_member"):
            (_, claim_src) = claim_member(equivs)
        if claim_src is not None and claim_src["name"] in todo_names:
            timer.skip()
            continue
        # get a base record
        # equivs is a redis set; sort so the chosen base record (and thus
        # label/field precedence in the merged output) is deterministic
        rec2 = None
        stop = False
        for pref in PREF_ORDER:
            for eq in sorted(equivs):
                if pref in eq:
                    baseUri = eq
                    (src, recid) = cfgs.split_uri(baseUri)
                    if recid in src["recordcache"]:
                        with timer.stage("fetch_base"):
                            rec = src["recordcache"][recid]
                        if rec is not None:
                            with timer.stage("reidentify"):
                                rec2 = reider.reidentify(rec)
                            if rec2 and rec2["yuid"] != yuid:
                                # The base record decides the YUID every row
                                # below is keyed by, so one that reidentifies
                                # out of this cluster would write the merged
                                # record AND its rewritten row into a YUID
                                # another worker owns -- while this cluster
                                # went unbuilt. Try the next candidate.
                                print(f"CLUSTER-ESCAPE: {src['name']}/{recid} is in "
                                      f"{yuid} but reidentifies to {rec2['yuid']}; "
                                      f"not using it as the base record")
                                rec2 = None
                            elif rec2:
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
            with timer.stage("merge"):
                rec3 = merger.merge(rec2, equivs)
            # Final tidy up
            try:
                with timer.stage("final_transform"):
                    rec3 = final.transform(rec3, rec3["data"]["type"])
            except:
                # NB: identifier was deleted above, so reporting it here
                # raised KeyError from inside the handler and buried the
                # real exception
                print(f"*** Final transform raised exception for {uri}")
            # Store it
            if rec3 is not None:
                try:
                    del rec3["identifier"]
                except:
                    pass
                with timer.stage("write_merged"):
                    merged_cache[rec3["yuid"]] = rec3
            else:
                print(f"*** Final transform returned None")

        with timer.stage("checkpoint"):
            merged_cache.checkpoint()

timer.finish()

# stop deferring and land everything still outstanding
merged_cache.resume_commits()

# force all postgres connections to close
poolman = PoolManager.get_instance()
poolman.put_all("localsocket")

with open(os.path.join(cfgs.log_dir, "flags", f"merge_is_done-{my_slice}.txt"), "w") as fh:
    fh.write("1\n")
