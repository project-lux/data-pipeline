"""The merge phase: run-merge.py's body as three functions.

`read_update_token` is the script's check of ``idmap_update_token.txt``.
`select_sources` is its argument handling: which sources, in config order.
`merge_slice` is everything after that: build the reidentifier, the
reference manager and the merger, walk this slice of each source's
recordcache building one merged record per cluster this worker owns, then
rebuild the clusters the references pass owns, and tidy up. The legacy
script parses argv and calls the three; the manager (item 24) calls them
from each worker.

The per-record loops are run-merge.py's, moved and not changed: nothing
inside a ``with timer.stage(...)`` block may be edited here. This is the
most tuned code in the repository -- the claim memoisation alone was
measured at 52% of the phase on the 88k-member cluster -- and which worker
builds which cluster, and from which base record, is decided by exactly
what these loops do.
"""

import collections
import datetime
import logging
import os

from lux_pipeline.process.merge_batch import MERGE_BATCH, prefetched
from lux_pipeline.process.merger import MergeHandler
from lux_pipeline.process.reference_manager import ReferenceManager
from lux_pipeline.process.reidentifier import Reidentifier
from lux_pipeline.process.timing import PhaseTimer
from lux_pipeline.progress import null_progress
from lux_pipeline.storage.cache.postgres import PoolManager

logger = logging.getLogger("lux_pipeline")


def read_update_token(cfgs):
    """The build token from ``<data_dir>/idmap_update_token.txt``, stripped.

    Raises ``ValueError`` when it is not ``__...__``; a missing file raises
    as ``open`` does. The Postgres idmap checks the same file when it is
    built, so this normally only confirms what it already read.
    """
    with open(os.path.join(cfgs.data_dir, "idmap_update_token.txt")) as fh:
        token = fh.read()
    token = token.strip()
    if not token.startswith("__") or not token.endswith("__"):
        raise ValueError("Idmap Update Token is badly formed, should be 8 character date with leading/trailing __")
    return token


def select_sources(cfgs, names=(), all_internal=False, only_refs=False, recids=()):
    """``[(name, cfg), ...]`` in the order the phase processes them.

    ``all_internal`` is every internal source (the script's ``--all``) and
    wins over everything else; ``only_refs`` (``--onlyrefs``) is no source
    at all, so only the references pass runs. Otherwise `names` are internal
    or external sources (``--<src>``), returned in configuration order with
    the internal ones first, as the script did; an unknown name, or none at
    all, raises ``ValueError``. `recids` restricts the run to those records,
    which needs exactly one selected source.
    """
    if all_internal:
        to_do = list(cfgs.internal.items())
    elif only_refs:
        to_do = []
    else:
        unknown = [n for n in names if n not in cfgs.internal and n not in cfgs.external]
        if unknown:
            raise ValueError(f"Unknown source: {', '.join(unknown)}")
        to_do = []
        for src, scfg in cfgs.internal.items():
            if src in names:
                to_do.append((src, scfg))
        for src, scfg in cfgs.external.items():
            if src in names:
                to_do.append((src, scfg))
        if not to_do:
            raise ValueError("No source selected")
    if recids and len(to_do) != 1:
        raise ValueError("Can only build individual records from a single source")
    return to_do


def fetch_records(rcache, ids, name):
    # explicit --recid list: still one fetch per id
    for r in ids:
        rec = rcache[r]
        if rec is None:
            logger.warning(f"Couldn't find {name} / {r}")
            continue
        yield rec


# Candidates per existence query in claim_member(). Small because the walk
# almost always stops in the first few; large enough that an ordinary
# cluster is one round trip.
CLAIM_CHUNK = 100

# Answers, by YUID. A cluster with N internal members produces N calls, and
# every one of them sorts all N -- 101ms per call and 52% of the phase on
# the 88,447-member cluster, once the query was chunked and the sort was
# what was left. Bounded, and small: the clusters worth remembering are the
# ones that repeat, and a cluster of three sorts in microseconds anyway.
CLAIM_CACHE_SIZE = 20000


def make_claim_member(cfgs, internal_namespaces, chunk_size=CLAIM_CHUNK,
                      cache_size=CLAIM_CACHE_SIZE):
    """run-merge.py's ``claim_member`` and ``_claim_member``, closed over
    what they read from the script's module: the configuration, the tuple
    of internal namespaces, the chunk size and a fresh answer cache of
    `cache_size` entries. Returns ``claim_member``; one per `merge_slice`
    call, so the cache is that call's own. The cache is exposed as
    ``claim_member.cache`` for tests.
    """
    cache = collections.OrderedDict()

    def claim_member(cluster, present=(), key=None):
        """Memoised wrapper on _claim_member. `key` is the cluster's YUID.

        Safe to memoise on that alone: the answer is a pure function of the
        cluster's membership, which is what the YUID names. `present` does not
        enter into it -- it only skips a lookup that would have returned True,
        because the caller is holding the record it names. Both call sites pass
        the same internal candidate set for a given YUID: the merge loop builds
        it from the cluster it just read, and the reference loop from
        idmap[uri], which is the same member set.
        """
        if key is None:
            return _claim_member(cluster, present)
        hit = cache.get(key)
        if hit is not None:
            cache.move_to_end(key)
            return hit
        ans = _claim_member(cluster, present)
        cache[key] = ans
        if len(cache) > cache_size:
            cache.popitem(last=False)
        return ans

    def _claim_member(cluster, present=()):
        """Which internal record builds this cluster's merged record, if any.
        Uncached; call claim_member().

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
        # The answer has to be the *smallest* surviving member, so the sorted
        # walk is the rule and cannot be reordered. What can change is how many
        # candidates are asked about at a time.
        #
        # `crecid in csrc["recordcache"]` inside the loop was a round trip per
        # candidate -- 1.2M calls at 28ms, the worst per-call cost in the phase.
        # Asking about *every* candidate at once is worse: the walk almost
        # always stops in the first handful, so on the 88,447-member cluster
        # that is one ANY() over 88,447 keys, per member, for an answer that was
        # three candidates in. Measured at 2.12 SECONDS per call, 94.6% of the
        # phase.
        #
        # So: a chunk at a time. One query per source per chunk, and the walk
        # still stops at the first member that exists. Every candidate in an
        # earlier chunk sorts before every candidate in a later one, so stopping
        # inside a chunk gives the same answer as walking them all.
        cands = sorted(m for m in cluster
                       if not m.startswith("__") and m.startswith(internal_namespaces))
        for i in range(0, len(cands), chunk_size):
            # parsed per chunk, not up front: on a huge cluster the candidates
            # past the first chunk are never looked at
            chunk = []
            for cand in cands[i:i + chunk_size]:
                try:
                    (csrc, crecid) = cfgs.split_uri(cfgs.split_qua(cand)[0])
                except Exception:
                    continue
                chunk.append((cand, csrc, crecid))

            asked = {}
            for cand, csrc, crecid in chunk:
                if cand in present:
                    return (cand, csrc)
                name = csrc["name"]
                if name not in asked:
                    mine = [c for (_u, s, c) in chunk if s["name"] == name]
                    asked[name] = csrc["recordcache"].has_multi(mine)
                if crecid in asked[name]:
                    return (cand, csrc)
        return (None, None)

    claim_member.cache = cache
    return claim_member


def merge_slice(cfgs, idmap, to_do, my_slice=-1, max_slice=-1, recids=(),
                do_references=True, resume=False, progress=null_progress, write_flag=True):
    """Merge this slice of every source in `to_do` (from `select_sources`),
    then the clusters the references pass owns, and tidy up.

    ``my_slice`` / ``max_slice`` are this worker's share of each recordcache
    and of the done-references file (``-1``: everything, unsliced, in this
    process). `recids` is an explicit record list for the (single) source.
    ``do_references=False`` is the script's ``--norefs``; `resume` its
    ``--resume``, skipping clusters the merged cache already holds.
    `progress` is a ``lux_pipeline.progress`` callable. `write_flag` writes
    the ``merge_is_done-<n>.txt`` flag the shell orchestration waits for.

    Reads the update token itself and sets it on `idmap`, so a caller need
    not. Returns ``{"records": <PhaseTimer summary>, "references": <summary,
    or None without the references pass>}``.
    """
    idmap.update_token = read_update_token(cfgs)
    recids = list(recids)

    order = sorted([(x["namespace"], x.get("merge_order", -1)) for x in cfgs.external.values()], key=lambda x: x[1])
    pref_order = [x[0] for x in order if x[1] >= 0]

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
    # batch=25: deferring already removed the fsync per write, which leaves one
    # round trip and one parse per record. 25 records per statement instead, in
    # key order. Costs nothing in memory -- the deferred statements already held
    # every record until the commit.
    merged_cache.defer_commits(every=500, batch=25)

    # namespaces of internal sources, for the cross-slice claim check below
    internal_namespaces = tuple(c["namespace"] for c in cfgs.internal.values())
    # the internal sources this run is actually merging
    todo_names = {s["name"] for (n, s) in to_do}

    claim_member = make_claim_member(cfgs, internal_namespaces)

    logger.info(start_time)

    # Where the time goes, and whether this is cpu bound or waiting on postgres.
    # Writes timing-merge-<slice>.json next to the logs.
    timer = PhaseTimer("merge", slice_n=my_slice, max_slice=max_slice,
                       out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir,
                       progress=progress)
    # What this slice expects to see, summed over the sources: a per-source
    # total would read as more than 100% once the second source starts
    running_total = 0
    records_summary = None
    references_summary = None

    for src_name, src in to_do:
        timer.mark(src["name"])
        rcache = src["recordcache"]
        running_total += len(recids) if recids else _estimate(rcache) // max(max_slice, 1)
        progress(total=running_total)

        # Iterate whole records rather than keys-then-fetch-each-key: the rows
        # come back in the same server-side cursor that found them, which drops
        # one round trip per record.
        if recids:
            records = fetch_records(rcache, recids, src["name"])
        elif my_slice > -1:
            logger.info(f"*** {src['name']}: slice {my_slice} ***")
            records = rcache.iter_records_slice(my_slice, max_slice)
        else:
            logger.info(f"*** {src['name']} ***")
            records = rcache.iter_records()

        for (rec, recid, recuri, qrecid, full_yuid, already_built) in prefetched(
                records, src, cfgs, idmap, merged_cache, timer, resume, MERGE_BATCH):
            distance = 0
            if not full_yuid:
                logger.warning(f" !!! Couldn't find YUID for internal record: {qrecid}")
                timer.skip()
                continue
            if already_built:
                timer.skip()
                continue
            # The YUID and its whole class. These were two sequential single-row
            # lookups (idmap_forward then idmap_cluster), 9.35 worker-hours of an
            # earlier build between them, for what postgres can answer in one
            # statement -- membership is derived from the yuid column, so the
            # forward pointer and the class it points at come back together.
            # prefetched() has already put both in the memory cache for this
            # batch, so this is a lookup and not a statement; it stays a call so
            # that a cache miss, or a backend without one, still works.
            with timer.stage("idmap_cluster"):
                (full_yuid, cluster) = idmap.get_cluster(qrecid)
            if not full_yuid:
                logger.warning(f" !!! Couldn't find YUID for internal record: {qrecid}")
                timer.skip()
                continue
            # Derived from what is actually in hand rather than trusting the
            # prefetch and this call to agree; nothing writes the map during
            # merge, so they do.
            yuid = full_yuid.rsplit("/", 1)[1]

            # Deterministic cross-slice claim: when several internal records
            # share this YUID, the insert_time guard above is a check-then-act
            # race between slices (whichever wrote first used to win). Instead,
            # only the lexicographically-smallest internal member that still
            # exists in its recordcache builds the merged record.
            other_internals = [e for e in (cluster or ())
                               if e != qrecid and not e.startswith("__")
                               and e.startswith(internal_namespaces)]
            if other_internals:
                # our own record is in hand, so it doesn't need a cache lookup
                with timer.stage("claim_member"):
                    (claimed, _) = claim_member(set(other_internals) | {qrecid},
                                                present=(qrecid,), key=yuid)
                if claimed != qrecid:
                    # a smaller, still-present member owns this YUID
                    timer.skip()
                    continue

            with timer.stage("reidentify"):
                rec2 = reider.reidentify(rec)
            if rec2 is None:
                # reidentify already reported why; indexing it would just turn
                # that into a TypeError mid-build
                logger.warning(f" *** Could not reidentify {src['name']}/{recid}")
                continue
            if rec2["yuid"] != yuid:
                # This worker owns `yuid`, not whatever the reidentifier came
                # back with; writing there would collide with the worker that
                # does own it. Means the idmap disagrees with itself about this
                # record, so leave it for identify to fix rather than guessing.
                logger.warning(f"CLUSTER-ESCAPE: {src['name']}/{recid} is in {yuid} but "
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
                logger.error(f"*** Final transform raised exception for {rec2['identifier']}")
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
                logger.warning(f"*** Final transform returned None")

            # record complete: a safe point for the cache to commit its batch
            with timer.stage("checkpoint"):
                merged_cache.checkpoint()
        merged_cache.flush()
        recids = []

    if do_references:
        records_summary = timer.finish()
        # A different shape of work: rebuilding merged records for references,
        # driven by the done_refs file rather than a slice of the record cache.
        timer = PhaseTimer("merge-refs", slice_n=my_slice, max_slice=max_slice,
                           out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir,
                           progress=progress)
        # the records timer's finish() has just reported this worker done and a
        # total that no longer applies; the file is read as it goes, no total
        progress(reset=True, description="references")
        item = 1
        # the YUID comes from the file: write_done_refs resolved it once, which
        # is also what guarantees one line -- and so one worker -- per YUID
        for dist, uri, ext_uri in ref_mgr.iter_done_refs(my_slice, max_slice):
            if not uri:
                logger.warning(f" *** No YUID for reference {ext_uri} from done_refs")
                continue
            yuid = uri.rsplit("/", 1)[-1]
            timer.step()
            if resume:
                with timer.stage("resume_check"):
                    ins_time = merged_cache.metadata(yuid, "insert_time")
                if ins_time is not None:
                    timer.skip()
                    continue

            with timer.stage("idmap_equivs"):
                equivs = idmap[uri]
            if not equivs:
                logger.warning(f"FAILED TO BUILD: {uri}")
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
                (_, claim_src) = claim_member(equivs, key=yuid)
            if claim_src is not None and claim_src["name"] in todo_names:
                timer.skip()
                continue
            # get a base record
            # equivs is a redis set; sort so the chosen base record (and thus
            # label/field precedence in the merged output) is deterministic
            rec2 = None
            stop = False
            for pref in pref_order:
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
                                    logger.warning(f"CLUSTER-ESCAPE: {src['name']}/{recid} is in "
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
                                    logger.warning(f" *** Could not reidentify {src['name']} {recid}")
                if stop:
                    break

            if rec2 is None:
                logger.warning(f" *** Could not find ANY record for {uri} in {equivs}")
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
                    logger.error(f"*** Final transform raised exception for {uri}")
                # Store it
                if rec3 is not None:
                    try:
                        del rec3["identifier"]
                    except:
                        pass
                    with timer.stage("write_merged"):
                        merged_cache[rec3["yuid"]] = rec3
                else:
                    logger.warning(f"*** Final transform returned None")

            with timer.stage("checkpoint"):
                merged_cache.checkpoint()

    summary = timer.finish()
    if do_references:
        references_summary = summary
    else:
        records_summary = summary

    # stop deferring and land everything still outstanding
    merged_cache.resume_commits()

    # force all postgres connections to close
    poolman = PoolManager.get_instance()
    poolman.put_all("localsocket")

    if write_flag:
        with open(os.path.join(cfgs.log_dir, "flags", f"merge_is_done-{my_slice}.txt"), "w") as fh:
            fh.write("1\n")

    return {"records": records_summary, "references": references_summary}


def _estimate(cache):
    """How many records `cache` holds, for the progress total; ``0`` for a
    backend without the Postgres estimate (progress is advisory, and a
    filesystem cache would have to scan to answer)."""
    est = getattr(cache, "len_estimate", None)
    return est() if est is not None else 0
