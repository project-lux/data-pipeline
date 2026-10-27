"""The reconcile phase: run-reconcile.py's body as two functions.

`select_sources` is the script's argument handling: which sources, which
records, in what order. `reconcile_slice` is everything after it: build the
reconciler, the reference manager and the assertion log, walk this slice of
each source's datacache, then claim and process references from the shared
queue, and tidy up. The legacy script parses argv and calls the two; the
manager (item 21) calls them from each worker.

The per-record loops are run-reconcile.py's, moved and not changed: nothing
inside a ``with timer.stage(...)`` block may be edited here. Identity is not
decided in this phase -- each record's equivalents are logged as assertions
and resolved afterwards by run-identify.py -- but what gets logged is exactly
what these loops produce, so the loops are the phase.
"""

import datetime
import logging
import os

from lux_pipeline.process.identity_resolver import IdentityResolver
from lux_pipeline.process.reconciler import Reconciler
from lux_pipeline.process.reference_manager import ReferenceManager
from lux_pipeline.process.timing import PhaseTimer
from lux_pipeline.progress import null_progress
from lux_pipeline.storage.cache.postgres import PoolManager

logger = logging.getLogger("lux_pipeline")


def select_sources(cfgs, names=(), all_internal=False, baseline=False, recids=()):
    """``[[name, cfg, recids], ...]`` in the order the phase processes them.

    ``all_internal`` is every internal source (the script's ``--all``);
    otherwise `names` are internal or external sources (``--<src>``), and an
    unknown name raises ``ValueError``. The list is sorted smallest datacache
    first (``len_estimate()``). `recids` restricts the run to those records,
    which needs exactly one selected source.

    ``baseline`` is the script's ``--baseline``: only the global terms a new
    idmap needs -- the aat globals and language concepts, then the wikidata
    globals -- as an explicit record list per source. As in the script, that
    list is not sorted and `recids` is ignored.
    """
    # NOTE: This implies the existence and use of AAT
    if baseline:
        # Only do global terms. Only needed with a new idmap
        recids = [x for x in list(cfgs.globals_cfg.values()) if x.startswith("3")]
        lngs = cfgs.external["aat"]["mapper"].process_langs.values()
        recids.extend([l.id.replace("http://vocab.getty.edu/aat/", "") for l in lngs])
        to_do = [["aat", cfgs.external["aat"], recids]]

        recids = [x for x in list(cfgs.globals_cfg.values()) if x.startswith("Q")]
        if recids:
            to_do.append(["wikidata", cfgs.external['wikidata'], recids])

        # TODO And now bring in world, continents, countries and states
        # TODO bring in major world eras?
        return to_do

    recids = list(recids)
    if all_internal:
        to_do = list([x,y,[]] for (x,y) in cfgs.internal.items())
    else:
        unknown = [n for n in names if n not in cfgs.internal and n not in cfgs.external]
        if unknown:
            raise ValueError(f"Unknown source: {', '.join(unknown)}")
        to_do = []
        for src, cfg in cfgs.internal.items():
            if src in names:
                to_do.append([src, cfg, []])
        for src, cfg in cfgs.external.items():
            if src in names:
                to_do.append([src, cfg, []])

    if not to_do:
        raise ValueError("No source selected")
    if recids and len(to_do) > 1:
        raise ValueError("Can only build individual records from a single source")
    to_do[0][2] = recids

    # order to_do from smallest to biggest datacache
    s_to_do = [(x, x[1]["datacache"].len_estimate()) for x in to_do]
    s_to_do.sort(key=lambda x: x[1])
    to_do = [x[0] for x in s_to_do]
    return to_do


def new_update_token(cfgs, idmap):
    """Mint the build token for a rebuild: ``__YYYYMMDD__`` for the first
    build of the day, then ``__YYYYMMDDa__``, ``b``, ... -- the letter after
    the one `idmap` holds when its token is today's.

    Written to ``<data_dir>/idmap_update_token.txt``, where every idmap
    reads it when built, and set on `idmap` itself: it was built before
    this and would otherwise stamp the rest of this process's work -- the
    identity resolution in the driver, a worker under the null engine --
    with the old token. manage-data.py ``--new-token`` as a function
    (``lux new-token``, item 22, calls it too). Returns the token.
    """
    now = datetime.datetime.now()
    mm = f"0{now.month}" if now.month < 10 else now.month
    dd = f"0{now.day}" if now.day < 10 else now.day
    stok = f"__{now.year}{mm}{dd}"
    if idmap.update_token.startswith(stok):
        if idmap.update_token[-3].isalpha():
            letter = chr(ord(idmap.update_token[-3]) + 1)
        else:
            letter = "a"
        tok = f"{stok}{letter}__"
    else:
        tok = f"{stok}__"
    fn = os.path.join(cfgs.data_dir, "idmap_update_token.txt")
    with open(fn, "w") as fh:
        fh.write(f"{tok}\n")
    idmap.update_token = tok
    logger.info(f"New update token: {tok}")
    return tok


def reconcile_slice(cfgs, idmap, networkmap, to_do, my_slice=-1, max_slice=-1,
                    do_references=True, progress=null_progress, write_flag=True):
    """Reconcile this slice of every source in `to_do` (from
    `select_sources`), then the references those records led to, and tidy
    up.

    ``my_slice`` / ``max_slice`` are this worker's share of each datacache
    (``-1``: everything, unsliced, in this process). ``do_references=False``
    is the script's ``--norefs``. `progress` is a ``lux_pipeline.progress``
    callable. `write_flag` writes the ``reconcile_is_done-<n>.txt`` flag the
    shell orchestration waits for (sliced runs only).

    Returns ``{"records": <PhaseTimer summary>, "references": <summary, or
    None without the references pass>}``.
    """
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

    logger.info("Starting...")
    logger.info(f"Update token is: {idmap.update_token}")

    # Where the time goes, and whether the phase is cpu bound or waiting. Writes
    # timing-reconcile-<slice>.json next to the logs so a 24-way run can be added
    # up rather than read across 24 files.
    timer = PhaseTimer("reconcile", slice_n=my_slice, max_slice=max_slice,
                       out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir,
                       progress=progress)
    # What this slice expects to see, summed over the sources: a per-source
    # total would read as more than 100% once the second source starts
    running_total = 0
    records_summary = None
    references_summary = None

    for name, cfg, recids in to_do:
        logger.info(f" *** {name} ***")
        timer.mark(name)
        in_db = cfg["datacache"]
        mapper = cfg["mapper"]
        acquirer = cfg["acquirer"]
        running_total += len(recids) if recids else in_db.len_estimate() // max(max_slice, 1)
        progress(total=running_total)

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
                logger.warning(f" *** Failed to acquire any record for {name}/{recid} ***")
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

    # now do references

    if do_references:
        logger.info("\nProcessing References...")
        records_summary = timer.finish()
        # A second phase, not more of the first: it claims from a shared queue
        # rather than walking a slice, so its rate means something different.
        timer = PhaseTimer("reconcile-refs", slice_n=my_slice, max_slice=max_slice,
                           out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir,
                           progress=progress)
        # the records timer's finish() has just reported this worker done and a
        # total that no longer applies; the queue has no total
        progress(reset=True, description="references")
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
                    logger.debug(f"Not processing: {uri}")
                # permanently unusable URI: mark done so it isn't re-queued
                ref_mgr.did_ref(quri, distance)
                continue
            if not source["type"] == "external":
                # Don't process internal or results
                logger.error(f"Got internal reference! {uri}")
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
                logger.warning(f"Failed to acquire {rectype} reference: {source['name']}:{recid}")
                timer.skip()

    summary = timer.finish()
    if do_references:
        references_summary = summary
    else:
        records_summary = summary

    # final tidy up
    assertion_log.close()
    ref_mgr.write_metatypes(my_slice)
    # force all postgres connections to close
    poolman = PoolManager.get_instance()
    poolman.put_all("localsocket")

    # Report Status for orchestration
    if write_flag and my_slice > -1:
        fn = os.path.join(cfgs.log_dir, "flags", f"reconcile_is_done-{my_slice}.txt")
        with open(fn, "w") as fh:
            fh.write("1")

    return {"records": records_summary, "references": references_summary}
