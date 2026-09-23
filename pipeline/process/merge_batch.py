"""Resolving a slice of records against the idmap a batch at a time.

run-merge.py walks a slice and asks the idmap two questions about every
record: what YUID is it in, and has that YUID already been built. Asked one
record at a time, on a map whose working set does not fit in memory, that is
the phase. Measured on a resumed run of 20.7M records:

    idmap_cluster   22.64 worker-hours   72.6%   3,930 us/call
    resume_check     3.98 worker-hours   12.8%     690 us/call

97.4% of those records were already built, so nearly all of that was a
cluster scan and a metadata lookup whose answers were discarded. Batched,
the same walk is two statements per 500 records.

Three things make it safe to hoist the lookups out of the loop:

*   The prefetches land in the idmap's memory cache under exactly the keys
    `get_cluster()` reads -- the forward pointer under the member uri, the
    member set under the YUID -- so the loop's call is a memory hit, not a
    second statement, and a miss still works.
*   Which worker builds which cluster is decided by `claim_member()`, not by
    the resume check. Two records of one cluster landing in the same batch
    therefore still produce one merged record: the second is dropped by the
    claim, where before it was dropped by the check. The resume check has
    only ever been an optimisation -- see the note on the claim in
    run-merge.py.
*   A key the batch cannot resolve is asked for directly, so a failed batch
    degrades to the per-record behaviour rather than writing off 500
    records.
"""

import os

# Records resolved per round trip. The record cursor already buffers 1000,
# so the marginal memory here is small; the marginal saving above ~500 is
# also small, since the round trips are already amortised.
MERGE_BATCH = int(os.getenv("LUX_MERGE_BATCH", "500"))


def in_batches(it, n):
    """Consecutive lists of at most n items, without reading it all in."""
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def built_yuids(cache, yuids):
    """Which of these YUIDs the merged cache already holds.

    has_multi() is the postgres batch check. A filesystem-backed merged
    cache has neither that nor metadata() -- so --resume never worked
    against one -- but it does have has_item(), so fall back to that rather
    than require the batch method."""
    batch = getattr(cache, "has_multi", None)
    if batch is not None:
        return set(batch(yuids))
    return {y for y in yuids if cache.has_item(y)}


def prefetched(records, src, cfgs, idmap, merged_cache, timer, resume,
               batch_size=MERGE_BATCH):
    """Records with their idmap lookups already answered, a batch at a time.

    Yields `(rec, recid, recuri, qrecid, full_yuid, already_built)` in the
    order the records arrived, one tuple per record, so the caller's loop is
    unchanged apart from not doing the lookups itself. `full_yuid` is None
    when the record is not in the map at all.
    """
    for chunk in in_batches(records, batch_size):
        pending = []
        for rec in chunk:
            timer.step()
            recid = rec["identifier"]
            # get() stamps this on every row it returns and merger.merge()
            # needs it; the iterators don't, so set it here for all 3 paths
            rec["source"] = src["name"]
            recuri = f"{src['namespace']}{recid}"
            pending.append((rec, recid, recuri,
                            cfgs.make_qua(recuri, rec["data"]["type"])))

        with timer.stage("prefetch_yuid"):
            fwd = idmap.get_multi([p[3] for p in pending])

        # Costs one round trip per record that is genuinely absent -- one
        # that is about to be reported missing anyway -- and keeps a failed
        # batch from silently skipping the whole chunk.
        missing = [p[3] for p in pending if fwd.get(p[3]) is None]
        if missing:
            with timer.stage("idmap_single"):
                for qrecid in missing:
                    fwd[qrecid] = idmap[qrecid]

        built = set()
        if resume:
            with timer.stage("resume_check"):
                built = built_yuids(
                    merged_cache,
                    [f.rsplit("/", 1)[1] for f in fwd.values() if f])

        # Member sets only for the records that will actually be built. On a
        # resumed run that is a few percent of the batch, which is the whole
        # reason the resume check comes first: the member scan is the
        # expensive half, and on a skipped record it is pure waste.
        wanted = []
        seen = set()
        for (_rec, _recid, _recuri, qrecid) in pending:
            full = fwd.get(qrecid)
            # deduped: several records of one cluster share a YUID, and an
            # oversized cluster would otherwise send the same key hundreds
            # of times in one ANY()
            if full and full not in seen and full.rsplit("/", 1)[1] not in built:
                seen.add(full)
                wanted.append(full)
        if wanted:
            with timer.stage("prefetch_cluster"):
                idmap.get_multi(wanted)

        for (rec, recid, recuri, qrecid) in pending:
            full = fwd.get(qrecid)
            yield (rec, recid, recuri, qrecid, full,
                   bool(full) and full.rsplit("/", 1)[1] in built)
