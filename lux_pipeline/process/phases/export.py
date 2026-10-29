"""The export phase: run-export.py's body, and the two exporters that only
existed as scripts, as three functions.

`export_slice` writes this worker's slice of the merged records as JSONL:
the MarkLogic documents, re-transforming only the records whose merged
record is newer than the cached document, or with ``no_ml`` the merged
records themselves. `export_sources_slice` is run-source-build.py: the
aat / ulan / lcsh / tgn datacaches mapped into their recordcaches and
written out as JSONL and N-Triples. `export_nt_slice` is manage-data.py's
``--nt``: the merged records as N-Triples for the QLever index. The legacy
scripts parse argv and call these; the manager (`ExportManager`, ``lux
export``) calls them from each worker. `export_external` has no script: it
writes the external records a build used in the form ``lux load --type
export`` reads.

The chunked loop in `export_slice` is run-export.py's, moved and not
changed: nothing inside a ``with timer.stage(...)`` block may be edited
here (the one mapper-error message inside a stage is logged instead of
printed, and that is the only line touched).

Export has no resume. The only way to resume a slice exactly is to fix the
order the rows come back in, and the only cheap way to do that is
``synchronize_seqscans = off`` -- which is what lets 24 workers scanning
the merged table share buffer reads instead of each doing its own full
scan. Paying that on every run to save an occasional rerun is the wrong
trade on a box that is bound by random reads. The expensive half is
resumable anyway, in the sense that matters: a document still fresh in the
marklogic cache is not re-transformed, so a rerun re-reads and re-writes
but does not re-map.
"""

import gzip
import logging
import os

import ujson

from lux_pipeline.process.merge_batch import in_batches
from lux_pipeline.process.timing import PhaseTimer
from lux_pipeline.progress import null_progress
from lux_pipeline.sources.lux.qlever.mapper2 import QleverMapper
from lux_pipeline.storage.cache.postgres import PoolManager

logger = logging.getLogger("lux_pipeline")

# Records per marklogic-cache lookup. Trades a chunk of documents held in
# memory against a round trip per record.
CHUNK = 1000

# Compression level for --gz. These documents are repetitive JSON and
# compress roughly an order of magnitude; the level is what the script read
# from LUX_EXPORT_GZIP.
GZIP_LEVEL = 9

# The sources run-source-build.py exports, in its order.
SOURCES = ("aat", "ulan", "lcsh", "tgn")


def dumps(obj):
    return ujson.dumps(obj, escape_forward_slashes=False)


def export_slice(cfgs, my_slice, max_slice, *, no_ml=False, gz=False, chunk_size=CHUNK,
                 gzip_level=GZIP_LEVEL, out_dir=None, progress=null_progress, write_flag=True):
    """Export slice `my_slice` of `max_slice` of the merged records to one
    JSONL file, ``export_full_<n>.jsonl`` in `out_dir` (``cfgs.exports_dir``).

    Each record is the MarkLogic document from the marklogic cache when the
    cached one is at least as new as the merged record, else freshly
    transformed and stored back; `chunk_size` records are checked against
    the cache per query. ``no_ml`` writes the merged records straight out,
    ``export_merged_<n>.jsonl``: no transform, no marklogic cache lookups,
    and no marklogic cache *writes* -- which is the difference between an
    export that grows the database and one that does not. The line is then
    the merged document itself, not the ``{"json": ...}`` envelope the
    marklogic export writes, so downstream readers need to know which they
    are being handed. ``gz`` compresses on the way out at `gzip_level` and
    adds ``.gz``; off by default so the output stays greppable and seekable,
    worth it when the volume is the constraint, which on this data is most
    of the time.

    `progress` is a ``lux_pipeline.progress`` callable. `write_flag` writes
    the ``export_is_done-<n>.txt`` flag the shell orchestration waits for.
    Returns ``{"records": <PhaseTimer summary>, "written": n, "path": fn}``;
    `written` leaves out the records the mapper could not transform.
    """
    idmap = cfgs.get_idmap()
    merged = cfgs.results["merged"]["recordcache"]
    ml = cfgs.results["marklogic"]["recordcache"]
    mapper = cfgs.results["marklogic"]["mapper"]

    # Only reading from idmap, not writing, so can cache
    idmap.enable_memory_cache()

    if out_dir is None:
        out_dir = cfgs.exports_dir
    os.makedirs(out_dir, exist_ok=True)

    stem = "export_merged" if no_ml else "export_full"
    suffix = ".jsonl.gz" if gz else ".jsonl"
    fn = os.path.join(out_dir, f"{stem}_{my_slice}{suffix}")

    timer = PhaseTimer("export", slice_n=my_slice, max_slice=max_slice,
                       out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir,
                       progress=progress)
    progress(total=_estimate(merged) // max(max_slice, 1))

    if not no_ml:
        # One commit per re-transformed record was an fsync per write; batch them.
        ml.defer_commits(every=500)

    logger.info(f"writing {fn}" + (" (merged records, no marklogic mapping)" if no_ml else ""))
    written = 0

    if gz:
        outh = gzip.open(fn, "wt", compresslevel=gzip_level, encoding="utf-8")
    else:
        outh = open(fn, "w")

    with outh:
        # raw: the merged data column arrives as JSON text and is only parsed on
        # the records we actually re-transform
        records = merged.iter_records_slice(my_slice, max_slice, raw=True)
        for chunk in in_batches(records, chunk_size):
            if no_ml:
                # already the JSON text postgres stored: no parse, no re-serialise
                for rec in chunk:
                    outh.write(rec["data"])
                    outh.write("\n")
                    written += 1
            else:
                # The ml cache persists across builds while YUIDs stay stable, so
                # a bare presence check exported LAST build's document for any
                # entity whose merged record changed. Presence, freshness and
                # payload come back together -- and for the whole chunk in one
                # statement, where this was a round trip per record.
                with timer.stage("fetch_cached"):
                    got = ml.get_multi([str(r["yuid"]) for r in chunk], raw=True)
                for rec in chunk:
                    yuid = str(rec["yuid"])
                    row = got.get(yuid)
                    since = rec.get("insert_time")
                    fresh = row is not None and (
                        since is None or (row.get("insert_time") is not None
                                          and row["insert_time"] >= since))
                    if fresh:
                        # already the exact JSON text we want
                        jstr = row["data"]
                    else:
                        with timer.stage("transform"):
                            rec["data"] = ujson.loads(rec["data"])
                            try:
                                rec2 = mapper.transform(rec, rec["data"]["type"])
                            except Exception as e:
                                logger.warning(f"{yuid} errored in marklogic mapper: {e}")
                                continue
                            ml[yuid] = rec2
                            jstr = dumps(rec2)
                    with timer.stage("write"):
                        outh.write(jstr)
                        outh.write("\n")
                    written += 1
                # records complete: a safe point for the cache to commit its batch
                ml.checkpoint()
            # one progress line per interval, not the per-record dot-and-flush
            # this used to do 168M times
            timer.step(len(chunk))

    if not no_ml:
        ml.resume_commits()
    summary = timer.finish()

    # Explicitly force all postgres connections to close
    poolman = PoolManager.get_instance()
    poolman.put_all("localsocket")

    logger.info(f"{written:,} records -> {fn} ({os.path.getsize(fn) / 1e9:.1f} GB"
                + (f" at gzip -{gzip_level})" if gz else ")"))

    if write_flag:
        flagdir = os.path.join(cfgs.log_dir, "flags")
        os.makedirs(flagdir, exist_ok=True)
        with open(os.path.join(flagdir, f"export_is_done-{my_slice}.txt"), "w") as fh:
            fh.write("1\n")

    return {"records": summary, "written": written, "path": fn}


def export_sources_slice(cfgs, my_slice, max_slice, sources=SOURCES, out_dir=None, nt_dir=None,
                         gzip_level=1, progress=null_progress):
    """run-source-build.py: map slice `my_slice` of `max_slice` of each of
    `sources`' datacaches into its recordcache, writing every mapped record
    to ``sources_<n>.jsonl.gz`` in `out_dir` (``cfgs.exports_dir``) and its
    triples, from `QleverMapper`, to ``sources_<n>.nt.gz`` in `nt_dir`
    (`out_dir`). ``my_slice == -1`` is unsliced: the whole datacache, and
    the idmap's memory cache on (parallel workers must not use it).

    The recordcache write is the point as much as the files: these are the
    records the merge reads for the external authorities. A record the
    mapper rejects (``None``) is skipped; one the QLever mapper cannot
    express is written as JSON but not as triples, and logged.

    Returns ``{"records": <summary>, "written": n, "paths": [jsonl, nt]}``.
    """
    idmap = cfgs.get_idmap()
    if my_slice > -1:
        # Running in parallel, will cause cross-process errors
        idmap.disable_memory_cache()
    else:
        # Running single, memory cache will remain accurate
        idmap.enable_memory_cache()

    if out_dir is None:
        out_dir = cfgs.exports_dir
    if nt_dir is None:
        nt_dir = out_dir
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(nt_dir, exist_ok=True)
    nt_fn = os.path.join(nt_dir, f"sources_{my_slice}.nt.gz")
    jsonl_fn = os.path.join(out_dir, f"sources_{my_slice}.jsonl.gz")

    logger.info("Starting...")
    logger.info(f"Update token is: {idmap.update_token}")

    timer = PhaseTimer("export-sources", slice_n=my_slice, max_slice=max_slice,
                       out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir,
                       progress=progress)
    running_total = 0
    written = 0

    with gzip.open(nt_fn, "wt", gzip_level) as fh, \
         gzip.open(jsonl_fn, "wt", gzip_level) as fh2:
        for source in sources:
            src = cfgs.external[source]
            logger.info(f" *** {source} ***")
            timer.mark(source)
            in_db = src["datacache"]
            out_db = src['recordcache']
            mapper = src["mapper"]

            ql_mpr = QleverMapper(src)

            running_total += _estimate(in_db) // max(max_slice, 1)
            progress(total=running_total)

            out_db.defer_commits(every=1000)
            if my_slice == -1:
                itr = in_db.iter_records()
            else:
                itr = in_db.iter_records_slice(my_slice, max_slice)
            for rec in itr:
                rec2 = mapper.transform(rec, None)
                if rec2 is not None:
                    idq = cfgs.make_qua(rec2['identifier'], rec2['data']['type'])
                    rec2['identifier'] = idq
                    out_db[idq] = rec2
                    out_db.checkpoint()

                    # Export JSON to JSONL files
                    jstr = ujson.dumps(rec2['data'])
                    fh2.write(jstr + "\n")

                    # Export NTriples
                    try:
                        res = ql_mpr.transform(rec2)
                        if res:
                            fh.write("\n".join(res))
                            fh.write("\n")
                    except Exception as e:
                        logger.warning(f"*** {rec2.get('yuid', '?')} failed in the qlever mapper: {e}")
                    written += 1
                    timer.step()
                else:
                    timer.skip()
            out_db.flush()
            # stop deferring and land anything still outstanding
            out_db.resume_commits()

    summary = timer.finish()

    # force all postgres connections to close
    poolman = PoolManager.get_instance()
    poolman.put_all("localsocket")

    logger.info(f"{written:,} records -> {jsonl_fn}, {nt_fn}")
    return {"records": summary, "written": written, "paths": [jsonl_fn, nt_fn]}


def export_nt_slice(cfgs, my_slice, max_slice, out_dir=None, gzip_level=1, progress=null_progress):
    """manage-data.py ``--nt``: slice `my_slice` of `max_slice` of the merged
    records as N-Triples, from `QleverMapper` over the marklogic results
    configuration, to ``lux_<n>.nt.gz`` in `out_dir` (``cfgs.exports_dir``).
    ``my_slice == -1`` is unsliced. A record the mapper cannot express is
    logged and skipped rather than ending the slice.

    Returns ``{"records": <summary>, "written": n, "path": fn}``.
    """
    if out_dir is None:
        out_dir = cfgs.exports_dir
    os.makedirs(out_dir, exist_ok=True)
    fn = os.path.join(out_dir, f"lux_{my_slice}.nt.gz")

    mpr = QleverMapper(cfgs.results["marklogic"])
    rc = cfgs.results["merged"]["recordcache"]

    timer = PhaseTimer("export-nt", slice_n=my_slice, max_slice=max_slice,
                       out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir,
                       progress=progress)
    progress(total=_estimate(rc) // max(max_slice, 1))
    written = 0

    with gzip.open(fn, "wt", gzip_level) as fh:
        # Iterate whole records rather than keys-then-fetch-each-key: the rows
        # arrive in the server-side cursor that found them, so this drops a
        # round trip per record -- tens of millions of them. Nothing here
        # writes back, so there is no reason to go and re-read each row.
        #
        # The jsonb -> dict parse already runs through ujson: postgres.py
        # registers it as the global jsonb loads, so raw=True plus an explicit
        # ujson.loads would be the same work in a different place. It only
        # pays where the JSON is passed straight through unparsed, as in
        # export_slice, and the mapper needs the dict.
        if my_slice == -1:
            itr = rc.iter_records()
        else:
            itr = rc.iter_records_slice(my_slice, max_slice)

        for rec in itr:
            try:
                res = mpr.transform(rec)
            except Exception as e:
                # One unmappable record used to end the whole slice's export
                logger.warning(f"*** {rec.get('yuid', '?')} failed in the qlever mapper: {e}")
                timer.skip()
                continue
            # One write per record rather than one per triple: there are tens
            # of triples each, and every write goes through gzip
            if res:
                fh.write("\n".join(res))
                fh.write("\n")
            written += 1
            timer.step()

    summary = timer.finish()

    # force all postgres connections to close
    poolman = PoolManager.get_instance()
    poolman.put_all("localsocket")

    logger.info(f"{written:,} records -> {fn}")
    return {"records": summary, "written": written, "path": fn}


def export_external(cfgs, name, out_dir=None, chunk_size=CHUNK, gzip_level=1,
                    progress=null_progress):
    """The datacache records of external source `name` that its recordcache
    was built from, to ``export_<name>.jsonl.gz`` in `out_dir`
    (``cfgs.exports_dir``): one ``<identifier>\t{"data": ...}`` line per
    record, the file `Loader.load_export2` -- ``lux load --type export`` --
    reads from ``<dumps_dir>/harvested/``. Another instance can then load
    just the external records a build used instead of whole dumps.

    The recordcache holds one record per identifier and type
    (``<identifier>##qua<type>``); each identifier is written once. A key
    whose datacache record has gone is counted as `missing` and skipped.
    The datacache is read `chunk_size` identifiers per query, and the JSON
    text is passed through unparsed. An empty recordcache leaves no file.

    Returns ``{"source": name, "written": n, "missing": n, "path": fn}``,
    with ``path`` None when nothing was written.
    """
    src = cfgs.external[name]
    rc = src["recordcache"]
    dc = src["datacache"]

    if out_dir is None:
        out_dir = cfgs.exports_dir
    os.makedirs(out_dir, exist_ok=True)
    fn = os.path.join(out_dir, f"export_{name}.jsonl.gz")

    progress(description=name, total=_estimate(rc))
    seen = set()

    def identifiers():
        for key in rc.iter_keys():
            progress(advance=1)
            ident = cfgs.split_qua(key)[0]
            if ident not in seen:
                seen.add(ident)
                yield ident

    written = missing = 0
    with gzip.open(fn, "wt", gzip_level, encoding="utf-8") as fh:
        for chunk in in_batches(identifiers(), chunk_size):
            got = _fetch_multi(dc, chunk)
            for ident in chunk:
                rec = got.get(ident)
                if rec is None:
                    missing += 1
                    continue
                data = rec["data"]
                if not isinstance(data, str):
                    data = dumps(data)
                fh.write(f'{ident}\t{{"data":{data}}}\n')
                written += 1

    # force all postgres connections to close
    poolman = PoolManager.get_instance()
    poolman.put_all("localsocket")

    if missing:
        logger.warning(f"{name}: {missing:,} identifiers in the recordcache have no datacache record")
    if not written:
        os.remove(fn)
        logger.info(f"{name}: nothing to export")
        return {"source": name, "written": 0, "missing": missing, "path": None}
    logger.info(f"{written:,} records -> {fn}")
    return {"source": name, "written": written, "missing": missing, "path": fn}


def _fetch_multi(cache, keys):
    """``{key: record}`` for those of `keys` that `cache` holds, with the
    data as JSON text where the backend can hand it over unparsed: one query
    for Postgres, a lookup per key for anything else."""
    get_multi = getattr(cache, "get_multi", None)
    if get_multi is not None:
        return get_multi(keys, raw=True)
    got = {}
    for k in keys:
        rec = cache[k]
        if rec is not None:
            got[k] = rec
    return got


def _estimate(cache):
    """How many records `cache` holds, for the progress total: Postgres
    estimates, FsCache counts its files; ``0`` for a store without
    len_estimate (progress is advisory)."""
    est = getattr(cache, "len_estimate", None)
    return est() if est is not None else 0
