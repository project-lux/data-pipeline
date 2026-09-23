"""Export merged records as gzipped JSONL, one file per slice.

    python run-export.py 0 24             # marklogic documents, raw jsonl
    python run-export.py 0 24 --gz        # gzip -9 instead
    python run-export.py 0 24 --no-ml     # the merged records themselves

No resume. The only way to resume a slice exactly is to fix the order the
rows come back in, and the only cheap way to do that is
`synchronize_seqscans = off` -- which is what lets 24 workers scanning this
table share buffer reads instead of each doing its own full scan. Paying
that on every run to save an occasional rerun is the wrong trade on a box
that is bound by random reads.

The expensive half is resumable anyway, in the sense that matters: a
document still fresh in the marklogic cache is not re-transformed, so a
rerun re-reads and re-writes but does not re-map.
"""

import cProfile
import gzip
import io
import os
import pstats
import sys
from pstats import SortKey

import ujson
from dotenv import load_dotenv

from pipeline.config import Config
from pipeline.process.merge_batch import in_batches
from pipeline.process.timing import PhaseTimer
from pipeline.storage.cache.postgres import PoolManager


def dumps(obj):
    return ujson.dumps(obj, escape_forward_slashes=False)


load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

merged = cfgs.results["merged"]["recordcache"]
ml = cfgs.results["marklogic"]["recordcache"]
mapper = cfgs.results["marklogic"]["mapper"]

profiling = "--profile" in sys.argv
if profiling:
    sys.argv.remove("--profile")

# Straight out of the merged cache, no marklogic mapping: no transform, no
# ml cache lookups, and no ml cache *writes* -- which is the difference
# between an export that grows the database and one that does not. The line
# is then the merged document itself, not the {"json": ...} envelope the
# marklogic export writes, so downstream readers need to know which they
# are being handed.
NO_ML = "--no-ml" in sys.argv

# Compress on the way out. Off by default so the output stays greppable and
# seekable; worth it when the volume is the constraint, which on this data
# is most of the time -- these documents are repetitive JSON and compress
# roughly an order of magnitude.
GZ = "--gz" in sys.argv

if len(sys.argv) > 2:
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])
else:
    my_slice = 0
    max_slice = 1

# Records per marklogic-cache lookup. Trades a chunk of documents held in
# memory against a round trip per record.
CHUNK = int(os.getenv("LUX_EXPORT_CHUNK", "1000"))
GZIP_LEVEL = int(os.getenv("LUX_EXPORT_GZIP", "9"))

# Only reading from idmap, not writing, so can cache
idmap.enable_memory_cache()

if not os.path.exists(cfgs.exports_dir):
    os.mkdir(cfgs.exports_dir)

stem = "export_merged" if NO_ML else "export_full"
suffix = ".jsonl.gz" if GZ else ".jsonl"
fn = os.path.join(cfgs.exports_dir, f"{stem}_{my_slice}{suffix}")

timer = PhaseTimer("export", slice_n=my_slice, max_slice=max_slice,
                   out_dir=cfgs.log_dir if hasattr(cfgs, "log_dir") else cfgs.data_dir)

if not NO_ML:
    # One commit per re-transformed record was an fsync per write; batch them.
    ml.defer_commits(every=500)

if profiling:
    pr = cProfile.Profile()
    pr.enable()

print(f"writing {fn}" + (" (merged records, no marklogic mapping)" if NO_ML else ""))
written = 0

if GZ:
    outh = gzip.open(fn, "wt", compresslevel=GZIP_LEVEL, encoding="utf-8")
else:
    outh = open(fn, "w")

with outh:
    # raw: the merged data column arrives as JSON text and is only parsed on
    # the records we actually re-transform
    records = merged.iter_records_slice(my_slice, max_slice, raw=True)
    for chunk in in_batches(records, CHUNK):
        if NO_ML:
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
                            print(f"{yuid} errored in marklogic mapper: {e}")
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
        if profiling and written >= 10000:
            break

if not NO_ML:
    ml.resume_commits()
timer.finish()

if profiling:
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats(SortKey.CUMULATIVE)
    ps.print_stats()
    print(s.getvalue())
    raise ValueError()

# Explicitly force all postgres connections to close
poolman = PoolManager.get_instance()
poolman.put_all("localsocket")

print(f"{written:,} records -> {fn} ({os.path.getsize(fn) / 1e9:.1f} GB"
      + (f" at gzip -{GZIP_LEVEL})" if GZ else ")"))

flagdir = os.path.join(cfgs.log_dir, "flags")
os.makedirs(flagdir, exist_ok=True)
with open(os.path.join(flagdir, f"export_is_done-{my_slice}.txt"), "w") as fh:
    fh.write("1\n")
