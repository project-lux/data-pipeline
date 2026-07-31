import os
import sys

import ujson
from dotenv import load_dotenv


def dumps(obj):
    # ujson is ~2x faster than stdlib json on these documents, but it escapes
    # every / by default -- that would change the output bytes and inflate
    # these URI-heavy documents by ~9%. Off, this is byte-identical to the
    # json.dumps(obj, separators=(",", ":")) it replaces.
    return ujson.dumps(obj, escape_forward_slashes=False)
from pipeline.config import Config
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

merged = cfgs.results["merged"]["recordcache"]
ml = cfgs.results["marklogic"]["recordcache"]
mapper = cfgs.results["marklogic"]["mapper"]

if "--profile" in sys.argv:
    sys.argv.remove("--profile")
    profiling = True
else:
    profiling = False

if len(sys.argv) > 2:
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])
else:
    my_slice = 0
    max_slice = 1

# Only reading from idmap, not writing, so can cache
idmap.enable_memory_cache()

if profiling:
    pr = cProfile.Profile()
    pr.enable()

if not os.path.exists(cfgs.exports_dir):
    os.mkdir(cfgs.exports_dir)

# One commit per re-transformed record was an fsync per write; batch them.
ml.defer_commits(every=500)

fn = os.path.join(cfgs.exports_dir, f"export_full_{my_slice}.jsonl")
with open(fn, "w") as outh:
    x = 0
    # raw: the merged data column arrives as JSON text and is only parsed on
    # the records we actually re-transform
    for rec in merged.iter_records_slice(my_slice, max_slice, raw=True):
        yuid = rec["yuid"]
        # The ML cache persists across builds while YUIDs stay stable, so a
        # bare presence check exported LAST build's document for any entity
        # whose merged record changed. get_fresh() answers presence,
        # staleness and payload in one query -- this used to be three or
        # four round trips per record.
        cached = ml.get_fresh(yuid, since=rec.get("insert_time"), raw=True)
        if cached is not None:
            # already the exact JSON text we want: no parse, no re-serialise
            jstr = cached["data"]
        else:
            rec["data"] = ujson.loads(rec["data"])
            try:
                rec2 = mapper.transform(rec, rec["data"]["type"])
            except Exception as e:
                print(f"{yuid} errored in marklogic mapper: {e}")
                continue
            ml[yuid] = rec2
            jstr = dumps(rec2)
        outh.write(jstr)
        outh.write("\n")
        # record complete: a safe point for the cache to commit its batch
        ml.checkpoint()
        sys.stdout.write(".")
        sys.stdout.flush()
        x += 1
        if profiling and x >= 10000:
            break

ml.resume_commits()


if profiling:
    pr.disable()
    s = io.StringIO()
    sortby = SortKey.CUMULATIVE
    # sortby = SortKey.TIME
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())
    raise ValueError()

# Explicitly force all postgres connections to close
poolman = PoolManager.get_instance()
poolman.put_all("localsocket")

with open(os.path.join(cfgs.log_dir, "flags", f"export_is_done-{my_slice}.txt"), "w") as fh:
    fh.write("1\n")
