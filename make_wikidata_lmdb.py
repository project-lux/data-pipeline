"""Export the wikidata record cache to LMDB, and to ntriples for qlever.

Same layout as make_dataset_lmdb.py:

    data   key -> zlib(json of the mapped record)
    index  class -> packed keys, idx_batch_size at a time (dupsort)

The dataset store's keys are 16 byte UUIDs; here they are the Q number as a
4 byte big-endian uint32, so they are fixed width (the index packing needs
that) and sort numerically. Q123 is (123).to_bytes(4, "big").

The same pass runs every record through the qlever mapper and writes the
triples to NT_PATH, as run-source-build.py does for the other externals.

    python make_wikidata_lmdb.py            # both
    python make_wikidata_lmdb.py --no-nt    # LMDB only
    python make_wikidata_lmdb.py --no-lmdb  # ntriples only
"""

import gzip
import os
import re
import sys
import zlib
from collections import defaultdict
from time import time

import lmdb
import ujson as json
from dotenv import load_dotenv

from pipeline.config import Config
from pipeline.sources.lux.qlever.mapper2 import QleverMapper

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

# --- Configuration ---

DB_PATH = "/data-io2/distribution/wikidata_store.lmdb"
NT_PATH = "/data-export/output/lux/nt/wikidata.nt.gz"

QID = re.compile(r"Q[1-9][0-9]*$")
UINT32_MAX = 2**32 - 1
idx_batch_size = 30

do_lmdb = "--no-lmdb" not in sys.argv
do_nt = "--no-nt" not in sys.argv
if not (do_lmdb or do_nt):
    sys.exit("Nothing to do with both --no-lmdb and --no-nt")

src = cfgs.external["wikidata"]
rcache = src["recordcache"]
total_recs = rcache.len_estimate()
# map_size is only a ceiling on the file, not an allocation
total_size = (8192 + 128) * max(total_recs, 1000000)


def qid_key(identifier):
    """'Q123##quaPerson' -> 4 byte key, or None if it isn't a usable Q-id"""
    qid = identifier.split("##qua", 1)[0]
    if not QID.match(qid):
        return None
    n = int(qid[1:])
    if n > UINT32_MAX:
        return None
    return n.to_bytes(4, "big")


def build():
    print(f"wikidata records (estimate): {total_recs}")
    print("Starting build...")

    if do_lmdb:
        env = lmdb.open(DB_PATH, map_size=total_size, max_dbs=3, metasync=False, sync=False, map_async=True)
        db = env.open_db(b"data", dupsort=False)
        idx = env.open_db(b"index", dupsort=True)
        txn = env.begin(write=True)
    batches = defaultdict(list)

    ql_mpr = QleverMapper(src) if do_nt else None
    fh = gzip.open(NT_PATH, "wt", 1) if do_nt else None

    n = bad = dupes = ql_fail = 0
    start = time()
    try:
        # The cache comes back in no particular order, so no append=True:
        # the records go into the tree wherever their key falls
        for rec in rcache.iter_records():
            js = rec["data"]

            if do_lmdb:
                key = qid_key(rec["identifier"])
                if key is None:
                    print(f"Skipping unusable identifier {rec['identifier']}")
                    bad += 1
                    continue
                value = zlib.compress(json.dumps(js).encode("utf-8"), level=1)
                # a Q-id cached under two types is a leftover from an earlier
                # build; keep the first and say so rather than index it twice
                if not txn.put(key=key, value=value, db=db, overwrite=False):
                    print(f"Duplicate Q-id {rec['identifier']}, keeping the first")
                    dupes += 1
                    continue

                cls = js["type"].encode("utf-8")
                cls_b = batches[cls]
                cls_b.append(key)
                if len(cls_b) == idx_batch_size:
                    txn.put(key=cls, value=b"".join(cls_b), db=idx)
                    batches[cls] = []

            if do_nt:
                try:
                    res = ql_mpr.transform(rec)
                    if res:
                        fh.write("\n".join(res))
                        fh.write("\n")
                except Exception as e:
                    print(f"*** {rec['identifier']} failed in the qlever mapper: {e}")
                    ql_fail += 1

            n += 1
            if not n % 100000:
                if do_lmdb:
                    txn.commit()
                    txn = env.begin(write=True)
                t = time()
                per = n / (t - start)
                print(
                    f"{n} records in {t - start:.2f}s = {per:.2f} records/s. "
                    f"Remaining: {max(total_recs - n, 0) / per:.2f}s"
                )
                sys.stdout.flush()

        if do_lmdb:
            for cls, keys in batches.items():
                if keys:
                    txn.put(key=cls, value=b"".join(keys), db=idx)
            txn.commit()
            env.sync()
    finally:
        if do_lmdb:
            env.close()
        if fh is not None:
            fh.close()

    print(f"Wrote {n} records in {time() - start:.2f}s")
    print(f"  {bad} unusable identifiers, {dupes} duplicate Q-ids, {ql_fail} qlever failures")


if __name__ == "__main__":
    build()
