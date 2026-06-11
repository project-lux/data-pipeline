import os
import sys
import zlib
from time import time

import lmdb
import ujson as json
from dotenv import load_dotenv

from pipeline.config import Config
from pipeline.process.reference_manager import ReferenceManager
from pipeline.process.update_manager import UpdateManager

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()


# --- Configuration ---

DB_PATH = "/data-io2/distribution/externals_store.lmdb"
total_recs = 0

sources = []

for ext in cfgs.external.values():
    name = ext["name"]
    recordcache = ext["recordcache"]
    datacache = ext["datacache"]
    recs = recordcache.len_estimate()
    total_recs += recs
    sources.append((name, recordcache, datacache))

total_size = 2048 * total_recs

sources.sort()

print(f"total records: {total_recs}")
print("Starting build...")
env = lmdb.open(
    DB_PATH,
    map_size=total_size,
    max_dbs=3,
    metasync=False,
    sync=False,
    map_async=True,
)
db = env.open_db(b"data", dupsort=False)

n = 0
dead = 0

start = time()
txn = env.begin(write=True)
for name, rcache, dcache in sources:
    # iterate through records
    print(f"Building {name}...")
    for id in rcache.iter_keys():
        id, _ = id.split("##qua")
        js = dcache[id]
        if js is None:
            sys.stdout.write(".")
            sys.stdout.flush()
            dead += 1
            continue
        else:
            js = js["data"]
        key = f"{name}:{id}".encode()

        try:
            value = zlib.compress(json.dumps(js).encode("utf-8"), level=1)
            txn.put(key=key, value=value, db=db, append=True)
        except Exception as e:
            print(f"Failed to compress {id}: {e}")
            continue

        n += 1
        if not (n + dead) % 100000:
            txn.commit()
            txn = env.begin(write=True)
            t = time()
            per = n / (t - start)
            print(
                f"{n} records in {t - start:.2f}s = {per:.2f} records/s. Remaining: {((total_recs - (n + dead)) / per):.2f}s"
            )

txn.commit()
env.sync()
env.close()
print(f"Write took {time() - start:.2f}s")
