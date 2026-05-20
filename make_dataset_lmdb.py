import gzip
import heapq
import sys
import uuid
import zlib
from collections import defaultdict
from time import time

import lmdb
import ujson as json

# --- Configuration ---

IN_PATH = "/data-export/output/lux/latest/export_full_"
DB_PATH = "/data-io2/distribution/lux_store.lmdb"

slices = 24
recs = 2000000
recs_size = 10000 * recs * slices
idx_size = 128 * recs * slices
total_size = recs_size + idx_size
idx_batch_size = 30


def record_generator(file_path):
    """Yields (uuid_bytes, class_id, raw_line) from a gzipped jsonl file."""
    if file_path.endswith(".gz"):
        f = gzip.open(file_path, "rb")
    else:
        f = open(file_path, "r")
    for line in f:
        data = json.loads(line)["json"]
        uu = data["id"].rsplit("/", 1)[-1]
        uid = uuid.UUID(uu).bytes  # 16 bytes
        cls = data["type"].encode("utf-8")
        yield (uid, cls, data)  # raw_line stays gzipped/binary for storage
    f.close()


def rebuild_database():
    print("Starting build...")
    env = lmdb.open(DB_PATH, map_size=total_size, max_dbs=3, metasync=False, sync=False, map_async=True)
    db = env.open_db(b"data", dupsort=False)
    idx = env.open_db(b"index", dupsort=True)

    batches = defaultdict(list)
    if "--gz" in sys.argv:
        INPUT_FILES = [f"{IN_PATH}{g}.jsonl.gz" for g in range(slices)]
    else:
        INPUT_FILES = [f"{IN_PATH}{g}.jsonl" for g in range(slices)]

    generators = [record_generator(f) for f in INPUT_FILES]
    # heapq.merge yields the smallest (uid, cls, line) across all generators
    merged_stream = heapq.merge(*generators, key=lambda x: x[0])

    n = 0
    start = time()
    txn = env.begin(write=True)

    for key, cls, js in merged_stream:
        value = zlib.compress(json.dumps(js).encode("utf-8"), level=1)
        txn.put(key=key, value=value, db=db, append=True)

        cls_b = batches[cls]
        cls_b.append(key)

        if len(cls_b) == idx_batch_size:
            keys_val = b"".join(batches[cls])
            txn.put(key=cls, value=keys_val, db=idx)
            batches[cls] = []

        n += 1
        if not n % 100000:
            txn.commit()
            txn = env.begin(write=True)
            t = time()
            print(f"{n} records in {t - start:.2f}s = {n / (t - start):.2f} records/s")

    for cls, keys in batches.items():
        if keys:
            txn.put(key=cls, value=b"".join(keys), db=idx)

    txn.commit()
    env.sync()
    env.close()
    print(f"Write took {time() - start:.2f}s")


def batch_read():
    env = lmdb.open(DB_PATH, max_dbs=3, readonly=True, lock=False)

    db = env.open_db(b"data", dupsort=False)
    idx = env.open_db(b"index", dupsort=True)

    print("Starting batch read of places...")
    start = time()

    with env.begin(buffers=True) as txn:
        idxc = txn.cursor(db=idx)
        # Sequential iteration is the fastest for disk
        n = 0
        idxc.set_range(b"Person")
        for packed in idxc.iternext_dup():
            # 30 UUIDs
            bkeys = [packed[i : i + 16] for i in range(0, len(packed), 16)]
            for bk in bkeys:
                value = txn.get(key=bk, db=db)
                data = json.loads(zlib.decompress(value).decode("utf-8"))
                # id = uuid.UUID(bytes=bytes(idb))
                n += 1
                if not n % 50000:
                    t = time()
                    print(f"{n} records in {t - start:.2f}s = {n / (t - start):.2f} records/s")

    total_time = time() - start
    print(f"Batch read of {n} places finished in {total_time:.2f}s ({int(n / total_time)} rec/sec)")


if __name__ == "__main__":
    rebuild_database()
    # batch_read()
