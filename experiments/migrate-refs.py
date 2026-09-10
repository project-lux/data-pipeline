# Copy all_refs and done_refs out of redis into their postgres tables.
#
#   python migrate-refs.py                 # copy both, then verify
#   python migrate-refs.py --verify-only   # just compare the two
#   python migrate-refs.py --map all_refs  # one of them
#
# Usually you do NOT need this. Both maps are transient: all_refs is the
# pending-reference queue and done_refs is drained into reference_uris.txt by
# write_done_refs(), and manage-data.py clears both between builds. A fresh
# reconcile refills them from nothing.
#
# It matters in one case: a build interrupted partway through the reference
# pass, where whatever is left in all_refs is unprocessed work that would
# otherwise be lost when you switch backends. Copying it costs seconds.
#
# Redis is only read.

import argparse
import json
import os
import sys
import time

import psycopg2
import psycopg2.extras
import redis
from dotenv import load_dotenv

from pipeline.config import Config
from pipeline.storage.idmap.postgres import ReferenceMap


def redis_conn(cfg):
    return redis.Redis(host=cfg.get("host", "localhost"),
                       port=int(cfg.get("port", 6379)),
                       db=int(cfg.get("db", 3)),
                       decode_responses=True,
                       socket_connect_timeout=10)


def read_batches(conn, batch=2000):
    """(key, fields) out of redis, one pipelined HGETALL per batch."""
    keys = []
    for key in conn.scan_iter(count=batch):
        keys.append(key)
        if len(keys) >= batch:
            yield from _hydrate(conn, keys)
            keys = []
    if keys:
        yield from _hydrate(conn, keys)


def _hydrate(conn, keys):
    with conn.pipeline(transaction=False) as pipe:
        for k in keys:
            pipe.hgetall(k)
        # a stray non-hash key answers WRONGTYPE; skip it rather than losing
        # the batch
        res = pipe.execute(raise_on_error=False)
    for k, fields in zip(keys, res):
        if isinstance(fields, Exception) or not fields:
            continue
        yield (k, fields)


def copy(rconn, store, batch=2000):
    print(f"--- {store.table}")
    n = skipped = 0
    start = time.time()
    rows = []

    def flush():
        nonlocal rows
        if not rows:
            return
        with store.conn.cursor() as cur:
            # An absent field stays NULL rather than becoming "": redis has no
            # entry for it, and _fields() omits a NULL, so this reproduces
            # exactly what get() answered before.
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {store.table} (uri, dist, ctype) VALUES %s "
                f"ON CONFLICT (uri) DO UPDATE SET dist = EXCLUDED.dist, "
                f"ctype = EXCLUDED.ctype",
                rows, page_size=len(rows))
        rows = []

    for (key, fields) in read_batches(rconn, batch):
        dist = fields.get("dist")
        try:
            dist = None if dist is None else int(dist)
        except ValueError:
            print(f"    !! {key} has a non-numeric dist {dist!r}; skipping")
            skipped += 1
            continue
        rows.append((key, dist, fields.get("type")))
        n += 1
        if len(rows) >= batch:
            flush()
        if n and not n % 200000:
            print(f"    {n:,} in {time.time() - start:.0f}s")
            sys.stdout.flush()
    flush()
    print(f"  copied {n:,} references in {time.time() - start:.1f}s"
          + (f", {skipped} skipped" if skipped else ""))
    return n


def verify(rconn, store, sample=2000):
    """Counts, then a field-by-field comparison of a sample."""
    r_total = rconn.dbsize()
    p_total = len(store)
    print(f"  redis {r_total:,} keys   postgres {p_total:,} rows   "
          f"{'MATCH' if r_total == p_total else 'DIFFER'}")
    checked = bad = 0
    diffs = []
    for (key, fields) in read_batches(rconn, 1000):
        expect = {}
        if "dist" in fields:
            expect["dist"] = int(fields["dist"])
        if "type" in fields:
            expect["type"] = fields["type"]
        got = store.get(key)
        checked += 1
        if got != expect:
            bad += 1
            if len(diffs) < 5:
                diffs.append((key, expect, got))
        if checked >= sample:
            break
    print(f"  compared {checked:,} references, {bad} differing")
    for (k, want, got) in diffs:
        print(f"     {k}\n       redis    {want}\n       postgres {got}")
    return r_total == p_total and not bad


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--map", default="all_refs,done_refs",
                    help="which maps to copy (default both)")
    ap.add_argument("--table", default=None,
                    help="target table name; only valid with a single --map")
    ap.add_argument("--verify-only", action="store_true")
    ap.add_argument("--sample", type=int, default=2000,
                    help="references to compare field-by-field (default 2000)")
    ap.add_argument("--batch", type=int, default=2000)
    args = ap.parse_args()

    load_dotenv()
    cfgs = Config(basepath=os.getenv("LUX_BASEPATH", ""))
    names = [m.strip() for m in args.map.split(",") if m.strip()]
    if args.table and len(names) != 1:
        raise SystemExit("--table needs exactly one --map")

    ok = True
    for name in names:
        mcfg = cfgs.map_stores.get(name)
        if mcfg is None:
            raise SystemExit(f"no such map store: {name}")
        rconn = redis_conn(mcfg)
        store = ReferenceMap({"all_configs": cfgs, "name": args.table or name})
        print(f"# redis db {mcfg.get('db')} -> postgres {store.table}")
        if not args.verify_only:
            copy(rconn, store, args.batch)
        ok = verify(rconn, store, args.sample) and ok
        print()

    print("Every reference matches." if ok
          else "!! differences above -- do not switch storeClass yet")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
