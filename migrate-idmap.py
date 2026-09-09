# Copy the identity map out of redis into the postgres tables, and check that
# what arrived is what left.
#
#   python migrate-idmap.py                              # copy, then verify
#   python migrate-idmap.py --table testidmap --verify-pct 0.5
#   python migrate-idmap.py --verify-only                # just compare the two
#
# Redis is only read, and the script waits for it if it is still loading a
# dump. Postgres is written with COPY into the tables the new backend uses;
# --table parks them somewhere else while testing.
#
# The forward pointers are the map. The reverse sets are not copied, because
# in the new schema they are derived: a YUID's members are the rows carrying
# it. What is copied from the sets is the registry of minted YUIDs (so a YUID
# with no members still exists, which set() checks) and the update token,
# which stops being a pseudo-member and becomes a column.
#
# Shape of the load, which is what matters at ~100M keys: the tables are
# created bare, COPYed into, and only then given their primary keys and
# indexes. Building a text primary key incrementally during the load costs
# several times as much, and a stall halfway through a run this long is
# expensive.

import argparse
import io
import json
import os
import random
import sys
import time

import psycopg2
import redis
from dotenv import load_dotenv

# Bare on purpose: constraints go on after the rows are in.
DDL_BARE = """
DROP TABLE IF EXISTS {idmap};
DROP TABLE IF EXISTS {yuids};
CREATE TABLE {idmap} (uri TEXT NOT NULL, yuid TEXT NOT NULL);
CREATE TABLE {yuids} (yuid TEXT NOT NULL, token TEXT, minted TIMESTAMP DEFAULT now());
"""


def is_token(member):
    return member.startswith("__") and member.endswith("__")


def esc(s):
    """COPY text format. These are URIs, but one stray tab or backslash would
    silently shift a column, and a bad row is worse than a slow one."""
    return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def wait_for_redis(conn, timeout=7200):
    """Block until redis has finished loading its dump.

    A redis restoring an RDB answers every command with LOADING, so a run that
    starts against one still coming up dies on its first call."""
    start = time.time()
    waited = False
    while True:
        try:
            info = conn.info("persistence")
            if not info.get("loading") and not info.get("async_loading"):
                if waited:
                    print(f"    ready after {time.time() - start:.0f}s")
                return
            pct = info.get("loading_loaded_perc")
            print(f"    loading{f' {pct:.1f}%' if pct else ''}...")
        except redis.exceptions.BusyLoadingError:
            print("    redis is loading the dataset into memory...")
        except redis.exceptions.ConnectionError as e:
            print(f"    waiting for redis: {str(e)[:80]}")
        waited = True
        if time.time() - start > timeout:
            raise SystemExit(f"redis still not ready after {timeout}s")
        sys.stdout.flush()
        time.sleep(5)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--table", default="idmap", help="target table name (default idmap)")
    ap.add_argument("--batch", type=int, default=200000, help="rows buffered per COPY")
    ap.add_argument("--scan-count", type=int, default=5000, help="redis SCAN batch size")
    ap.add_argument("--verify-pct", type=float, default=0.5,
                    help="percent of keys to read back and compare; 0 compares counts only")
    ap.add_argument("--verify-only", action="store_true")
    ap.add_argument("--redis-host", default=None)
    ap.add_argument("--redis-port", type=int, default=None)
    ap.add_argument("--redis-db", type=int, default=None)
    ap.add_argument("--pg-dbname", default=None)
    ap.add_argument("--pg-user", default=None)
    ap.add_argument("--pg-host", default=None)
    args = ap.parse_args()

    load_dotenv()
    basepath = os.getenv("LUX_BASEPATH", "")

    rcfg = {}
    f = os.path.join(basepath, "config_cache", "map_idmap.json")
    if os.path.exists(f):
        rcfg = json.load(open(f))
    rdb = args.redis_db if args.redis_db is not None else int(rcfg.get("db", 0))
    rconn = redis.Redis(host=args.redis_host or rcfg.get("host", "localhost"),
                        port=args.redis_port or int(rcfg.get("port", 6379)),
                        db=rdb, decode_responses=True,
                        socket_connect_timeout=10, socket_keepalive=True)
    print("# waiting for redis")
    wait_for_redis(rconn)

    pcfg = {}
    f = os.path.join(basepath, "config_cache", "caches.json")
    if os.path.exists(f):
        pcfg = json.load(open(f))
    kw = {"user": args.pg_user or pcfg.get("user") or os.getenv("USER"),
          "dbname": args.pg_dbname or pcfg.get("dbname") or os.getenv("USER"),
          "keepalives": 1, "keepalives_idle": 30}
    host = args.pg_host if args.pg_host is not None else pcfg.get("host", "")
    if host:
        kw["host"] = host
        kw["port"] = int(pcfg.get("port", 5432))
        if pcfg.get("password"):
            kw["password"] = pcfg["password"]
    conn = psycopg2.connect(**kw)
    conn.autocommit = True

    table = args.table
    ytable = f"{table}_yuid"
    total_keys = rconn.dbsize()
    print(f"# redis db {rdb}: {total_keys:,} keys -> postgres {kw['dbname']}.{table}")
    with conn.cursor() as cur:
        # A rebuildable copy of data that still exists in redis: if this dies
        # halfway we start again, so don't pay an fsync per commit for it
        cur.execute("SET synchronous_commit = off")
        cur.execute("SET maintenance_work_mem = '2GB'")
        cur.execute("SET work_mem = '256MB'")

    if not args.verify_only:
        migrate(rconn, conn, table, ytable, args.batch, args.scan_count, total_keys)
    verify(rconn, conn, table, ytable, args.verify_pct, args.scan_count)


def migrate(rconn, conn, table, ytable, batch_size, scan_count, total_keys):
    with conn.cursor() as cur:
        cur.execute(DDL_BARE.format(idmap=table, yuids=ytable))
    print("--- copying")
    start = time.time()

    n_fwd = n_yuid = n_token = 0
    fwd_buf, yuid_buf = io.StringIO(), io.StringIO()
    buffered = {"fwd": 0, "yuid": 0}
    last_report = [0]

    def flush_fwd():
        nonlocal fwd_buf
        if not buffered["fwd"]:
            return
        fwd_buf.seek(0)
        with conn.cursor() as cur:
            cur.copy_expert(f"COPY {table} (uri, yuid) FROM STDIN", fwd_buf)
        fwd_buf = io.StringIO()
        buffered["fwd"] = 0

    def flush_yuid():
        nonlocal yuid_buf
        if not buffered["yuid"]:
            return
        yuid_buf.seek(0)
        with conn.cursor() as cur:
            cur.copy_expert(f"COPY {ytable} (yuid, token) FROM STDIN", yuid_buf)
        yuid_buf = io.StringIO()
        buffered["yuid"] = 0

    def drain_strings(keys):
        nonlocal n_fwd
        # MGET gives nil for a key holding a non-string, so anything in the db
        # that isn't a forward pointer is skipped rather than mangled
        for k, v in zip(keys, rconn.mget(keys)):
            if v is None:
                continue
            fwd_buf.write(f"{esc(k)}\t{esc(v)}\n")
            buffered["fwd"] += 1
            n_fwd += 1
        if buffered["fwd"] >= batch_size:
            flush_fwd()

    def drain_sets(keys):
        nonlocal n_yuid, n_token
        with rconn.pipeline(transaction=False) as pipe:
            for k in keys:
                pipe.smembers(k)
            # A key under yuid: that isn't a set answers WRONGTYPE; take the
            # error per key rather than losing the batch and the run with it
            results = pipe.execute(raise_on_error=False)
        for k, members in zip(keys, results):
            if isinstance(members, Exception):
                print(f"    skipping {k}: {str(members)[:60]}")
                continue
            tokens = [m for m in members if is_token(m)]
            # \N is COPY's null marker: a yuid never seen in a build has none
            token = esc(tokens[0]) if tokens else "\\N"
            if tokens:
                n_token += 1
            yuid_buf.write(f"{esc(k)}\t{token}\n")
            buffered["yuid"] += 1
            n_yuid += 1
        if buffered["yuid"] >= batch_size:
            flush_yuid()

    def report():
        done = n_fwd + n_yuid
        if done - last_report[0] < 2000000:
            return
        last_report[0] = done
        el = time.time() - start
        rate = done / el if el else 0
        left = (total_keys - done) / rate if rate else 0
        print(f"    {done:,}/{total_keys:,} keys in {el / 60:.0f}m "
              f"({rate:,.0f}/s, ~{left / 60:.0f}m left) -- "
              f"{n_fwd:,} pointers, {n_yuid:,} yuids")
        sys.stdout.flush()

    strings, sets = [], []
    for key in rconn.scan_iter(count=scan_count):
        (sets if key.startswith("yuid:") else strings).append(key)
        if len(strings) >= 1000:
            drain_strings(strings)
            strings = []
            report()
        if len(sets) >= 1000:
            drain_sets(sets)
            sets = []
            report()
    if strings:
        drain_strings(strings)
    if sets:
        drain_sets(sets)
    flush_fwd()
    flush_yuid()

    el = time.time() - start
    print(f"  {n_fwd:,} forward pointers")
    print(f"  {n_yuid:,} yuids registered, {n_token:,} carrying an update token")
    print(f"  copied in {el / 60:.1f} min ({(n_fwd + n_yuid) / max(el, 1):,.0f} keys/s)")
    sys.stdout.flush()

    print("--- constraints")
    steps = [
        (f"ALTER TABLE {ytable} ADD PRIMARY KEY (yuid)", "yuid primary key"),
        # ON CONFLICT does the de-duplicating, which needs that key in place.
        # An anti-join here instead (NOT IN, or NOT EXISTS) ran for twelve
        # minutes on a million rows in testing; this is the same job in a pass.
        (f"INSERT INTO {ytable} (yuid) SELECT DISTINCT yuid FROM {table} "
         f"ON CONFLICT (yuid) DO NOTHING", "yuids that had no set key"),
        (f"ALTER TABLE {table} ADD PRIMARY KEY (uri)", "uri primary key"),
        (f"CREATE INDEX {table}_yuid_idx ON {table} (yuid)", "reverse index"),
        (f"CREATE INDEX {ytable}_token_idx ON {ytable} (token)", "token index"),
        (f"ANALYZE {table}", "analyze"),
        (f"ANALYZE {ytable}", "analyze yuids"),
    ]
    for sql, label in steps:
        t0 = time.time()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                extra = f", {cur.rowcount:,} rows" if cur.rowcount and cur.rowcount > 0 else ""
        except psycopg2.errors.UniqueViolation as e:
            print(f"  !! {label} failed on a duplicate: {str(e).strip()[:160]}")
            print(f"     redis keys are unique, so this means the copy doubled a row. Find them:")
            print(f"       SELECT uri, count(*) FROM {table} GROUP BY uri HAVING count(*) > 1 LIMIT 5;")
            raise
        print(f"  {label:<34} {time.time() - t0:>6.0f}s{extra}")
        sys.stdout.flush()

    with conn.cursor() as cur:
        cur.execute("SELECT pg_total_relation_size(%s) + pg_total_relation_size(%s)",
                    (table, ytable))
        size = cur.fetchone()[0]
    print(f"  {size / 1e9:.1f}GB on disk")
    print(f"  total {(time.time() - start) / 60:.1f} min")


def verify(rconn, conn, table, ytable, pct, scan_count):
    """Read keys back out of both and compare. The forward direction has to
    match exactly; the reverse has to match after dropping the update tokens,
    which are a column here rather than members."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        pg_fwd = cur.fetchone()[0]
        cur.execute(f"SELECT count(*) FROM {ytable}")
        pg_yuid = cur.fetchone()[0]
    total_keys = rconn.dbsize()
    print(f"\n--- verifying")
    print(f"  redis holds {total_keys:,} keys; postgres has {pg_fwd:,} pointers "
          f"and {pg_yuid:,} yuids ({pg_fwd + pg_yuid:,} together)")
    if pct <= 0:
        print("  --verify-pct 0: counts only")
        return
    print(f"  reading back {pct}% of keys from both")

    r_fwd = r_yuid = checked_f = checked_s = 0
    n_missing = n_bad_fwd = n_bad_set = 0
    missing, bad_fwd, bad_set = [], [], []
    rate = pct / 100.0
    rng = random.Random(20260901)
    start = time.time()
    last = [0]

    def check_strings(keys):
        nonlocal checked_f, n_missing, n_bad_fwd
        vals = rconn.mget(keys)
        with conn.cursor() as cur:
            cur.execute(f"SELECT uri, yuid FROM {table} WHERE uri = ANY(%s)", (keys,))
            got = dict(cur.fetchall())
        for k, v in zip(keys, vals):
            if v is None:
                continue
            checked_f += 1
            if got.get(k) != v:
                if k not in got:
                    n_missing += 1
                    if len(missing) < 10:
                        missing.append(k)
                else:
                    n_bad_fwd += 1
                    if len(bad_fwd) < 10:
                        bad_fwd.append((k, v, got.get(k)))

    def check_sets(keys):
        nonlocal checked_s, n_bad_set
        with rconn.pipeline(transaction=False) as pipe:
            for k in keys:
                pipe.smembers(k)
            results = pipe.execute(raise_on_error=False)
        with conn.cursor() as cur:
            cur.execute(f"SELECT yuid, uri FROM {table} WHERE yuid = ANY(%s)", (keys,))
            members = {}
            for y, u in cur.fetchall():
                members.setdefault(y, set()).add(u)
        for k, mem in zip(keys, results):
            if isinstance(mem, Exception):
                continue
            checked_s += 1
            expect = {m for m in mem if not is_token(m)}
            if members.get(k, set()) != expect:
                n_bad_set += 1
                if len(bad_set) < 10:
                    bad_set.append((k, sorted(expect)[:4], sorted(members.get(k, set()))[:4]))

    strings, sets = [], []
    scanned = 0
    for key in rconn.scan_iter(count=scan_count):
        scanned += 1
        if key.startswith("yuid:"):
            r_yuid += 1
            if rng.random() < rate:
                sets.append(key)
        else:
            r_fwd += 1
            if rng.random() < rate:
                strings.append(key)
        if len(strings) >= 1000:
            check_strings(strings)
            strings = []
        if len(sets) >= 1000:
            check_sets(sets)
            sets = []
        if scanned - last[0] >= 10000000:
            last[0] = scanned
            print(f"    scanned {scanned:,}/{total_keys:,}, compared "
                  f"{checked_f + checked_s:,} ({time.time() - start:.0f}s)")
            sys.stdout.flush()
    if strings:
        check_strings(strings)
    if sets:
        check_sets(sets)

    print(f"\n  forward pointers  redis {r_fwd:>13,}   postgres {pg_fwd:>13,}   "
          f"{'MATCH' if r_fwd == pg_fwd else 'DIFFER'}")
    print(f"  yuids             redis {r_yuid:>13,}   postgres {pg_yuid:>13,}   "
          f"{'MATCH' if r_yuid == pg_yuid else 'DIFFER'}")
    print(f"  compared {checked_f:,} pointers and {checked_s:,} member sets")
    if not (n_missing or n_bad_fwd or n_bad_set):
        print("  every sampled key resolves identically in both")
        return
    print(f"  !! {n_missing:,} pointers absent from postgres, "
          f"{n_bad_fwd:,} pointing elsewhere, {n_bad_set:,} sets differing")
    for k in missing[:5]:
        print(f"     absent: {k}")
    for k, want, got in bad_fwd[:5]:
        print(f"     {k}\n       redis {want}\n       pg    {got}")
    for k, want, got in bad_set[:5]:
        print(f"     {k}\n       redis {want}\n       pg    {got}")
    sys.exit(1)


if __name__ == "__main__":
    main()
