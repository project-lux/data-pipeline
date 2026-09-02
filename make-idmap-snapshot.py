# Freeze the identity map into an LMDB file that the read-only phases can use
# instead of querying postgres.
#
#   python make-idmap-snapshot.py                       # to the configured path
#   python make-idmap-snapshot.py --out /data/idmap.lmdb
#
# Build it at the phase boundary: after identity resolution has finished and
# before merge and export start. Those phases only read the map -- they
# already call enable_memory_cache() for that reason -- so a frozen copy
# cannot go stale underneath them. The backend enforces this rather than
# trusting it: writing while the snapshot is enabled raises.
#
# Everything goes in by default.
#
# An earlier version held only objects and works, on the grounds that a person
# or a place that is 1:1 today may be merged tomorrow. That reasoning applies
# to where WRITES go -- always postgres -- not to what a snapshot may hold: the
# file is rebuilt from scratch each run, after identity resolution, and is only
# ever read by phases where writing raises. Nothing in it can go stale inside
# its own lifetime, whatever type it is. Scoping by type only shrank the file
# and lowered the hit rate: on the merge read path it left 92% of lookups going
# to postgres, because a record's references are people and concepts whatever
# the record is.
#
# --types still narrows it if you want a smaller file.
#
# Both directions go in, keyed exactly as postgres and redis key them, so
# lookups need no translation:
#
#     <uri>##quaType -> <yuid>                the forward pointer
#     yuid:<slug>/<uuid> -> <uri>\t<uri>      the members, tab separated
#     __token__   -> __YYYYMMDD__             which build this was taken from
#     __types__   -> HumanMadeObject\t...     what it holds, so the reader
#     __slugs__   -> object\tvisual\t...       can route without guessing
#
# The token is what stops last build's file answering this build's questions:
# enable_snapshot() refuses a file whose token doesn't match.

import argparse
import json
import os
import shutil
import sys
import time

import psycopg2
from dotenv import load_dotenv

from pipeline.config import Config

# LMDB's key limit; the few URIs longer than this stay postgres-only
MAX_KEY = 500

from pipeline.storage.idmap.postgres import SNAPSHOT_TYPES


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=None, help="output path (default: snapshotPath from map_idmap.json)")
    ap.add_argument("--table", default="idmap")
    ap.add_argument("--map-size", type=float, default=0,
                    help="LMDB map size in GB (default: estimated from the row count)")
    ap.add_argument("--types", default="",
                    help="narrow to these record types (default: all of them)")
    ap.add_argument("--no-reverse", action="store_true",
                    help="forward pointers only; halves the file, but merge's "
                         "cluster lookups then go to postgres")
    ap.add_argument("--pg-dbname", default=None)
    ap.add_argument("--pg-user", default=None)
    ap.add_argument("--pg-host", default=None)
    args = ap.parse_args()

    import lmdb

    load_dotenv()
    # Config knows where the token file and the cache credentials live; the
    # token is under base_dir/files, which is not the config directory
    cfgs = Config(basepath=os.getenv("LUX_BASEPATH", ""))

    mcfg = cfgs.map_stores.get(cfgs.idmap_name, {})
    out = args.out or mcfg.get("snapshotPath")
    if not out:
        print("No --out, and no snapshotPath in the idmap map config")
        sys.exit(1)

    with open(os.path.join(cfgs.data_dir, "idmap_update_token.txt")) as fh:
        token = fh.read().strip()

    pcfg = cfgs.caches
    kw = {"user": args.pg_user or pcfg.get("user") or os.getenv("USER"),
          "dbname": args.pg_dbname or pcfg.get("dbname") or os.getenv("USER")}
    host = args.pg_host if args.pg_host is not None else pcfg.get("host", "")
    if host:
        kw["host"] = host
        kw["port"] = int(pcfg.get("port", 5432))
        if pcfg.get("password"):
            kw["password"] = pcfg["password"]

    # type -> slug is the same mapping mint() uses to build the YUID, so the
    # reverse direction can be selected by slug
    okt = cfgs.ok_record_types
    types = [t.strip() for t in args.types.split(",") if t.strip()]
    everything = not types
    if everything:
        types = sorted(okt)
    unknown = [t for t in types if t not in okt]
    if unknown:
        print(f"Unknown record types: {', '.join(unknown)}")
        sys.exit(1)
    slugs = sorted({okt[t] for t in types})
    if everything:
        # No type predicate at all: a cheaper query, and it also picks up the
        # YUIDs minted without a slug, which a slug filter cannot match
        print("# types: all")
    else:
        print(f"# types: {', '.join(types)}")
        print(f"# slugs: {', '.join(slugs)}")

    conn = psycopg2.connect(**kw)
    conn.autocommit = False   # server-side cursors need a transaction
    table = args.table
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        n_rows = cur.fetchone()[0]
        if everything:
            n_scope = n_rows
        else:
            cur.execute(f"SELECT count(*) FROM {table} "
                        f"WHERE split_part(uri, '##qua', 2) = ANY(%s)", (types,))
            n_scope = cur.fetchone()[0]
    conn.rollback()
    print(f"# {n_scope:,} of {n_rows:,} rows are in scope "
          f"({n_scope / max(n_rows, 1) * 100:.1f}%) -> {out} (token {token})")
    n_rows = n_scope

    if args.map_size:
        map_size = int(args.map_size * 1e9)
    else:
        # A ceiling on a sparse file, not an allocation -- but too small fails
        # the build partway through, so leave room. Measured density is ~200
        # bytes per entry and both directions go in, so budget generously.
        map_size = max(int(n_rows * 1000), 4 << 30)
    print(f"# map size ceiling {map_size / 1e9:.0f}GB")

    tmp = out + ".building"
    shutil.rmtree(tmp, ignore_errors=True)
    env = lmdb.open(tmp, map_size=map_size, subdir=True, writemap=True,
                    metasync=False, sync=False, map_async=True)

    start = time.time()
    n_fwd = write_forward(conn, env, table, n_rows, start, None if everything else types)
    n_rev = 0 if args.no_reverse else write_reverse(conn, env, table, start,
                                                    None if everything else slugs)

    with env.begin(write=True) as txn:
        txn.put(b"__token__", token.encode("utf-8"))
        # "*" tells the reader it holds everything, so it can skip the
        # per-key routing test entirely
        txn.put(b"__types__", b"*" if everything else "\t".join(types).encode("utf-8"))
        txn.put(b"__slugs__", b"*" if everything else "\t".join(slugs).encode("utf-8"))
    env.sync(True)
    env.close()
    conn.rollback()

    # Renamed into place only once complete, so a snapshot that died halfway
    # never gets loaded
    shutil.rmtree(out, ignore_errors=True)
    os.rename(tmp, out)
    # st_blocks, not getsize: data.mdb is sparse, so its apparent size is the
    # map_size ceiling (52GB) rather than the 20GB it actually occupies
    size = sum(os.stat(os.path.join(out, f)).st_blocks * 512 for f in os.listdir(out))
    el = time.time() - start
    print(f"\n  {n_fwd:,} forward + {n_rev:,} reverse entries")
    print(f"  {size / 1e9:.2f}GB in {el / 60:.1f} min")
    print(f"\nEnable it with idmap.enable_snapshot() in the read-only phases.")


def write_forward(conn, env, table, n_rows, start, types):
    """Sorted by key so LMDB can append rather than split pages."""
    print("--- forward pointers")
    n = skipped = 0
    with conn.cursor(name="snap_fwd") as cur:
        cur.itersize = 50000
        if types is None:
            cur.execute(f"SELECT uri, yuid FROM {table} ORDER BY uri")
        else:
            cur.execute(f"SELECT uri, yuid FROM {table} "
                        f"WHERE split_part(uri, '##qua', 2) = ANY(%s) ORDER BY uri", (types,))
        batch = []
        for uri, yuid in cur:
            if len(uri) > MAX_KEY:
                skipped += 1
                continue
            batch.append((uri.encode("utf-8"), yuid.encode("utf-8")))
            if len(batch) >= 100000:
                n += put(env, batch)
                batch = []
                if not n % 1000000:
                    el = time.time() - start
                    print(f"    {n:,}/{n_rows:,} in {el:.0f}s ({n / el:,.0f}/s)")
                    sys.stdout.flush()
        if batch:
            n += put(env, batch)
    if skipped:
        print(f"    {skipped} uris over {MAX_KEY} bytes left out; they resolve from postgres")
    return n


def write_reverse(conn, env, table, start, slugs):
    """yuid -> tab separated members, which is what get(yuid) returns.

    Selected by the slug mint() put in the YUID, so the set of YUIDs here is
    the same set of types as the forward direction. Members of an in-scope
    YUID all go in, even if one of them is of another type: the answer has to
    be the whole class or it is wrong."""
    print("--- member sets")
    n = 0
    where = ""
    if slugs is not None:
        where = "WHERE " + (" OR ".join([f"yuid LIKE 'yuid:{s}/%%'" for s in slugs]) or "false")
    with conn.cursor(name="snap_rev") as cur:
        cur.itersize = 50000
        cur.execute(f"SELECT yuid, string_agg(uri, E'\\t') FROM {table} "
                    f"{where} GROUP BY yuid ORDER BY yuid")
        batch = []
        for yuid, members in cur:
            if len(yuid) > MAX_KEY or members is None:
                continue
            batch.append((yuid.encode("utf-8"), members.encode("utf-8")))
            if len(batch) >= 100000:
                n += put(env, batch)
                batch = []
                if not n % 1000000:
                    el = time.time() - start
                    print(f"    {n:,} in {el:.0f}s")
                    sys.stdout.flush()
        if batch:
            n += put(env, batch)
    return n


def put(env, batch):
    with env.begin(write=True) as txn:
        with txn.cursor() as cur:
            consumed, added = cur.putmulti(batch, append=True)
            if added != len(batch):
                # append=True requires strictly ascending keys; postgres
                # collation and byte order disagree on some URIs, so fall back
                # for this batch rather than dropping entries
                cur.putmulti(batch, append=False)
    return len(batch)


if __name__ == "__main__":
    main()
