# What is reconcile actually waiting on? Run this WHILE the slow run is going.
#
#   python diagnose-refs.py            # sample for 30s, then report
#   python diagnose-refs.py --seconds 60
#
# Samples pg_stat_activity to find where the workers are, and reports table
# bloat and lock waits. Read-only; safe to run against a live build.

import argparse
import os
import time
from collections import Counter

import psycopg2
from dotenv import load_dotenv

from pipeline.config import Config


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seconds", type=int, default=30)
    ap.add_argument("--interval", type=float, default=0.25)
    ap.add_argument("--tables", default="all_refs,done_refs,idmap")
    args = ap.parse_args()

    load_dotenv()
    cfgs = Config(basepath=os.getenv("LUX_BASEPATH", ""))
    db = cfgs.caches
    kw = {"user": db.get("user"), "dbname": db.get("dbname")}
    if db.get("host"):
        kw["host"] = db["host"]
        kw["port"] = int(db.get("port", 5432))
        if db.get("password"):
            kw["password"] = db["password"]
    conn = psycopg2.connect(**kw)
    conn.autocommit = True
    cur = conn.cursor()
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    print(f"--- sampling pg_stat_activity for {args.seconds}s")
    states = Counter()
    waits = Counter()
    stmts = Counter()
    backends = []
    n = 0
    end = time.time() + args.seconds
    while time.time() < end:
        cur.execute("""SELECT state, wait_event_type, wait_event, left(query, 80)
                       FROM pg_stat_activity
                       WHERE datname = current_database() AND pid <> pg_backend_pid()""")
        rows = cur.fetchall()
        backends.append(len(rows))
        for (state, wtype, wevent, query) in rows:
            states[state or "?"] += 1
            if state == "active":
                waits[f"{wtype or 'running'}/{wevent or '-'}"] += 1
                q = " ".join((query or "").split())
                for verb in ("DELETE FROM", "INSERT INTO", "SELECT count", "SELECT uri",
                             "SELECT yuid", "UPDATE", "EXECUTE"):
                    if q.startswith(verb):
                        q = verb + " " + q[len(verb):][:46]
                        break
                stmts[q[:70]] += 1
        n += 1
        time.sleep(args.interval)

    if not n or not backends:
        print("  no samples")
        return
    print(f"  {n} samples, {sum(backends)/len(backends):.1f} backends on average "
          f"(peak {max(backends)})")
    total_state = sum(states.values()) or 1
    print("\n  backend state")
    for s, c in states.most_common():
        print(f"    {s:24} {c/total_state*100:5.1f}%")
    if waits:
        total_w = sum(waits.values()) or 1
        print("\n  what the active ones are doing (wait_event_type/wait_event)")
        for w, c in waits.most_common(8):
            print(f"    {w:34} {c/total_w*100:5.1f}%")
    if stmts:
        total_s = sum(stmts.values()) or 1
        print("\n  statements seen active")
        for q, c in stmts.most_common(8):
            print(f"    {c/total_s*100:5.1f}%  {q}")

    print("\n--- table health")
    for t in tables:
        cur.execute("""SELECT n_live_tup, n_dead_tup, last_autovacuum, autovacuum_count
                       FROM pg_stat_user_tables WHERE relname = %s""", (t,))
        row = cur.fetchone()
        if row is None:
            print(f"  {t}: no such table")
            continue
        live, dead, last_av, av_count = row
        cur.execute("SELECT pg_size_pretty(pg_relation_size(%s)), "
                    "pg_size_pretty(pg_total_relation_size(%s))", (t, t))
        heap, total = cur.fetchone()
        ratio = dead / max(live + dead, 1) * 100
        flag = "  <-- BLOATED" if ratio > 40 else ""
        print(f"  {t:12} {live:>10,} live {dead:>12,} dead ({ratio:4.1f}%) "
              f"{heap:>9} heap  autovacuums={av_count}{flag}")
        if last_av is None and dead > 50000:
            print(f"               autovacuum has never run on it -- "
                  f"see REF_STORAGE in storage/idmap/postgres.py")
        cur.execute(f"SELECT reloptions FROM pg_class WHERE relname = %s", (t,))
        opts = cur.fetchone()[0]
        print(f"               storage options: {opts or '(defaults)'}")

    print("\n--- lock waits right now")
    cur.execute("""SELECT count(*) FROM pg_locks WHERE NOT granted""")
    print(f"  ungranted locks: {cur.fetchone()[0]}")
    cur.execute("""SELECT relation::regclass::text, mode, count(*)
                   FROM pg_locks WHERE NOT granted AND relation IS NOT NULL
                   GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 5""")
    for r in cur.fetchall():
        print(f"    {r}")


if __name__ == "__main__":
    main()
