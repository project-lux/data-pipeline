# Read-only survey of the identity map in redis, to size and shape the
# postgres/LMDB proposal against real production data rather than estimates.
#
# Answers three questions:
#   1. How much of the map is 1:1 -- both as a share of classes and, the number
#      that actually matters for a read cache, as a share of *members*.
#   2. Which namespaces those 1:1 members belong to, so the stable subset can
#      be identified by prefix rather than by guesswork.
#   3. How volatile the map is between builds -- run with --fingerprint before
#      and after a build, then --diff the two files.
#
#   python idmap-survey.py                              # survey, 1% sample
#   python idmap-survey.py --sample-pct 5               # tighter numbers
#   python idmap-survey.py --fingerprint before.tsv     # survey + record state
#   python idmap-survey.py --diff before.tsv after.tsv  # churn between builds
#
# Only SCAN, GET, SMEMBERS, MEMORY USAGE and INFO are issued: nothing here
# writes, deletes or blocks. SCAN walks in cursor order at COUNT 1000, so it
# does not hold the server the way KEYS would; the per-key commands are
# pipelined and only touch the sampled subset.
#
# Sampling is deterministic (crc32 of the key), so two runs sample the same
# keys and the fingerprints are directly comparable.

import argparse
import json
import os
import sys
import time
import zlib
from collections import Counter, defaultdict

import redis
from dotenv import load_dotenv

SAMPLE_SPACE = 10000  # sample rate is expressed in ten-thousandths


def sampled(key, rate_bp):
    return zlib.crc32(key.encode("utf-8")) % SAMPLE_SPACE < rate_bp


def is_token(member):
    return member.startswith("__") and member.endswith("__")


def namespace_of(key):
    """Which source a key belongs to.

    External identifiers are stored prefix-compressed (wikidata:Q42##quaPerson)
    so the CURIE prefix names the source. Internal ones are not -- only the
    namespaces in the idmap's prefix map get compressed -- so they stay full
    URLs and have to be attributed by host plus the first path segment, or
    every internal record in the map lands in one bucket called "https"."""
    if key.startswith("http"):
        rest = key.split("://", 1)[-1]
        parts = rest.split("/")
        host = parts[0]
        seg = parts[1] if len(parts) > 1 and parts[1] and "##qua" not in parts[1] else ""
        return f"{host}/{seg}" if seg else host
    return key.split(":", 1)[0] if ":" in key else "(none)"


def type_of(key):
    return key.split("##qua", 1)[1] if "##qua" in key else "(none)"


def connect(args):
    host, port, db = args.host, args.port, args.db
    load_dotenv()
    basepath = os.getenv("LUX_BASEPATH", "")
    cfgfile = os.path.join(basepath, "config_cache", "map_idmap.json")
    if not args.host and os.path.exists(cfgfile):
        with open(cfgfile) as fh:
            cfg = json.load(fh)
        host = cfg.get("host", "localhost")
        port = int(cfg.get("port", 6379))
        if db is None:
            db = int(cfg.get("db", 0))
        print(f"# using {cfgfile}")
    host = host or "localhost"
    port = port or 6379
    db = 0 if db is None else db
    print(f"# redis {host}:{port} db {db}")
    return redis.Redis(host=host, port=port, db=db, decode_responses=True)


def keyspace_report(conn):
    """Per-db key counts and total memory: free, and it says whether the
    identity map is even the thing using the memory."""
    info = conn.info("memory")
    print("\n--- instance")
    print(f"  used_memory      {info['used_memory_human']}")
    print(f"  used_memory_rss  {info.get('used_memory_rss_human', '?')}")
    print(f"  peak             {info.get('used_memory_peak_human', '?')}")
    persist = conn.info("persistence")
    print(f"  rdb last save    {persist.get('rdb_last_bgsave_status')}, "
          f"last cow {persist.get('rdb_last_cow_size', 0) / 1e9:.2f}GB, "
          f"aof {'on' if persist.get('aof_enabled') else 'off'}")
    save = conn.config_get("save").get("save", "")
    print(f"  save policy      {save or '(none)'}")

    ks = conn.info("keyspace")
    print("\n--- keyspace (which db holds what)")
    named = {0: "idmap", 2: "networkmap", 3: "all_refs", 4: "done_refs", 7: "redirects"}
    for name, stats in sorted(ks.items()):
        n = int(name.replace("db", ""))
        print(f"  db {n:<2} {named.get(n, ''):<12} {stats['keys']:>12,} keys")
    if not ks:
        print("  (empty)")


def survey(conn, rate_bp, fingerprint=None):
    print(f"\n--- scanning db (sampling {rate_bp / 100:.2f}% of keys for detail)")
    start = time.time()

    n_scanned = 0
    n_forward = 0          # uri -> yuid string keys
    n_class = 0            # yuid -> {uri} set keys
    fwd_batch, set_batch = [], []
    class_sizes = Counter()             # members per class, tokens excluded
    tokened = 0
    ns_members = Counter()              # members seen, by namespace
    ns_singleton = Counter()            # of those, in a 1:1 class
    type_members = Counter()
    type_singleton = Counter()
    sampled_fwd = 0
    sampled_sets = 0
    mem_forward, mem_class = [], []
    fh = open(fingerprint, "w") if fingerprint else None

    def drain_forward():
        nonlocal sampled_fwd
        if not fwd_batch:
            return
        vals = conn.mget(fwd_batch)
        for k, v in zip(fwd_batch, vals):
            if v is None:
                continue
            sampled_fwd += 1
            if fh is not None:
                fh.write(f"{k}\t{v}\n")
        fwd_batch.clear()

    def drain_sets():
        nonlocal sampled_sets, tokened
        if not set_batch:
            return
        with conn.pipeline(transaction=False) as pipe:
            for k in set_batch:
                pipe.smembers(k)
            results = pipe.execute()
        for members in results:
            sampled_sets += 1
            real = [m for m in members if not is_token(m)]
            if len(real) != len(members):
                tokened += 1
            class_sizes[len(real)] += 1
            for m in real:
                ns_members[namespace_of(m)] += 1
                type_members[type_of(m)] += 1
                if len(real) == 1:
                    ns_singleton[namespace_of(m)] += 1
                    type_singleton[type_of(m)] += 1
        set_batch.clear()

    for key in conn.scan_iter(count=1000):
        n_scanned += 1
        if key.startswith("yuid:"):
            n_class += 1
            if sampled(key, rate_bp):
                set_batch.append(key)
                if len(set_batch) >= 1000:
                    drain_sets()
        else:
            n_forward += 1
            if sampled(key, rate_bp):
                fwd_batch.append(key)
                if len(fwd_batch) >= 1000:
                    drain_forward()
        if not n_scanned % 1000000:
            el = time.time() - start
            print(f"    {n_scanned:,} keys in {el:.0f}s ({n_scanned / el:,.0f}/s)")
            sys.stdout.flush()
    drain_forward()
    drain_sets()

    # bytes per key, from a small second sample -- MEMORY USAGE is O(1) but
    # not free, so only a few thousand of each kind
    for key in conn.scan_iter(count=1000, match="yuid:*"):
        mem_class.append(conn.memory_usage(key) or 0)
        if len(mem_class) >= 2000:
            break
    for key in conn.scan_iter(count=1000):
        if not key.startswith("yuid:"):
            mem_forward.append(conn.memory_usage(key) or 0)
            if len(mem_forward) >= 2000:
                break
    if fh is not None:
        fh.close()

    el = time.time() - start
    print(f"\n--- shape ({n_scanned:,} keys in {el:.0f}s)")
    print(f"  forward pointers (uri -> yuid) {n_forward:>14,}")
    print(f"  classes          (yuid -> set) {n_class:>14,}")
    if n_class:
        print(f"  members per class              {n_forward / n_class:>14.2f}")

    total_sampled_classes = sum(class_sizes.values())
    if not total_sampled_classes:
        print("\n  no classes sampled -- raise --sample-pct")
        return
    total_sampled_members = sum(n * c for n, c in class_sizes.items())

    print(f"\n--- class size (sampled {total_sampled_classes:,} classes, "
          f"{tokened / total_sampled_classes * 100:.0f}% carry an update token)")
    print(f"  {'members':>9}  {'classes':>10}  {'% classes':>10}  {'% of all members':>17}")
    cumulative_members = 0
    for size in sorted(class_sizes):
        c = class_sizes[size]
        share_c = c / total_sampled_classes * 100
        share_m = size * c / total_sampled_members * 100
        if size <= 6 or share_c > 0.5:
            print(f"  {size:>9}  {c:>10,}  {share_c:>9.1f}%  {share_m:>16.1f}%")
        cumulative_members += size * c
    big = {s: c for s, c in class_sizes.items() if s > 6}
    if big:
        c = sum(big.values())
        m = sum(s * c2 for s, c2 in big.items())
        print(f"  {'7+':>9}  {c:>10,}  {c / total_sampled_classes * 100:>9.1f}%  "
              f"{m / total_sampled_members * 100:>16.1f}%")

    orphans = class_sizes.get(0, 0)
    if orphans:
        print(f"\n  {orphans:,} sampled classes ({orphans / total_sampled_classes * 100:.1f}%) "
              f"hold only an update token and no members --")
        print(f"  the case delete_yuid() exists for, and dead weight in any copy of the map")

    one_c = class_sizes.get(1, 0)
    one_m = one_c  # a 1:1 class contributes exactly one member
    print(f"\n  1:1 classes      {one_c / total_sampled_classes * 100:>5.1f}% of classes")
    print(f"  members in them  {one_m / total_sampled_members * 100:>5.1f}% of members "
          f"<-- the hit rate a read-only snapshot of the stable subset would get")
    print(f"  projected whole map: {int(n_class * one_c / total_sampled_classes):,} 1:1 classes, "
          f"{int(n_forward * one_m / total_sampled_members):,} members in them")

    print("\n--- by namespace (sampled members)")
    print(f"  {'prefix':<14} {'members':>10}  {'% of map':>9}  {'in 1:1':>8}")
    for ns, count in ns_members.most_common(20):
        singles = ns_singleton.get(ns, 0)
        print(f"  {ns:<14} {count:>10,}  {count / total_sampled_members * 100:>8.1f}%  "
              f"{singles / count * 100:>7.1f}%")

    # The type is what decides which store a key is routed to, so this is the
    # table that says whether that split is the right one: a type that is
    # overwhelmingly 1:1 is one whose identities don't move.
    print("\n--- by record type (sampled members)")
    print(f"  {'type':<20} {'members':>10}  {'% of map':>9}  {'in 1:1':>8}")
    snapshot_types = {"HumanMadeObject", "DigitalObject", "VisualItem", "LinguisticObject"}
    in_scope = in_scope_single = 0
    for t, count in type_members.most_common(15):
        singles = type_singleton.get(t, 0)
        mark = "  <- snapshot" if t in snapshot_types else ""
        print(f"  {t:<20} {count:>10,}  {count / total_sampled_members * 100:>8.1f}%  "
              f"{singles / count * 100:>7.1f}%{mark}")
    for t, count in type_members.items():
        if t in snapshot_types:
            in_scope += count
            in_scope_single += type_singleton.get(t, 0)
    if in_scope:
        print(f"\n  objects and works are {in_scope / total_sampled_members * 100:.1f}% of members, "
              f"{in_scope_single / in_scope * 100:.1f}% of them 1:1")
        print(f"  everything else is {(total_sampled_members - in_scope) / total_sampled_members * 100:.1f}%, "
              f"{(sum(type_singleton.values()) - in_scope_single) / max(total_sampled_members - in_scope, 1) * 100:.1f}% 1:1")

    if mem_forward and mem_class:
        avg_f = sum(mem_forward) / len(mem_forward)
        avg_c = sum(mem_class) / len(mem_class)
        total = n_forward * avg_f + n_class * avg_c
        print(f"\n--- memory attribution (MEMORY USAGE, {len(mem_forward)}+{len(mem_class)} keys sampled)")
        print(f"  forward pointer  {avg_f:>7.0f} bytes avg -> {n_forward * avg_f / 1e9:>7.2f}GB")
        print(f"  class set        {avg_c:>7.0f} bytes avg -> {n_class * avg_c / 1e9:>7.2f}GB")
        print(f"  this db          {total / 1e9:>7.2f}GB of the instance total above")

    if fingerprint:
        print(f"\n--- wrote {sampled_fwd:,} sampled forward pointers to {fingerprint}")
        print("    run a build, then: python idmap-survey.py --fingerprint after.tsv")
        print(f"    and: python idmap-survey.py --diff {fingerprint} after.tsv")


def diff(before_path, after_path):
    """Churn between two fingerprints. Both are sampled on the same
    deterministic hash, so the same URIs appear in both files and the rates
    below are estimates of the whole map."""
    print(f"--- {before_path} -> {after_path}")
    before = {}
    with open(before_path) as fh:
        for line in fh:
            uri, yuid = line.rstrip("\n").split("\t", 1)
            before[uri] = yuid
    after = {}
    with open(after_path) as fh:
        for line in fh:
            uri, yuid = line.rstrip("\n").split("\t", 1)
            after[uri] = yuid

    same = moved = 0
    ns_moved = Counter()
    ns_total = Counter()
    for uri, old in before.items():
        ns_total[namespace_of(uri)] += 1
        new = after.get(uri)
        if new is None:
            continue
        if new == old:
            same += 1
        else:
            moved += 1
            ns_moved[namespace_of(uri)] += 1
    gone = sum(1 for u in before if u not in after)
    added = sum(1 for u in after if u not in before)
    overlap = same + moved

    print(f"\n  before {len(before):,} sampled   after {len(after):,} sampled")
    if not overlap:
        print("  no overlap -- were both files sampled at the same --sample-pct?")
        return
    print(f"  unchanged   {same:>10,}  {same / overlap * 100:>6.2f}%")
    print(f"  moved yuid  {moved:>10,}  {moved / overlap * 100:>6.2f}%  <-- the volatile fraction")
    print(f"  disappeared {gone:>10,}")
    print(f"  new         {added:>10,}")

    if moved:
        print("\n  churn by namespace")
        print(f"  {'prefix':<14} {'sampled':>10}  {'moved':>8}  {'rate':>7}")
        for ns, total in ns_total.most_common(20):
            m = ns_moved.get(ns, 0)
            print(f"  {ns:<14} {total:>10,}  {m:>8,}  {m / total * 100:>6.2f}%")
        print("\n  namespaces at 0.00% are candidates for the read-only snapshot:")
        stable = [ns for ns, t in ns_total.items() if t >= 1000 and not ns_moved.get(ns)]
        print(f"    {', '.join(sorted(stable)) if stable else '(none at this sample size)'}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default=None, help="redis host (default: from map_idmap.json)")
    ap.add_argument("--port", type=int, default=None)
    ap.add_argument("--db", type=int, default=None, help="redis db (default: 0, the idmap)")
    ap.add_argument("--sample-pct", type=float, default=1.0,
                    help="percent of keys to fetch detail for (default 1.0)")
    ap.add_argument("--fingerprint", metavar="FILE",
                    help="also write sampled uri->yuid pairs, for --diff after the next build")
    ap.add_argument("--diff", nargs=2, metavar=("BEFORE", "AFTER"),
                    help="compare two fingerprint files instead of surveying")
    args = ap.parse_args()

    if args.diff:
        diff(*args.diff)
        return

    rate_bp = max(1, int(args.sample_pct * SAMPLE_SPACE / 100))
    conn = connect(args)
    keyspace_report(conn)
    survey(conn, rate_bp, args.fingerprint)


if __name__ == "__main__":
    main()
