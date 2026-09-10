# Ask the same questions of the redis identity map and the postgres one, and
# report every answer that differs.
#
#   python compare-idmap.py                      # 20k random keys, both directions
#   python compare-idmap.py --keys 200000
#   python compare-idmap.py --writes             # also exercise mint/set/merge
#
# This is the check to run before switching storeClass over: identity is the
# one thing in the pipeline that cannot be regenerated, so the two backends
# agreeing on real keys is worth more than any unit test.
#
# Reads are compared against production data. Writes are exercised against a
# scratch table (--write-table), never the migrated map.

import argparse
import os
import random
import sys
import time

from dotenv import load_dotenv

from pipeline.config import Config
from pipeline.storage.idmap import redis as redis_idmap
from pipeline.storage.idmap import postgres as pg_idmap


def build(cfgs, args):
    mcfg = dict(cfgs.map_stores[cfgs.idmap_name])
    mcfg["all_configs"] = cfgs
    mcfg.pop("store", None)
    old = redis_idmap.IdMap(mcfg)

    pcfg = dict(cfgs.caches)
    pcfg["all_configs"] = cfgs
    pcfg["tableName"] = args.table
    new = pg_idmap.IdMap(pcfg)
    return old, new


def sample_keys(old, n):
    """Real keys, taken by SCAN rather than made up: URI shapes in this map
    are not uniform and a synthetic key tests nothing."""
    fwd, rev = [], []
    for key in old.conn.scan_iter(count=1000):
        if key.startswith("yuid:"):
            if len(rev) < n // 2:
                rev.append(key)
        elif len(fwd) < n - n // 2:
            fwd.append(key)
        if len(fwd) + len(rev) >= n:
            break
    return fwd, rev


def compare_reads(old, new, keys, label):
    """Both backends take and return full URIs, so the comparison is on what
    a caller would actually receive."""
    print(f"\n--- {label} ({len(keys):,} keys)")
    ndiff = 0
    diffs = []
    t_old = t_new = 0.0

    def norm(v):
        """Redis keeps the update token as a pseudo-member of the YUID set;
        postgres keeps it in a column, so it is simply not a member. That is
        the intended difference -- run-merge already strips it off the redis
        answer (`if idmap.update_token in equivs: equivs.remove(...)`) -- so
        compare membership, not bookkeeping."""
        if isinstance(v, set):
            real = {x for x in v if not (x.startswith("__") and x.endswith("__"))}
            # A YUID whose only "member" was a token has no members at all.
            # Redis answers {token}; postgres answers None, because a registry
            # row with no member rows is exactly that. Callers already cope --
            # run-merge does `idmap[yuid] or set()` -- so treat them as equal.
            return real or None
        return v

    for ikey in keys:
        key = old._manage_key_out(ikey)
        t0 = time.perf_counter()
        a = old.get(key)
        t_old += time.perf_counter() - t0
        t0 = time.perf_counter()
        b = new.get(key)
        t_new += time.perf_counter() - t0
        if norm(a) != norm(b):
            ndiff += 1
            if len(diffs) < 8:
                diffs.append((key, a, b))
    n = max(len(keys), 1)
    print(f"  redis    {t_old / n * 1e6:>7.1f} us/lookup")
    print(f"  postgres {t_new / n * 1e6:>7.1f} us/lookup")
    if not ndiff:
        print(f"  all {len(keys):,} answers identical")
    else:
        print(f"  !! {ndiff:,} of {len(keys):,} differ")
        for key, a, b in diffs:
            print(f"     {key}")
            print(f"       redis    {sorted(a) if isinstance(a, set) else a}")
            print(f"       postgres {sorted(b) if isinstance(b, set) else b}")
    return ndiff


def compare_multi(old, new, keys):
    """get_multi is the call the hot loops use, so it gets its own check."""
    print(f"\n--- get_multi ({len(keys):,} keys)")
    full = [old._manage_key_out(k) for k in keys]
    t0 = time.perf_counter()
    a = old.get_multi(full)
    t_old = time.perf_counter() - t0
    t0 = time.perf_counter()
    b = new.get_multi(full)
    t_new = time.perf_counter() - t0
    n = max(len(keys), 1)
    print(f"  redis    {t_old / n * 1e6:>7.1f} us/key ({t_old:.2f}s total)")
    print(f"  postgres {t_new / n * 1e6:>7.1f} us/key ({t_new:.2f}s total)")
    def norm(v):
        if isinstance(v, set):
            real = {x for x in v if not (x.startswith("__") and x.endswith("__"))}
            return real or None
        return v

    bad = [k for k in full if norm(a.get(k)) != norm(b.get(k))]
    if not bad:
        print(f"  all {len(full):,} answers identical")
    else:
        print(f"  !! {len(bad):,} differ, e.g. {bad[:3]}")
    return len(bad)


def exercise_writes(cfgs, args):
    """mint, merge, tokens and deletion, against a scratch table.

    The merge is the case worth watching: assigning a key that already belongs
    to another YUID has to move every member of that class, which is where the
    redis version needs its retry loop."""
    print("\n--- writes (scratch table)")
    pcfg = dict(cfgs.caches)
    pcfg["all_configs"] = cfgs
    pcfg["tableName"] = args.write_table
    m = pg_idmap.IdMap(pcfg)
    m.clear()

    # NOT cfgs.internal_uri: a member URI under it compresses to a yuid: key
    # and would be looked up as a member set instead of a forward pointer
    ns = "https://example.org/idmap-writetest/"
    typ = "Person"
    a = f"{ns}compare-a"
    b = f"{ns}compare-b"
    ya = m.mint(a, "person", typ)
    yb = m.mint(b, "person", typ)
    print(f"  minted two: {ya.rsplit('/', 1)[-1][:8]}..., {yb.rsplit('/', 1)[-1][:8]}...")
    assert ya != yb, "two mints produced the same yuid"
    assert m.get(a, typ) == ya, "mint did not stick"
    assert m.get(ya) == {cfgs.make_qua(a, typ)}, f"reverse wrong: {m.get(ya)}"

    # minting the same key twice adopts the identity that already exists
    again = m.mint(a, "person", typ)
    assert again == ya, f"re-mint invented a new identity: {again} != {ya}"
    print("  re-minting an existing key adopts the identity already assigned")

    # the merge
    m.set(a, yb, typ)
    assert m.get(a, typ) == yb, "merge left the key behind"
    members = m.get(yb)
    assert members == {cfgs.make_qua(a, typ), cfgs.make_qua(b, typ)}, f"members: {members}"
    assert m.get(ya) is None, "old yuid still has members"
    print(f"  merged: both keys now resolve to one yuid, {len(members)} members")

    # tokens
    m.add_update_token(yb)
    assert m.has_update_token(yb), "token did not stick"
    print(f"  update token {m.update_token} set and read back")

    # deletion
    m.delete(a, typ)
    m.delete(b, typ)
    assert m.get(yb) is None, "members survived deletion"
    assert m.delete_yuid(yb), "empty yuid would not delete"
    print("  members deleted, then the empty yuid dropped")

    m.clear()
    print("  scratch table cleared")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--keys", type=int, default=20000)
    ap.add_argument("--table", default="idmap")
    ap.add_argument("--write-table", default="idmap_writetest")
    ap.add_argument("--writes", action="store_true")
    args = ap.parse_args()

    load_dotenv()
    cfgs = Config(basepath=os.getenv("LUX_BASEPATH", ""))
    old, new = build(cfgs, args)
    print(f"# redis {old.host}:{old.port}/{old.db} vs postgres {args.table}")

    print("# sampling real keys")
    fwd, rev = sample_keys(old, args.keys)
    bad = 0
    bad += compare_reads(old, new, fwd, "forward: uri -> yuid")
    bad += compare_reads(old, new, rev, "reverse: yuid -> members")
    bad += compare_multi(old, new, fwd)
    if args.writes:
        bad += exercise_writes(cfgs, args)

    print()
    if bad:
        print(f"!! {bad} disagreements -- do not switch storeClass yet")
        sys.exit(1)
    print("Both backends agree on every key checked.")


if __name__ == "__main__":
    main()
