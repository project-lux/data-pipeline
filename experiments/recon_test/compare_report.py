"""Compute the comparison metrics between legacy and deterministic
identity builds produced by legacy_compare.py.

Metrics per approach:
  determinism  - do two arrival orders produce identical member->YUID maps?
  stability    - on rebuild, what fraction of members keep their
                 production YUID?
  conflicts    - how many injected differentFrom pairs ended up
                 co-clustered (over-merged)?
  integrity    - fragmented clusters (members of one production cluster
                 split over several YUIDs) and cross-cluster merges,
                 excluding the intentionally-conflicted clusters
  speed        - identity wall seconds, redis ops, redis round trips

Usage: python compare_report.py --cmp-dir /tmp/cmp
"""

import argparse
import json
import sys
import uuid
import zlib
from collections import defaultdict
from pathlib import Path

import lmdb
import orjson

DB_PATH = "/Users/wjm55/lux-concepts-scc/concepts.lmdb"


def load_mapping(run_dir):
    m = {}
    with open(Path(run_dir) / "mapping.tsv") as fh:
        for line in fh:
            k, v = line.rstrip("\n").split("\t")
            m[k] = v
    return m


def load_stats(run_dir):
    return json.loads((Path(run_dir) / "stats.json").read_text())


def production_clusters(db_path):
    env = lmdb.open(str(db_path), max_dbs=2, readonly=True, lock=False,
                    readahead=False, subdir=True)
    db = env.open_db(b"data")
    clusters = {}
    with env.begin(buffers=True, db=db) as txn:
        for key, val in txn.cursor():
            rec = orjson.loads(zlib.decompress(bytes(val)))
            members = sorted({e["id"].strip() for e in rec.get("equivalent", [])
                              if e.get("id")} - {rec.get("id", "")})
            if members:
                clusters[rec.get("id", str(uuid.UUID(bytes=bytes(key))))] = members
    env.close()
    return clusters


def integrity(mapping, clusters, skip_members):
    """(fragmented, over_merged): production clusters split over multiple
    YUIDs, and pairs of production clusters sharing one YUID."""
    frag = 0
    yuid_owner = {}
    over = set()
    for prod_yuid, members in clusters.items():
        if any(m in skip_members for m in members):
            continue
        got = {mapping.get(m) for m in members if m in mapping}
        got.discard(None)
        if len(got) > 1:
            frag += 1
        for y in got:
            if y in yuid_owner and yuid_owner[y] != prod_yuid:
                over.add(tuple(sorted((yuid_owner[y], prod_yuid))))
            else:
                yuid_owner[y] = prod_yuid
    return frag, len(over)


def conflict_outcomes(mapping, conflicts):
    violated = kept_separate = cr_with_c = 0
    for c in conflicts:
        yb, yc = mapping.get(c["b"]), mapping.get(c["c"])
        if yb is not None and yb == yc:
            violated += 1
        else:
            kept_separate += 1
        if mapping.get(c["cr"]) == yc:
            cr_with_c += 1
    return violated, kept_separate, cr_with_c


def diff_count(m1, m2):
    keys = set(m1) | set(m2)
    return sum(1 for k in keys if m1.get(k) != m2.get(k)), len(keys)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cmp-dir", required=True)
    ap.add_argument("--db", default=DB_PATH)
    args = ap.parse_args()
    cmp_dir = Path(args.cmp_dir)

    clusters = production_clusters(args.db)
    prod_map = {}
    for y, members in clusters.items():
        for m in members:
            prod_map[m] = y

    conflicts = json.loads((cmp_dir / "legacy-cold-1" / "conflicts_expected.json").read_text())
    conflicted_members = {c["b"] for c in conflicts} | {c["c"] for c in conflicts}

    report = {}
    for mode in ("legacy", "new"):
        r = {}
        for kind in ("cold", "prior"):
            m1 = load_mapping(cmp_dir / f"{mode}-{kind}-1")
            m2 = load_mapping(cmp_dir / f"{mode}-{kind}-2")
            s1 = load_stats(cmp_dir / f"{mode}-{kind}-1")
            s2 = load_stats(cmp_dir / f"{mode}-{kind}-2")
            ndiff, total = diff_count(m1, m2)
            frag, over = integrity(m1, clusters, conflicted_members)
            viol, sep, crc = conflict_outcomes(m1, conflicts)
            entry = {
                "identical_across_orders": s1["mapping_sha256"] == s2["mapping_sha256"],
                "members_differing_across_orders": ndiff,
                "members_total": total,
                "fragmented_clusters": frag,
                "cross_cluster_merges": over,
                "diff_pairs_violated": viol,
                "diff_pairs_kept_separate": sep,
                "conflict_rec_joined_majority_side": crc,
                "wall_s": [s1["identity_wall_s"], s2["identity_wall_s"]],
                "redis_ops": [s1["redis_ops"], s2["redis_ops"]],
                "redis_round_trips": [s1.get("redis_round_trips"),
                                      s2.get("redis_round_trips")],
            }
            if kind == "prior":
                real = [m for m in m1 if not m.startswith("test://")]
                kept = sum(1 for m in real if prod_map.get(m) == m1[m])
                entry["production_yuids_kept"] = kept
                entry["production_yuids_total"] = len(real)
                entry["stability_pct"] = round(100.0 * kept / len(real), 3)
            r[kind] = entry
        report[mode] = r

    print(json.dumps(report, indent=2))
    with open(cmp_dir / "comparison.json", "w") as fh:
        json.dump(report, fh, indent=2)


if __name__ == "__main__":
    main()
