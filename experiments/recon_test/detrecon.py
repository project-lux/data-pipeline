# /// script
# requires-python = ">=3.11"
# dependencies = ["lmdb", "orjson"]
# ///
"""
Deterministic reconciliation prototype.

Tests a redesign of the reconcile/identify phases where the output is a
pure function of the *input assertions*, not of process scheduling:

  Phase 1 (emit)    - W parallel workers stream merged concept records from
                      an LMDB snapshot and emit sameAs *assertions*
                      (asserter, a, b) to per-worker files. Emission order is
                      deliberately shuffled per worker/seed to prove the rest
                      of the pipeline is order-independent.
  Phase 2 (cluster) - aggregate assertions into voted pair-edges, then run a
                      constrained union-find: edges are processed in a fixed
                      global order (most distinct asserters first, then
                      lexicographic), and a union is refused when any
                      differentFrom pair spans the two clusters. Refused
                      assertions are written to a conflicts report with their
                      asserters and vote context.
  Phase 3 (assign)  - each cluster gets a YUID: members vote to reuse the
                      YUID they held in the prior idmap (so adding a URI to a
                      cluster never changes its YUID); competing claims after
                      a cluster split are resolved deterministically; only
                      clusters with no prior YUID mint uuid5(min member).

The input LMDB is a snapshot of merged concept records (key = 16-byte UUID,
value = zlib(orjson)); each record's `equivalent` array supplies the members
of its cluster and the record `id` supplies the prior-build YUID.
"""

import argparse
import hashlib
import multiprocessing as mp
import os
import random
import sys
import time
import uuid
import zlib
from collections import defaultdict
from pathlib import Path

import lmdb
import orjson

YUID_PREFIX = "https://lux.collections.yale.edu/data/concept/"
MINT_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, YUID_PREFIX)


# ---------------------------------------------------------------------------
# LMDB access
# ---------------------------------------------------------------------------

def open_concepts(db_path):
    env = lmdb.open(str(db_path), max_dbs=2, readonly=True, lock=False,
                    readahead=False, subdir=True)
    db = env.open_db(b"data")
    return env, db


def iter_concepts(db_path):
    env, db = open_concepts(db_path)
    with env.begin(buffers=True, db=db) as txn:
        for key, val in txn.cursor():
            rec = orjson.loads(zlib.decompress(bytes(val)))
            yield str(uuid.UUID(bytes=bytes(key))), rec
    env.close()


def record_members(rec):
    """The URIs that would enter a fresh build for this cluster."""
    members = set()
    for eq in rec.get("equivalent", []):
        mid = eq.get("id")
        if mid:
            members.add(mid.strip())
    members.discard(rec.get("id", ""))
    return sorted(members)


def is_internal(uri):
    return ".yale.edu" in uri


# ---------------------------------------------------------------------------
# Phase 1: emit assertions
# ---------------------------------------------------------------------------

def extract_assertions(members):
    """Simulate per-record sameAs assertions for one cluster's members.

    Chain edges connect the sorted members (each member asserts equivalence
    to its successor, like a reconciler walking a record's equivalents), and
    an internal hub member re-asserts every link so that popular pairs carry
    more than one vote. A singleton emits a self-assertion so the node still
    receives a YUID.
    """
    if not members:
        return []
    if len(members) == 1:
        m = members[0]
        return [(m, m, m)]
    out = []
    for i in range(len(members) - 1):
        out.append((members[i], members[i], members[i + 1]))
    internal = [m for m in members if is_internal(m)]
    if internal:
        hub = internal[0]
        for m in members:
            if m != hub:
                out.append((hub, hub, m))
    return out


def variant_uri(uri):
    """A plausible duplicate form of the same identifier (scheme swap)."""
    if uri.startswith("https://"):
        return "http://" + uri[8:]
    if uri.startswith("http://"):
        return "https://" + uri[7:]
    return uri + "#variant"


def apply_mutation(members, mut):
    """Dirty one record's members on the way in.

    Returns a list of member-groups; each group is emitted independently
    (a 'split' produces two disconnected assertion sets, as if the record
    bridging them failed to acquire this build).
    """
    if mut is None:
        return [members]
    kind = mut["kind"]
    if kind == "drop":
        return [[m for m in members if m != mut["member"]]]
    if kind == "variant":
        return [sorted(set(members) | {variant_uri(mut["member"])})]
    if kind == "split":
        cut = mut["cut"]
        return [members[:cut], members[cut:]]
    raise ValueError(kind)


def _emit_worker(db_path, out_path, worker, n_workers, seed, mutations_file):
    mutations = {}
    if mutations_file:
        with open(mutations_file, "rb") as fh:
            mutations = orjson.loads(fh.read())
    lines = []
    for idx, (rec_uuid, rec) in enumerate(iter_concepts(db_path)):
        if idx % n_workers != worker:
            continue
        for group in apply_mutation(record_members(rec), mutations.get(rec_uuid)):
            for asserter, a, b in extract_assertions(group):
                lines.append(f"{asserter}\t{a}\t{b}\n")
    # Shuffle to prove downstream order-independence
    random.Random(seed * 1000 + worker).shuffle(lines)
    with open(out_path, "w") as fh:
        fh.writelines(lines)
    return len(lines)


def emit(db_path, out_dir, n_workers, seed, mutations_file=None):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    jobs = [(str(db_path), str(out_dir / f"assertions-{w}.tsv"), w, n_workers,
             seed, str(mutations_file) if mutations_file else None)
            for w in range(n_workers)]
    with mp.Pool(n_workers) as pool:
        counts = pool.starmap(_emit_worker, jobs)
    return sum(counts)


# ---------------------------------------------------------------------------
# Phase 2: deterministic constrained clustering
# ---------------------------------------------------------------------------

def load_assertions(files):
    """pair -> set(asserters). Canonical pair = sorted URI strings."""
    edges = defaultdict(set)
    for fn in sorted(str(f) for f in files):
        with open(fn) as fh:
            for line in fh:
                asserter, a, b = line.rstrip("\n").split("\t")
                if a > b:
                    a, b = b, a
                edges[(a, b)].add(asserter)
    return edges


def load_diffs(files):
    diffs = set()
    for fn in sorted(str(f) for f in files):
        with open(fn) as fh:
            for line in fh:
                a, b = line.rstrip("\n").split("\t")[:2]
                if a > b:
                    a, b = b, a
                diffs.add((a, b))
    return diffs


class DSU:
    def __init__(self):
        self.parent = {}
        self.size = {}

    def add(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.size[x] = 1

    def find(self, x):
        root = x
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[x] != root:
            self.parent[x], x = root, self.parent[x]
        return root

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return rx
        # Union by size; ties keep the lexicographically smaller root so the
        # representative (not just the partition) is schedule-independent.
        if (self.size[rx], ry) < (self.size[ry], rx):
            rx, ry = ry, rx
        self.parent[ry] = rx
        self.size[rx] += self.size[ry]
        return rx


def cluster(edges, diffs):
    """Constrained union-find over voted edges.

    Edges are processed most-voted-first (lexicographic tie-break) so that
    when an assertion conflicts with a differentFrom constraint, the side
    with more independent support wins the disputed node -- the "record 3"
    case: 1=A,B / 2=C,D / 3=B,C with B!=C assigns 3 by the other assertions
    about 3, B and C, and reports the refused link.
    """
    dsu = DSU()
    for (a, b) in edges:
        dsu.add(a)
        dsu.add(b)
    for (a, b) in diffs:
        dsu.add(a)
        dsu.add(b)

    # enemy constraints indexed by current root
    enemies = defaultdict(set)
    for (a, b) in diffs:
        ra, rb = dsu.find(a), dsu.find(b)
        if ra != rb:
            enemies[ra].add(rb)
            enemies[rb].add(ra)

    order = sorted(edges.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    conflicts = []
    for (a, b), asserters in order:
        if a == b:
            continue
        ra, rb = dsu.find(a), dsu.find(b)
        if ra == rb:
            continue
        if rb in enemies[ra]:
            conflicts.append({
                "pair": [a, b],
                "votes": len(asserters),
                "asserters": sorted(asserters),
                "cluster_sizes": [dsu.size[ra], dsu.size[rb]],
            })
            continue
        merged = dsu.union(a, b)
        loser = rb if merged == ra else ra
        if enemies[loser]:
            for e in enemies.pop(loser):
                enemies[e].discard(loser)
                enemies[e].add(merged)
                enemies[merged].add(e)

    clusters = defaultdict(list)
    for node in dsu.parent:
        clusters[dsu.find(node)].append(node)
    # Canonical form: keyed and ordered by min member
    out = {}
    for members in clusters.values():
        members.sort()
        out[members[0]] = members
    conflicts.sort(key=lambda c: (c["pair"][0], c["pair"][1]))
    return out, conflicts


# ---------------------------------------------------------------------------
# Phase 3: deterministic YUID assignment
# ---------------------------------------------------------------------------

def assign(clusters, prior):
    """cluster (keyed by min member) -> YUID.

    Members vote for the YUID they held in the prior idmap; the majority
    (tie: lexicographically smallest YUID) wins, so adding new URIs to a
    cluster never changes its identity. If a cluster split leaves two
    clusters claiming one YUID, the claim with more voters (tie: smaller
    cluster key) keeps it and the loser mints. Clusters with no prior YUID
    mint uuid5(min member).
    """
    claims = defaultdict(list)  # yuid -> [(votes, cluster_key)]
    for key, members in clusters.items():
        tally = defaultdict(int)
        for m in members:
            y = prior.get(m)
            if y:
                tally[y] += 1
        if tally:
            # most votes wins; tie -> lexicographically smallest yuid
            top = max(tally.values())
            best = min(y for y, n in tally.items() if n == top)
            claims[best].append((tally[best], key))

    assigned = {}
    for yuid, claimants in claims.items():
        claimants.sort(key=lambda vc: (-vc[0], vc[1]))
        assigned[claimants[0][1]] = yuid

    result = {}
    for key, members in clusters.items():
        y = assigned.get(key)
        if y is None:
            y = YUID_PREFIX + str(uuid.uuid5(MINT_NAMESPACE, key))
        result[key] = y
    return result


# ---------------------------------------------------------------------------
# Build orchestration
# ---------------------------------------------------------------------------

def make_prior(db_path, out_file):
    """member URI -> production YUID, from the merged records themselves."""
    mapping = {}
    dupes = 0
    for _, rec in iter_concepts(db_path):
        yuid = rec.get("id", "")
        for m in record_members(rec):
            if m in mapping and mapping[m] != yuid:
                dupes += 1
                if yuid < mapping[m]:
                    mapping[m] = yuid
            else:
                mapping[m] = yuid
    with open(out_file, "w") as fh:
        for m in sorted(mapping):
            fh.write(f"{m}\t{mapping[m]}\n")
    return len(mapping), dupes


def load_prior(fn):
    prior = {}
    with open(fn) as fh:
        for line in fh:
            m, y = line.rstrip("\n").split("\t")
            prior[m] = y
    return prior


def inject_conflicts(db_path, n, out_assertions, out_diffs, seed=42):
    """Fabricate the 'record 3' scenario n times against real clusters.

    For each seeded pair of records (clusters X, Y): b = max member of X,
    c = min member of Y, declare b != c, and add a conflict record CR that
    asserts CR=b (1 vote) and CR=c (2 votes, seconded by a helper record).
    Expected outcome: X and Y stay separate, CR lands with c's cluster.
    """
    keys = []
    for k, rec in iter_concepts(db_path):
        members = record_members(rec)
        if members:
            keys.append((k, members))
    keys.sort()
    rng = random.Random(seed)
    lines, diffs, expected = [], [], []
    for i in range(n):
        (k1, m1), (k2, m2) = rng.sample(keys, 2)
        b, c = m1[-1], m2[0]
        if b == c:
            continue
        cr = f"test://conflict/{i}"
        helper = f"test://conflict-helper/{i}"
        lines.append(f"{cr}\t{cr}\t{b}\n")
        lines.append(f"{cr}\t{cr}\t{c}\n")
        lines.append(f"{helper}\t{cr}\t{c}\n")
        da, db_ = (b, c) if b < c else (c, b)
        diffs.append(f"{da}\t{db_}\n")
        expected.append({"cr": cr, "b": b, "c": c})
    with open(out_assertions, "w") as fh:
        fh.writelines(lines)
    with open(out_diffs, "w") as fh:
        fh.writelines(diffs)
    return expected


def make_dirty(db_path, out_dir, seed=77, wrong_links=2000,
               p_split=0.02, p_drop=0.05, p_variant=0.03):
    """Un-clean the input: seeded, schedule-independent dirt + expectations.

    Per-record mutations (applied by emit workers, keyed by record uuid):
      split   - the cluster's assertions arrive as two disconnected halves
      drop    - an external member fails to acquire this build
      variant - a scheme-variant duplicate URI joins the cluster
    Cross-record dirt (extra assertion/diff files):
      wrong sameAs links between two unrelated clusters, asserted by a real
      member of one of them; half are contradicted by a differentFrom (must
      be refused), half are not (clusters merge; majority YUID must win).
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    mutations = {}
    expect = {"split": [], "drop": [], "variant": [],
              "merge": [], "blocked": []}
    clean = []  # records available for wrong-link pairing

    # First pass: everything up front, so variant URIs can be checked
    # against the global member set (a scheme-swapped duplicate must be a
    # *new* URI, not an accidental bridge into another cluster).
    records = []
    all_members = set()
    for rec_uuid, rec in iter_concepts(db_path):
        members = record_members(rec)
        if not members:
            continue
        records.append((rec_uuid, rec.get("id", ""), members))
        all_members.update(members)

    for rec_uuid, yuid, members in records:
        externals = [m for m in members if not is_internal(m)
                     and variant_uri(m) not in all_members]
        r = rng.random()
        if r < p_split and len(members) >= 4:
            cut = rng.randrange(2, len(members) - 1)
            mutations[rec_uuid] = {"kind": "split", "cut": cut}
            expect["split"].append({"yuid": yuid, "part_a": members[:cut],
                                    "part_b": members[cut:]})
        elif r < p_split + p_drop and len(members) >= 3 and externals:
            victim = rng.choice(externals)
            mutations[rec_uuid] = {"kind": "drop", "member": victim}
            expect["drop"].append({"yuid": yuid, "member": victim,
                                   "kept": [m for m in members if m != victim]})
        elif r < p_split + p_drop + p_variant and externals:
            m = rng.choice(externals)
            mutations[rec_uuid] = {"kind": "variant", "member": m}
            expect["variant"].append({"yuid": yuid, "member": m,
                                      "variant": variant_uri(m)})
        else:
            clean.append((rec_uuid, yuid, members))

    # Wrong cross-cluster links between otherwise untouched clusters; each
    # cluster participates at most once so expected outcomes stay exact.
    link_lines, diff_lines = [], []
    pool = clean.copy()
    rng.shuffle(pool)
    it = iter(pool)
    made = 0
    for (u1, y1, m1), (u2, y2, m2) in zip(it, it):
        if made >= wrong_links:
            break
        a, b = m1[-1], m2[0]
        if a == b:
            continue
        asserter = m1[0]
        link_lines.append(f"{asserter}\t{a}\t{b}\n")
        entry = {"a": a, "b": b, "yuid_a": y1, "yuid_b": y2,
                 "size_a": len(m1), "size_b": len(m2)}
        if made % 2 == 0:
            da, db_ = (a, b) if a < b else (b, a)
            diff_lines.append(f"{da}\t{db_}\n")
            expect["blocked"].append(entry)
        else:
            # merged cluster: YUID with more prior voters wins (tie -> min)
            if len(m1) > len(m2):
                entry["winner"] = y1
            elif len(m2) > len(m1):
                entry["winner"] = y2
            else:
                entry["winner"] = min(y1, y2)
            expect["merge"].append(entry)
        made += 1

    with open(out_dir / "mutations.json", "wb") as fh:
        fh.write(orjson.dumps(mutations))
    with open(out_dir / "wrong_links.tsv", "w") as fh:
        fh.writelines(link_lines)
    with open(out_dir / "wrong_diffs.tsv", "w") as fh:
        fh.writelines(diff_lines)
    with open(out_dir / "expectations.json", "wb") as fh:
        fh.write(orjson.dumps(expect))
    return {k: len(v) for k, v in expect.items()}


def make_perturbation(db_path, every, out_assertions):
    """Add a brand-new URI to every Nth cluster: its YUID must not change."""
    lines = []
    perturbed = []
    for idx, (k, rec) in enumerate(iter_concepts(db_path)):
        if idx % every:
            continue
        members = record_members(rec)
        if not members:
            continue
        fake = f"test://new-uri/{k}"
        lines.append(f"{members[0]}\t{members[0]}\t{fake}\n")
        perturbed.append((fake, rec["id"]))
    with open(out_assertions, "w") as fh:
        fh.writelines(lines)
    return perturbed


def build(db_path, out_dir, n_workers, seed, prior_file=None,
          extra_assertions=(), diff_files=(), mutations_file=None):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stats = {"workers": n_workers, "seed": seed}

    t0 = time.time()
    n_asserts = emit(db_path, out_dir, n_workers, seed, mutations_file)
    stats["phase1_emit_s"] = round(time.time() - t0, 2)
    stats["assertions"] = n_asserts

    t0 = time.time()
    files = sorted(out_dir.glob("assertions-*.tsv")) + [Path(f) for f in extra_assertions]
    edges = load_assertions(files)
    diffs = load_diffs([Path(f) for f in diff_files])
    stats["pairs"] = len(edges)
    stats["diff_pairs"] = len(diffs)
    clusters, conflicts = cluster(edges, diffs)
    stats["phase2_cluster_s"] = round(time.time() - t0, 2)
    stats["clusters"] = len(clusters)
    stats["conflicts_blocked"] = len(conflicts)

    t0 = time.time()
    prior = load_prior(prior_file) if prior_file else {}
    yuids = assign(clusters, prior)
    stats["phase3_assign_s"] = round(time.time() - t0, 2)

    prior_yuids = set(prior.values())
    stats["reused_yuids"] = sum(1 for y in yuids.values() if y in prior_yuids)
    stats["minted_yuids"] = len(yuids) - stats["reused_yuids"]

    # Canonical outputs (byte-stable across schedules)
    with open(out_dir / "clusters.tsv", "w") as fh:
        rows = []
        for key, members in clusters.items():
            y = yuids[key]
            for m in members:
                rows.append(f"{m}\t{y}\n")
        rows.sort()
        fh.writelines(rows)
    with open(out_dir / "conflicts.jsonl", "wb") as fh:
        for c in conflicts:
            fh.write(orjson.dumps(c) + b"\n")

    for name in ("clusters.tsv", "conflicts.jsonl"):
        h = hashlib.sha256((out_dir / name).read_bytes()).hexdigest()
        stats[f"sha256_{name}"] = h
    with open(out_dir / "stats.json", "wb") as fh:
        fh.write(orjson.dumps(stats, option=orjson.OPT_INDENT_2))
    return stats


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("make-prior")
    p.add_argument("--db", required=True)
    p.add_argument("--out", required=True)

    p = sub.add_parser("inject-conflicts")
    p.add_argument("--db", required=True)
    p.add_argument("--n", type=int, default=1000)
    p.add_argument("--out-assertions", required=True)
    p.add_argument("--out-diffs", required=True)

    p = sub.add_parser("perturb")
    p.add_argument("--db", required=True)
    p.add_argument("--every", type=int, default=500)
    p.add_argument("--out-assertions", required=True)

    p = sub.add_parser("make-dirty")
    p.add_argument("--db", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--seed", type=int, default=77)
    p.add_argument("--wrong-links", type=int, default=2000)

    p = sub.add_parser("build")
    p.add_argument("--db", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--prior")
    p.add_argument("--extra-assertions", nargs="*", default=[])
    p.add_argument("--diffs", nargs="*", default=[])
    p.add_argument("--mutations")

    args = ap.parse_args()
    if args.cmd == "make-prior":
        n, dupes = make_prior(args.db, args.out)
        print(f"prior idmap: {n} members ({dupes} URIs shared across clusters)")
    elif args.cmd == "inject-conflicts":
        exp = inject_conflicts(args.db, args.n, args.out_assertions, args.out_diffs)
        print(f"injected {len(exp)} conflict scenarios")
    elif args.cmd == "perturb":
        pert = make_perturbation(args.db, args.every, args.out_assertions)
        print(f"perturbed {len(pert)} clusters with new URIs")
    elif args.cmd == "make-dirty":
        counts = make_dirty(args.db, args.out, seed=args.seed,
                            wrong_links=args.wrong_links)
        print(orjson.dumps(counts).decode())
    elif args.cmd == "build":
        stats = build(args.db, args.out, args.workers, args.seed,
                      prior_file=args.prior,
                      extra_assertions=args.extra_assertions,
                      diff_files=args.diffs,
                      mutations_file=args.mutations)
        print(orjson.dumps(stats, option=orjson.OPT_INDENT_2).decode())


if __name__ == "__main__":
    main()
