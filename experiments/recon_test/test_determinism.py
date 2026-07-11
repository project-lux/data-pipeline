# /// script
# requires-python = ">=3.11"
# dependencies = ["lmdb", "orjson"]
# ///
"""
Test harness for the deterministic reconciliation prototype.

Unit tests exercise the constrained/voted clustering semantics, then the
integration run replays all ~505k production concept clusters through the
prototype under different worker counts and emission orders and requires
byte-identical outputs, 100% YUID reuse against the production idmap, and
YUID stability under perturbation (new URIs added to clusters).

Usage:
  uv run test_determinism.py --db /Users/wjm55/lux-concepts-scc/concepts.lmdb \
      --work /tmp/recon-test
"""

import argparse
import hashlib
import sys
import time
from collections import defaultdict
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).parent))
from detrecon import (assign, build, cluster, inject_conflicts, load_prior,
                      make_dirty, make_perturbation, make_prior)

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
failures = []


def check(name, ok, detail=""):
    print(f"  [{PASS if ok else FAIL}] {name}" + (f"  ({detail})" if detail else ""))
    if not ok:
        failures.append(name)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def edges_from(assertions):
    edges = defaultdict(set)
    for asserter, a, b in assertions:
        if a > b:
            a, b = b, a
        edges[(a, b)].add(asserter)
    return edges


def unit_tests():
    print("Unit tests:")

    # The canonical scenario: 1=A,B / 2=C,D / 3=B,C but B != C.
    # 3 must still be assigned to one side (not dropped), the B=C link is
    # refused, and the two clusters stay separate.
    base = [
        ("1", "1", "A"), ("1", "1", "B"), ("1", "A", "B"),
        ("2", "2", "C"), ("2", "2", "D"), ("2", "C", "D"),
        ("3", "3", "B"), ("3", "3", "C"), ("3", "B", "C"),
    ]
    clusters, conflicts = cluster(edges_from(base), {("B", "C")})
    roots = {m: k for k, ms in clusters.items() for m in ms}
    check("B and C stay separate", roots["B"] != roots["C"])
    check("record 3 joins one cluster, not neither",
          roots["3"] in (roots["B"], roots["C"]))
    check("3=B/3=C tie broken deterministically (lexicographic -> B side)",
          roots["3"] == roots["B"])
    refused = {tuple(c["pair"]) for c in conflicts}
    check("refused links reported with asserters",
          ("B", "C") in refused and all(c["asserters"] for c in conflicts))

    # Voting: a second record also asserts 3=C -> C side now outweighs B.
    voted = base + [("4", "3", "C")]
    clusters, conflicts = cluster(edges_from(voted), {("B", "C")})
    roots = {m: k for k, ms in clusters.items() for m in ms}
    check("extra assertion flips 3 to the C side (voting works)",
          roots["3"] == roots["C"] and roots["3"] != roots["B"])

    # differentFrom applies cluster-wide, not just to the asserted pair:
    # chain A~B, B~C, C~D with diff(A, D) must refuse whichever link would
    # transitively connect A and D, no matter which record asserted it.
    clusters, conflicts = cluster(edges_from([
        ("1", "A", "B"), ("2", "C", "D"), ("3", "B", "C"),
    ]), {("A", "D")})
    roots = {m: k for k, ms in clusters.items() for m in ms}
    check("diff constraint blocks merge via any member (cluster-level)",
          roots["A"] != roots["D"] and len(conflicts) == 1)

    # Determinism of clustering under input order is covered by the
    # integration runs; here check YUID assignment semantics.
    prior = {"A": "yuid-1", "B": "yuid-1", "C": "yuid-2", "D": "yuid-2"}
    clusters, _ = cluster(edges_from(base), {("B", "C")})
    yuids = assign(clusters, prior)
    roots = {m: k for k, ms in clusters.items() for m in ms}
    check("clusters reuse their prior YUIDs",
          yuids[roots["A"]] == "yuid-1" and yuids[roots["C"]] == "yuid-2")

    # Adding a brand-new URI to a cluster must not change its YUID.
    grown = base + [("1", "A", "NEW")]
    clusters, _ = cluster(edges_from(grown), {("B", "C")})
    yuids = assign(clusters, prior)
    roots = {m: k for k, ms in clusters.items() for m in ms}
    check("new URI in cluster does not change YUID",
          yuids[roots["NEW"]] == "yuid-1")

    # Cluster split: majority keeps the YUID, minority mints a fresh one.
    split = [("1", "A", "B"), ("1", "B", "E"), ("2", "C", "C")]
    prior2 = {"A": "yuid-9", "B": "yuid-9", "E": "yuid-9", "C": "yuid-9"}
    clusters, _ = cluster(edges_from(split), set())
    yuids = assign(clusters, prior2)
    roots = {m: k for k, ms in clusters.items() for m in ms}
    check("split: majority cluster keeps YUID",
          yuids[roots["A"]] == "yuid-9")
    check("split: minority cluster mints a new deterministic YUID",
          yuids[roots["C"]] != "yuid-9" and yuids[roots["C"]].startswith("https://"))


# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------

def sha(fn):
    return hashlib.sha256(Path(fn).read_bytes()).hexdigest()


def integration(db, work):
    work = Path(work)
    work.mkdir(parents=True, exist_ok=True)
    print("\nIntegration (505k production concept clusters):")

    prior_file = work / "prior_idmap.tsv"
    if not prior_file.exists():
        t0 = time.time()
        n, dupes = make_prior(db, prior_file)
        print(f"  prior idmap: {n} members, {dupes} shared URIs "
              f"({time.time()-t0:.1f}s)")

    conf_a = work / "conflict_assertions.tsv"
    conf_d = work / "conflict_diffs.tsv"
    expected_conflicts = inject_conflicts(db, 1000, conf_a, conf_d)
    print(f"  injected {len(expected_conflicts)} synthetic conflict scenarios")

    # Two rebuilds with different parallelism and shuffled emission order
    runs = {}
    for name, workers, seed in [("A", 24, 11), ("B", 8, 999)]:
        t0 = time.time()
        stats = build(db, work / f"run{name}", workers, seed,
                      prior_file=str(prior_file),
                      extra_assertions=[str(conf_a)], diff_files=[str(conf_d)])
        stats["wall_s"] = round(time.time() - t0, 1)
        runs[name] = stats
        print(f"  run {name}: workers={workers} seed={seed} "
              f"wall={stats['wall_s']}s emit={stats['phase1_emit_s']}s "
              f"cluster={stats['phase2_cluster_s']}s "
              f"assign={stats['phase3_assign_s']}s "
              f"clusters={stats['clusters']} blocked={stats['conflicts_blocked']}")

    check("clusters.tsv byte-identical across schedules",
          runs["A"]["sha256_clusters.tsv"] == runs["B"]["sha256_clusters.tsv"],
          runs["A"]["sha256_clusters.tsv"][:16])
    check("conflicts.jsonl byte-identical across schedules",
          runs["A"]["sha256_conflicts.jsonl"] == runs["B"]["sha256_conflicts.jsonl"])

    # YUID reuse against production
    prior = load_prior(prior_file)
    got = {}
    with open(work / "runA" / "clusters.tsv") as fh:
        for line in fh:
            m, y = line.rstrip("\n").split("\t")
            got[m] = y
    real = [m for m in got if not m.startswith("test://")]
    matched = sum(1 for m in real if prior.get(m) == got[m])
    pct = 100.0 * matched / len(real)
    check("production YUIDs reproduced on rebuild",
          pct >= 99.99, f"{matched}/{len(real)} = {pct:.3f}%")

    # Synthetic conflicts resolved as designed: b/c separated, CR on c's side
    ok_sep = ok_cr = 0
    for e in expected_conflicts:
        yb, yc, ycr = got.get(e["b"]), got.get(e["c"]), got.get(e["cr"])
        if yb != yc:
            ok_sep += 1
        if ycr == yc:
            ok_cr += 1
    n = len(expected_conflicts)
    check("diff-constrained clusters stayed separate", ok_sep == n,
          f"{ok_sep}/{n}")
    check("conflict record joined the higher-vote side", ok_cr == n,
          f"{ok_cr}/{n}")

    # Perturbation: new URIs added to every 500th cluster; YUIDs must hold
    pert_a = work / "perturb_assertions.tsv"
    perturbed = make_perturbation(db, 500, pert_a)
    stats = build(db, work / "runC", 24, 11, prior_file=str(prior_file),
                  extra_assertions=[str(conf_a), str(pert_a)],
                  diff_files=[str(conf_d)])
    gotc = {}
    with open(work / "runC" / "clusters.tsv") as fh:
        for line in fh:
            m, y = line.rstrip("\n").split("\t")
            gotc[m] = y
    stable = sum(1 for fake, yuid in perturbed if gotc.get(fake) == yuid)
    check("perturbed clusters keep their YUID (new URI joined it)",
          stable == len(perturbed), f"{stable}/{len(perturbed)}")
    unchanged = sum(1 for m in real if gotc.get(m) == got[m])
    check("all other members unaffected by perturbation",
          unchanged == len(real), f"{unchanged}/{len(real)}")

    # Cold start (no prior idmap) must also be schedule-independent
    cold = {}
    for name, workers, seed in [("D1", 24, 7), ("D2", 6, 123)]:
        stats = build(db, work / f"run{name}", workers, seed,
                      extra_assertions=[str(conf_a)], diff_files=[str(conf_d)])
        cold[name] = stats
    check("cold start (all-minted uuid5) byte-identical across schedules",
          cold["D1"]["sha256_clusters.tsv"] == cold["D2"]["sha256_clusters.tsv"])


def integration_dirty(db, work):
    """Replay the concepts as *unclean* input: wrong cross-cluster links,
    split assertion sets, vanished members, URI variants -- verify the
    engine is still schedule-independent and resolves the dirt as designed.
    """
    work = Path(work) / "dirty"
    work.mkdir(parents=True, exist_ok=True)
    print("\nIntegration, unclean input:")

    prior_file = Path(work).parent / "prior_idmap.tsv"
    if not prior_file.exists():
        make_prior(db, prior_file)
    prior = load_prior(prior_file)

    counts = make_dirty(db, work)
    print(f"  dirt: {counts['split']} splits, {counts['drop']} vanished "
          f"members, {counts['variant']} URI variants, "
          f"{counts['merge']} uncontradicted wrong links, "
          f"{counts['blocked']} contradicted wrong links")
    expect = orjson.loads((work / "expectations.json").read_bytes())

    runs = {}
    for name, workers, seed in [("A", 24, 5), ("B", 8, 4242)]:
        t0 = time.time()
        stats = build(db, work / f"run{name}", workers, seed,
                      prior_file=str(prior_file),
                      extra_assertions=[str(work / "wrong_links.tsv")],
                      diff_files=[str(work / "wrong_diffs.tsv")],
                      mutations_file=work / "mutations.json")
        stats["wall_s"] = round(time.time() - t0, 1)
        runs[name] = stats
        print(f"  run {name}: workers={workers} seed={seed} "
              f"wall={stats['wall_s']}s clusters={stats['clusters']} "
              f"blocked={stats['conflicts_blocked']}")

    check("dirty build byte-identical across schedules",
          runs["A"]["sha256_clusters.tsv"] == runs["B"]["sha256_clusters.tsv"]
          and runs["A"]["sha256_conflicts.jsonl"] == runs["B"]["sha256_conflicts.jsonl"],
          runs["A"]["sha256_clusters.tsv"][:16])

    got = {}
    with open(work / "runA" / "clusters.tsv") as fh:
        for line in fh:
            m, y = line.rstrip("\n").split("\t")
            got[m] = y

    # Untouched clusters must keep their production YUIDs
    touched = set()
    for e in expect["split"]:
        touched.update(e["part_a"] + e["part_b"])
    for e in expect["drop"]:
        touched.update(e["kept"] + [e["member"]])
    for e in expect["variant"]:
        touched.add(e["variant"])
    for e in expect["merge"] + expect["blocked"]:
        touched.update([e["a"], e["b"]])
    merged_yuids = {e["yuid_a"] for e in expect["merge"]}
    merged_yuids |= {e["yuid_b"] for e in expect["merge"]}
    untouched = [m for m in got
                 if m in prior and m not in touched
                 and prior[m] not in merged_yuids]
    ok = sum(1 for m in untouched if got[m] == prior[m])
    check("untouched clusters keep production YUIDs",
          ok == len(untouched), f"{ok}/{len(untouched)}")

    # Wrong links WITHOUT a differentFrom: clusters merge, majority YUID wins
    ok = sum(1 for e in expect["merge"]
             if got.get(e["a"]) == got.get(e["b"]) == e["winner"])
    check("uncontradicted wrong links merge to the majority YUID",
          ok == len(expect["merge"]), f"{ok}/{len(expect['merge'])}")

    # Wrong links WITH a differentFrom: refused, both sides keep their YUIDs
    ok = sum(1 for e in expect["blocked"]
             if got.get(e["a"]) == e["yuid_a"] and got.get(e["b"]) == e["yuid_b"])
    check("contradicted wrong links refused, YUIDs preserved",
          ok == len(expect["blocked"]), f"{ok}/{len(expect['blocked'])}")
    refused = set()
    with open(work / "runA" / "conflicts.jsonl", "rb") as fh:
        for line in fh:
            c = orjson.loads(line)
            refused.add(tuple(sorted(c["pair"])))
    ok = sum(1 for e in expect["blocked"]
             if tuple(sorted([e["a"], e["b"]])) in refused)
    check("every refused wrong link appears in the conflict report",
          ok == len(expect["blocked"]), f"{ok}/{len(expect['blocked'])}")

    # Splits: majority part keeps the YUID, minority mints deterministically
    ok_keep = ok_mint = 0
    for e in expect["split"]:
        a, b = e["part_a"], e["part_b"]
        if len(a) == len(b):
            # tie: assign() gives the YUID to the claim with the smaller
            # cluster key (min member)
            big, small = (a, b) if min(a) < min(b) else (b, a)
        elif len(a) > len(b):
            big, small = a, b
        else:
            big, small = b, a
        if all(got.get(m) == e["yuid"] for m in big):
            ok_keep += 1
        smalls = {got.get(m) for m in small}
        if len(smalls) == 1 and e["yuid"] not in smalls:
            ok_mint += 1
    n = len(expect["split"])
    check("split clusters: majority part keeps YUID", ok_keep == n,
          f"{ok_keep}/{n}")
    check("split clusters: minority part mints one new YUID", ok_mint == n,
          f"{ok_mint}/{n}")

    # Vanished members: cluster keeps YUID despite the stale prior entry
    ok = sum(1 for e in expect["drop"]
             if all(got.get(m) == e["yuid"] for m in e["kept"])
             and e["member"] not in got)
    check("clusters with vanished members keep their YUID",
          ok == len(expect["drop"]), f"{ok}/{len(expect['drop'])}")

    # URI variants: the duplicate joins, YUID unmoved
    ok = sum(1 for e in expect["variant"]
             if got.get(e["variant"]) == e["yuid"])
    check("URI-variant duplicates join their cluster, YUID unmoved",
          ok == len(expect["variant"]), f"{ok}/{len(expect['variant'])}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/Users/wjm55/lux-concepts-scc/concepts.lmdb")
    ap.add_argument("--work", default="/tmp/recon-test")
    ap.add_argument("--skip-integration", action="store_true")
    ap.add_argument("--only-dirty", action="store_true")
    args = ap.parse_args()

    unit_tests()
    if not args.skip_integration:
        if not args.only_dirty:
            integration(args.db, args.work)
        integration_dirty(args.db, args.work)

    print()
    if failures:
        print(f"{len(failures)} FAILED: {failures}")
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    main()
