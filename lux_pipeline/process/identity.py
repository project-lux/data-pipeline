"""Deterministic identity resolution for the reconcile phase.

Replaces per-record, in-flight YUID minting/merging (ReferenceManager.
manage_identifiers) with a three step flow whose output is a pure function
of the input assertions, independent of worker count and scheduling:

1. During reconciliation each worker slice logs sameAs *assertions*
   (asserter, subject, object) to an append-only file via AssertionWriter.
   No idmap writes happen during reconciliation.
2. After all slices finish, run-identify.py aggregates the assertion files
   into voted pair-edges and clusters them with a constrained union-find:
   edges are processed most-voted-first (lexicographic tie-break) and a
   union is refused when any differentFrom pair spans the two clusters --
   the constraint applies to the whole cluster regardless of which records
   contributed the links. Refused assertions are reported with their
   asserters so conflict policy can evolve without touching the engine.
3. Each cluster is assigned a YUID: members vote to reuse the YUID they
   held in the previous build (so adding a URI to a cluster never changes
   its identity); competing claims after a cluster split are resolved by
   voter count with deterministic tie-breaks; only clusters with no prior
   YUID mint uuid5(min member). The resulting map is bulk-loaded into the
   redis idmap in pipelined batches for the merge phase to use as before.

Validated against all 505k production concept clusters in
experiments/recon_test (byte-identical output across schedules, 100% YUID
reproduction on rebuild, YUIDs stable under new-URI perturbation, and
correct voted resolution of injected wrong links / splits / vanished
members / URI variants).
"""

import glob
import os
import uuid
from collections import defaultdict

import ujson as json


class AssertionWriter:
    """Per-slice log of sameAs assertions.

    Files are written to configs.temp_dir alongside metatypes-*.json and
    reference_uris.txt, named assertions-{slice}.tsv. The reconcile task
    runs its record and reference phases as separate worker invocations,
    so each phase passes its number to keep the files distinct
    (assertions-p{phase}-{slice}.tsv); the identify step aggregates every
    assertions-*.tsv it finds.
    """

    def __init__(self, configs, my_slice=-1, phase=None):
        self.configs = configs
        sfx = f"{my_slice}" if my_slice > -1 else "single"
        if phase is not None:
            fn = f"assertions-p{phase}-{sfx}.tsv"
        else:
            fn = f"assertions-{sfx}.tsv"
        tmp = getattr(configs, "temp_dir", "") or ""
        if tmp:
            fn = os.path.join(tmp, fn)
        self.filename = fn
        self.fh = open(fn, "w")

    def write_record(self, rec):
        """Log the record's equivalence assertions (mirrors the qua handling
        of manage_identifiers). A record with no equivalents logs a
        self-assertion so it still receives a YUID."""
        if not rec or "data" not in rec or "id" not in rec["data"]:
            return
        recid = rec["data"]["id"]
        typ = rec["data"]["type"]
        qrecid = self.configs.make_qua(recid, typ)
        wrote = False
        for eq in rec["data"].get("equivalent", []):
            eqid = eq.get("id")
            if not eqid or eqid == recid:
                continue
            qeq = self.configs.make_qua(eqid, typ)
            self.fh.write(f"{qrecid}\t{qrecid}\t{qeq}\n")
            wrote = True
        if not wrote:
            self.fh.write(f"{qrecid}\t{qrecid}\t{qrecid}\n")

    def close(self):
        self.fh.close()


def load_assertions(files):
    """Aggregate assertion files into pair -> set(asserters).

    Canonical pair = sorted URI strings, so the result is independent of
    file order, line order, and which slice emitted which assertion.
    """
    edges = defaultdict(set)
    for fn in sorted(str(f) for f in files):
        with open(fn) as fh:
            for line in fh:
                try:
                    asserter, a, b = line.rstrip("\n").split("\t")
                except ValueError:
                    continue
                if a > b:
                    a, b = b, a
                edges[(a, b)].add(asserter)
    return edges


def load_diff_pairs(diff_index, nodes):
    """differentFrom pairs from the merged differents index (TabLmdb of
    URI -> URI(s)), lifted onto the qua'd node forms present in the graph.

    The raw table has no types; a diff between two URIs constrains every
    qua'd form of those URIs, matching how the GlobalReconciler applies
    diffs against equivalent ids irrespective of type.
    """
    by_uri = defaultdict(list)
    for n in nodes:
        by_uri[n.split("##qua")[0]].append(n)

    diffs = set()
    if diff_index is None:
        return diffs
    for k in diff_index.keys():
        vals = diff_index[k]
        if isinstance(vals, str):
            vals = [vals]
        for v in vals:
            for qa in by_uri.get(k, ()):
                for qb in by_uri.get(v, ()):
                    if qa == qb:
                        continue
                    diffs.add((qa, qb) if qa < qb else (qb, qa))
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
        # Union by size; ties keep the lexicographically smaller root so
        # the representative is schedule-independent too.
        if (self.size[rx], ry) < (self.size[ry], rx):
            rx, ry = ry, rx
        self.parent[ry] = rx
        self.size[rx] += self.size[ry]
        return rx


def cluster(edges, diffs):
    """Constrained union-find over voted edges.

    Returns (clusters keyed by min member, refused-assertion report).
    Processing order is fixed (votes desc, then pair) so the partition is
    deterministic; when an assertion conflicts with a differentFrom
    constraint, the side with more independent support wins the disputed
    node and the refused link is reported rather than silently dropped.
    """
    dsu = DSU()
    for (a, b) in edges:
        dsu.add(a)
        dsu.add(b)
    for (a, b) in diffs:
        dsu.add(a)
        dsu.add(b)

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
    out = {}
    for members in clusters.values():
        members.sort()
        out[members[0]] = members
    conflicts.sort(key=lambda c: (c["pair"][0], c["pair"][1]))
    return out, conflicts


def mint_yuid(configs, cluster_key):
    """Deterministic mint for clusters with no prior YUID:
    uuid5 of the cluster's canonical (min) member, in the same
    {internal_uri}{slug}/{uuid} shape as IdMap.mint."""
    base = configs.internal_uri
    typ = ""
    if configs.is_qua(cluster_key):
        typ = configs.split_qua(cluster_key)[1]
    # The qua carries the parent type, so subtypes (e.g. Material) share
    # their parent's slug; cosmetic difference only from IdMap.mint.
    slug = configs.ok_record_types.get(typ, "")
    namespace = uuid.uuid5(uuid.NAMESPACE_URL, base)
    uu = str(uuid.uuid5(namespace, cluster_key))
    if slug:
        return f"{base}{slug}/{uu}"
    return f"{base}{uu}"


def assign(clusters, prior, configs):
    """cluster (keyed by min member) -> YUID.

    Members vote for the YUID they held in the prior idmap; most voters
    wins (tie: lexicographically smallest YUID), so adding new URIs to a
    cluster never changes its identity. If a cluster split leaves two
    clusters claiming one YUID, the claim with more voters (tie: smaller
    cluster key) keeps it and the loser mints.
    """
    claims = defaultdict(list)
    for key, members in clusters.items():
        tally = defaultdict(int)
        for m in members:
            y = prior.get(m)
            if y:
                tally[y] += 1
        if tally:
            top = max(tally.values())
            best = min(y for y, n in tally.items() if n == top)
            claims[best].append((tally[best], key))

    assigned = {}
    for yuid, claimants in claims.items():
        claimants.sort(key=lambda vc: (-vc[0], vc[1]))
        assigned[claimants[0][1]] = yuid

    result = {}
    for key in clusters:
        y = assigned.get(key)
        if y is None:
            y = mint_yuid(configs, key)
        result[key] = y
    return result


# ---------------------------------------------------------------------------
# idmap interaction (redis)
# ---------------------------------------------------------------------------

BATCH = 5000


def fetch_prior(idmap, members):
    """Pipelined member -> YUID lookup from the current idmap."""
    prior = {}
    members = list(members)
    conn = idmap.conn
    for i in range(0, len(members), BATCH):
        chunk = members[i:i + BATCH]
        pipe = conn.pipeline(transaction=False)
        for m in chunk:
            pipe.get(idmap._manage_key_in(m))
        for m, val in zip(chunk, pipe.execute(raise_on_error=False)):
            if isinstance(val, str) and val:
                prior[m] = idmap._manage_value_out(val)
    return prior


def apply_assignments(idmap, clusters, yuids, prior):
    """Bulk-load the resolved identity map into the redis idmap.

    Keeps the same storage shape the merge phase expects: member -> yuid
    (string) and yuid -> set(members + current update token). Members that
    moved between YUIDs are removed from their old set; YUID sets left
    holding only tokens are deleted.
    """
    conn = idmap.conn
    token = idmap.update_token
    touched_old = set()
    stats = {"set": 0, "moved": 0, "clusters": len(clusters)}

    items = list(clusters.items())
    for i in range(0, len(items), BATCH):
        pipe = conn.pipeline(transaction=False)
        for key, members in items[i:i + BATCH]:
            yuid = yuids[key]
            iyuid = idmap._manage_value_in(yuid)
            imembers = []
            for m in members:
                im = idmap._manage_key_in(m)
                imembers.append(im)
                old = prior.get(m)
                if old and old != yuid:
                    iold = idmap._manage_value_in(old)
                    pipe.srem(iold, im)
                    touched_old.add(iold)
                    stats["moved"] += 1
                pipe.set(im, iyuid)
                stats["set"] += 1
            pipe.sadd(iyuid, *imembers, token)
        pipe.execute(raise_on_error=False)

    # Remove yuid sets that lost all real members (merged/split away)
    dead = 0
    touched_old = sorted(touched_old)
    for i in range(0, len(touched_old), BATCH):
        chunk = touched_old[i:i + BATCH]
        pipe = conn.pipeline(transaction=False)
        for iold in chunk:
            pipe.smembers(iold)
        left = pipe.execute(raise_on_error=False)
        pipe = conn.pipeline(transaction=False)
        for iold, vals in zip(chunk, left):
            if isinstance(vals, set) and all(v.startswith("__") for v in vals):
                pipe.delete(iold)
                dead += 1
        pipe.execute(raise_on_error=False)
    stats["deleted_yuids"] = dead
    return stats


def resolve_identity(configs, idmap, assertion_files, diff_index=None,
                     conflicts_file="identity_conflicts.jsonl"):
    """Aggregate -> cluster -> assign -> bulk-load. Returns stats."""
    edges = load_assertions(assertion_files)
    nodes = set()
    for (a, b) in edges:
        nodes.add(a)
        nodes.add(b)
    diffs = load_diff_pairs(diff_index, nodes)

    clusters, conflicts = cluster(edges, diffs)
    with open(conflicts_file, "w") as fh:
        for c in conflicts:
            fh.write(json.dumps(c) + "\n")

    prior = fetch_prior(idmap, nodes)
    yuids = assign(clusters, prior, configs)
    stats = apply_assignments(idmap, clusters, yuids, prior)
    stats.update({
        "pairs": len(edges),
        "nodes": len(nodes),
        "diff_pairs": len(diffs),
        "conflicts": len(conflicts),
        "reused": sum(1 for y in yuids.values() if y in set(prior.values())),
    })
    stats["minted"] = stats["clusters"] - stats["reused"]
    return stats


def resolve_identity_from_config(configs, idmap, conflicts_file=None):
    """Locate the assertion logs and differents index via configs, then
    resolve_identity. Shared by the reconcile task's identify step and the
    standalone `identify` CLI command (for re-running resolution from
    existing logs)."""
    files = sorted(glob.glob(os.path.join(configs.temp_dir, "assertions-*.tsv")))
    if not files:
        raise RuntimeError(
            "No assertion logs found in temp_dir; run reconcile first")
    mcfg = configs.results.get("merged", {})
    diff_index = mcfg.get("indexes", {}).get("differents", {}).get("index", None)
    if diff_index is None and "indexes" not in mcfg:
        configs.instantiate("merged", "results")
        diff_index = mcfg.get("indexes", {}).get("differents", {}).get("index", None)
    if diff_index is not None and diff_index.index is None:
        diff_index.open()
    if conflicts_file is None:
        conflicts_file = os.path.join(configs.temp_dir, "identity_conflicts.jsonl")
    return resolve_identity(configs, idmap, files, diff_index=diff_index,
                            conflicts_file=conflicts_file)
