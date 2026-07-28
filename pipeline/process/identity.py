"""Deterministic identity resolution for the reconcile phase.

Replaces per-record, in-flight YUID minting/merging (ReferenceManager.
manage_identifiers) with a three step flow whose output is a pure function
of the input assertions, independent of worker count and scheduling:

1. During reconciliation each worker slice logs sameAs *assertions*
   (subject, object, asserter) to an append-only file via AssertionWriter.
   URIs are shortened to the same curie form the idmap stores internally
   (``aat:300010001##quaType``) and each line is written in canonical order
   -- lesser URI, greater URI, then the asserting record -- so the file can
   be sorted and streamed without loading it into memory. No idmap writes
   happen during reconciliation.
2. After all slices finish, run-identify.py resolves the assertion files
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

At production scale (~45M records, tens of millions of URIs) the whole flow
is executed by ``resolve_identity`` as a sequence of unix ``sort`` passes
over temporary files with streaming Python in between, so peak memory
scales with the *non-singleton* subgraph (URIs that share a cluster with at
least one other URI) rather than with the total record count. The bulk of
records are singletons -- a URI with no equivalents -- and never enter the
in-memory union-find at all.

The pure in-memory helpers (load_assertions, cluster, assign,
apply_assignments, ...) remain the reference implementation: they encode
the exact semantics the streaming path reproduces and back the unit tests /
legacy comparison harness.
"""

import os
import shutil
import subprocess
import tempfile
import uuid
from collections import defaultdict

import ujson as json


# ---------------------------------------------------------------------------
# URI shortening (mirrors IdMap._manage_key_in / _manage_key_out exactly)
# ---------------------------------------------------------------------------

def build_prefix_maps(configs):
    """Reproduce the idmap's namespace<->curie maps from configs alone.

    Matches ``IdMap.__init__``: ``prefix_out`` maps a short name to its full
    namespace (yuid first, then each external source in config order),
    ``prefix_in`` is its inverse. Building from the same source in the same
    order guarantees the on-disk short forms are byte-identical to what redis
    stores internally, which is what lets the srem/sadd round-trip in
    apply_assignments match without any conversion.
    """
    prefix_out = {"yuid": configs.internal_uri}
    for cf in configs.external.values():
        prefix_out[cf["name"]] = cf["namespace"]
    prefix_in = {}
    for k, v in prefix_out.items():
        prefix_in[v] = k
    return prefix_in, prefix_out


def shorten(uri, prefix_in):
    """Full URI -> curie (``http://vocab.getty.edu/aat/1##quaType`` ->
    ``aat:1##quaType``). Byte-for-byte identical to
    RedisCache._manage_key_in: only ``http`` URIs are touched, first
    namespace match (in insertion order) wins, and the ``##qua`` suffix is
    left untouched so is_qua/split_qua still work on the short form. URIs
    with no matching namespace pass through unchanged."""
    if uri.startswith("http"):
        for k, v in prefix_in.items():
            if uri.startswith(k):
                return uri.replace(k, f"{v}:")
    return uri


def expand(uri, prefix_out):
    """Curie -> full URI. Byte-for-byte identical to
    RedisCache._manage_key_out (== IdMap._manage_value_out): only non-``http``
    strings are touched, first short-name match wins. Inverse of shorten for
    every URI a namespace matched."""
    if not uri.startswith("http"):
        for k, v in prefix_out.items():
            if uri.startswith(k):
                return uri.replace(f"{k}:", v)
    return uri


# ---------------------------------------------------------------------------
# Assertion log
# ---------------------------------------------------------------------------

class AssertionWriter:
    """Per-slice append-only log of sameAs assertions.

    Files are written to the working directory alongside metatypes-*.json
    and reference_uris.txt, named assertions-{slice}.tsv. Each line is
    ``lo<TAB>hi<TAB>asserter`` where (lo, hi) are the two URIs of the
    assertion in sorted order and asserter is the record that made it (one
    of the two, in short form). A record with no equivalents logs a
    self-assertion (``s  s  s``) so it still receives a YUID.
    """

    def __init__(self, configs, my_slice=-1):
        self.configs = configs
        self.prefix_in, _ = build_prefix_maps(configs)
        if my_slice > -1:
            fn = f"assertions-{my_slice}.tsv"
        else:
            fn = "assertions-single.tsv"
        fn = os.path.join(configs.temp_dir, fn)
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
        qrecid = shorten(self.configs.make_qua(recid, typ), self.prefix_in)
        wrote = False
        for eq in rec["data"].get("equivalent", []):
            eqid = eq.get("id")
            if not eqid or eqid == recid:
                continue
            qeq = shorten(self.configs.make_qua(eqid, typ), self.prefix_in)
            lo, hi = (qrecid, qeq) if qrecid <= qeq else (qeq, qrecid)
            self.fh.write(f"{lo}\t{hi}\t{qrecid}\n")
            wrote = True
        if not wrote:
            self.fh.write(f"{qrecid}\t{qrecid}\t{qrecid}\n")

    def close(self):
        self.fh.close()


def load_assertions(files):
    """Aggregate assertion files into pair -> set(asserters).

    In-memory reference implementation. Canonical pair = sorted URI strings,
    so the result is independent of file order, line order, and which slice
    emitted which assertion. The streaming path in resolve_identity produces
    the same voted edges without holding this dict in memory.
    """
    edges = defaultdict(set)
    for fn in sorted(str(f) for f in files):
        with open(fn) as fh:
            for line in fh:
                parts = line.rstrip("\n").split("\t")
                if len(parts) == 3:
                    a, b, asserter = parts
                elif len(parts) == 2:
                    # legacy 2-column form: asserter was the first column
                    a, b = parts
                    asserter = a
                else:
                    continue
                if a > b:
                    a, b = b, a
                edges[(a, b)].add(asserter)
    return edges


def load_diff_pairs(diff_index, nodes, prefix_in=None):
    """differentFrom pairs from the merged differents index (TabLmdb of
    URI -> URI(s)), lifted onto the qua'd node forms present in the graph.

    The raw table has no types; a diff between two URIs constrains every
    qua'd form of those URIs, matching how the GlobalReconciler applies
    diffs against equivalent ids irrespective of type.

    ``nodes`` are curie-shortened when the graph is (pass ``prefix_in`` so
    the full-URI diff keys are shortened to match); otherwise both sides are
    full URIs and prefix_in is left None.
    """
    by_uri = defaultdict(list)
    for n in nodes:
        by_uri[n.split("##qua")[0]].append(n)

    diffs = set()
    if diff_index is None:
        return diffs

    def key_of(uri):
        if prefix_in is not None:
            uri = shorten(uri, prefix_in)
        return uri.split("##qua")[0]

    for k in diff_index.keys():
        vals = diff_index[k]
        if isinstance(vals, str):
            vals = [vals]
        ks = key_of(k)
        srcs = by_uri.get(ks)
        if not srcs:
            continue
        for v in vals:
            for qa in srcs:
                for qb in by_uri.get(key_of(v), ()):
                    if qa == qb:
                        continue
                    diffs.add((qa, qb) if qa < qb else (qb, qa))
    return diffs


# ---------------------------------------------------------------------------
# Union-find
# ---------------------------------------------------------------------------

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


def _constrained_union(dsu, edge_stream, diffs, want_conflicts=True):
    """Run the constrained union-find, consuming ``edge_stream`` -- an
    iterable of ``(a, b, votes)`` already ordered most-voted-first, then by
    pair -- against the ``diffs`` enemy set. Shared by the in-memory
    ``cluster`` and the streaming path so both refuse the same links.

    Returns a list of conflict dicts (without asserters filled in, since the
    streaming caller attaches those from disk). ``dsu`` must already contain
    every node that can be unioned.
    """
    enemies = defaultdict(set)
    for (a, b) in diffs:
        ra, rb = dsu.find(a), dsu.find(b)
        if ra != rb:
            enemies[ra].add(rb)
            enemies[rb].add(ra)

    conflicts = []
    for a, b, votes in edge_stream:
        if a == b:
            continue
        ra, rb = dsu.find(a), dsu.find(b)
        if ra == rb:
            continue
        if rb in enemies[ra]:
            if want_conflicts:
                conflicts.append({
                    "pair": [a, b],
                    "votes": votes,
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
    return conflicts


def cluster(edges, diffs):
    """Constrained union-find over voted edges (in-memory reference).

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

    order = sorted(edges.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    stream = ((a, b, len(asserters)) for (a, b), asserters in order)
    conflicts = _constrained_union(dsu, stream, diffs)
    # attach asserters for the (rare) refused links
    want = {(c["pair"][0], c["pair"][1]): c for c in conflicts}
    for (a, b), asserters in edges.items():
        c = want.get((a, b))
        if c is not None:
            c["asserters"] = sorted(asserters)
    for c in conflicts:
        c.setdefault("asserters", [])

    clusters = defaultdict(list)
    for node in dsu.parent:
        clusters[dsu.find(node)].append(node)
    out = {}
    for members in clusters.values():
        members.sort()
        out[members[0]] = members
    conflicts.sort(key=lambda c: (c["pair"][0], c["pair"][1]))
    return out, conflicts


# ---------------------------------------------------------------------------
# YUID assignment
# ---------------------------------------------------------------------------

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


def _best_claim(prior_values):
    """Given the prior YUIDs held by a cluster's members (Nones/blanks
    allowed), return (best_yuid, votes) or None if no member had a prior.
    Most votes wins; ties break to the lexicographically smallest YUID --
    identical for the in-memory and streaming assignment paths."""
    tally = defaultdict(int)
    for y in prior_values:
        if y:
            tally[y] += 1
    if not tally:
        return None
    top = max(tally.values())
    best = min(y for y, n in tally.items() if n == top)
    return best, tally[best]


def assign(clusters, prior, configs):
    """cluster (keyed by min member) -> YUID (in-memory reference).

    Members vote for the YUID they held in the prior idmap; most voters
    wins (tie: lexicographically smallest YUID), so adding new URIs to a
    cluster never changes its identity. If a cluster split leaves two
    clusters claiming one YUID, the claim with more voters (tie: smaller
    cluster key) keeps it and the loser mints.
    """
    claims = defaultdict(list)
    for key, members in clusters.items():
        claim = _best_claim(prior.get(m) for m in members)
        if claim:
            best, votes = claim
            claims[best].append((votes, key))

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
    """Pipelined member -> YUID lookup from the current idmap (in-memory
    reference; the streaming path fetches priors in batches on the fly)."""
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
    """Bulk-load the resolved identity map into the redis idmap (in-memory
    reference used by the comparison harness; resolve_identity applies the
    streaming equivalent).

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

    stats["deleted_yuids"] = _delete_dead_yuids(idmap, sorted(touched_old))
    return stats


def _delete_dead_yuids(idmap, iold_keys):
    """Remove yuid sets that lost all real members (only update tokens left).
    ``iold_keys`` is an iterable of already-internal (short) yuid keys."""
    conn = idmap.conn
    dead = 0
    iold_keys = list(iold_keys)
    for i in range(0, len(iold_keys), BATCH):
        chunk = iold_keys[i:i + BATCH]
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
    return dead


# ---------------------------------------------------------------------------
# Streaming resolution (the production path)
# ---------------------------------------------------------------------------

def _sort(inputs, output, keys, tmpdir, buffer):
    """Run ``LC_ALL=C sort`` (byte order == Python str order for UTF-8, so
    the on-disk order matches every in-memory min/max the code does)."""
    cmd = ["sort", *keys, "-t", "\t", "-T", tmpdir, "-S", buffer,
           "-o", output, *[str(i) for i in inputs]]
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    subprocess.run(cmd, env=env, check=True)


def _iter_tsv(path):
    with open(path) as fh:
        for line in fh:
            yield line.rstrip("\n").split("\t")


def _grouped(path):
    """Yield (key, [rows]) for runs of rows sharing the first column. The
    file must already be sorted on that column."""
    cur = None
    batch = []
    for parts in _iter_tsv(path):
        k = parts[0]
        if cur is None:
            cur = k
        if k != cur:
            yield cur, batch
            cur, batch = k, []
        batch.append(parts)
    if batch:
        yield cur, batch


def _chunks(iterable, n):
    chunk = []
    for x in iterable:
        chunk.append(x)
        if len(chunk) >= n:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _aggregate_edges(sorted_path, edges_path, selfs_path):
    """Stream the sorted assertions; collapse each (a,b) run to one voted
    edge (distinct asserters = votes). Real edges (a!=b) go to edges_path
    and their endpoints into the returned dsu_nodes set (the one large
    in-memory structure -- non-singleton nodes only); self-markers (a==b)
    have their node written to selfs_path for the singleton pass."""
    dsu_nodes = set()
    n_edges = 0
    with open(edges_path, "w") as fe, open(selfs_path, "w") as fs:
        cur = None
        votes = 0
        last_as = None
        for parts in _iter_tsv(sorted_path):
            if len(parts) != 3:
                continue
            a, b, asserter = parts
            key = (a, b)
            if key != cur:
                if cur is not None:
                    if cur[0] == cur[1]:
                        fs.write(f"{cur[0]}\n")
                    else:
                        fe.write(f"{cur[0]}\t{cur[1]}\t{votes}\n")
                        dsu_nodes.add(cur[0])
                        dsu_nodes.add(cur[1])
                        n_edges += 1
                cur = key
                votes = 0
                last_as = None
            if asserter != last_as:
                votes += 1
                last_as = asserter
        if cur is not None:
            if cur[0] == cur[1]:
                fs.write(f"{cur[0]}\n")
            else:
                fe.write(f"{cur[0]}\t{cur[1]}\t{votes}\n")
                dsu_nodes.add(cur[0])
                dsu_nodes.add(cur[1])
                n_edges += 1
    return dsu_nodes, n_edges


def _attach_asserters(sorted_path, conflicts):
    """Fill in the asserter lists for the (rare) refused links by a single
    scan of the sorted assertions -- the vote aggregation dropped asserter
    identities, but conflicts are few so we recover them by pair."""
    want = {(c["pair"][0], c["pair"][1]): set() for c in conflicts}
    if want:
        for parts in _iter_tsv(sorted_path):
            if len(parts) != 3:
                continue
            a, b, asserter = parts
            s = want.get((a, b))
            if s is not None:
                s.add(asserter)
    for c in conflicts:
        c["asserters"] = sorted(want.get((c["pair"][0], c["pair"][1]), ()))


def _build_clusters(edges_byvote, dsu_nodes, diffs, clusters_path):
    """Constrained union-find over the vote-ordered edge file; write
    ``root<TAB>member`` for every non-singleton node and return the conflict
    report (asserters not yet attached)."""
    dsu = DSU()
    for n in dsu_nodes:
        dsu.add(n)
    stream = ((a, b, int(v)) for a, b, v in _iter_tsv(edges_byvote))
    conflicts = _constrained_union(dsu, stream, diffs)
    with open(clusters_path, "w") as fh:
        for node in dsu.parent:
            fh.write(f"{dsu.find(node)}\t{node}\n")
    conflicts.sort(key=lambda c: (c["pair"][0], c["pair"][1]))
    return conflicts


def _append_singletons(selfs_sorted, dsu_nodes, clusters_path):
    """Every distinct self-marked node that is not in a real cluster is its
    own singleton cluster. Appended to the clusters file (which is re-sorted
    by root afterwards)."""
    n = 0
    prev = None
    with open(clusters_path, "a") as fh:
        for parts in _iter_tsv(selfs_sorted):
            node = parts[0]
            if node == prev:
                continue
            prev = node
            if node not in dsu_nodes:
                fh.write(f"{node}\t{node}\n")
                n += 1
    return n


def _fetch_prior_stream(idmap, clusters_sorted, out_path):
    """Batched member -> prior-YUID lookup, streamed. Reads the by-root
    clusters file and writes ``root<TAB>member<TAB>prior`` (prior blank when
    the member had none), preserving order so the file stays grouped."""
    conn = idmap.conn
    with open(out_path, "w") as fout:
        for chunk in _chunks(_iter_tsv(clusters_sorted), BATCH):
            pipe = conn.pipeline(transaction=False)
            for root, member in chunk:
                pipe.get(idmap._manage_key_in(member))
            vals = pipe.execute(raise_on_error=False)
            for (root, member), val in zip(chunk, vals):
                prior = ""
                if isinstance(val, str) and val:
                    prior = idmap._manage_value_out(val)
                fout.write(f"{root}\t{member}\t{prior}\n")


def _emit_claims(clusters_prior, prefix_out, claims_path, detail_path):
    """Per cluster (grouped by root): compute the full-URI cluster key (min
    over expanded members -- byte-identical to the in-memory min member),
    tally prior YUIDs into one claim, and write the detail rows the apply
    pass needs. Returns the cluster count."""
    n_clusters = 0
    with open(claims_path, "w") as fc, open(detail_path, "w") as fd:
        for _root, rows in _grouped(clusters_prior):
            members = [(r[1], r[2] if len(r) > 2 else "") for r in rows]
            full_key = min(expand(m, prefix_out) for m, _ in members)
            claim = _best_claim(p for _, p in members)
            if claim:
                best, votes = claim
                fc.write(f"{best}\t{votes}\t{full_key}\n")
            for m, p in members:
                fd.write(f"{full_key}\t{m}\t{p}\n")
            n_clusters += 1
    return n_clusters


def _resolve_claims(claims_sorted, won_path):
    """Claims are sorted (yuid asc, votes desc, key asc), so the first row of
    each yuid group is the winner; it keeps the YUID, everyone else mints."""
    with open(won_path, "w") as fh:
        for yuid, rows in _grouped(claims_sorted):
            winner = rows[0]  # (yuid, votes, full_key)
            fh.write(f"{winner[2]}\t{yuid}\n")


def _apply_stream(idmap, configs, detail_sorted, won_sorted, touched_path):
    """Merge the by-key detail with the by-key winners, minting where a
    cluster kept no prior YUID, and bulk-load the result into redis. Old
    yuid keys that lost a member are appended to touched_path for the final
    dead-set sweep."""
    conn = idmap.conn
    token = idmap.update_token
    stats = {"set": 0, "moved": 0, "clusters": 0}

    won = _grouped(won_sorted)
    won_key = None
    won_yuid = None

    def advance_to(key):
        nonlocal won_key, won_yuid
        while True:
            if won_key is not None and won_key >= key:
                return
            try:
                k, rows = next(won)
            except StopIteration:
                won_key = None
                won_yuid = None
                return
            won_key, won_yuid = k, rows[0][1]

    pipe = conn.pipeline(transaction=False)
    ops = 0
    with open(touched_path, "w") as ftouched:
        for full_key, rows in _grouped(detail_sorted):
            advance_to(full_key)
            if won_key == full_key:
                yuid = won_yuid
            else:
                yuid = mint_yuid(configs, full_key)
            iyuid = idmap._manage_value_in(yuid)
            imembers = []
            for r in rows:
                member = r[1]
                prior = r[2] if len(r) > 2 else ""
                im = idmap._manage_key_in(member)
                imembers.append(im)
                if prior and prior != yuid:
                    iold = idmap._manage_value_in(prior)
                    pipe.srem(iold, im)
                    ftouched.write(f"{iold}\n")
                    stats["moved"] += 1
                pipe.set(im, iyuid)
                stats["set"] += 1
                ops += 1
            pipe.sadd(iyuid, *imembers, token)
            ops += 1
            stats["clusters"] += 1
            if ops >= BATCH:
                pipe.execute(raise_on_error=False)
                pipe = conn.pipeline(transaction=False)
                ops = 0
        if ops:
            pipe.execute(raise_on_error=False)
    return stats


def _distinct(path):
    prev = None
    for parts in _iter_tsv(path):
        if parts and parts[0] != prev:
            prev = parts[0]
            yield prev


def resolve_identity(configs, idmap, assertion_files, diff_index=None,
                     conflicts_file="identity_conflicts.jsonl",
                     work_dir=None, keep_temp=False):
    """Resolve the identity map from the assertion logs, streaming through
    unix ``sort`` so peak memory scales with the non-singleton subgraph
    rather than the total record count. Returns stats.

    The steps mirror the in-memory reference (load_assertions -> cluster ->
    assign -> apply_assignments) exactly, done on disk:

      1. sort every assertion line by (a, b, asserter)
      2. stream -> voted real edges + self-markers; collect non-singleton
         nodes (the only large in-memory set)
      3. re-sort edges most-voted-first and run the constrained union-find
      4. singletons (self-marked, not in a real cluster) join as 1-member
         clusters
      5. per cluster: tally prior YUIDs, resolve split contention by sort,
         mint where there is no prior
      6. bulk-load into redis, then sweep now-empty old YUID sets
    """
    prefix_in = idmap.prefix_map_in
    prefix_out = idmap.prefix_map_out
    buffer = os.getenv("LUX_SORT_BUFFER", "1G")

    if work_dir is None:
        work_dir = "."
    td = tempfile.mkdtemp(prefix="identity-", dir=work_dir)
    p = lambda name: os.path.join(td, name)
    try:
        srt = p("assertions.sorted")
        edg = p("edges.tsv")
        slf = p("selfs.tsv")
        slf_s = p("selfs.sorted")
        edgv = p("edges.byvote")
        clu = p("clusters.tsv")
        clus = p("clusters.sorted")
        clup = p("clusters.prior")
        clm = p("claims.tsv")
        clms = p("claims.sorted")
        det = p("detail.tsv")
        dets = p("detail.sorted")
        won = p("won.tsv")
        wons = p("won.sorted")
        tch = p("touched.tsv")
        tch_s = p("touched.sorted")

        print("sorting assertions...")
        _sort(assertion_files, srt,
              ["-k1,1", "-k2,2", "-k3,3"], td, buffer)

        print("aggregating voted edges...")
        dsu_nodes, n_edges = _aggregate_edges(srt, edg, slf)

        print("loading diff pairs...")
        diffs = load_diff_pairs(diff_index, dsu_nodes, prefix_in=prefix_in)

        print("clustering...")
        _sort([edg], edgv, ["-k3,3nr", "-k1,1", "-k2,2"], td, buffer)
        conflicts = _build_clusters(edgv, dsu_nodes, diffs, clu)
        _attach_asserters(srt, conflicts)
        with open(conflicts_file, "w") as fh:
            for c in conflicts:
                fh.write(json.dumps(c) + "\n")

        print("adding singletons...")
        _sort([slf], slf_s, ["-u", "-k1,1"], td, buffer)
        n_singletons = _append_singletons(slf_s, dsu_nodes, clu)
        _sort([clu], clus, ["-k1,1"], td, buffer)

        print("fetching prior...")
        _fetch_prior_stream(idmap, clus, clup)

        print("assigning...")
        n_clusters = _emit_claims(clup, prefix_out, clm, det)
        _sort([clm], clms, ["-k1,1", "-k2,2nr", "-k3,3"], td, buffer)
        _resolve_claims(clms, won)

        print("applying assignments...")
        _sort([det], dets, ["-k1,1"], td, buffer)
        _sort([won], wons, ["-k1,1"], td, buffer)
        stats = _apply_stream(idmap, configs, dets, wons, tch)

        _sort([tch], tch_s, ["-u", "-k1,1"], td, buffer)
        stats["deleted_yuids"] = _delete_dead_yuids(idmap, _distinct(tch_s))

        stats.update({
            "pairs": n_edges,
            "nodes": len(dsu_nodes) + n_singletons,
            "diff_pairs": len(diffs),
            "conflicts": len(conflicts),
            "clusters": n_clusters,
            "singletons": n_singletons,
        })
        return stats
    finally:
        if not keep_temp:
            shutil.rmtree(td, ignore_errors=True)
