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

"""

import os
import shutil
import subprocess
import tempfile
import uuid
import glob
from collections import defaultdict

import ujson as json

from pipeline.storage.idmap.lmdb import TabLmdb

# ---------------------------------------------------------------------------
# Union-find implementation
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


class IdentityResolver(object):

    """Per-slice append-only log of sameAs assertions.

    Files are written to the temp directory, named assertions-{slice}.tsv. Each line is
    ``lo<TAB>hi<TAB>asserter`` where (lo, hi) are the two URIs of the
    assertion in sorted order and asserter is the record that made it (one
    of the two, in short form). A record with no equivalents logs a
    self-assertion (``s  s  s``) so it still receives a YUID.
    """
    
    def __init__(self, configs, idmap, my_slice=-1, max_slice=-1):
        self.configs = configs
        self.idmap = idmap
        self.my_slice = my_slice
        self.max_slice = max_slice        

        self.batch_size = 5000
        self.sort_buffer_size = os.getenv("LUX_SORT_BUFFER", "1G")

        self.sorted_path = "assertions.sorted"
        self.edges_path = "edges.tsv"
        self.selfs_path = "selfs.tsv"
        self.selfs_sorted_path = "selfs.sorted"
        self.edges_by_vote_path = "edges.byvote"
        self.clusters_path = "clusters.tsv"
        self.clusters_sorted_path = "clusters.sorted"
        self.prior_path = "clusters.prior"
        self.claims_path = "claims.tsv"
        self.claims_sorted_path = "claims.sorted"
        self.detail_path = "detail.tsv"
        self.detail_sorted_path = "detail.sorted"
        self.won_path = "won.tsv"
        self.won_sorted_path = "won.sorted"
        self.touched_path = "touched.tsv"
        self.touched_sorted_path = "touched.sorted"

        self.prefix_in = idmap.prefix_map_in
        self.prefix_out = idmap.prefix_map_out

        if my_slice > -1:
            fn = f"assertions-{my_slice}.tsv"
        else:
            fn = "assertions-single.tsv"
        fn = os.path.join(configs.temp_dir, fn)
        self.filename = fn
        self.fh = open(fn, "w", buffering=1024 * 1024)
        

    def shorten(self, uri):
        if uri.startswith("http"):
            for k, v in self.prefix_in.items():
                if uri.startswith(k):
                    return uri.replace(k, f"{v}:")
        return uri
    
    
    def expand(self, uri):
        if not uri.startswith("http"):
            for k, v in self.prefix_out.items():
                if uri.startswith(k):
                    return uri.replace(f"{k}:", v)
        return uri

    # def build_prefix_maps(self):
    #     prefix_out = {"yuid": self.configs.internal_uri}
    #     for cf in self.configs.external.values():
    #         prefix_out[cf["name"]] = cf["namespace"]
    #     prefix_in = {}
    #     for k, v in prefix_out.items():
    #         prefix_in[v] = k
    #     return prefix_in, prefix_out


    def write_record(self, rec):
        """Log the record's equivalence assertions (mirrors the qua handling
        of manage_identifiers). A record with no equivalents logs a
        self-assertion so it still receives a YUID."""
        if not rec or "data" not in rec or "id" not in rec["data"]:
            return
        recid = rec["data"]["id"]
        typ = rec["data"]["type"]
        qrecid = self.shorten(self.configs.make_qua(recid, typ))
        wrote = False
        for eq in rec["data"].get("equivalent", []):
            eqid = eq.get("id")
            if not eqid or eqid == recid:
                continue
            qeq = self.shorten(self.configs.make_qua(eqid, typ))
            lo, hi = (qrecid, qeq) if qrecid <= qeq else (qeq, qrecid)
            self.fh.write(f"{lo}\t{hi}\t{qrecid}\n")
            wrote = True
        if not wrote:
            self.fh.write(f"{qrecid}\t{qrecid}\t{qrecid}\n")
        # No flush per record: that was a write syscall for every one of tens
        # of millions of records. close() flushes, and a worker that dies
        # mid-slice has to be re-run anyway -- its assertions are only
        # consumed once every slice has finished.

    def close(self):
        self.fh.close()


    # --- merge assertion files and load clusters ---
    
    def load_diff_pairs(self, nodes):
        """differentFrom pairs from the merged differents index (TabLmdb of
        URI -> URI(s)), lifted onto the qua'd node forms present in the graph.
    
        The raw table has no types; a diff between two URIs constrains every
        qua'd form of those URIs, matching how the GlobalReconciler applies
        diffs against equivalent ids irrespective of type.
    
        ``nodes`` are curie-shortened when the graph is (pass ``prefix_in`` so
        the full-URI diff keys are shortened to match); otherwise both sides are
        full URIs and prefix_in is left None.
        """

        prefix_in = self.prefix_in
        diffs = set()
        diff_index = None        
        by_uri = defaultdict(list)
        
        fn = self.configs.results["merged"].get("differentDbPath", "")
        if fn:
            try:
                diff_index = TabLmdb.open(fn, "r", readahead=False, writemap=True)
            except Exception as e:
                print(f"Could not open differents index {fn}: {e}")
        else:
            print("No differentDbPath configured; clustering without diff constraints")
        
        if diff_index is None:
            return diffs

        for n in nodes:
            by_uri[n.split("##qua")[0]].append(n)
        
        def key_of(uri):
            return self.shorten(uri).split("##qua")[0]
    
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
    
    def _constrained_union(self, dsu, edge_stream, diffs, want_conflicts=True):
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
    
    
    def cluster(self, edges, diffs):
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
        conflicts = self._constrained_union(dsu, stream, diffs)
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
    
    def mint_yuid(self, cluster_key):
        """Deterministic mint for clusters with no prior YUID:
        uuid5 of the cluster's canonical (min) member, in the same
        {internal_uri}{slug}/{uuid} shape as IdMap.mint."""
        base = self.configs.internal_uri
        typ = ""
        if self.configs.is_qua(cluster_key):
            typ = self.configs.split_qua(cluster_key)[1]
        # The qua carries the parent type, so subtypes (e.g. Material) share
        # their parent's slug; cosmetic difference only from IdMap.mint.
        slug = self.configs.ok_record_types.get(typ, "")
        namespace = uuid.uuid5(uuid.NAMESPACE_URL, base)
        uu = str(uuid.uuid5(namespace, cluster_key))
        if slug:
            return f"{base}{slug}/{uu}"
        else:
            raise ValueError(f"no slug for cluster key {cluster_key}")    
    
    def _best_claim(self, prior_values):
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
    
    
    def assign(self, clusters, prior):
        """cluster (keyed by min member) -> YUID (in-memory reference).
    
        Members vote for the YUID they held in the prior idmap; most voters
        wins (tie: lexicographically smallest YUID), so adding new URIs to a
        cluster never changes its identity. If a cluster split leaves two
        clusters claiming one YUID, the claim with more voters (tie: smaller
        cluster key) keeps it and the loser mints.
        """
        claims = defaultdict(list)
        for key, members in clusters.items():
            claim = self._best_claim(prior.get(m) for m in members)
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
                y = self.mint_yuid(key)
            result[key] = y
        return result
    
    
    # ---------------------------------------------------------------------------
    # idmap interaction (redis)
    # ---------------------------------------------------------------------------
    

    def fetch_prior(self, members):
        """Pipelined member -> YUID lookup from the current idmap (in-memory
        reference; the streaming path fetches priors in batches on the fly)."""
        prior = {}
        members = list(members)
        for i in range(0, len(members), self.batch_size):
            chunk = members[i:i + self.batch_size]
            for m, val in self.idmap.get_multi(chunk).items():
                # A member key resolves to a single YUID; anything else here
                # would be a yuid key, which members never are
                if isinstance(val, str) and val:
                    prior[m] = val
        return prior
    
    
    def apply_assignments(self, clusters, yuids, prior):
        """Bulk-load the resolved identity map into the redis idmap (in-memory
        reference used by the comparison harness; resolve_identity applies the
        streaming equivalent).
    
        Keeps the same storage shape the merge phase expects: member -> yuid
        (string) and yuid -> set(members + current update token). Members that
        moved between YUIDs are removed from their old set; YUID sets left
        holding only tokens are deleted.
        """
        touched_old = set()
        stats = {"set": 0, "moved": 0, "clusters": 0}

        items = list(clusters.items())
        for i in range(0, len(items), self.batch_size):
            batch = []
            for key, members in items[i:i + self.batch_size]:
                yuid = yuids[key]
                for m in members:
                    old = prior.get(m)
                    if old and old != yuid:
                        touched_old.add(old)
                batch.append((yuid, list(members), prior))
            got = self.idmap.assign_bulk(batch)
            for k in ("set", "moved", "clusters"):
                stats[k] += got.get(k, 0)

        stats["deleted_yuids"] = self._delete_dead_yuids(sorted(touched_old))
        return stats
    
    
    def _delete_dead_yuids(self, old_keys):
        """Remove YUIDs that lost all their real members.

        ``old_keys`` is an iterable of full YUID URIs. What "empty" means
        differs per backend -- a redis set holding only update tokens, a
        postgres registry row with no member rows -- so the backend decides."""
        return self.idmap.delete_empty_yuids(list(old_keys))
    
    
    # ---------------------------------------------------------------------------
    # Streaming resolution (the production path)
    # ---------------------------------------------------------------------------
    
    def _sort(self, inputs, output, keys, tmpdir):
        """Run ``LC_ALL=C sort`` (byte order == Python str order for UTF-8, so
        the on-disk order matches every in-memory min/max the code does)."""
        cmd = ["sort", *keys, "-t", "\t", "-T", tmpdir, "-S", self.sort_buffer_size,
            "-o", output, *[str(i) for i in inputs]]
        env = dict(os.environ)
        env["LC_ALL"] = "C"
        subprocess.run(cmd, env=env, check=True)
    
    
    def _iter_tsv(self, path):
        with open(path) as fh:
            for line in fh:
                yield line.rstrip("\n").split("\t")
    
    
    def _grouped(self, path):
        """Yield (key, [rows]) for runs of rows sharing the first column. The
        file must already be sorted on that column."""
        cur = None
        batch = []
        for parts in self._iter_tsv(path):
            k = parts[0]
            if cur is None:
                cur = k
            if k != cur:
                yield cur, batch
                cur, batch = k, []
            batch.append(parts)
        if batch:
            yield cur, batch
    
    
    def _chunks(self, iterable, n):
        chunk = []
        for x in iterable:
            chunk.append(x)
            if len(chunk) >= n:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
    
    
    def _aggregate_edges(self):
        """Stream the sorted assertions; collapse each (a,b) run to one voted
        edge (distinct asserters = votes). Real edges (a!=b) go to edges_path
        and their endpoints into the returned dsu_nodes set (the one large
        in-memory structure -- non-singleton nodes only); self-markers (a==b)
        have their node written to selfs_path for the singleton pass."""
        dsu_nodes = set()
        n_edges = 0
        with open(self.edges_path, "w") as fe, open(self.selfs_path, "w") as fs:
            cur = None
            votes = 0
            last_as = None
            for parts in self._iter_tsv(self.sorted_path):
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
    
    
    def _attach_asserters(self, conflicts):
        """Fill in the asserter lists for the (rare) refused links by a single
        scan of the sorted assertions -- the vote aggregation dropped asserter
        identities, but conflicts are few so we recover them by pair."""
        want = {(c["pair"][0], c["pair"][1]): set() for c in conflicts}
        if want:
            for parts in self._iter_tsv(self.sorted_path):
                if len(parts) != 3:
                    continue
                a, b, asserter = parts
                s = want.get((a, b))
                if s is not None:
                    s.add(asserter)
        for c in conflicts:
            c["asserters"] = sorted(want.get((c["pair"][0], c["pair"][1]), ()))
    
    
    def _build_clusters(self, dsu_nodes, diffs):
        """Constrained union-find over the vote-ordered edge file; write
        ``root<TAB>member`` for every non-singleton node and return the conflict
        report (asserters not yet attached)."""
        dsu = DSU()
        for n in dsu_nodes:
            dsu.add(n)
        stream = ((a, b, int(v)) for a, b, v in self._iter_tsv(self.edges_by_vote_path))
        conflicts = self._constrained_union(dsu, stream, diffs)
        with open(self.clusters_path, "w") as fh:
            for node in dsu.parent:
                fh.write(f"{dsu.find(node)}\t{node}\n")
        conflicts.sort(key=lambda c: (c["pair"][0], c["pair"][1]))
        return conflicts
    
    
    def _append_singletons(self, dsu_nodes):
        """Every distinct self-marked node that is not in a real cluster is its
        own singleton cluster. Appended to the clusters file (which is re-sorted
        by root afterwards)."""
        n = 0
        prev = None
        with open(self.clusters_path, "a") as fh:
            for parts in self._iter_tsv(self.selfs_sorted_path):
                node = parts[0]
                if node == prev:
                    continue
                prev = node
                if node not in dsu_nodes:
                    fh.write(f"{node}\t{node}\n")
                    n += 1
        return n
    
    
    def _fetch_prior_stream(self):
        """Batched member -> prior-YUID lookup, streamed. Reads the by-root
        clusters file and writes ``root<TAB>member<TAB>prior`` (prior blank when
        the member had none), preserving order so the file stays grouped."""
        with open(self.prior_path, "w") as fout:
            for chunk in self._chunks(self._iter_tsv(self.clusters_sorted_path), self.batch_size):
                got = self.idmap.get_multi([member for _root, member in chunk])
                for (root, member) in chunk:
                    val = got.get(member)
                    prior = val if isinstance(val, str) and val else ""
                    fout.write(f"{root}\t{member}\t{prior}\n")
        
    def _emit_claims(self):
        """Per cluster (grouped by root): compute the full-URI cluster key (min
        over expanded members -- byte-identical to the in-memory min member),
        tally prior YUIDs into one claim, and write the detail rows the apply
        pass needs. Returns the cluster count."""

        n_clusters = 0
        with open(self.claims_path, "w") as fc, open(self.detail_path, "w") as fd:
            for _root, rows in self._grouped(self.prior_path):
                members = [(r[1], r[2] if len(r) > 2 else "") for r in rows]
                full_key = min(self.expand(m) for m, _ in members)
                claim = self._best_claim(p for _, p in members)
                if claim:
                    best, votes = claim
                    fc.write(f"{best}\t{votes}\t{full_key}\n")
                for m, p in members:
                    fd.write(f"{full_key}\t{m}\t{p}\n")
                n_clusters += 1
        return n_clusters
    
    def _resolve_claims(self):
        """Claims are sorted (yuid asc, votes desc, key asc), so the first row of
        each yuid group is the winner; it keeps the YUID, everyone else mints."""
        with open(self.won_path, "w") as fh:
            for yuid, rows in self._grouped(self.claims_sorted_path):
                winner = rows[0]  # (yuid, votes, full_key)
                fh.write(f"{winner[2]}\t{yuid}\n")
    
    
    def _apply_stream(self):
        """Merge the by-key detail with the by-key winners, minting where a
        cluster kept no prior YUID, and bulk-load the result into redis. Old
        yuid keys that lost a member are appended to touched_path for the final
        dead-set sweep."""
        stats = {"set": 0, "moved": 0, "clusters": 0}

        won = self._grouped(self.won_sorted_path)
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
    
        # One batch of clusters at a time, handed to the backend whole: it
        # knows whether that is a redis pipeline or a pair of upserts. Batched
        # by member count, as before, so memory does not scale with the file.
        batch = []
        pending = 0

        def flush():
            nonlocal batch, pending
            if not batch:
                return
            got = self.idmap.assign_bulk(batch)
            for k in ("set", "moved", "clusters"):
                stats[k] += got.get(k, 0)
            batch = []
            pending = 0

        with open(self.touched_path, "w") as ftouched:
            for full_key, rows in self._grouped(self.detail_sorted_path):
                advance_to(full_key)
                if won_key == full_key:
                    yuid = won_yuid
                else:
                    yuid = self.mint_yuid(full_key)
                members = []
                prior_map = {}
                for r in rows:
                    member = r[1]
                    prior = r[2] if len(r) > 2 else ""
                    members.append(member)
                    if prior and prior != yuid:
                        prior_map[member] = prior
                        # full URI now, not the internal short form: the sweep
                        # takes what every other public call takes
                        ftouched.write(f"{prior}\n")
                    pending += 1
                batch.append((yuid, members, prior_map))
                pending += 1
                if pending >= self.batch_size:
                    flush()
            flush()
        return stats
    
    
    def _distinct(self, path):
        prev = None
        for parts in self._iter_tsv(path):
            if parts and parts[0] != prev:
                prev = parts[0]
                yield prev
    
    
    def resolve_identity(self, conflicts_file="identity_conflicts.jsonl",
                        work_dir=None, keep_temp=True):
        """Resolve the identity map from the assertion logs, streaming through
        unix ``sort`` so peak memory scales with the non-singleton subgraph
        rather than the total record count. Returns stats.
    
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

        flist = os.path.join(self.configs.temp_dir, "assertions-*.tsv")
        files = sorted(glob.glob(flist))
        if not files:
            raise ValueError("No assertions-*.tsv files found; did reconcile run?")
                    
        if work_dir is None:
            work_dir = self.configs.temp_dir
        td = tempfile.mkdtemp(prefix="identity-", dir=work_dir)
        self.temp_work_dir_path = td

        p = lambda name: os.path.join(td, name)
        try:
            self.sorted_path = p("assertions.sorted")
            self.edges_path = p("edges.tsv")
            self.selfs_path = p("selfs.tsv")
            self.selfs_sorted_path = p("selfs.sorted")
            self.edges_by_vote_path = p("edges.byvote")
            self.clusters_path = p("clusters.tsv")
            self.clusters_sorted_path = p("clusters.sorted")
            self.prior_path = p("clusters.prior")
            self.claims_path = p("claims.tsv")
            self.claims_sorted_path = p("claims.sorted")
            self.detail_path = p("detail.tsv")
            self.detail_sorted_path = p("detail.sorted")
            self.won_path = p("won.tsv")
            self.won_sorted_path = p("won.sorted")
            self.touched_path = p("touched.tsv")
            self.touched_sorted_path = p("touched.sorted")
    
            # print("sorting assertions...")
            self._sort(files, self.sorted_path,
                ["-k1,1", "-k2,2", "-k3,3"], td)
    
            # print("aggregating voted edges...")
            dsu_nodes, n_edges = self._aggregate_edges()
    
            # print("loading diff pairs...")
            diffs = self.load_diff_pairs(dsu_nodes)
    
            # print("clustering...")
            self._sort([self.edges_path], self.edges_by_vote_path, 
                ["-k3,3nr", "-k1,1", "-k2,2"], td)
            conflicts = self._build_clusters(dsu_nodes, diffs)
            self._attach_asserters(conflicts)
            if conflicts_file:
                with open(conflicts_file, "w") as fh:
                    for c in conflicts:
                        fh.write(json.dumps(c) + "\n")
    
            # print("adding singletons...")
            self._sort([self.selfs_path], self.selfs_sorted_path, ["-u", "-k1,1"], td)
            n_singletons = self._append_singletons(dsu_nodes)
            self._sort([self.clusters_path], self.clusters_sorted_path, ["-k1,1"], td)
    
            # print("fetching prior...")
            self._fetch_prior_stream() # clus clup
    
            #print("assigning...")
            n_clusters = self._emit_claims()
            self._sort([self.claims_path], self.claims_sorted_path, ["-k1,1", "-k2,2nr", "-k3,3"], td)
            self._resolve_claims()
    
            # print("applying assignments...")
            self._sort([self.detail_path], self.detail_sorted_path, ["-k1,1"], td)
            self._sort([self.won_path], self.won_sorted_path, ["-k1,1"], td)
            stats = self._apply_stream()
    
            self._sort([self.touched_path], self.touched_sorted_path, ["-u", "-k1,1"], td)
            stats["deleted_yuids"] = self._delete_dead_yuids(self._distinct(self.touched_sorted_path))
    
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
