"""Parity + correctness tests for the streaming resolve_identity pipeline.

The streaming path (external ``sort`` + streaming Python) must reproduce the
in-memory reference (load_assertions -> cluster -> assign -> apply). These
tests exercise the whole pipeline against an in-memory fake idmap (no redis)
and compare the resulting identity map / cluster partition to the reference.
"""

import glob
import random
import sys
import uuid
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.process import identity as I
from pipeline.process.identity import (AssertionWriter, build_prefix_maps,
                                        cluster, expand, load_assertions,
                                        resolve_identity, shorten)


# ---------------------------------------------------------------------------
# stubs
# ---------------------------------------------------------------------------

class StubConfigs:
    internal_uri = "https://lux.collections.yale.edu/data/"
    ok_record_types = {"Type": "concept", "Person": "person"}
    parent_record_types = {}
    external = {
        "aat": {"name": "aat", "namespace": "http://vocab.getty.edu/aat/"},
        "wikidata": {"name": "wikidata",
                     "namespace": "http://www.wikidata.org/entity/"},
        "viaf": {"name": "viaf", "namespace": "http://viaf.org/viaf/"},
    }

    def is_qua(self, recid):
        return "##qua" in recid

    def make_qua(self, recid, typ):
        if "##qua" in recid:
            return recid
        typ = self.parent_record_types.get(typ, typ)
        return f"{recid}##qua{typ}"

    def split_qua(self, recid):
        return recid.split("##qua")


CFGS = StubConfigs()


class _FakePipe:
    def __init__(self, store):
        self.store = store
        self.ops = []

    def get(self, k):
        self.ops.append(("get", k))

    def set(self, k, v):
        self.ops.append(("set", k, v))

    def sadd(self, k, *vs):
        self.ops.append(("sadd", k, vs))

    def srem(self, k, v):
        self.ops.append(("srem", k, v))

    def smembers(self, k):
        self.ops.append(("smembers", k))

    def delete(self, k):
        self.ops.append(("delete", k))

    def execute(self, raise_on_error=False):
        res = []
        for op in self.ops:
            t = op[0]
            if t == "get":
                v = self.store.get(op[1])
                res.append(v if isinstance(v, str) else None)
            elif t == "set":
                self.store[op[1]] = op[2]
                res.append(True)
            elif t == "sadd":
                s = self.store.get(op[1])
                if not isinstance(s, set):
                    s = set()
                    self.store[op[1]] = s
                s.update(op[2])
                res.append(len(op[2]))
            elif t == "srem":
                s = self.store.get(op[1])
                if isinstance(s, set):
                    s.discard(op[2])
                res.append(1)
            elif t == "smembers":
                s = self.store.get(op[1])
                res.append(set(s) if isinstance(s, set) else set())
            elif t == "delete":
                self.store.pop(op[1], None)
                res.append(1)
        self.ops = []
        return res


class _FakeConn:
    def __init__(self):
        self.store = {}

    def pipeline(self, transaction=False):
        return _FakePipe(self.store)


class FakeIdMap:
    """Minimal stand-in for storage.idmap.redis.IdMap: same prefix maps and
    same forward/reverse storage shape, backed by an in-memory dict."""

    def __init__(self, configs, token="__testtok__"):
        self.configs = configs
        self.prefix_map_in, self.prefix_map_out = build_prefix_maps(configs)
        self.conn = _FakeConn()
        self.update_token = token

    def _manage_key_in(self, k):
        return shorten(k, self.prefix_map_in)

    def _manage_key_out(self, k):
        return expand(k, self.prefix_map_out)

    _manage_value_in = _manage_key_in
    _manage_value_out = _manage_key_out

    def seed(self, member_full, yuid_full):
        im = self._manage_key_in(member_full)
        iy = self._manage_value_in(yuid_full)
        self.conn.store[im] = iy
        s = self.conn.store.get(iy)
        if not isinstance(s, set):
            s = set()
            self.conn.store[iy] = s
        s.add(im)
        s.add(self.update_token)

    def forward(self):
        """Full member URI -> full YUID for every string-valued key."""
        out = {}
        for k, v in self.conn.store.items():
            if isinstance(v, str):
                out[self._manage_key_out(k)] = self._manage_value_out(v)
        return out

    def yuid_set(self, yuid_full):
        return set(self.conn.store.get(self._manage_value_in(yuid_full), set()))


# ---------------------------------------------------------------------------
# synthetic data
# ---------------------------------------------------------------------------

NAMESPACES = [
    "http://vocab.getty.edu/aat/",
    "http://www.wikidata.org/entity/",
    "http://viaf.org/viaf/",
    "https://lux.collections.yale.edu/data/concept/",
    "http://example.org/unmatched/",   # no namespace -> stays full
]


def _uri(rng, n):
    return rng.choice(NAMESPACES) + f"{n}"


def make_clusters(rng, n_clusters):
    """Random member sets; ~half are singletons, the rest size 2-5."""
    clusters = []
    counter = [0]

    def fresh():
        counter[0] += 1
        return _uri(rng, counter[0])

    for _ in range(n_clusters):
        if rng.random() < 0.5:
            clusters.append([fresh()])
        else:
            size = rng.randint(2, 5)
            clusters.append([fresh() for _ in range(size)])
    return clusters


def write_assertions(clusters, out_dir, n_slices, rng):
    """Emit each cluster as records-with-equivalents through AssertionWriter,
    sharded across slices in shuffled order (proves order independence)."""
    writers = [AssertionWriter(CFGS, s) for s in range(n_slices)]
    jobs = []
    for members in clusters:
        for m in members:
            eqs = [{"id": o, "type": "Type"} for o in members if o != m]
            jobs.append({"data": {"id": m, "type": "Type", "equivalent": eqs}})
    rng.shuffle(jobs)
    for rec in jobs:
        rng.choice(writers).write_record(rec)
    for w in writers:
        w.close()
    return sorted(glob.glob(str(Path(out_dir) / "assertions-*.tsv")))


def qua(uri):
    return f"{uri}##quaType"


def reference_partition(files):
    """The in-memory reference cluster partition, as full-URI member sets."""
    _, prefix_out = build_prefix_maps(CFGS)
    edges = load_assertions([Path(f) for f in files])
    clusters, _ = cluster(edges, set())
    parts = set()
    for members in clusters.values():
        parts.add(frozenset(expand(m, prefix_out) for m in members))
    return parts


def streaming_partition(idmap):
    """Reconstruct the partition from the idmap forward map: members sharing
    a YUID are in the same cluster."""
    by_yuid = defaultdict(set)
    for member, yuid in idmap.forward().items():
        by_yuid[yuid].add(member)
    return {frozenset(v) for v in by_yuid.values()}


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_stream_partition_matches_reference(tmp_path, monkeypatch):
    """No priors -> every cluster mints a unique YUID, so members grouped by
    YUID recover exactly the reference cluster partition."""
    monkeypatch.chdir(tmp_path)
    for seed in range(6):
        rng = random.Random(seed)
        work = tmp_path / f"run{seed}"
        work.mkdir()
        monkeypatch.chdir(work)
        clusters = make_clusters(rng, 60)
        files = write_assertions(clusters, work, 4, rng)

        idmap = FakeIdMap(CFGS)
        stats = resolve_identity(CFGS, idmap, files, work_dir=str(work),
                                 conflicts_file=str(work / "conf.jsonl"))

        assert streaming_partition(idmap) == reference_partition(files)
        # one distinct YUID per cluster (all minted, all unique)
        assert stats["clusters"] == len(clusters)
        assert len(set(idmap.forward().values())) == len(clusters)


def test_stream_prior_reuse_exact(tmp_path, monkeypatch):
    """Every member seeded with its cluster's prior YUID -> every cluster
    reuses it; forward map must equal the seeded map exactly."""
    monkeypatch.chdir(tmp_path)
    rng = random.Random(99)
    clusters = make_clusters(rng, 80)
    files = write_assertions(clusters, tmp_path, 3, rng)

    idmap = FakeIdMap(CFGS)
    expected = {}
    for i, members in enumerate(clusters):
        y = f"{CFGS.internal_uri}concept/prior-{i}"
        for m in members:
            idmap.seed(qua(m), y)
            expected[qua(m)] = y

    resolve_identity(CFGS, idmap, files, work_dir=str(tmp_path),
                     conflicts_file=str(tmp_path / "conf.jsonl"))
    assert idmap.forward() == expected


def test_stream_minority_moves_and_old_set_cleaned(tmp_path, monkeypatch):
    """A 3-member cluster: two members held YUID-A, one held YUID-B. The
    majority YUID-A wins, the minority member moves to A, and YUID-B (now
    holding only the update token) is deleted."""
    monkeypatch.chdir(tmp_path)
    rng = random.Random(7)
    members = [_uri(rng, i) for i in range(3)]
    files = write_assertions([members], tmp_path, 1, rng)

    idmap = FakeIdMap(CFGS)
    ya = f"{CFGS.internal_uri}concept/AAA"
    yb = f"{CFGS.internal_uri}concept/BBB"
    idmap.seed(qua(members[0]), ya)
    idmap.seed(qua(members[1]), ya)
    idmap.seed(qua(members[2]), yb)

    stats = resolve_identity(CFGS, idmap, files, work_dir=str(tmp_path),
                             conflicts_file=str(tmp_path / "conf.jsonl"))

    fwd = idmap.forward()
    assert fwd[qua(members[0])] == ya
    assert fwd[qua(members[1])] == ya
    assert fwd[qua(members[2])] == ya            # moved from B to A
    assert idmap.yuid_set(ya) == {idmap._manage_key_in(qua(m))
                                  for m in members} | {idmap.update_token}
    assert idmap.yuid_set(yb) == set()           # B deleted (token-only)
    assert stats["moved"] == 1
    assert stats["deleted_yuids"] == 1


class _DiffIndex:
    def __init__(self, mapping):
        self._m = mapping  # full uri -> full uri (single value)

    def keys(self):
        return list(self._m.keys())

    def __getitem__(self, k):
        return self._m[k]


def test_stream_differentfrom_keeps_separate(tmp_path, monkeypatch):
    """A chain A-B-C-D with differentFrom(A, D): the link that would connect
    A and D transitively is refused, so A and D land in different clusters."""
    monkeypatch.chdir(tmp_path)
    rng = random.Random(3)
    a = "http://vocab.getty.edu/aat/1"
    b = "http://www.wikidata.org/entity/2"
    c = "http://viaf.org/viaf/3"
    d = "http://example.org/unmatched/4"
    # three separate records make the chain A-B, B-C, C-D
    clusters_records = [[a, b], [b, c], [c, d]]
    files = write_assertions(clusters_records, tmp_path, 2, rng)

    diff = _DiffIndex({a: d})
    idmap = FakeIdMap(CFGS)
    stats = resolve_identity(CFGS, idmap, files, diff_index=diff,
                             work_dir=str(tmp_path),
                             conflicts_file=str(tmp_path / "conf.jsonl"))

    fwd = idmap.forward()
    assert fwd[qua(a)] != fwd[qua(d)]
    assert stats["conflicts"] == 1
    conf = (tmp_path / "conf.jsonl").read_text().strip().splitlines()
    assert len(conf) == 1
    # the refused link is reported with its asserter(s)
    import json as _json
    rec = _json.loads(conf[0])
    assert rec["asserters"]
