"""Parity + correctness tests for the streaming resolve_identity pipeline.

The streaming path (external ``sort`` + streaming Python) must reproduce the
in-memory reference (assertion log -> cluster -> assign -> apply). These tests
exercise the whole pipeline against an in-memory fake idmap (no redis, no
postgres) and compare the resulting identity map / cluster partition to the
reference.

Ported from the old `pipeline.process.identity` module API. Three things
moved and the tests move with them:

* the free functions are now methods on `IdentityResolver`;
* the idmap is postgres-shaped, not redis-shaped -- the resolver touches
  exactly `get_multi`, `assign_bulk` and `delete_empty_yuids`, so the fake is
  a dict rather than a pipeline emulator;
* `resolve_identity()` globs `assertions-*.tsv` out of `configs.temp_dir`
  instead of taking a file list, so each test gets its own temp_dir.
"""

import glob
import json
import random
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.process.identity_resolver import IdentityResolver


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
    # load_diff_pairs reads differentDbPath from here; empty means "cluster
    # without diff constraints", which is what most of these tests want
    results = {"merged": {}}

    def __init__(self, temp_dir="."):
        self.temp_dir = str(temp_dir)

    def is_qua(self, recid):
        return "##qua" in recid

    def make_qua(self, recid, typ):
        if "##qua" in recid:
            return recid
        typ = self.parent_record_types.get(typ, typ)
        return f"{recid}##qua{typ}"

    def split_qua(self, recid):
        return recid.split("##qua")


def _prefix_maps(configs):
    """As storage.idmap.postgres.IdMap builds them."""
    out = {"yuid": configs.internal_uri}
    for cf in configs.external.values():
        out[cf["name"]] = cf["namespace"]
    return {v: k for (k, v) in out.items()}, out


def shorten(uri, prefix_in):
    if uri.startswith("http"):
        for k, v in prefix_in.items():
            if uri.startswith(k):
                return uri.replace(k, f"{v}:")
    return uri


def expand(uri, prefix_out):
    if not uri.startswith("http"):
        for k, v in prefix_out.items():
            if uri.startswith(f"{k}:"):
                return uri.replace(f"{k}:", v)
    return uri


class FakeIdMap:
    """Stand-in for storage.idmap.postgres.IdMap over two dicts.

    The resolver's whole idmap surface is `get_multi`, `assign_bulk` and
    `delete_empty_yuids` plus the two prefix maps. Members are stored in
    shortened form, as postgres stores them, so the curie round trip is under
    test rather than bypassed."""

    def __init__(self, configs, token="__testtok__"):
        self.configs = configs
        self.prefix_map_in, self.prefix_map_out = _prefix_maps(configs)
        self.update_token = token
        self.members = {}      # shortened member -> shortened yuid
        self.yuids = {}        # shortened yuid -> update token

    def _manage_key_in(self, k):
        return shorten(k, self.prefix_map_in)

    def _manage_key_out(self, k):
        return expand(k, self.prefix_map_out)

    _manage_value_in = _manage_key_in
    _manage_value_out = _manage_key_out

    def get_multi(self, keys, chunk=1000):
        out = {}
        for key in keys:
            if not self.configs.is_qua(key) and self.prefix_map_out["yuid"] not in key:
                raise ValueError(f"Need a type: {key}")
            v = self.members.get(self._manage_key_in(key))
            if v is not None:
                out[key] = self._manage_value_out(v)
        return out

    def assign_bulk(self, items):
        stats = {"set": 0, "moved": 0, "clusters": 0}
        rows, yuids = {}, {}
        for yuid, members, prior in items:
            iyuid = self._manage_value_in(yuid)
            yuids[iyuid] = self.update_token
            stats["clusters"] += 1
            for m in members:
                rows[self._manage_key_in(m)] = iyuid
                stats["set"] += 1
                old = (prior or {}).get(m)
                if old and old != yuid:
                    stats["moved"] += 1
        self.yuids.update(yuids)
        self.members.update(rows)
        return stats

    def delete_empty_yuids(self, yuids):
        """A YUID with no members left is dropped -- the NOT EXISTS sweep."""
        live = set(self.members.values())
        dead = 0
        for y in yuids:
            iyuid = self._manage_value_in(y)
            if iyuid in self.yuids and iyuid not in live:
                del self.yuids[iyuid]
                dead += 1
        return dead

    # --- test helpers, not part of the real interface ---

    def seed(self, member_full, yuid_full):
        self.members[self._manage_key_in(member_full)] = \
            self._manage_value_in(yuid_full)
        self.yuids[self._manage_value_in(yuid_full)] = self.update_token

    def forward(self):
        """Full member URI -> full YUID."""
        return {self._manage_key_out(k): self._manage_value_out(v)
                for k, v in self.members.items()}


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


def write_assertions(cfgs, clusters, n_slices, rng):
    """Emit each cluster as records-with-equivalents through the assertion
    log, sharded across slices in shuffled order (proves order independence).

    Writes into cfgs.temp_dir, which is where resolve_identity globs."""
    idmap = FakeIdMap(cfgs)
    writers = [IdentityResolver(cfgs, idmap, s) for s in range(n_slices)]
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
    return sorted(glob.glob(str(Path(cfgs.temp_dir) / "assertions-*.tsv")))


def qua(uri):
    return f"{uri}##quaType"


def load_assertions(paths):
    """{(lo, hi): {asserter}} straight off the assertion TSVs.

    The production reader is the external sort feeding `_aggregate_edges`,
    which yields vote counts rather than asserter sets; this gives the
    in-memory `cluster()` the shape it wants so the two paths can be
    compared."""
    edges = defaultdict(set)
    for path in paths:
        with open(path) as fh:
            for line in fh:
                lo, hi, asserter = line.rstrip("\n").split("\t")
                edges[(lo, hi)].add(asserter)
    return edges


def reference_partition(cfgs, files):
    """The in-memory reference cluster partition, as full-URI member sets."""
    _, prefix_out = _prefix_maps(cfgs)
    ref = IdentityResolver(cfgs, FakeIdMap(cfgs))
    clusters, _ = ref.cluster(load_assertions(files), set())
    return {frozenset(expand(m, prefix_out) for m in members)
            for members in clusters.values()}


def streaming_partition(idmap):
    """Reconstruct the partition from the idmap forward map: members sharing
    a YUID are in the same cluster."""
    by_yuid = defaultdict(set)
    for member, yuid in idmap.forward().items():
        by_yuid[yuid].add(member)
    return {frozenset(v) for v in by_yuid.values()}


def resolve(cfgs, idmap, work, **kw):
    r = IdentityResolver(cfgs, idmap)
    return r.resolve_identity(conflicts_file=str(Path(work) / "conf.jsonl"),
                              work_dir=str(work), **kw)


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_stream_partition_matches_reference(tmp_path):
    """No priors -> every cluster mints a unique YUID, so members grouped by
    YUID recover exactly the reference cluster partition."""
    for seed in range(6):
        rng = random.Random(seed)
        work = tmp_path / f"run{seed}"
        work.mkdir()
        cfgs = StubConfigs(temp_dir=work)
        clusters = make_clusters(rng, 60)
        files = write_assertions(cfgs, clusters, 4, rng)

        idmap = FakeIdMap(cfgs)
        stats = resolve(cfgs, idmap, work)

        assert streaming_partition(idmap) == reference_partition(cfgs, files)
        # one distinct YUID per cluster (all minted, all unique)
        assert stats["clusters"] == len(clusters)
        assert len(set(idmap.forward().values())) == len(clusters)


def test_stream_prior_reuse_exact(tmp_path):
    """Every member seeded with its cluster's prior YUID -> every cluster
    reuses it; forward map must equal the seeded map exactly."""
    rng = random.Random(99)
    cfgs = StubConfigs(temp_dir=tmp_path)
    clusters = make_clusters(rng, 80)
    write_assertions(cfgs, clusters, 3, rng)

    idmap = FakeIdMap(cfgs)
    expected = {}
    for i, members in enumerate(clusters):
        y = f"{cfgs.internal_uri}concept/prior-{i}"
        for m in members:
            idmap.seed(qua(m), y)
            expected[qua(m)] = y

    resolve(cfgs, idmap, tmp_path)
    assert idmap.forward() == expected


def test_stream_minority_moves_and_old_set_cleaned(tmp_path):
    """A 3-member cluster: two members held YUID-A, one held YUID-B. The
    majority YUID-A wins, the minority member moves to A, and YUID-B (now
    with no members left) is deleted."""
    rng = random.Random(7)
    cfgs = StubConfigs(temp_dir=tmp_path)
    members = [_uri(rng, i) for i in range(3)]
    write_assertions(cfgs, [members], 1, rng)

    idmap = FakeIdMap(cfgs)
    ya = f"{cfgs.internal_uri}concept/AAA"
    yb = f"{cfgs.internal_uri}concept/BBB"
    idmap.seed(qua(members[0]), ya)
    idmap.seed(qua(members[1]), ya)
    idmap.seed(qua(members[2]), yb)

    stats = resolve(cfgs, idmap, tmp_path)

    fwd = idmap.forward()
    assert fwd[qua(members[0])] == ya
    assert fwd[qua(members[1])] == ya
    assert fwd[qua(members[2])] == ya            # moved from B to A
    assert idmap._manage_value_in(yb) not in idmap.yuids   # B swept
    assert stats["moved"] == 1
    assert stats["deleted_yuids"] == 1


def test_stream_differentfrom_keeps_separate(tmp_path, monkeypatch):
    """A chain A-B-C-D with differentFrom(A, D): the link that would connect
    A and D transitively is refused, so A and D land in different clusters.

    The diff set now comes from a TabLmdb named by
    configs.results["merged"]["differentDbPath"]; injecting at load_diff_pairs
    is the same seam the old diff_index argument gave us, without standing up
    an lmdb for one pair."""
    rng = random.Random(3)
    cfgs = StubConfigs(temp_dir=tmp_path)
    a = "http://vocab.getty.edu/aat/1"
    b = "http://www.wikidata.org/entity/2"
    c = "http://viaf.org/viaf/3"
    d = "http://example.org/unmatched/4"
    # three separate records make the chain A-B, B-C, C-D
    write_assertions(cfgs, [[a, b], [b, c], [c, d]], 2, rng)

    idmap = FakeIdMap(cfgs)
    prefix_in, _ = _prefix_maps(cfgs)
    pair = (shorten(qua(a), prefix_in), shorten(qua(d), prefix_in))
    monkeypatch.setattr(IdentityResolver, "load_diff_pairs",
                        lambda self, nodes: {pair})

    stats = resolve(cfgs, idmap, tmp_path)

    fwd = idmap.forward()
    assert fwd[qua(a)] != fwd[qua(d)]
    assert stats["conflicts"] == 1
    conf = (tmp_path / "conf.jsonl").read_text().strip().splitlines()
    assert len(conf) == 1
    # the refused link is reported with its asserter(s)
    assert json.loads(conf[0])["asserters"]


def test_single_file_matches_sliced(tmp_path):
    """One unsliced assertion file must resolve to the same partition as the
    same assertions split across slices.

    run-reconcile.py leaves my_slice at -1 unless both argv[1] and argv[2] are
    numeric, and writes `assertions-single.tsv` in that case. Nothing in the
    resolver branches on the file count -- discovery is a plain glob and only
    the first of the eight sorts ever sees more than one input -- so the two
    shapes are required to agree."""
    rng = random.Random(11)
    clusters = make_clusters(rng, 40)

    def run(label, n_slices):
        work = tmp_path / label
        work.mkdir()
        cfgs = StubConfigs(temp_dir=work)
        idmap = FakeIdMap(cfgs)
        # n_slices=0 means the unsliced writer: my_slice stays -1
        if n_slices:
            write_assertions(cfgs, clusters, n_slices, random.Random(5))
        else:
            w = IdentityResolver(cfgs, idmap)
            for members in clusters:
                for m in members:
                    w.write_record({"data": {
                        "id": m, "type": "Type",
                        "equivalent": [{"id": o, "type": "Type"}
                                       for o in members if o != m]}})
            w.close()
        names = sorted(p.name for p in Path(work).glob("assertions-*.tsv"))
        return resolve(cfgs, idmap, work), streaming_partition(idmap), names

    single_stats, single_part, single_names = run("single", 0)
    sliced_stats, sliced_part, sliced_names = run("sliced", 4)

    assert single_names == ["assertions-single.tsv"]
    assert len(sliced_names) == 4
    assert single_part == sliced_part
    assert single_stats["clusters"] == sliced_stats["clusters"] == len(clusters)
