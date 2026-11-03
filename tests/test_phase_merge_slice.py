"""`merge_slice` pinned against what run-merge.py did, with the reidentifier,
the reference manager, the merger and the pool stubbed but the real
`prefetched` batching driven by a stub idmap: the order of calls per record,
the claim deciding who builds a shared cluster, --resume, CLUSTER-ESCAPE in
both loops, what the equivalents lose before the merge, the base-record
search of the references pass, the deferred-commit calls, --norefs, the
flag file, the progress calls and the return value."""

import logging
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.phases import merge as phase
from lux_pipeline.process.phases.merge import merge_slice

LUX = "https://lux.collections.yale.edu/data/"
YCBA = "https://ycba.example/"
YPM = "https://ypm.example/"
AAT = "http://vocab.getty.edu/aat/"
WD = "http://www.wikidata.org/entity/"
VIAF = "http://viaf.org/viaf/"
TOKEN = "__20261027__"


def qua(uri, typ="Type"):
    return f"{uri}##qua{typ}"


def rec(ns, recid):
    return {"identifier": recid, "data": {"id": f"{ns}{recid}", "type": "Type"}}


# --- stubs -------------------------------------------------------------------


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)


class Idmap:
    """Clusters as the map holds them: a forward pointer per (qua'd) member
    and the member set per YUID. Answers get_multi, [], get_cluster."""

    def __init__(self, log, clusters):
        self.log = log
        self.members = {f"{LUX}{y}": list(m) for y, m in clusters.items()}
        self.fwd = {m: full for full, ms in self.members.items() for m in ms
                    if not m.startswith("__")}
        self.update_token = None

    def enable_memory_cache(self):
        self.log.append(("memory_cache", True))

    def get_multi(self, keys):
        keys = list(keys)
        self.log.append(("get_multi", keys))
        return {k: set(self.members[k]) if k in self.members else self.fwd.get(k) for k in keys}

    def __getitem__(self, key):
        self.log.append(("idmap_get", key))
        if key in self.members:
            return set(self.members[key])
        return self.fwd.get(key)

    def get_cluster(self, qrecid):
        self.log.append(("get_cluster", qrecid))
        full = self.fwd.get(qrecid)
        if full is None:
            return (None, None)
        return (full, set(self.members[full]))


class Recordcache:
    def __init__(self, log, name, rows):
        self.log = log
        self.name = name
        self.rows = {r["identifier"]: r for r in rows}

    def len_estimate(self):
        return len(self.rows)

    def iter_records(self):
        self.log.append(("iter_records", self.name))
        for r in self.rows.values():
            yield dict(r)

    def iter_records_slice(self, my_slice, max_slice):
        self.log.append(("iter_records_slice", self.name, my_slice, max_slice))
        for i, r in enumerate(self.rows.values()):
            if i % max_slice == my_slice:
                yield dict(r)

    def __getitem__(self, recid):
        self.log.append(("fetch", self.name, recid))
        r = self.rows.get(recid)
        return dict(r) if r else None

    def __contains__(self, recid):
        return recid in self.rows

    def has_multi(self, keys):
        keys = list(keys)
        self.log.append(("has_multi", self.name, sorted(keys)))
        return {k for k in keys if k in self.rows}


class Recordcache2:
    def __init__(self, log, name):
        self.log = log
        self.name = name

    def __setitem__(self, key, value):
        self.log.append(("recordcache2", self.name, key, value))


class MergedCache:
    def __init__(self, log, built=()):
        self.log = log
        self.config = {}
        self.built = set(built)
        self.stored = {}

    def defer_commits(self, every=100, batch=0):
        self.log.append(("defer_commits", every, batch))

    def resume_commits(self):
        self.log.append(("resume_commits",))

    def checkpoint(self):
        self.log.append(("checkpoint",))

    def flush(self):
        self.log.append(("flush",))

    def has_multi(self, keys):
        keys = list(keys)
        self.log.append(("merged_has_multi", sorted(keys)))
        return {k for k in keys if k in self.built}

    def metadata(self, key, field="insert_time"):
        self.log.append(("metadata", key, field))
        return "2026-10-27" if key in self.built else None

    def __setitem__(self, key, value):
        self.log.append(("write_merged", key, value))
        self.stored[key] = value


class Final:
    def __init__(self, log, none_for=(), raise_for=()):
        self.log = log
        self.none_for = set(none_for)
        self.raise_for = set(raise_for)

    def transform(self, record, typ):
        self.log.append(("transform", record["yuid"], typ))
        if record["yuid"] in self.raise_for:
            raise RuntimeError("transform failed")
        if record["yuid"] in self.none_for:
            return None
        return record


def make_stubs(log, done_refs, escapes, fail_reidentify):
    """The classes the phase builds, recording through `log`. The
    reidentifier answers from the idmap's forward pointers, except that
    `escapes` (identifier -> yuid) sends a record to another cluster and
    `fail_reidentify` (identifiers) returns None."""

    class Reidentifier:
        def __init__(self, cfgs, idmap):
            log.append(("Reidentifier", cfgs, idmap))
            self.cfgs = cfgs
            self.idmap = idmap

        def reidentify(self, record):
            log.append(("reidentify", record["data"]["id"]))
            if record["identifier"] in fail_reidentify:
                return None
            full = self.idmap.fwd[qua(record["data"]["id"], record["data"]["type"])]
            yuid = escapes.get(record["identifier"]) or full.rsplit("/", 1)[1]
            return {"identifier": record["identifier"], "yuid": yuid,
                    "data": {"id": f"{LUX}{yuid}", "type": record["data"]["type"],
                             "from": record["data"]["id"]}}

    class ReferenceManager:
        def __init__(self, cfgs, idmap):
            log.append(("ReferenceManager", cfgs, idmap))

        def iter_done_refs(self, my_slice, max_slice):
            log.append(("iter_done_refs", my_slice, max_slice))
            yield from done_refs

    class MergeHandler:
        def __init__(self, cfgs, idmap, ref_mgr):
            log.append(("MergeHandler", cfgs, idmap, type(ref_mgr).__name__))

        def merge(self, rec2, equivs):
            log.append(("merge", rec2["yuid"], sorted(equivs)))
            return {"identifier": rec2.get("identifier", "<deleted>"), "yuid": rec2["yuid"],
                    "data": dict(rec2["data"], merged=sorted(equivs))}

    class PoolManager:
        instance = None

        @classmethod
        def get_instance(cls):
            if cls.instance is None:
                cls.instance = cls()
            return cls.instance

        def put_all(self, name):
            log.append(("put_all", name))

    return Reidentifier, ReferenceManager, MergeHandler, PoolManager


class StubConfigs:
    def __init__(self, tmp_path, log, ycba=(), ypm=(), aat=(), wikidata=(), built=(),
                 none_for=(), raise_for=(), token=TOKEN):
        self.log_dir = str(tmp_path / "logs")
        self.data_dir = str(tmp_path)
        os.makedirs(os.path.join(self.log_dir, "flags"))
        (tmp_path / "idmap_update_token.txt").write_text(token + "\n")

        def source(name, ns, rows, typ, **extra):
            return {"name": name, "type": typ, "namespace": ns,
                    "recordcache": Recordcache(log, name, rows),
                    "recordcache2": Recordcache2(log, name), **extra}

        self.internal = {"ycba": source("ycba", YCBA, ycba, "internal"),
                         "ypm": source("ypm", YPM, ypm, "internal")}
        # merge_order: wikidata before aat; viaf has none and is never a base
        self.external = {"aat": source("aat", AAT, aat, "external", merge_order=1),
                         "wikidata": source("wikidata", WD, wikidata, "external", merge_order=0),
                         "viaf": source("viaf", VIAF, [], "external")}
        self.results = {"merged": {"recordcache": MergedCache(log, built),
                                   "mapper": Final(log, none_for, raise_for)}}

    def make_qua(self, uri, typ):
        return uri if "##qua" in uri else qua(uri, typ)

    def split_qua(self, uri):
        return uri.split("##qua")

    def split_uri(self, uri):
        uri = uri.split("##qua")[0]
        for cfg in [*self.internal.values(), *self.external.values()]:
            if uri.startswith(cfg["namespace"]):
                return cfg, uri[len(cfg["namespace"]):]
        raise ValueError(uri)


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """Build a config, an idmap and the stubs, patch the classes into the
    phase module, and run `merge_slice`; the shared `log` is the record of
    what happened."""

    class Harness:
        def __init__(self):
            self.log = []
            self.done_refs = []
            self.escapes = {}
            self.fail_reidentify = set()
            self.cfgs = None
            self.idmap = None

        def configure(self, clusters=None, **kw):
            self.cfgs = StubConfigs(tmp_path, self.log, **kw)
            self.idmap = Idmap(self.log, clusters or {})
            return self.cfgs

        def run(self, to_do=None, **kw):
            if self.cfgs is None:
                self.configure()
            classes = make_stubs(self.log, self.done_refs, self.escapes, self.fail_reidentify)
            for cls in classes:
                monkeypatch.setattr(phase, cls.__name__, cls)
            if to_do is None:
                to_do = self.todo("ycba")
            return merge_slice(self.cfgs, self.idmap, to_do, **kw)

        def todo(self, *names):
            return [(n, self.cfgs.internal[n]) for n in names]

        def names(self):
            return [c[0] for c in self.log]

        def only(self, *kinds):
            return [c for c in self.log if c[0] in kinds]

        def flag(self, n):
            return Path(self.cfgs.log_dir) / "flags" / f"merge_is_done-{n}.txt"

        def merged(self):
            return self.cfgs.results["merged"]["recordcache"].stored

    return Harness()


# --- the records pass --------------------------------------------------------


def test_each_record_goes_through_the_steps_in_order(harness):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1"), qua(AAT + "300"), TOKEN],
                          "YU2": [qua(YCBA + "2")]},
                ycba=[rec(YCBA, "1"), rec(YCBA, "2")])
    result = h.run()

    # the token is read and set on the idmap; the collaborators are built
    # with what the script gave them; the merged cache is set to overwrite
    assert h.idmap.update_token == TOKEN
    assert h.log[0] == ("Reidentifier", h.cfgs, h.idmap)
    assert h.log[1] == ("ReferenceManager", h.cfgs, h.idmap)
    assert h.log[2] == ("MergeHandler", h.cfgs, h.idmap, "ReferenceManager")
    assert h.cfgs.results["merged"]["recordcache"].config == {"overwrite": True}
    assert h.log[3] == ("memory_cache", True)
    assert h.log[4] == ("defer_commits", 500, 25)
    assert h.log[5] == ("iter_records", "ycba")
    # the batch: the forward pointers, then the member sets of what will be built
    assert h.log[6] == ("get_multi", [qua(YCBA + "1"), qua(YCBA + "2")])
    assert h.log[7] == ("get_multi", [LUX + "YU1", LUX + "YU2"])

    per_record = ["get_cluster", "reidentify", "recordcache2", "idmap_get", "merge",
                  "transform", "write_merged", "checkpoint"]
    names = h.names()
    assert names[8:24] == per_record * 2
    # then the source is flushed, the references pass runs, and the tidy-up
    assert names[24:] == ["flush", "iter_done_refs", "resume_commits", "put_all"]
    assert h.log[-1] == ("put_all", "localsocket")

    # the rewritten record goes to the source's recordcache2 under the YUID
    assert h.only("recordcache2") == [
        ("recordcache2", "ycba", "YU1", {"id": LUX + "YU1", "type": "Type", "from": YCBA + "1"}),
        ("recordcache2", "ycba", "YU2", {"id": LUX + "YU2", "type": "Type", "from": YCBA + "2"})]
    # the equivalents are the cluster without the record itself and the token
    assert h.only("merge") == [("merge", "YU1", [qua(AAT + "300")]), ("merge", "YU2", [])]
    assert h.only("transform") == [("transform", "YU1", "Type"), ("transform", "YU2", "Type")]
    # stored under the YUID with the identifier removed
    assert sorted(h.merged()) == ["YU1", "YU2"]
    assert "identifier" not in h.merged()["YU1"]
    assert h.merged()["YU1"]["data"]["merged"] == [qua(AAT + "300")]

    assert result["records"]["phase"] == "merge" and result["records"]["records"] == 2
    assert result["references"]["phase"] == "merge-refs" and result["references"]["records"] == 0
    # the script wrote the flag unsliced too
    assert h.flag(-1).read_text() == "1\n"


def test_the_equivalents_lose_the_record_its_plain_uri_and_the_token(harness):
    """An external source can be merged by name (`--aat`). Its plain uri is
    not an internal claim candidate, so this is where the plain-uri removal
    is reachable; for an internal source the claim would hand the cluster
    to the plain uri first, and the record would be a skip."""
    h = harness
    h.configure(clusters={"YU1": [qua(AAT + "300"), AAT + "300", TOKEN, qua(WD + "Q5")]},
                aat=[rec(AAT, "300")])
    h.run([("aat", h.cfgs.external["aat"])])
    assert h.only("has_multi") == []
    assert h.only("merge") == [("merge", "YU1", [qua(WD + "Q5")])]
    assert sorted(h.merged()) == ["YU1"]


def test_a_record_the_idmap_does_not_know_is_skipped_with_a_warning(harness, caplog):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")]}, ycba=[rec(YCBA, "1"), rec(YCBA, "9")])
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        result = h.run()
    # the batch missed it, so it was asked for singly, then reported
    assert ("idmap_get", qua(YCBA + "9")) in h.log
    assert f" !!! Couldn't find YUID for internal record: {qua(YCBA + '9')}" in caplog.text
    assert h.only("get_cluster") == [("get_cluster", qua(YCBA + "1"))]
    assert sorted(h.merged()) == ["YU1"]
    # the batch counted it as seen; the loop counted the skip
    assert (result["records"]["records"], result["records"]["skipped"]) == (2, 1)


def test_resume_skips_the_clusters_the_merged_cache_already_holds(harness):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")], "YU2": [qua(YCBA + "2")]},
                ycba=[rec(YCBA, "1"), rec(YCBA, "2")], built=["YU1"])
    result = h.run(resume=True)
    assert h.only("merged_has_multi") == [("merged_has_multi", ["YU1", "YU2"])]
    # only the unbuilt cluster's members are fetched, and only it is built
    assert ("get_multi", [LUX + "YU2"]) in h.log
    assert h.only("get_cluster") == [("get_cluster", qua(YCBA + "2"))]
    assert sorted(h.merged()) == ["YU2"]
    assert (result["records"]["records"], result["records"]["skipped"]) == (2, 1)


def test_without_resume_the_merged_cache_is_not_consulted(harness):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")]}, ycba=[rec(YCBA, "1")], built=["YU1"])
    h.run()
    assert not h.only("merged_has_multi", "metadata")
    assert sorted(h.merged()) == ["YU1"]


def test_only_the_smallest_present_member_builds_a_shared_cluster(harness):
    """Two records of one cluster in the same slice: the claim gives the
    cluster to the smallest member, and the second record is a skip. The
    record in hand is `present`, so no cache is asked; the second answer
    comes from the claim cache."""
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1"), qua(YCBA + "2"), qua(YPM + "a")]},
                ycba=[rec(YCBA, "1"), rec(YCBA, "2")], ypm=[rec(YPM, "a")])
    result = h.run()
    assert h.only("has_multi") == []
    assert [c[1] for c in h.only("reidentify")] == [YCBA + "1"]
    assert sorted(h.merged()) == ["YU1"]
    assert (result["records"]["records"], result["records"]["skipped"]) == (2, 1)


def test_a_missing_smaller_member_does_not_block_the_claim(harness):
    """The cluster still lists ycba/1 but its recordcache no longer has it,
    so ycba/2 is the smallest *surviving* member and builds the record."""
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1"), qua(YCBA + "2")]}, ycba=[rec(YCBA, "2")])
    h.run()
    assert h.only("has_multi") == [("has_multi", "ycba", ["1", "2"])]
    assert sorted(h.merged()) == ["YU1"]
    assert h.merged()["YU1"]["data"]["from"] == YCBA + "2"


def test_the_claim_cache_is_this_calls_own(harness):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1"), qua(YCBA + "2")]}, ycba=[rec(YCBA, "2")])
    h.run()
    h.run()
    # asked again on the second run: nothing remembered across calls
    assert len(h.only("has_multi")) == 2


def test_a_record_that_cannot_be_reidentified_is_logged_and_left(harness, caplog):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")]}, ycba=[rec(YCBA, "1")])
    h.fail_reidentify.add("1")
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        result = h.run()
    assert " *** Could not reidentify ycba/1" in caplog.text
    assert not h.only("recordcache2", "merge", "write_merged", "checkpoint")
    # as the script: not counted as a skip
    assert (result["records"]["records"], result["records"]["skipped"]) == (1, 0)


def test_cluster_escape_in_the_records_pass_is_skipped(harness, caplog):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")]}, ycba=[rec(YCBA, "1")])
    h.escapes["1"] = "YU9"
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        h.run()
    assert "CLUSTER-ESCAPE: ycba/1 is in YU1 but reidentifies to YU9; skipping" in caplog.text
    assert not h.only("recordcache2", "merge", "write_merged", "checkpoint")


def test_a_final_transform_that_returns_none_stores_nothing(harness, caplog):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")]}, ycba=[rec(YCBA, "1")], none_for=["YU1"])
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        h.run()
    assert "*** Final transform returned None" in caplog.text
    assert not h.only("write_merged")
    assert h.only("checkpoint") == [("checkpoint",)]


def test_a_final_transform_that_raises_in_the_records_pass_is_fatal(harness, caplog):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")]}, ycba=[rec(YCBA, "1")], raise_for=["YU1"])
    with caplog.at_level(logging.ERROR, logger="lux_pipeline"):
        with pytest.raises(RuntimeError, match="transform failed"):
            h.run()
    assert "*** Final transform raised exception for 1" in caplog.text
    assert not h.only("write_merged")


def test_recids_fetch_each_id_and_report_the_missing(harness, caplog):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")]}, ycba=[rec(YCBA, "1"), rec(YCBA, "2")])
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        h.run(recids=["1", "9"])
    assert h.only("fetch") == [("fetch", "ycba", "1"), ("fetch", "ycba", "9")]
    assert "Couldn't find ycba / 9" in caplog.text
    assert not h.only("iter_records", "iter_records_slice")
    assert sorted(h.merged()) == ["YU1"]


def test_sliced_run(harness, caplog):
    h = harness
    h.configure(clusters={f"YU{i}": [qua(YCBA + str(i))] for i in range(4)},
                ycba=[rec(YCBA, str(i)) for i in range(4)])
    with caplog.at_level(logging.INFO, logger="lux_pipeline"):
        h.run(my_slice=1, max_slice=2)
    assert "*** ycba: slice 1 ***" in caplog.text
    assert h.only("iter_records_slice") == [("iter_records_slice", "ycba", 1, 2)]
    assert sorted(h.merged()) == ["YU1", "YU3"]
    assert h.only("iter_done_refs") == [("iter_done_refs", 1, 2)]
    assert h.flag(1).read_text() == "1\n"
    assert not h.flag(-1).exists()


def test_several_sources_are_flushed_in_turn_and_recids_apply_to_the_first_only(harness):
    """Why select_sources rejects --recid with more than one source: the
    loop spends the list on the first and walks the rest in full."""
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")], "YU2": [qua(YCBA + "2")],
                          "YUa": [qua(YPM + "a")], "YUb": [qua(YPM + "b")]},
                ycba=[rec(YCBA, "1"), rec(YCBA, "2")], ypm=[rec(YPM, "a"), rec(YPM, "b")])
    h.run(h.todo("ycba", "ypm"), recids=["1"])
    assert h.only("fetch") == [("fetch", "ycba", "1")]
    assert h.only("iter_records") == [("iter_records", "ypm")]
    assert sorted(h.merged()) == ["YU1", "YUa", "YUb"]
    assert len(h.only("flush")) == 2


def test_write_flag_false_writes_no_flag(harness):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")]}, ycba=[rec(YCBA, "1")])
    h.run(my_slice=0, max_slice=1, write_flag=False)
    assert not h.flag(0).exists() and not h.flag(-1).exists()


def test_no_references_pass(harness):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")]}, ycba=[rec(YCBA, "1")])
    h.done_refs.append((1, LUX + "YU1", AAT + "300"))
    result = h.run(do_references=False)
    assert not h.only("iter_done_refs")
    assert result["records"]["phase"] == "merge" and result["records"]["records"] == 1
    assert result["references"] is None
    assert h.names()[-2:] == ["resume_commits", "put_all"]
    assert h.flag(-1).exists()


def test_a_bad_token_stops_everything_before_it_starts(harness):
    h = harness
    h.configure(token="20261027")
    with pytest.raises(ValueError, match="badly formed"):
        h.run()
    assert h.log == []


# --- the references pass -----------------------------------------------------


def test_a_reference_cluster_is_rebuilt_from_the_preferred_base_record(harness):
    """No internal member, so the references pass owns it: the base record
    is the first source in merge_order that has one, its uri leaves the
    equivalents, and the whole reidentified record goes to recordcache2."""
    h = harness
    h.configure(clusters={"YU5": [qua(AAT + "300"), qua(WD + "Q5"), TOKEN]},
                aat=[rec(AAT, "300")], wikidata=[rec(WD, "Q5")])
    h.done_refs.append((1, LUX + "YU5", AAT + "300"))
    result = h.run([])

    names = h.names()
    assert names[5:] == ["iter_done_refs", "idmap_get", "fetch", "reidentify", "recordcache2",
                         "merge", "transform", "write_merged", "checkpoint",
                         "resume_commits", "put_all"]
    assert h.only("has_multi") == []                      # no internal candidate to ask about
    assert h.only("fetch") == [("fetch", "wikidata", "Q5")]   # wikidata before aat
    (rc2,) = h.only("recordcache2")
    assert rc2[1:3] == ("wikidata", "YU5")
    assert rc2[3]["yuid"] == "YU5" and "identifier" not in rc2[3] and "data" in rc2[3]
    # the base uri leaves; the token does not (only the records pass strips it)
    assert h.only("merge") == [("merge", "YU5", [TOKEN, qua(AAT + "300")])]
    assert sorted(h.merged()) == ["YU5"] and "identifier" not in h.merged()["YU5"]
    assert result["references"]["records"] == 1
    assert result["records"]["records"] == 0


def test_a_reference_cluster_owned_by_a_source_being_merged_is_skipped(harness):
    h = harness
    h.configure(clusters={"YU6": [qua(YCBA + "1"), qua(AAT + "300")]},
                ycba=[rec(YCBA, "1")], aat=[rec(AAT, "300")])
    h.done_refs.append((1, LUX + "YU6", AAT + "300"))
    result = h.run(h.todo("ycba"))
    # built once, by the records pass; the references pass asked and stood down
    assert h.only("has_multi") == [("has_multi", "ycba", ["1"])]
    assert [c[1] for c in h.only("write_merged")] == ["YU6"]
    assert h.merged()["YU6"]["data"]["from"] == YCBA + "1"
    assert (result["references"]["records"], result["references"]["skipped"]) == (1, 1)


def test_a_reference_cluster_whose_owner_is_not_being_merged_is_rebuilt(harness):
    h = harness
    h.configure(clusters={"YU6": [qua(YCBA + "1"), qua(AAT + "300")]},
                ycba=[rec(YCBA, "1")], aat=[rec(AAT, "300")])
    h.done_refs.append((1, LUX + "YU6", AAT + "300"))
    h.run(h.todo("ypm"))
    assert h.only("fetch") == [("fetch", "aat", "300")]
    assert h.only("merge") == [("merge", "YU6", [qua(YCBA + "1")])]
    assert h.merged()["YU6"]["data"]["from"] == AAT + "300"


def test_a_reference_without_a_yuid_is_reported_and_not_counted(harness, caplog):
    h = harness
    h.configure()
    h.done_refs.append((1, "", AAT + "x"))
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        result = h.run([])
    assert f" *** No YUID for reference {AAT}x from done_refs" in caplog.text
    assert not h.only("idmap_get", "checkpoint")
    assert result["references"]["records"] == 0


def test_a_reference_with_no_equivalents_fails_to_build(harness, caplog):
    h = harness
    h.configure()
    h.done_refs.append((1, LUX + "YU9", AAT + "x"))
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        result = h.run([])
    assert f"FAILED TO BUILD: {LUX}YU9" in caplog.text
    assert not h.only("checkpoint")
    assert (result["references"]["records"], result["references"]["skipped"]) == (1, 1)


def test_resume_skips_a_reference_already_in_the_merged_cache(harness):
    h = harness
    h.configure(clusters={"YU5": [qua(AAT + "300")]}, aat=[rec(AAT, "300")], built=["YU5"])
    h.done_refs.append((1, LUX + "YU5", AAT + "300"))
    result = h.run([], resume=True)
    assert h.only("metadata") == [("metadata", "YU5", "insert_time")]
    assert not h.only("idmap_get", "fetch")
    assert (result["references"]["records"], result["references"]["skipped"]) == (1, 1)


def test_a_reference_with_no_base_record_anywhere(harness, caplog):
    """Members that are not in their recordcache, or whose source has no
    merge_order, are not candidates; nothing is merged but the checkpoint
    still lands."""
    h = harness
    h.configure(clusters={"YU5": [qua(AAT + "300"), qua(VIAF + "7")]})
    h.done_refs.append((1, LUX + "YU5", AAT + "300"))
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        h.run([])
    assert f" *** Could not find ANY record for {LUX}YU5 in " in caplog.text
    assert not h.only("fetch", "merge", "write_merged")
    assert h.only("checkpoint") == [("checkpoint",)]


def test_a_base_record_that_escapes_the_cluster_is_passed_over(harness, caplog):
    h = harness
    h.configure(clusters={"YU5": [qua(AAT + "300"), qua(WD + "Q5")]},
                aat=[rec(AAT, "300")], wikidata=[rec(WD, "Q5")])
    h.escapes["Q5"] = "YU8"
    h.done_refs.append((1, LUX + "YU5", AAT + "300"))
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        h.run([])
    assert ("CLUSTER-ESCAPE: wikidata/Q5 is in YU5 but reidentifies to YU8; "
            "not using it as the base record") in caplog.text
    assert h.only("fetch") == [("fetch", "wikidata", "Q5"), ("fetch", "aat", "300")]
    assert h.only("recordcache2")[0][1] == "aat"
    # the rejected candidate stays in the equivalents
    assert h.only("merge") == [("merge", "YU5", [qua(WD + "Q5")])]
    assert h.merged()["YU5"]["data"]["from"] == AAT + "300"


def test_a_base_record_that_cannot_be_reidentified_is_passed_over(harness, caplog):
    h = harness
    h.configure(clusters={"YU5": [qua(AAT + "300"), qua(WD + "Q5")]},
                aat=[rec(AAT, "300")], wikidata=[rec(WD, "Q5")])
    h.fail_reidentify.add("Q5")
    h.done_refs.append((1, LUX + "YU5", AAT + "300"))
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        h.run([])
    assert " *** Could not reidentify wikidata Q5" in caplog.text
    assert h.merged()["YU5"]["data"]["from"] == AAT + "300"


def test_a_failing_final_transform_in_the_references_pass_is_logged_and_the_record_still_stored(harness, caplog):
    """The script's behaviour, kept: unlike the records pass the exception
    is swallowed, and the merged record is stored untransformed."""
    h = harness
    h.configure(clusters={"YU5": [qua(AAT + "300")]}, aat=[rec(AAT, "300")], raise_for=["YU5"])
    h.done_refs.append((1, LUX + "YU5", AAT + "300"))
    with caplog.at_level(logging.ERROR, logger="lux_pipeline"):
        h.run([])
    assert f"*** Final transform raised exception for {LUX}YU5" in caplog.text
    assert sorted(h.merged()) == ["YU5"]
    assert h.only("checkpoint") == [("checkpoint",)]


# --- progress ----------------------------------------------------------------


def test_progress_reports_a_running_total_and_resets_for_the_references(harness):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")], "YU2": [qua(YCBA + "2")],
                          "YUa": [qua(YPM + "a")], "YU5": [qua(AAT + "300")]},
                ycba=[rec(YCBA, "1"), rec(YCBA, "2")], ypm=[rec(YPM, "a")], aat=[rec(AAT, "300")])
    h.done_refs.append((1, LUX + "YU5", AAT + "300"))
    progress = Recorder()
    h.run(h.todo("ycba", "ypm"), my_slice=0, max_slice=1, progress=progress)
    calls = progress.calls

    assert [c["phase"] for c in calls if "phase" in c] == ["merge", "merge-refs"]
    assert [c["total"] for c in calls if "total" in c] == [2, 3]
    assert [c["description"] for c in calls if "description" in c] == ["ycba", "ypm", "references"]
    assert sum(c.get("advance", 0) for c in calls) == 4
    # records done, then the reset, then the references done
    dones = [i for i, c in enumerate(calls) if c.get("done")]
    (reset,) = [i for i, c in enumerate(calls) if c.get("reset")]
    assert len(dones) == 2 and dones[0] < reset < dones[1]
    assert calls[reset] == {"reset": True, "description": "references"}


def test_progress_total_for_recids_is_the_list_length(harness):
    h = harness
    h.configure(clusters={"YU1": [qua(YCBA + "1")]}, ycba=[rec(YCBA, "1"), rec(YCBA, "2")])
    progress = Recorder()
    h.run(recids=["1", "9"], progress=progress)
    assert [c["total"] for c in progress.calls if "total" in c] == [2]
