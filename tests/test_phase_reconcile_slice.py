"""`reconcile_slice` pinned against what run-reconcile.py did, with every
collaborator stubbed: the order of calls per record, the source stamp on
streamed rows, sliced / unsliced / recid iteration, memory-cache switching,
every branch of the references loop, --norefs, the flag file, the tidy-up
and the progress calls."""

import logging
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.phases import reconcile as phase
from lux_pipeline.process.phases.reconcile import reconcile_slice

AAT = "http://vocab.getty.edu/aat/"
YCBA = "https://ycba.example/"


# --- stubs -------------------------------------------------------------------


class Recorder:
    def __init__(self):
        self.calls = []

    def __call__(self, **kw):
        self.calls.append(kw)


class Idmap:
    update_token = "__20261027__"

    def __init__(self, log):
        self.log = log

    def enable_memory_cache(self):
        self.log.append(("memory_cache", True))

    def disable_memory_cache(self):
        self.log.append(("memory_cache", False))


class Datacache:
    key = "identifier"

    def __init__(self, log, name, rows):
        self.log = log
        self.config = {"name": name}
        self.rows = rows

    def len_estimate(self):
        return len(self.rows)

    def iter_records(self):
        self.log.append(("iter_records", self.config["name"]))
        for r in self.rows:
            yield dict(r)

    def iter_records_slice(self, my_slice, max_slice):
        self.log.append(("iter_records_slice", self.config["name"], my_slice, max_slice))
        for i, r in enumerate(self.rows):
            if i % max_slice == my_slice:
                yield dict(r)


class Acquirer:
    def __init__(self, log, source, namespace, multiple=False, fail=()):
        self.log = log
        self.source = source
        self.namespace = namespace
        self.multiple = multiple
        self.fail = set(fail)

    def returns_multiple(self):
        return self.multiple

    def _record(self, recid, suffix=""):
        return {"identifier": recid + suffix,
                "data": {"id": f"{self.namespace}{recid}{suffix}", "type": "Type",
                         "source": self.source}}

    def acquire(self, recid, data=None, rectype=None):
        self.log.append(("acquire", self.source, recid, data, rectype))
        if recid in self.fail:
            return None
        return self._record(recid)

    def acquire_all(self, recid, data=None, rectype=None):
        self.log.append(("acquire_all", self.source, recid, data, rectype))
        if recid in self.fail:
            return []
        return [self._record(recid, "-a"), self._record(recid, "-b")]


class Mapper:
    def __init__(self, log, source):
        self.log = log
        self.source = source

    def post_reconcile(self, rec):
        self.log.append(("post_reconcile", self.source, rec["data"]["id"]))


def make_stubs(log, queue):
    """The classes the phase builds, recording through `log`; the reference
    manager hands out `queue` in order."""

    class Reconciler:
        def __init__(self, cfgs, idmap, networkmap):
            log.append(("Reconciler", cfgs, idmap, networkmap))

        def reconcile(self, rec):
            log.append(("reconcile", rec["data"]["id"]))
            rec["data"]["reconciled"] = True
            return rec

    class ReferenceManager:
        def __init__(self, cfgs, idmap, workers=1):
            log.append(("ReferenceManager", cfgs, idmap, workers))
            self.queue = list(queue)

        def walk_top_for_refs(self, data, distance):
            log.append(("walk_top_for_refs", data["id"], distance))

        def pop_ref(self):
            item = self.queue.pop(0) if self.queue else None
            log.append(("pop_ref", item))
            return item

        def did_ref(self, quri, distance):
            log.append(("did_ref", quri, distance))

        def write_metatypes(self, my_slice):
            log.append(("write_metatypes", my_slice))

    class IdentityResolver:
        def __init__(self, cfgs, idmap, my_slice=-1, max_slice=-1):
            log.append(("IdentityResolver", cfgs, idmap, my_slice, max_slice))

        def write_record(self, rec):
            log.append(("write_record", rec["data"]["id"], rec["data"].get("reconciled")))

        def close(self):
            log.append(("close",))

    class PoolManager:
        instance = None

        @classmethod
        def get_instance(cls):
            if cls.instance is None:
                cls.instance = cls()
            return cls.instance

        def put_all(self, name):
            log.append(("put_all", name))

    return Reconciler, ReferenceManager, IdentityResolver, PoolManager


class StubConfigs:
    debug_reconciliation = False
    max_distance = 2

    def __init__(self, tmp_path, log, ycba_rows=(), aat_rows=(), fail=(), multiple=False):
        self.log_dir = str(tmp_path / "logs")
        self.data_dir = str(tmp_path)
        self.temp_dir = str(tmp_path)
        os.makedirs(os.path.join(self.log_dir, "flags"))
        self.internal = {
            "ycba": {"name": "ycba", "type": "internal", "namespace": YCBA,
                     "datacache": Datacache(log, "ycba", list(ycba_rows)),
                     "mapper": Mapper(log, "ycba"),
                     "acquirer": Acquirer(log, "ycba", YCBA, multiple=multiple, fail=fail)},
        }
        self.external = {
            "aat": {"name": "aat", "type": "external", "namespace": AAT,
                    "datacache": Datacache(log, "aat", list(aat_rows)),
                    "mapper": Mapper(log, "aat"),
                    "acquirer": Acquirer(log, "aat", AAT, fail=fail)},
        }

    def is_qua(self, uri):
        return "##qua" in uri

    def split_qua(self, uri):
        uri, typ = uri.split("##qua")
        return uri, typ

    def split_uri(self, uri):
        for cfg in [*self.internal.values(), *self.external.values()]:
            if uri.startswith(cfg["namespace"]):
                return cfg, uri[len(cfg["namespace"]):]
        raise ValueError(uri)


def row(recid):
    return {"identifier": recid, "data": {"id": f"{YCBA}{recid}", "type": "Type"}}


@pytest.fixture
def harness(tmp_path, monkeypatch):
    """Build a config and stubs, patch them into the phase module, and run
    `reconcile_slice`; the shared `log` is the record of what happened."""

    class Harness:
        def __init__(self):
            self.log = []
            self.queue = []
            self.idmap = Idmap(self.log)
            self.networkmap = object()
            self.cfgs = None

        def configure(self, **kw):
            self.cfgs = StubConfigs(tmp_path, self.log, **kw)
            return self.cfgs

        def run(self, to_do, **kw):
            if self.cfgs is None:
                self.configure()
            classes = make_stubs(self.log, self.queue)
            for cls in classes:
                monkeypatch.setattr(phase, cls.__name__, cls)
            return reconcile_slice(self.cfgs, self.idmap, self.networkmap, to_do, **kw)

        def names(self):
            return [c[0] for c in self.log]

        def only(self, *kinds):
            return [c for c in self.log if c[0] in kinds]

        def flag(self, n):
            return Path(self.cfgs.log_dir) / "flags" / f"reconcile_is_done-{n}.txt"

    return Harness()


def ycba_todo(h, recids=()):
    return [["ycba", h.cfgs.internal["ycba"], list(recids)]]


# --- the records pass --------------------------------------------------------


def test_each_record_goes_through_the_steps_in_order(harness):
    h = harness
    h.configure(ycba_rows=[row("1"), row("2")])
    result = h.run(ycba_todo(h))

    # set-up: the three collaborators, built with what the script gave them
    assert h.log[0] == ("Reconciler", h.cfgs, h.idmap, h.networkmap)
    assert h.log[1] == ("ReferenceManager", h.cfgs, h.idmap, 1)      # unsliced: one worker
    assert h.log[2] == ("IdentityResolver", h.cfgs, h.idmap, -1, -1)
    assert h.log[3] == ("memory_cache", True)                       # unsliced: cache stays accurate
    assert h.log[4] == ("iter_records", "ycba")

    per_record = ["acquire", "reconcile", "post_reconcile", "walk_top_for_refs", "write_record"]
    names = h.names()
    assert names[5:15] == per_record * 2
    assert h.only("reconcile") == [("reconcile", f"{YCBA}1"), ("reconcile", f"{YCBA}2")]
    assert h.only("walk_top_for_refs") == [("walk_top_for_refs", f"{YCBA}1", 0),
                                           ("walk_top_for_refs", f"{YCBA}2", 0)]
    # the assertion log sees the record after reconcile and post_reconcile
    assert h.only("write_record") == [("write_record", f"{YCBA}1", True),
                                      ("write_record", f"{YCBA}2", True)]
    # then the references pass (an empty queue ends it) and the tidy-up
    assert names[15:] == ["pop_ref", "close", "write_metatypes", "put_all"]
    assert h.log[-3:] == [("close",), ("write_metatypes", -1), ("put_all", "localsocket")]

    assert result["records"]["records"] == 2 and result["records"]["phase"] == "reconcile"
    assert result["references"]["records"] == 0 and result["references"]["phase"] == "reconcile-refs"
    assert not h.flag(-1).exists() and not list(h.flag(0).parent.iterdir())


def test_streamed_rows_are_stamped_with_the_source_and_handed_to_acquire(harness):
    h = harness
    h.configure(ycba_rows=[row("1")])
    h.run(ycba_todo(h))
    (call,) = h.only("acquire")
    assert call[1:3] == ("ycba", "1")
    supplied = call[3]
    assert supplied["source"] == "ycba" and supplied["identifier"] == "1"
    assert call[4] is None


def test_recids_are_acquired_without_a_row(harness):
    h = harness
    h.configure(ycba_rows=[row("1"), row("2"), row("3")])
    h.run(ycba_todo(h, recids=["2", "9"]))
    assert h.only("acquire") == [("acquire", "ycba", "2", None, None),
                                 ("acquire", "ycba", "9", None, None)]
    assert not h.only("iter_records", "iter_records_slice")


def test_sliced_run(harness):
    h = harness
    h.configure(ycba_rows=[row("0"), row("1"), row("2"), row("3")])
    h.run(ycba_todo(h), my_slice=1, max_slice=2)
    assert ("ReferenceManager", h.cfgs, h.idmap, 2) in h.log
    assert ("IdentityResolver", h.cfgs, h.idmap, 1, -1) in h.log
    assert ("memory_cache", False) in h.log and ("memory_cache", True) not in h.log
    assert h.only("iter_records_slice") == [("iter_records_slice", "ycba", 1, 2)]
    assert [c[2] for c in h.only("acquire")] == ["1", "3"]
    assert ("write_metatypes", 1) in h.log
    assert h.flag(1).read_text() == "1"


def test_write_flag_false_writes_no_flag(harness):
    h = harness
    h.configure(ycba_rows=[row("0")])
    h.run(ycba_todo(h), my_slice=0, max_slice=1, write_flag=False)
    assert not h.flag(0).exists()


def test_a_record_that_cannot_be_acquired_is_skipped_with_a_warning(harness, caplog):
    h = harness
    h.configure(ycba_rows=[row("1"), row("2")], fail=["1"])
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        result = h.run(ycba_todo(h))
    assert " *** Failed to acquire any record for ycba/1 ***" in caplog.text
    assert [c[1] for c in h.only("reconcile")] == [f"{YCBA}2"]
    assert (result["records"]["records"], result["records"]["skipped"]) == (1, 1)


def test_an_acquirer_that_returns_multiple_records(harness):
    h = harness
    h.configure(ycba_rows=[row("1")], multiple=True)
    h.run(ycba_todo(h))
    (call,) = h.only("acquire_all")
    assert call[1:3] == ("ycba", "1") and call[3]["source"] == "ycba"
    assert [c[1] for c in h.only("reconcile")] == [f"{YCBA}1-a", f"{YCBA}1-b"]
    assert len(h.only("write_record")) == 2


def test_several_sources_in_order(harness):
    h = harness
    h.configure(ycba_rows=[row("1")], aat_rows=[{"identifier": "300000001", "data": {}}])
    h.run([["aat", h.cfgs.external["aat"], []], ["ycba", h.cfgs.internal["ycba"], []]])
    assert [c[1] for c in h.only("iter_records")] == ["aat", "ycba"]
    assert [c[1:3] for c in h.only("acquire")] == [("aat", "300000001"), ("ycba", "1")]


# --- the references pass -----------------------------------------------------


def test_references_loop_handles_every_kind_of_item(harness, caplog):
    h = harness
    h.configure(fail=["4"])
    h.queue[:] = [
        (f"{AAT}1##quaType", {"dist": 1, "type": "Type"}),      # acquired: did_ref after acquire
        (f"{AAT}2##quaType", {"dist": 3, "type": "Type"}),      # beyond max_distance: untouched
        ("http://unknown.example/x##quaType", {"dist": 1, "type": "Type"}),  # unsplittable: done, skipped
        (f"{AAT}3##quaType", {"dist": 1}),                      # no type: skipped
        ("garbage",),                                           # malformed: skipped
        (f"{AAT}4##quaType", {"dist": 2, "type": "Type"}),      # acquire fails: not marked done
        None,
    ]
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        result = h.run([])
    refs = h.log[h.names().index("pop_ref"):]
    assert [c for c in refs if c[0] != "pop_ref"] == [
        ("acquire", "aat", "1", None, "Type"),
        ("did_ref", f"{AAT}1##quaType", 1),
        ("reconcile", f"{AAT}1"),
        ("post_reconcile", "aat", f"{AAT}1"),
        ("walk_top_for_refs", f"{AAT}1", 1),
        ("write_record", f"{AAT}1", True),
        ("did_ref", "http://unknown.example/x##quaType", 1),
        ("acquire", "aat", "4", None, "Type"),
        ("close",), ("write_metatypes", -1), ("put_all", "localsocket"),
    ]
    assert len(h.only("pop_ref")) == 7
    assert "Failed to acquire Type reference: aat:4" in caplog.text
    assert (result["references"]["records"], result["references"]["skipped"]) == (1, 1)


def test_reference_without_qua_raises(harness):
    h = harness
    h.queue[:] = [(f"{AAT}1", {"dist": 1, "type": "Type"}), None]
    with pytest.raises(ValueError, match="No qua in referenced"):
        h.run([])


def test_internal_reference_raises(harness, caplog):
    h = harness
    h.queue[:] = [(f"{YCBA}obj/1##quaType", {"dist": 1, "type": "Type"}), None]
    with caplog.at_level(logging.ERROR, logger="lux_pipeline"):
        with pytest.raises(ValueError, match=f"{YCBA}obj/1"):
            h.run([])
    assert f"Got internal reference! {YCBA}obj/1" in caplog.text
    assert not h.only("acquire", "did_ref")


def test_unusable_uri_is_logged_only_with_debug_reconciliation(harness, caplog):
    h = harness
    h.queue[:] = [("http://unknown.example/x##quaType", {"dist": 1, "type": "Type"}), None]
    with caplog.at_level(logging.DEBUG, logger="lux_pipeline"):
        h.run([])
    assert "Not processing" not in caplog.text
    h.log.clear()
    h.queue[:] = [("http://unknown.example/x##quaType", {"dist": 1, "type": "Type"}), None]
    h.cfgs.debug_reconciliation = True
    with caplog.at_level(logging.DEBUG, logger="lux_pipeline"):
        h.run([])
    assert "Not processing: http://unknown.example/x" in caplog.text


def test_no_references_pass(harness):
    h = harness
    h.configure(ycba_rows=[row("1")])
    h.queue[:] = [(f"{AAT}1##quaType", {"dist": 1, "type": "Type"}), None]
    result = h.run(ycba_todo(h), do_references=False)
    assert not h.only("pop_ref")
    assert h.names()[-3:] == ["close", "write_metatypes", "put_all"]
    assert result["references"] is None
    assert result["records"]["records"] == 1 and result["records"]["phase"] == "reconcile"


# --- progress ----------------------------------------------------------------


def test_progress_running_total_and_reset_for_references(harness):
    h = harness
    h.configure(ycba_rows=[row(str(i)) for i in range(10)],
                aat_rows=[{"identifier": str(i), "data": {}} for i in range(6)])
    h.queue[:] = [(f"{AAT}1##quaType", {"dist": 1, "type": "Type"}), None]
    rec = Recorder()
    h.run([["ycba", h.cfgs.internal["ycba"], []], ["aat", h.cfgs.external["aat"], []]],
          my_slice=0, max_slice=2, progress=rec)
    totals = [c["total"] for c in rec.calls if "total" in c]
    assert totals == [5, 8]                                      # 10 // 2, then + 6 // 2
    descriptions = [c["description"] for c in rec.calls if "description" in c]
    assert descriptions == ["ycba", "aat", "references"]
    phases = [c["phase"] for c in rec.calls if "phase" in c]
    assert phases == ["reconcile", "reconcile-refs"]
    done = [i for i, c in enumerate(rec.calls) if c.get("done")]
    reset = [i for i, c in enumerate(rec.calls) if c.get("reset")]
    assert len(done) == 2 and len(reset) == 1 and done[0] < reset[0] < done[1]
    advances = sum(c.get("advance", 0) for c in rec.calls)
    assert advances == 8 + 1                                     # this slice's records, one reference


def test_progress_total_for_recids_is_their_count(harness):
    h = harness
    h.configure(ycba_rows=[row(str(i)) for i in range(10)])
    rec = Recorder()
    h.run(ycba_todo(h, recids=["1", "2", "3"]), progress=rec, do_references=False)
    assert [c["total"] for c in rec.calls if "total" in c] == [3]
