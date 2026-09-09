"""run-identify.py must not destroy its own input.

`IdentityResolver` is two things behind one class: run-reconcile.py builds it
per slice to *write* an assertion log, and run-identify.py builds it once, with
no slice, to *resolve* those logs. `__init__` used to open the log file
unconditionally, in `"w"` mode:

    if my_slice > -1:
        fn = f"assertions-{my_slice}.tsv"
    else:
        fn = "assertions-single.tsv"
    self.fh = open(fn, "w", buffering=1024 * 1024)

In a sliced build that only littered a stray empty `assertions-single.tsv`
beside the real per-slice files, which is why it survived. In an unsliced
build -- run-reconcile.py leaves `my_slice` at -1 whenever argv[1] and argv[2]
are not both numeric -- `assertions-single.tsv` is exactly the file reconcile
just wrote, so identify truncated its input before reading it and then
resolved an empty assertion set: `nodes=0 pairs=0 clusters=0`, nothing written
to the idmap, exit status 0.

It was silent because resolve_identity's guard counts files, not bytes, and
the zero-byte file it had just created satisfied it.

The log is now opened on the first write. These tests pin that.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from pipeline.process.identity_resolver import IdentityResolver


class StubConfigs:
    internal_uri = "https://lux.collections.yale.edu/data/"
    ok_record_types = {"Type": "concept"}
    parent_record_types = {}
    external = {"aat": {"name": "aat",
                        "namespace": "http://vocab.getty.edu/aat/"}}
    results = {"merged": {}}

    def __init__(self, temp_dir):
        self.temp_dir = str(temp_dir)

    def is_qua(self, recid):
        return "##qua" in recid

    def make_qua(self, recid, typ):
        return recid if "##qua" in recid else f"{recid}##qua{typ}"

    def split_qua(self, recid):
        return recid.split("##qua")


class StubIdmap:
    def __init__(self, cfgs):
        self.prefix_map_out = {"yuid": cfgs.internal_uri,
                               "aat": "http://vocab.getty.edu/aat/"}
        self.prefix_map_in = {v: k for k, v in self.prefix_map_out.items()}


def a_record(n):
    return {"data": {"id": f"http://example.org/rec/{n}", "type": "Type",
                     "equivalent": [{"id": f"http://vocab.getty.edu/aat/{n}",
                                     "type": "Type"}]}}


def test_construction_does_not_truncate_an_existing_log(tmp_path):
    """The bug, directly: reconcile writes, identify constructs, data survives."""
    cfgs = StubConfigs(tmp_path)
    log = tmp_path / "assertions-single.tsv"

    writer = IdentityResolver(cfgs, StubIdmap(cfgs))     # unsliced reconcile
    for n in range(50):
        writer.write_record(a_record(n))
    writer.close()
    written = log.read_bytes()
    assert written, "fixture should have produced a non-empty log"

    IdentityResolver(cfgs, StubIdmap(cfgs))              # run-identify.py:29

    assert log.read_bytes() == written


def test_a_resolver_that_never_writes_creates_no_file(tmp_path):
    """No write, no file -- so a sliced run gets no stray 25th input either."""
    cfgs = StubConfigs(tmp_path)
    IdentityResolver(cfgs, StubIdmap(cfgs))
    assert list(tmp_path.glob("assertions-*.tsv")) == []


def test_close_without_a_write_is_safe(tmp_path):
    """close() has to cope with a handle that was never opened."""
    cfgs = StubConfigs(tmp_path)
    r = IdentityResolver(cfgs, StubIdmap(cfgs))
    r.close()
    r.close()                                            # idempotent
    assert list(tmp_path.glob("assertions-*.tsv")) == []


def test_writer_still_creates_and_fills_its_own_slice_file(tmp_path):
    """The lazy open must not cost the writer anything."""
    cfgs = StubConfigs(tmp_path)
    w = IdentityResolver(cfgs, StubIdmap(cfgs), 7)
    assert not (tmp_path / "assertions-7.tsv").exists()   # nothing yet
    w.write_record(a_record(1))
    w.close()
    lines = (tmp_path / "assertions-7.tsv").read_text().splitlines()
    assert len(lines) == 1
    assert lines[0].split("\t")[2] == "http://example.org/rec/1##quaType"


def test_a_record_the_writer_skips_creates_no_file(tmp_path):
    """The early return in write_record runs before the file is opened."""
    cfgs = StubConfigs(tmp_path)
    w = IdentityResolver(cfgs, StubIdmap(cfgs), 2)
    w.write_record({})
    w.write_record({"data": {"type": "Type"}})           # no id
    w.close()
    assert list(tmp_path.glob("assertions-*.tsv")) == []


def test_no_assertions_at_all_now_raises(tmp_path):
    """Previously the constructor's own zero-byte file satisfied the guard and
    identify "succeeded" over an empty input. With nothing on disk the guard
    fires, which is what a build where every slice emitted nothing deserves."""
    cfgs = StubConfigs(tmp_path)
    r = IdentityResolver(cfgs, StubIdmap(cfgs))
    with pytest.raises(ValueError, match="No assertions"):
        r.resolve_identity(conflicts_file=str(tmp_path / "conf.jsonl"),
                           work_dir=str(tmp_path))
