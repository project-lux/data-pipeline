"""The resolver's sorts: presorted slices are merged, whole-line order where
that is the order, the buffer a share of memory.

Identify is nine external sorts over files of a line per record, and the
first -- every slice's assertions together -- is the largest. The slices
now sort their own file when they close, on the reconcile workers' cores
in parallel, and the resolver checks every input with ``sort -c`` and
merges (``sort -m``, one linear pass) when they all are. The key options
are gone from the sorts whose order is the whole line: a tab sorts below
every other byte, so field-by-field order on every field of a
tab-separated line is whole-line order, and `sort` without keys compares
lines with one memcmp instead of locating fields for every comparison.
"""

import os
import random
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from lux_pipeline.process import identity_resolver as ir
from lux_pipeline.process.identity_resolver import IdentityResolver

from test_assertion_log_lifecycle import StubConfigs, StubIdmap, a_record


def resolver(tmp_path):
    r = object.__new__(IdentityResolver)
    r.sort_buffer_size = "1M"
    r.sort_parallel = "2"
    r._timer = None
    r._step_label = None
    return r


# --- the slice presorts its file ------------------------------------------------

def test_close_sorts_the_slice_file_in_byte_order(tmp_path):
    cfgs = StubConfigs(tmp_path)
    w = IdentityResolver(cfgs, StubIdmap(cfgs), 3)
    for n in (5, 1, 10, 2):
        w.write_record(a_record(n))
    w.close()
    lines = (tmp_path / "assertions-3.tsv").read_text().splitlines()
    assert lines == sorted(lines)
    assert len(lines) == 4
    assert subprocess.run(["sort", "-c", str(tmp_path / "assertions-3.tsv")],
                          env={**os.environ, "LC_ALL": "C"}).returncode == 0


def test_a_failed_presort_leaves_the_file_and_warns(tmp_path, monkeypatch, caplog):
    cfgs = StubConfigs(tmp_path)
    w = IdentityResolver(cfgs, StubIdmap(cfgs), 4)
    w.write_record(a_record(2))
    w.write_record(a_record(1))

    def boom(cmd, **kw):
        raise OSError("no sort here")
    monkeypatch.setattr(ir.subprocess, "run", boom)
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        w.close()
    assert "could not presort" in caplog.text
    lines = (tmp_path / "assertions-4.tsv").read_text().splitlines()
    assert len(lines) == 2 and lines != sorted(lines)


# --- the resolver merges what is sorted, sorts what is not ---------------------

def test_all_sorted_checks_every_input(tmp_path, caplog):
    r = resolver(tmp_path)
    a = tmp_path / "a.tsv"
    b = tmp_path / "b.tsv"
    a.write_text("a\tb\tc\nb\tb\tc\n")
    b.write_text("z\tb\tc\ny\tb\tc\n")
    assert r._all_sorted([str(a)]) is True
    with caplog.at_level("INFO", logger="lux_pipeline"):
        assert r._all_sorted([str(a), str(b)]) is False
    assert "1 of 2 assertion files not presorted" in caplog.text
    assert "b.tsv" in caplog.text


def test_merge_interleaves_sorted_inputs(tmp_path):
    r = resolver(tmp_path)
    a = tmp_path / "a.tsv"
    b = tmp_path / "b.tsv"
    a.write_text("a\t1\nc\t1\n")
    b.write_text("b\t1\nd\t1\n")
    out = tmp_path / "out"
    r._sort([str(a), str(b)], str(out), [], str(tmp_path), merge=True)
    assert out.read_text().splitlines() == ["a\t1", "b\t1", "c\t1", "d\t1"]


def test_sort_assertions_merges_presorted_slices(tmp_path, monkeypatch, caplog):
    """End to end: the writers presort, so the resolver's first sort is a
    merge -- and an unsorted stray makes it a sort, with the same result."""
    from test_identity_streaming import FakeIdMap, StubConfigs as SC, make_clusters, write_assertions

    rng = random.Random(8)
    cfgs = SC(temp_dir=tmp_path)
    clusters = make_clusters(rng, 30)
    write_assertions(cfgs, clusters, 3, rng)
    calls = []
    real = IdentityResolver._sort

    def spy(self, inputs, output, keys, tmpdir, merge=False):
        calls.append((os.path.basename(output), tuple(keys), merge))
        return real(self, inputs, output, keys, tmpdir, merge)
    monkeypatch.setattr(IdentityResolver, "_sort", spy)

    r = IdentityResolver(cfgs, FakeIdMap(cfgs))
    r.resolve_identity(conflicts_file=str(tmp_path / "c.jsonl"), work_dir=str(tmp_path), keep_temp=True)
    assert calls[0] == ("assertions.sorted", (), True)
    merged = next(tmp_path.glob("identity-*/assertions.sorted")).read_text().splitlines()
    assert merged == sorted(merged)

    # a slice that did not presort (a worker whose sort failed)
    write_assertions(cfgs, clusters, 2, random.Random(9))
    stray = sorted(tmp_path.glob("assertions-*.tsv"))[0]
    stray.write_text("".join(reversed(stray.read_text().splitlines(True))))
    calls.clear()
    with caplog.at_level("INFO", logger="lux_pipeline"):
        IdentityResolver(cfgs, FakeIdMap(cfgs)).resolve_identity(
            conflicts_file=str(tmp_path / "c.jsonl"), work_dir=str(tmp_path), keep_temp=True)
    assert calls[0] == ("assertions.sorted", (), False)
    assert "not presorted" in caplog.text


# --- the keys that remain ----------------------------------------------------------

def test_only_the_sorts_whose_order_is_not_the_line_keep_keys(tmp_path, monkeypatch):
    from test_identity_streaming import FakeIdMap, StubConfigs as SC, make_clusters, write_assertions

    rng = random.Random(10)
    cfgs = SC(temp_dir=tmp_path)
    write_assertions(cfgs, make_clusters(rng, 20), 2, rng)
    calls = {}
    real = IdentityResolver._sort

    def spy(self, inputs, output, keys, tmpdir, merge=False):
        calls[os.path.basename(output)] = tuple(keys)
        return real(self, inputs, output, keys, tmpdir, merge)
    monkeypatch.setattr(IdentityResolver, "_sort", spy)
    monkeypatch.delenv("LUX_PRIOR_PROBE", raising=False)

    IdentityResolver(cfgs, FakeIdMap(cfgs)).resolve_identity(
        conflicts_file=str(tmp_path / "c.jsonl"), work_dir=str(tmp_path))
    assert calls == {
        "assertions.sorted": (),
        "edges.byvote": ("-k3,3nr", "-k1,1", "-k2,2"),
        "selfs.sorted": ("-u",),
        "map.sorted": (),
        "clusters.bymember": ("-k2,2",),
        "clusters.prior": (),
        "claims.sorted": ("-k1,1", "-k2,2nr", "-k3,3"),
        "detail.sorted": (),
        "won.sorted": (),
        "touched.sorted": ("-u",),
    }
    assert "clusters.sorted" not in calls


def test_whole_line_order_is_field_order_for_tab_separated_lines(tmp_path):
    """The equivalence the dropped keys rest on, checked against sort
    itself: a tab (0x09) sorts below every byte a key can hold, so a key
    that is a prefix of another sorts first either way."""
    lines = ["a\tb\tc\n", "ab\tb\tc\n", "a\tbb\tc\n", "a\tb\tcc\n", "a\tb\t\n", "a\t\tz\n"]
    src = tmp_path / "in"
    src.write_text("".join(lines))
    env = {**os.environ, "LC_ALL": "C"}
    keyed = subprocess.run(["sort", "-t", "\t", "-k1,1", "-k2,2", "-k3,3", str(src)],
                           env=env, capture_output=True, text=True, check=True).stdout
    plain = subprocess.run(["sort", str(src)], env=env, capture_output=True, text=True,
                           check=True).stdout
    assert keyed == plain


# --- the buffer --------------------------------------------------------------------

def test_the_buffer_defaults_to_a_share_of_memory(monkeypatch):
    monkeypatch.delenv("LUX_SORT_BUFFER", raising=False)
    monkeypatch.delenv("LUX_SORT_PARALLEL", raising=False)
    cfgs = StubConfigs("/tmp")
    r = IdentityResolver(cfgs, StubIdmap(cfgs))
    assert r.sort_buffer_size == "25%"
    assert 1 <= int(r.sort_parallel) <= 16
    monkeypatch.setenv("LUX_SORT_BUFFER", "40G")
    monkeypatch.setenv("LUX_SORT_PARALLEL", "4")
    r = IdentityResolver(cfgs, StubIdmap(cfgs))
    assert r.sort_buffer_size == "40G" and r.sort_parallel == "4"


def test_sort_passes_the_buffer_and_parallelism(tmp_path, monkeypatch):
    r = resolver(tmp_path)
    r.sort_buffer_size = "30%"
    r.sort_parallel = "6"
    seen = []
    monkeypatch.setattr(ir.subprocess, "run", lambda cmd, **kw: seen.append(cmd))
    src = tmp_path / "x"
    src.write_text("b\na\n")
    r._sort([str(src)], str(tmp_path / "y"), ["-u"], str(tmp_path))
    [cmd] = seen
    assert cmd[:2] == ["sort", "-u"]
    assert "-S" in cmd and cmd[cmd.index("-S") + 1] == "30%"
    assert "--parallel=6" in cmd
    assert "-m" not in cmd
