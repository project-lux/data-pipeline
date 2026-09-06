"""Nested stages are reported, never subtracted twice.

`timing.stage("acquire.map")` runs inside `timer.stage("acquire")`. Summing
every stage and subtracting from the wall clock therefore counts that time
twice, and once the sub-stages were added to the acquirer a real build
reported **-16.51 worker-hours unattributed, -53.1%** -- a number that cannot
exist and that makes every percentage in the phase wrong.

The second half is subtler: the sub-stages are not a breakdown of their
parent either. `timing.stage()` attributes to whichever PhaseTimer is active,
so the acquirer's stages accumulate whether it was called from the top of the
loop or from inside `reconcile()`. In the run that prompted this the four
`acquire.*` stages totalled 19.03 worker-hours against `acquire`'s 13.32.
Reporting them as children of `acquire` without saying so implies a partition
that isn't one.
"""

import io
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipeline.process.timing import PhaseTimer, split_stages


def timer(elapsed=100.0):
    t = PhaseTimer("reconcile", out_dir=None, stream=io.StringIO())
    t.t0 = time.perf_counter() - elapsed      # control the wall clock
    return t


# --- the rule ---------------------------------------------------------------

def test_a_dotted_name_is_a_child_of_its_prefix():
    top, kids = split_stages({"acquire": 1, "acquire.map": 2, "reconcile": 3})
    assert set(top) == {"acquire", "reconcile"}
    assert kids == {"acquire": {"acquire.map": 2}}


def test_a_child_with_no_parent_stage_is_promoted():
    """Otherwise its time is attributed nowhere and silently becomes
    unaccounted."""
    top, kids = split_stages({"acquire.map": 2, "reconcile": 3})
    assert set(top) == {"acquire.map", "reconcile"}
    assert kids == {}


def test_only_the_first_dot_splits():
    top, kids = split_stages({"a": 1, "a.b.c": 2})
    assert kids == {"a": {"a.b.c": 2}}


# --- what it fixes ----------------------------------------------------------

def test_unattributed_is_not_driven_negative_by_children():
    t = timer(elapsed=100.0)
    t.add("acquire", 60.0, calls=10)
    t.add("reconcile", 30.0, calls=10)
    t.add("acquire.post_map", 40.0, calls=10)     # inside acquire
    t.add("acquire.map", 15.0, calls=10)          # inside acquire
    s = t.summary()
    # 100 - (60 + 30), not 100 - (60 + 30 + 40 + 15)
    assert 9.0 < s["unaccounted_seconds"] < 11.0, s["unaccounted_seconds"]


def test_the_real_regression_shape():
    """reconcile, run 2: 31.1 worker-hours, acquire.* totalling more than
    acquire. Reported -53.1% unattributed; the true figure is 8.2%."""
    t = timer(elapsed=31.1)
    for name, hours in [("acquire", 13.32), ("reconcile", 7.53), ("walk_refs", 7.44),
                        ("assertions", 0.24), ("post_reconcile", 0.03),
                        ("acquire.post_map", 9.53), ("acquire.cache_hit", 3.82),
                        ("acquire.map", 3.64), ("acquire.fetch", 2.04)]:
        t.add(name, hours, calls=1)
    s = t.summary()
    assert s["unaccounted_seconds"] > 0
    assert 2.4 < s["unaccounted_seconds"] < 2.7, s["unaccounted_seconds"]


def test_stages_are_marked_so_a_reader_knows_not_to_add_them_up():
    t = timer()
    t.add("acquire", 60.0)
    t.add("acquire.map", 15.0)
    st = t.summary()["stages"]
    assert st["acquire"]["nested"] is False
    assert st["acquire.map"]["nested"] is True


# --- what it prints ---------------------------------------------------------

def test_children_are_printed_apart_from_the_stage_table():
    t = timer(elapsed=100.0)
    t.add("acquire", 60.0, calls=10)
    t.add("reconcile", 30.0, calls=10)
    t.add("acquire.map", 15.0, calls=10)
    t.finish()
    out = t.stream.getvalue()
    table = out.split("inside acquire")[0]
    assert "acquire.map" not in table, "a child must not sit in the additive table"
    assert "already counted above, not additional" in out
    assert "acquire.map" in out.split("inside acquire")[1]


def test_children_exceeding_their_parent_say_so():
    t = timer(elapsed=100.0)
    t.add("acquire", 10.0, calls=10)
    t.add("acquire.map", 25.0, calls=10)      # also called from elsewhere
    t.finish()
    out = t.stream.getvalue()
    assert "runs inside other stages" in out
    assert "not a breakdown of acquire" in out


def test_the_progress_line_ranks_parents_not_children():
    t = timer(elapsed=100.0)
    t.add("acquire", 20.0, calls=10)
    t.add("acquire.map", 90.0, calls=10)
    t.report()
    line = [l for l in t.stream.getvalue().splitlines() if "%" in l][-1]
    assert "acquire.map" not in line
    assert "acquire" in line
