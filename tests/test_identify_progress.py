"""identify says what it is doing while it does it.

`resolve_identity()` printed nothing until it was completely finished, so a
run that had been going for two hours was indistinguishable from a hung one --
there was no way to tell whether it was still on the first sort or nearly
done. The step boundaries were already there, commented out.

Two properties matter. Every step announces itself *before* it starts, because
the useful line is the one naming what you are currently waiting for, not the
one confirming what already finished. And the timing lands in the same JSON
every other phase writes, so timing-report.py rolls identify in and the phase
finally gets a baseline.
"""

import io
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from pipeline.process.identity_resolver import IdentityResolver
from pipeline.process.timing import PhaseTimer


def resolver(with_timer=True):
    r = object.__new__(IdentityResolver)
    r.sort_buffer_size = "1G"
    if with_timer:
        r._timer = PhaseTimer("identify", out_dir=None, stream=io.StringIO())
    return r


# --- formatting -------------------------------------------------------------

def test_durations_are_readable_at_every_scale():
    hms = IdentityResolver._hms
    assert hms(9) == "9s"
    assert hms(90) == "1m30s"
    assert hms(8410) == "2h20m10s"      # the run that prompted this


def test_sizes_are_readable():
    b = IdentityResolver._bytes
    assert b(31_400_000_000) == "31.4GB"
    assert b(13_000_000) == "13MB"


# --- the announcements ------------------------------------------------------

def test_a_step_announces_itself_before_it_runs(capsys):
    """Announcing only on completion would leave you blind for exactly as
    long as the step takes, which is the whole problem."""
    r = resolver()
    with r._step("sort_assertions"):
        during = capsys.readouterr().out
        assert "sort_assertions ..." in during
    after = capsys.readouterr().out
    assert "done in" in after


def test_a_step_reports_even_when_it_raises(capsys):
    """A step that dies is the one you most want the elapsed time for."""
    r = resolver()
    with pytest.raises(RuntimeError):
        with r._step("cluster"):
            raise RuntimeError("sort died")
    out = capsys.readouterr().out
    assert "cluster done in" in out


def test_steps_land_in_the_timing_json():
    r = resolver()
    with r._step("aggregate_edges"):
        pass
    with r._step("cluster"):
        pass
    s = r._timer.summary()
    assert set(s["stages"]) == {"aggregate_edges", "cluster"}
    # marks give timing-report.py the per-step boundaries
    assert [m[0] for m in s["marks"]] == ["aggregate_edges", "cluster"]


def test_the_phase_is_named_so_the_rollup_finds_it():
    r = resolver()
    assert r._timer.summary()["phase"] == "identify"


# --- the writer path must not break ----------------------------------------

def test_the_helpers_are_harmless_without_a_timer(capsys):
    """run-reconcile constructs this class purely as an assertion writer and
    never calls resolve_identity, so _timer is never set there."""
    r = resolver(with_timer=False)
    assert r._timer is None
    with r._step("whatever"):
        pass
    assert "whatever done in" in capsys.readouterr().out


def test_the_class_default_means_no_attribute_error():
    """A partially built instance -- which is what run-reconcile has before
    resolve_identity would ever run -- must not explode on _timer."""
    assert IdentityResolver._timer is None


def test_a_sort_is_recorded_as_nested_inside_its_step():
    """split_stages() treats a dotted name as a child of its prefix. A sort
    recorded under a name that does not start with its enclosing step's would
    be summed alongside it -- the double-count that made a phase report -53%
    unattributed (see §3.6)."""
    from pipeline.process.timing import split_stages

    r = resolver()
    r._timer.t0 -= 2500
    with r._step("sort_assertions"):
        assert r._step_label == "sort_assertions"
        r._timer.add(f"{r._step_label}.sort:assertions.sorted", 2410)
    top, kids = split_stages(r._timer.stages)
    assert sorted(top) == ["sort_assertions"]
    assert kids == {"sort_assertions": {"sort_assertions.sort:assertions.sorted":
                                        r._timer.stages["sort_assertions.sort:assertions.sorted"]}}
    assert r._timer.summary()["unaccounted_seconds"] >= 0


def test_the_step_label_is_restored_after_nesting():
    r = resolver()
    with r._step("outer"):
        with r._step("inner"):
            assert r._step_label == "inner"
        assert r._step_label == "outer"
    assert r._step_label is None
