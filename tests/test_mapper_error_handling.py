"""Malformed input should cost the field, not the record.

Every one of these was an exception escaping a mapper into acquirer.acquire(),
which catches it, prints one line and returns None -- so the whole record was
dropped from the build because one date, or one optional XML section, was not
the shape the code assumed.

The counts came from a 24-worker reconcile: ~2000 `'xml'` lines per worker,
~150 `NoneType + timedelta`, plus a long tail of lcnaf date failures.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from pipeline.process.utils.mapper_utils import make_datetime
from pipeline.sources.authorities.lc.mapper import LcnafMapper


# --- make_datetime ----------------------------------------------------------

def test_open_ended_edtf_interval_returns_none_not_a_crash():
    """EDTF gives +/-inf, a float, for an open interval end. Reading .tm_year
    off it was "'float' object has no attribute 'tm_year'" -- raised out of
    make_datetime and into the caller's record.

    These specific forms are the ones dateutil declines, so they reach the
    EDTF branch; plain "1980/.." never gets there because dateutil parses
    1980 out of it first."""
    for value in ["../..", "1980~/..", "../1980~", "1980-01-01~/.."]:
        assert make_datetime(value) is None, value


def test_unparsable_dates_return_none_rather_than_raising():
    for value in ["", "not a date at all", "9999", "0000", "????"]:
        assert make_datetime(value) is None, value


def test_make_datetime_never_raises_on_junk():
    """The contract the callers rely on: None or a (begin, end) pair."""
    junk = ["+", "-", "0/0", "31 Feb 1900", "1900-13-45", "u", "XXXX",
            "1234567890123456789012345678901234", "12:00:00+25:00",
            "1999-01-01T00:00:00+99:00"]
    for value in junk:
        try:
            out = make_datetime(value)
        except Exception as e:
            pytest.fail(f"make_datetime({value!r}) raised {type(e).__name__}: {e}")
        assert out is None or (isinstance(out, tuple) and len(out) == 2), value


# --- lcnaf activity dates ---------------------------------------------------

def test_activity_date_handles_every_shape_without_raising():
    """The three shapes the type ladder knew about, plus the ones it didn't:
    a list left `asdd` unbound, and an unparsable value left it None."""
    d = LcnafMapper._activity_date
    assert d({}, "p") is None                                  # absent
    assert d({"p": ["1923", "1924"]}, "p") is None             # list: was UnboundLocalError
    assert d({"p": None}, "p") is None
    assert d({"p": {"no@value": 1}}, "p") is None
    assert d({"p": "gibberish"}, "p") is None                  # was NoneType subscript
    assert d({"p": {"@value": "gibberish"}}, "p") is None

    for raw in ["1923", {"@value": "1923"}, 1923]:
        got = d({"p": raw}, "p")
        assert got is not None and len(got) == 2, raw
        assert got[0].startswith("1923"), got


def test_end_date_cannot_inherit_the_start_date():
    """The silent one. Both blocks assigned the same `asdd`, so an end date
    that fell through the type checks left the START date's value in place
    and the record got it as end_of_the_end."""
    rwo = {"madsrdf:activityStartDate": "1923",
           "madsrdf:activityEndDate": ["1945"]}       # a list: no date taken
    assert LcnafMapper._activity_date(rwo, "madsrdf:activityStartDate")[0].startswith("1923")
    assert LcnafMapper._activity_date(rwo, "madsrdf:activityEndDate") is None


def test_raw_value_is_not_mistaken_for_the_parsed_pair():
    """make_datetime returns the (begin, end) pair; the RAW value is a
    string, so indexing that instead yields its first character -- a
    begin_of_the_begin of "1" for "1923-05-01"."""
    got = LcnafMapper._activity_date({"p": "1923-05-01"}, "p")
    assert got[0] != "1"
    assert got[0].startswith("1923-05-01")
