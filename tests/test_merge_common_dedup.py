"""merge_common() dedups with sets, and merges the same record as before.

It runs once per cluster member, and its dedup collections hold everything
accumulated so far. With `in list` that is a scan of the whole merged record
per candidate -- quadratic in cluster size. On a europeana cluster it stopped
being theoretical: 21 workers pegged at 100% cpu, py-spy reporting 92% of
OwnTime inside this one function (in the scans, not in anything it calls) and
merged_recordcache growing at 24/s against a normal 1,150/s.

The risk in the change is silent: a set dedups the *output* too if anyone
writes to it instead of the list. So these pin the merged document, not the
timing -- order included, because the final mapper's first-wins field
selection depends on it.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from pipeline.process.merger import RecordMerger


class Configs:
    pass


def merger():
    m = object.__new__(RecordMerger)
    m.configs = Configs()
    # the three vocabulary ids merge_common reads for name handling
    m.globals = {"primaryName": "aat:primary",
                 "alternateName": "aat:alternate",
                 "sortName": "aat:sort"}
    return m


def rec(**kw):
    base = {"id": "https://lux.example/a", "type": "HumanMadeObject"}
    base.update(kw)
    return base


def ids_of(doc, field):
    return [x.get("id") for x in doc[field]]


# --- the merged document is unchanged ---------------------------------------

def test_equivalents_accumulate_in_order_without_duplicates():
    a = rec(equivalent=[{"id": "e1"}, {"id": "e2"}])
    b = rec(equivalent=[{"id": "e2"}, {"id": "e3"}])
    merger().merge_common(a, b)
    assert ids_of(a, "equivalent") == ["e1", "e2", "e3"]


def test_classified_as_and_member_of_take_the_same_path():
    a = rec(classified_as=[{"id": "c1"}], member_of=[{"id": "m1"}])
    b = rec(classified_as=[{"id": "c1"}, {"id": "c2"}],
            member_of=[{"id": "m2"}])
    merger().merge_common(a, b)
    assert ids_of(a, "classified_as") == ["c1", "c2"]
    assert ids_of(a, "member_of") == ["m1", "m2"]


def test_an_entry_without_an_id_is_left_alone():
    """The membership test is guarded by `"id" in i`, so a bare entry is
    neither added nor allowed to crash the set build."""
    a = rec(equivalent=[{"id": "e1"}])
    b = rec(equivalent=[{"_label": "no id here"}, {"id": "e2"}])
    merger().merge_common(a, b)
    assert ids_of(a, "equivalent") == ["e1", "e2"]


def test_referred_to_by_dedups_on_content():
    a = rec(referred_to_by=[{"type": "LinguisticObject", "content": "one"}])
    b = rec(referred_to_by=[{"type": "LinguisticObject", "content": " one "},
                            {"type": "LinguisticObject", "content": "two"}])
    merger().merge_common(a, b)
    assert [x["content"] for x in a["referred_to_by"]] == ["one", "two"]


def test_referred_to_by_dedups_on_id_when_there_is_no_content():
    a = rec(referred_to_by=[{"id": "r1", "content": "x"}])
    b = rec(referred_to_by=[{"id": "r1"}, {"id": "r2"}])
    merger().merge_common(a, b)
    assert [x.get("id") for x in a["referred_to_by"]] == ["r1", "r2"]


def test_only_one_ai_generated_statement_survives():
    """has_ai short-circuits now; it must still reach the same verdict."""
    a = rec(referred_to_by=[{"content": "AI generated description one"}])
    b = rec(referred_to_by=[{"content": "AI generated description two"}])
    merger().merge_common(a, b)
    assert len(a["referred_to_by"]) == 1


def test_representation_dedups_on_access_point():
    def ap(url):
        return {"digitally_shown_by": [{"access_point": [{"id": url}]}]}
    a = rec(representation=[ap("u1")])
    b = rec(representation=[ap("u1"), ap("u2")])
    merger().merge_common(a, b)
    assert len(a["representation"]) == 2       # u1 skipped, u2 added


def test_a_broken_referred_to_by_does_not_stop_the_merge():
    """The bare `except` around the content scan has to keep catching."""
    a = rec(referred_to_by=[{"content": ["a list, not a string"]}])
    b = rec(referred_to_by=[{"content": "fine"}])
    merger().merge_common(a, b)          # must not raise


# --- the complexity is actually gone ----------------------------------------

def _merge_n(n, per_member=4):
    """One record accumulating equivalents from n cluster members, which is
    what merge() does: merge_common() once per member into the same rec."""
    m = merger()
    a = rec(equivalent=[])
    for i in range(n):
        m.merge_common(a, rec(equivalent=[{"id": f"e{i}-{j}"}
                                          for j in range(per_member)]))
    return a


def test_every_members_equivalents_land():
    a = _merge_n(50)
    assert len(a["equivalent"]) == 200


def test_the_dedup_collections_are_sets():
    """Not a timing assertion -- those are too noisy to gate a build on --
    but the thing the timing depended on. If someone turns one back into a
    list, the membership test goes from O(1) to O(everything merged so far).

    Note what this does NOT fix: the set is still rebuilt from the
    accumulated list on every call, so merging a cluster is still quadratic
    in its size. Measured against the previous implementation the change is
    ~1.4x, not a change of complexity. Making it linear means carrying the
    dedup sets across merge_common() calls for the life of one cluster
    merge, which is a larger change than this one.
    """
    import inspect

    from pipeline.process import merger as mod

    src = inspect.getsource(mod.RecordMerger.merge_common)
    assert 'ids = {x["id"] for x in rec[rp] if "id" in x}' in src
    assert 'ids = [x["id"] for x in rec[rp] if "id" in x]' not in src
    assert "conts = {" in src and "conts = [" not in src
    assert "rec_aps = set()" in src
    assert "curr = set()" in src
    # the any() has to stay a generator so it stops at the first match
    assert 'any("AI generated" in x for x in conts)' in src
