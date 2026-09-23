"""process_entity's top-level equivalent block, over a whole cluster.

`all_equivs` is every member of the record's YUID class -- 122,058 of them
for the largest cluster in production -- so everything in that block runs
once per member, per record. It had three things that could not afford it:
two full passes over the list, a method call per member for a one-line
split, and a membership test against a list. py-spy after the merge_common
fix: process_entity 33% OwnTime (the scan), the listcomps 17%, split_qua
17%.

These pin the output, not the speed. The rewrite reorders a filter around a
split, which is only safe because splitting at ##qua cannot change whether a
string starts with __ -- so that property gets its own test.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest


def old_way(all_equivs, record, split_qua):
    """The implementation being replaced, for differential testing."""
    all_equivs = [split_qua(x)[0] for x in all_equivs]
    all_equivs = [x for x in all_equivs if not x.startswith("__")]
    my_equivs = [x["id"] for x in record.get("equivalent", [])]
    out = list(record.get("equivalent", []))
    if set(all_equivs) != set(my_equivs):
        lbl = record.get("_label", "")
        out = list(record.get("equivalent", []))
        added = []
        for eq in all_equivs:
            if not eq in my_equivs:
                added.append({"id": eq, "type": record["type"], "_label": lbl})
        return added, True
    return out, False


def new_way(all_equivs, record):
    all_equivs = [x.split("##qua", 1)[0] for x in all_equivs
                  if not x.startswith("__")]
    my_equivs = {x["id"] for x in record.get("equivalent", [])}
    if set(all_equivs) != my_equivs:
        lbl = record.get("_label", "")
        added = []
        for eq in all_equivs:
            if not eq in my_equivs:
                added.append({"id": eq, "type": record["type"], "_label": lbl})
        return added, True
    return list(record.get("equivalent", [])), False


def split_qua(recid):
    """pipeline.config.Config.split_qua"""
    return recid.split("##qua")


CASES = [
    # (cluster members, the record's own equivalents)
    (["aat:1##quaType", "wd:Q2##quaPerson"], []),
    (["aat:1##quaType", "__20260921__"], [{"id": "aat:1"}]),
    (["aat:1##quaType", "aat:2##quaType"], [{"id": "aat:1"}, {"id": "aat:2"}]),
    (["aat:1##quaType"], [{"id": "aat:1"}, {"id": "aat:9"}]),
    ([], [{"id": "aat:1"}]),
    (["aat:1"], []),                                  # no qua at all
    (["__token__##quaType"], []),                     # token that also has a qua
    (["aat:1##quaType##quaType"], []),                # split more than once
    (["x:1##quaType", "x:1##quaPerson"], []),         # same id, two quas
]


@pytest.mark.parametrize("members,equivs", CASES)
def test_new_matches_old_exactly(members, equivs):
    rec = {"type": "Type", "_label": "L", "equivalent": list(equivs)}
    assert new_way(members, dict(rec)) == old_way(members, dict(rec), split_qua)


def test_filtering_before_the_split_is_the_same_answer():
    """The reordering the rewrite depends on: ##qua is never a prefix, so
    the split can only remove a suffix, never expose or hide a leading __."""
    for s in ["__tok__", "__tok__##quaType", "aat:1", "aat:1##quaType",
              "a__b##quaType", "##quaType", ""]:
        assert s.startswith("__") == s.split("##qua", 1)[0].startswith("__")


def test_split_with_maxsplit_keeps_the_first_field():
    """split_qua() used an unbounded split and took [0]; maxsplit=1 has to
    give the same first element while allocating one less."""
    for s in ["a##quab", "a##quab##quac", "a", "", "##quab"]:
        assert s.split("##qua")[0] == s.split("##qua", 1)[0]


def test_two_quas_of_one_uri_yield_a_duplicate_id_as_before():
    """Stripping the qua is a formatting step -- the published `equivalent`
    wants http://vocab.getty.edu/aat/300123456, not the internal key -- so
    two members differing only in qua reduce to the same id and are both
    appended. Nothing is merged by this; it is one id listed twice in one
    record's equivalent array.

    It should not arise: the qua is part of the identity key, so a Type and
    a Person are separate clusters under separate YUIDs and cannot share an
    all_equivs. If it ever does, that is identify putting two types in one
    cluster, and this duplicate is the symptom. Pinned only so the rewrite
    is not what changed it."""
    added, changed = new_way(["x:1##quaType", "x:1##quaPerson"],
                             {"type": "Type", "equivalent": []})
    assert changed
    assert [a["id"] for a in added] == ["x:1", "x:1"]


def test_order_of_added_equivalents_follows_the_cluster():
    members = ["c:3##quaType", "c:1##quaType", "c:2##quaType"]
    added, _ = new_way(members, {"type": "Type", "equivalent": []})
    assert [a["id"] for a in added] == ["c:3", "c:1", "c:2"]


def test_unchanged_cluster_keeps_the_records_own_list():
    equivs = [{"id": "aat:1", "_label": "keep me"}]
    out, changed = new_way(["aat:1##quaType"], {"type": "Type",
                                                "equivalent": list(equivs)})
    assert not changed
    assert out == equivs


def test_the_source_has_no_scan_and_no_split_qua_call():
    import inspect

    from pipeline.process.reidentifier import Reidentifier

    src = inspect.getsource(Reidentifier.process_entity)
    assert 'my_equivs = {x["id"] for x in record.get("equivalent", [])}' in src
    assert 'my_equivs = [x["id"]' not in src
    assert "self.configs.split_qua(x)[0] for x in all_equivs" not in src
    assert 'x.split("##qua", 1)[0] for x in all_equivs' in src


# --- capping the equivalents of an oversized cluster -------------------------
#
# `yuid:person/bcd070a7-...` had 88,447 members, so every member record was
# being handed an `equivalent` array of 88,447 entries. External equivalents
# are never capped -- a hundred wikidata or aat equivalents would be a
# different problem, and they are the ones worth having.

INT_NS = ("https://europeana.linked.art/data/", "https://ycba.example/")


def capper(limit=100):
    from pipeline.process.reidentifier import Reidentifier
    r = object.__new__(Reidentifier)
    r.internal_ns = INT_NS
    r.MAX_CLUSTER_EQUIVS = limit
    return r


def internals(n, ns=INT_NS[0]):
    return [f"{ns}person/p{i:06d}" for i in range(n)]


EXTERNALS = ["http://vocab.getty.edu/aat/300404670",
             "http://www.wikidata.org/entity/Q42",
             "http://viaf.org/viaf/113230702"]


def test_the_default_cap_is_100():
    from pipeline.process.reidentifier import Reidentifier
    assert Reidentifier.MAX_CLUSTER_EQUIVS == 100


def test_a_normal_cluster_is_returned_unchanged():
    got = internals(30) + EXTERNALS
    assert capper()._cap_equivs(list(got)) == got


def test_internal_members_are_capped():
    out = capper(10)._cap_equivs(internals(5000))
    assert len(out) == 10


def test_externals_are_never_capped():
    """Even when the list as a whole is way over the limit."""
    out = capper(10)._cap_equivs(internals(5000) + EXTERNALS)
    assert set(EXTERNALS) <= set(out)
    assert len(out) == 10 + len(EXTERNALS)


def test_a_cluster_of_externals_alone_is_untouched():
    """200 aat equivalents is a different problem, and not this one."""
    many = [f"http://vocab.getty.edu/aat/{300000000+i}" for i in range(200)]
    assert capper(10)._cap_equivs(list(many)) == many


def test_both_internal_namespaces_count_as_internal():
    out = capper(10)._cap_equivs(internals(500) + internals(500, INT_NS[1]))
    assert len(out) == 10


def test_the_cut_is_deterministic():
    """Two workers building the same cluster must advertise the same
    equivalents. _lookup() hands back a set, whose order varies between
    processes, so the cap has to impose an order of its own."""
    import random
    base = internals(500) + EXTERNALS
    first = capper(10)._cap_equivs(list(base))
    for seed in range(5):
        shuffled = list(base)
        random.Random(seed).shuffle(shuffled)
        got = capper(10)._cap_equivs(shuffled)
        assert sorted(got) == sorted(first)


def test_nothing_is_invented():
    out = capper(10)._cap_equivs(internals(5000) + EXTERNALS)
    assert set(out) <= set(internals(5000) + EXTERNALS)
    assert len(set(out)) == len(out)          # no duplicates
