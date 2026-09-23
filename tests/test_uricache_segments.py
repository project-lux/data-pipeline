"""The idmap's memory cache holds the keys that come back, not the newest.

Merge walks a slice of internal records and visits each once, so a record's
own uri -> yuid entry is inserted and never read again. Under a single LRU
those insertions evict the external members -- the aat concept, the wikidata
place -- that thousands of records all reference, which is why a 200k cache
showed almost no hit rate across 100M records.

`reusable` splits the cache in two so the one-shot keys can only evict each
other.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from pipeline.storage.uricache import URICache


def external(key):
    return key.startswith("aat:") or key.startswith("wd:")


# --- the split ---------------------------------------------------------------

def test_one_shot_keys_cannot_evict_a_reusable_one():
    """The whole point: a scan of throwaway keys leaves the useful one in."""
    c = URICache(10, reusable=external, reusable_share=0.5)
    c["aat:300404670##quaType"] = "yuid:concept/AAA"
    for i in range(1000):
        c[f"https://lux.collections.yale.edu/data/object/{i}##quaHMO"] = f"yuid:{i}"
    assert c["aat:300404670##quaType"] == "yuid:concept/AAA"


def test_without_a_predicate_it_is_the_plain_lru_it_was():
    c = URICache(3)
    for i in range(4):
        c[f"k{i}"] = str(i)
    assert c.get("k0") is URICache.missing
    assert c["k3"] == "3"
    assert len(c) == 3


def test_each_segment_evicts_within_itself():
    c = URICache(10, reusable=external, reusable_share=0.5)   # 5 hot, 5 cold
    for i in range(8):
        c[f"aat:{i}"] = str(i)
    for i in range(8):
        c[f"http://internal/{i}"] = str(i)
    s = c.stats()
    assert s["hot"] == 5 and s["cold"] == 5
    assert c.get("aat:0") is URICache.missing        # evicted by aat:5..7
    assert c["aat:7"] == "7"
    assert c.get("http://internal/0") is URICache.missing
    assert c["http://internal/7"] == "7"


def test_capacity_is_still_the_total():
    c = URICache(100, reusable=external, reusable_share=0.9)
    assert c.hot_capacity == 90
    assert c.capacity == 10
    for i in range(500):
        c[f"aat:{i}"] = "x"
        c[f"http://internal/{i}"] = "x"
    assert len(c) == 100


def test_a_tiny_capacity_still_gives_both_segments_room():
    """int(1 * 0.9) is 0, and a zero-capacity segment would drop every key
    written to it."""
    c = URICache(1, reusable=external)
    assert c.hot_capacity >= 1 and c.capacity >= 1
    c["aat:1"] = "a"
    c["http://internal/1"] = "b"
    assert c["aat:1"] == "a"
    assert c["http://internal/1"] == "b"


# --- behaviour that must not have changed -----------------------------------

def test_a_key_keeps_the_segment_it_landed_in():
    """reusable() is a prediction; re-running it on every write to the same
    key would let a rewrite silently move an entry mid-life."""
    c = URICache(10, reusable=lambda k: k == "sometimes")
    c["sometimes"] = "a"
    assert c.hot and not c.cache
    c["sometimes"] = "b"
    assert c["sometimes"] == "b"
    assert len(c.hot) == 1 and not c.cache


def test_none_is_still_a_cacheable_value():
    """canonicalize() caches its negatives, so missing has to be distinct
    from a stored None -- in both segments."""
    c = URICache(10, reusable=external)
    c["aat:1"] = None
    c["http://internal/1"] = None
    assert c["aat:1"] is None
    assert c["http://internal/1"] is None
    assert c.get("aat:2") is URICache.missing


def test_delete_and_clear_reach_both_segments():
    c = URICache(10, reusable=external)
    c["aat:1"] = "a"
    c["http://internal/1"] = "b"
    del c["aat:1"]
    assert "aat:1" not in c
    assert "http://internal/1" in c
    c.clear()
    assert len(c) == 0 and "http://internal/1" not in c


def test_deleting_an_absent_key_is_not_an_error():
    c = URICache(10, reusable=external)
    del c["aat:nope"]


def test_zero_capacity_is_still_refused():
    with pytest.raises(ValueError):
        URICache(0, reusable=external)


def test_stats_separate_hot_hits_from_the_rest():
    """The number that says whether the predicate is right about which keys
    come back."""
    c = URICache(10, reusable=external)
    c["aat:1"] = "a"
    c["http://internal/1"] = "b"
    c.get("aat:1")
    c.get("http://internal/1")
    c.get("aat:missing")
    s = c.stats()
    assert s["hot_hits"] == 1
    assert s["hits"] == 2
    assert s["misses"] == 1
    assert s["hit_rate"] == pytest.approx(2 / 3)


# --- the idmap's own predicate ----------------------------------------------

class Configs:
    internal_uri = "https://lux.collections.yale.edu/data/"
    external = {
        "aat": {"name": "aat", "namespace": "http://vocab.getty.edu/aat/"},
        "wd": {"name": "wd", "namespace": "http://www.wikidata.org/entity/"},
    }


def _idmap():
    from pipeline.storage.idmap.postgres import IdMap
    m = object.__new__(IdMap)
    m.configs = Configs()
    m.prefix_map_out = {"yuid": Configs.internal_uri}
    for cf in Configs.external.values():
        m.prefix_map_out[cf["name"]] = cf["namespace"]
    m._external_prefixes = frozenset(p for p in m.prefix_map_out if p != "yuid")
    return m


@pytest.mark.parametrize("key,reusable", [
    ("aat:300404670##quaType", True),         # external member: reused
    ("wd:Q42##quaPerson", True),
    ("yuid:person/abc", False),               # a cluster's member set
    ("https://linked-art.ycba.internal/object/1##quaHMO", False),  # internal
    ("", False),
    (":leading", False),
    ("nosuchprefix:1", False),
])
def test_the_idmap_predicate_picks_out_external_members(key, reusable):
    assert _idmap()._reusable_key(key) is reusable


def test_yuid_is_not_treated_as_an_external_prefix():
    """It is in prefix_map_out like the others, and a YUID's member set is
    read once per record and then never again."""
    m = _idmap()
    assert "yuid" in m.prefix_map_out
    assert "yuid" not in m._external_prefixes
