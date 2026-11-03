"""`select_sources` (lux_pipeline/process/phases/reconcile.py): the source
selection run-reconcile.py did from argv, on a stub config -- --all, named
sources, --recid, the smallest-first order, and --baseline's global terms."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.phases.reconcile import select_sources


class Cache:
    def __init__(self, n):
        self.n = n

    def len_estimate(self):
        return self.n


class Lang:
    def __init__(self, ident):
        self.id = ident


class Mapper:
    process_langs = {"en": Lang("http://vocab.getty.edu/aat/300388277"),
                     "fr": Lang("http://vocab.getty.edu/aat/300388306")}


class StubConfigs:
    def __init__(self, aat_size=50):
        self.internal = {"ycba": {"name": "ycba", "datacache": Cache(300)},
                         "ypm": {"name": "ypm", "datacache": Cache(100)},
                         "ils": {"name": "ils", "datacache": Cache(200)}}
        self.external = {"aat": {"name": "aat", "datacache": Cache(aat_size), "mapper": Mapper()},
                         "wikidata": {"name": "wikidata", "datacache": Cache(5000)}}
        self.globals_cfg = {"primary_name": "300404670", "english": "300388277",
                            "human": "Q5", "answer": "Q42"}


def names_of(to_do):
    return [t[0] for t in to_do]


def test_all_internal_sorted_smallest_datacache_first():
    cfgs = StubConfigs()
    to_do = select_sources(cfgs, all_internal=True)
    assert names_of(to_do) == ["ypm", "ils", "ycba"]
    assert [t[1] for t in to_do] == [cfgs.internal["ypm"], cfgs.internal["ils"], cfgs.internal["ycba"]]
    assert [t[2] for t in to_do] == [[], [], []]


def test_all_internal_ignores_names():
    assert names_of(select_sources(StubConfigs(), names=["aat", "nope"], all_internal=True)) == \
        ["ypm", "ils", "ycba"]


def test_named_internal_and_external_sources():
    to_do = select_sources(StubConfigs(), names=["ycba", "aat"])
    assert names_of(to_do) == ["aat", "ycba"]


def test_ties_keep_the_config_order():
    cfgs = StubConfigs(aat_size=300)
    assert names_of(select_sources(cfgs, names=["aat", "ycba"])) == ["ycba", "aat"]


def test_unknown_source_is_rejected():
    with pytest.raises(ValueError, match="Unknown source: nope"):
        select_sources(StubConfigs(), names=["ycba", "nope"])


def test_no_source_is_rejected():
    with pytest.raises(ValueError, match="No source selected"):
        select_sources(StubConfigs())
    with pytest.raises(ValueError, match="No source selected"):
        select_sources(StubConfigs(), names=[])


def test_recids_go_to_the_one_selected_source():
    to_do = select_sources(StubConfigs(), names=["aat"], recids=("300010001", "300010002"))
    assert to_do == [["aat", StubConfigs().external["aat"] | to_do[0][1], ["300010001", "300010002"]]]


def test_recids_with_two_sources_are_rejected():
    with pytest.raises(ValueError, match="single source"):
        select_sources(StubConfigs(), names=["aat", "ycba"], recids=["300010001"])
    with pytest.raises(ValueError, match="single source"):
        select_sources(StubConfigs(), all_internal=True, recids=["obj/1"])


def test_recids_with_all_and_one_internal_source():
    cfgs = StubConfigs()
    cfgs.internal = {"ycba": cfgs.internal["ycba"]}
    assert select_sources(cfgs, all_internal=True, recids=["obj/1"]) == [["ycba", cfgs.internal["ycba"], ["obj/1"]]]


# --- baseline ----------------------------------------------------------------


def test_baseline_is_the_aat_and_wikidata_globals():
    cfgs = StubConfigs(aat_size=99999)
    to_do = select_sources(cfgs, baseline=True)
    # not sorted: aat first although its datacache is the bigger one
    assert names_of(to_do) == ["aat", "wikidata"]
    assert to_do[0][1] is cfgs.external["aat"]
    # the 3... globals, then the languages with the aat namespace stripped
    assert to_do[0][2] == ["300404670", "300388277", "300388277", "300388306"]
    assert to_do[1][2] == ["Q5", "Q42"]


def test_baseline_without_wikidata_globals_is_aat_only():
    cfgs = StubConfigs()
    cfgs.globals_cfg = {"primary_name": "300404670"}
    to_do = select_sources(cfgs, baseline=True)
    assert names_of(to_do) == ["aat"]
    assert to_do[0][2] == ["300404670", "300388277", "300388306"]


def test_baseline_ignores_names_and_recids():
    to_do = select_sources(StubConfigs(), names=["ycba"], recids=["obj/1"], baseline=True)
    assert names_of(to_do) == ["aat", "wikidata"]
    assert "obj/1" not in to_do[0][2]
