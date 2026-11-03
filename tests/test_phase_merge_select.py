"""`read_update_token` and `select_sources` (lux_pipeline/process/phases/
merge.py): the token check and the source selection run-merge.py did from
argv, on a stub config -- --all, named sources in config order, --onlyrefs,
and --recid needing exactly one source."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.phases.merge import read_update_token, select_sources


class StubConfigs:
    def __init__(self, tmp_path=None):
        self.internal = {"ycba": {"name": "ycba"}, "ypm": {"name": "ypm"}, "ils": {"name": "ils"}}
        self.external = {"aat": {"name": "aat"}, "wikidata": {"name": "wikidata"}}
        self.data_dir = str(tmp_path) if tmp_path is not None else None


def names_of(to_do):
    return [t[0] for t in to_do]


# --- the token ---------------------------------------------------------------


def test_a_well_formed_token_is_read_and_stripped(tmp_path):
    (tmp_path / "idmap_update_token.txt").write_text("  __20261027a__\n")
    assert read_update_token(StubConfigs(tmp_path)) == "__20261027a__"


@pytest.mark.parametrize("token", ["20261027", "__20261027", "20261027__", "", "  \n"])
def test_a_badly_formed_token_is_rejected(tmp_path, token):
    (tmp_path / "idmap_update_token.txt").write_text(token)
    with pytest.raises(ValueError, match="badly formed"):
        read_update_token(StubConfigs(tmp_path))


def test_a_missing_token_file_raises_as_open_does(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_update_token(StubConfigs(tmp_path))


# --- the sources -------------------------------------------------------------


def test_all_internal_in_config_order():
    """Not sorted by size: merge takes the units in the order configured."""
    cfgs = StubConfigs()
    to_do = select_sources(cfgs, all_internal=True)
    assert to_do == list(cfgs.internal.items())
    assert names_of(to_do) == ["ycba", "ypm", "ils"]


def test_all_internal_ignores_names_and_only_refs():
    assert names_of(select_sources(StubConfigs(), names=["aat", "nope"], all_internal=True)) == \
        ["ycba", "ypm", "ils"]
    assert names_of(select_sources(StubConfigs(), all_internal=True, only_refs=True)) == \
        ["ycba", "ypm", "ils"]


def test_named_sources_come_back_in_config_order_internal_first():
    cfgs = StubConfigs()
    to_do = select_sources(cfgs, names=["aat", "ils", "ycba"])
    assert names_of(to_do) == ["ycba", "ils", "aat"]
    assert to_do[2] == ("aat", cfgs.external["aat"])


def test_only_refs_is_no_source_at_all():
    assert select_sources(StubConfigs(), only_refs=True) == []
    assert select_sources(StubConfigs(), names=["ycba"], only_refs=True) == []


def test_unknown_source_is_rejected():
    with pytest.raises(ValueError, match="Unknown source: nope"):
        select_sources(StubConfigs(), names=["ycba", "nope"])


def test_no_source_is_rejected():
    with pytest.raises(ValueError, match="No source selected"):
        select_sources(StubConfigs())
    with pytest.raises(ValueError, match="No source selected"):
        select_sources(StubConfigs(), names=[])


def test_recids_need_exactly_one_source():
    cfgs = StubConfigs()
    assert select_sources(cfgs, names=["ycba"], recids=["obj/1"]) == [("ycba", cfgs.internal["ycba"])]
    with pytest.raises(ValueError, match="single source"):
        select_sources(cfgs, names=["ycba", "aat"], recids=["obj/1"])
    with pytest.raises(ValueError, match="single source"):
        select_sources(cfgs, all_internal=True, recids=["obj/1"])
    with pytest.raises(ValueError, match="single source"):
        select_sources(cfgs, only_refs=True, recids=["obj/1"])


def test_recids_with_all_and_one_internal_source():
    cfgs = StubConfigs()
    cfgs.internal = {"ycba": cfgs.internal["ycba"]}
    assert select_sources(cfgs, all_internal=True, recids=["obj/1"]) == [("ycba", cfgs.internal["ycba"])]
