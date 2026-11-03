"""The library mapper opens the ils headings index on first use, not in
__init__, and copes with an ils record that names no index loader."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.sources.yale.library.mapper import YulMapper


def make(tmp_path, index_loader):
    ils = {"name": "ils", "indexLoader": index_loader}
    cfgs = SimpleNamespace(internal={"ils": ils}, external={}, results={"merged": {}}, data_dir=str(tmp_path),
                           globals={}, get_idmap=lambda: {})
    return YulMapper({"name": "ils", "all_configs": cfgs, "namespace": "https://linked-art.library.yale.edu/node/"})


def test_constructs_without_an_index_loader_and_warns_on_first_use(tmp_path, caplog):
    m = make(tmp_path, None)
    assert m._headings_index is None
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        assert m.headings_index == {}
        assert m.headings_index == {}
    assert caplog.text.count("no headings index") == 1


def test_index_loader_is_asked_once_on_first_use(tmp_path):
    calls = []

    class Ldr:
        def load_index(self):
            calls.append(1)
            return {"h1": ["x"]}

    m = make(tmp_path, Ldr())
    assert calls == []
    assert m.headings_index == {"h1": ["x"]} and m.headings_index["h1"] == ["x"]
    assert calls == [1]
    m.headings_index = {"other": 1}
    assert m.headings_index == {"other": 1}


def test_loader_without_a_headings_path_gives_an_empty_index(tmp_path, caplog):
    class Ldr:
        def load_index(self):
            return None

    m = make(tmp_path, Ldr())
    with caplog.at_level("WARNING", logger="lux_pipeline"):
        assert m.headings_index == {}
    assert "no headings index" in caplog.text
