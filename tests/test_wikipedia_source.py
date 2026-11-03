"""The wikipedia source: a JSON-lines loader keyed by article title and
a mapper that produces nothing."""

import gzip
import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.config import importObject
from lux_pipeline.process.base.loader import Loader
from lux_pipeline.sources.wikipedia.loader import WpLoader
from lux_pipeline.sources.wikipedia.mapper import WpMapper

ROOT = Path(__file__).parent.parent


def test_loader_keys_articles_by_title_and_drops_untitled(tmp_path):
    d = tmp_path / "wikipedia"
    d.mkdir()
    lines = [{"title": "Alba Fucens", "text": "A Roman town."}, {"text": "no title"},
             {"title": "", "text": "empty title"}, {"title": "Athens", "text": "A city."}]
    with gzip.open(d / "article-texts.json.gz", "wt") as fh:
        fh.write("\n".join(json.dumps(l) for l in lines) + "\n")
    ldr = WpLoader({"name": "wikipedia", "datacache": {}, "dumps_dir": "wikipedia",
                    "all_configs": SimpleNamespace(dumps_dir=str(tmp_path), temp_dir=str(tmp_path)),
                    "input_files": {"records": [{"path": "article-texts.json.gz", "type": "lines.gz/json"}]}})
    ldr.prepare(0, 1)
    assert ldr.uses_dsl is True
    assert ldr.process() == 2
    assert set(ldr.out_cache) == {"Alba Fucens", "Athens"}
    assert ldr.out_cache["Athens"]["text"] == "A city."
    assert WpLoader.load is Loader.load


def test_class_names_the_repository_record_uses_resolve():
    assert importObject("sources.wikipedia.loader.Loader") is WpLoader
    assert importObject("sources.wikipedia.loader.WpLoader") is WpLoader
    assert importObject("sources.wikipedia.mapper.WpMapper") is WpMapper


def test_mapper_produces_nothing():
    m = WpMapper.__new__(WpMapper)
    assert m.transform({"identifier": "Athens", "data": {"title": "Athens"}}, "Place") is None


def test_sample_parses_and_resolves():
    d = json.load(open(ROOT / "docs" / "sample_config" / "wikipedia.json"))
    for k, v in d.items():
        if k.endswith("Class") and v:
            assert importObject(v) is not None
    assert d["input_files"]["records"][0]["type"] == "lines.gz/json"
