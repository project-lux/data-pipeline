"""The Getty redirects the reference manager applies: <data_dir>/replacements.json,
optional. A fresh installation has no such file, and `lux reconcile` failed
in every worker with FileNotFoundError."""

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.reference_manager import load_redirects


def test_without_the_file_nothing_is_redirected(tmp_path, caplog):
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert load_redirects(str(tmp_path)) == {}
    assert "replacements.json: references to replaced Getty ids are not redirected" in caplog.text


def test_the_sparql_result_maps_old_to_new(tmp_path):
    rows = [("http://vocab.getty.edu/aat/300281837", "http://vocab.getty.edu/aat/300140808"),
            ("http://vocab.getty.edu/ulan/500396271", "http://vocab.getty.edu/ulan/500353823")]
    (tmp_path / "replacements.json").write_text(json.dumps({
        "head": {"vars": ["from", "to"]},
        "results": {"bindings": [{"from": {"type": "uri", "value": f}, "to": {"type": "uri", "value": t}}
                                 for f, t in rows]}}))
    assert load_redirects(str(tmp_path)) == dict(rows)
