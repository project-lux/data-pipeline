"""The Getty vocabularies' replaced ids: <data_dir>/aat_replacement_uris.json,
optional, read by the reference manager (reconcile redirects references) and
the Getty fetcher (fetches the replacement). A fresh installation has no
such file, and `lux reconcile` once failed in every worker with
FileNotFoundError. Before 2026-11 the two read separate copies,
replacements.json and getty_replacements.json."""

import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.reference_manager import REDIRECTS_FILE, load_redirects
from lux_pipeline.sources.getty.fetcher import GettyFetcher

ROWS = [("http://vocab.getty.edu/aat/300281837", "http://vocab.getty.edu/aat/300140808"),
        ("http://vocab.getty.edu/ulan/500396271", "http://vocab.getty.edu/ulan/500353823")]


def write(path, rows=ROWS):
    path.write_text(json.dumps({
        "head": {"vars": ["from", "to"]},
        "results": {"bindings": [{"from": {"type": "uri", "value": f}, "to": {"type": "uri", "value": t}}
                                 for f, t in rows]}}))


def test_the_file_is_named_for_what_it_holds():
    assert REDIRECTS_FILE == "aat_replacement_uris.json"


def test_without_the_file_nothing_is_redirected(tmp_path, caplog):
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert load_redirects(str(tmp_path)) == {}
    assert "aat_replacement_uris.json: references to replaced Getty ids are not redirected" in caplog.text


def test_the_sparql_result_maps_old_to_new(tmp_path):
    write(tmp_path / "aat_replacement_uris.json")
    assert load_redirects(str(tmp_path)) == dict(ROWS)


def test_an_old_name_is_read_with_a_warning_to_rename_it(tmp_path, caplog):
    write(tmp_path / "getty_replacements.json", ROWS[:1])
    write(tmp_path / "replacements.json")
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert load_redirects(str(tmp_path)) == dict(ROWS)
    assert "replacements.json: rename it to" in caplog.text
    # the new name wins once it is there
    write(tmp_path / "aat_replacement_uris.json", ROWS[1:])
    assert load_redirects(str(tmp_path)) == dict(ROWS[1:])


def fetcher(tmp_path):
    cfgs = SimpleNamespace(data_dir=str(tmp_path), allow_network=False)
    return GettyFetcher({"name": "aat", "fetch": "https://data.getty.edu/vocab/aat/{identifier}",
                         "all_configs": cfgs})


def test_the_getty_fetcher_reads_the_same_file(tmp_path):
    write(tmp_path / "aat_replacement_uris.json")
    assert fetcher(tmp_path).redirects == {"aat/300281837": "aat/300140808",
                                           "ulan/500396271": "ulan/500353823"}


def test_the_getty_fetcher_without_the_file_is_quiet(tmp_path, caplog):
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        assert fetcher(tmp_path).redirects == {}
    assert caplog.text == ""
