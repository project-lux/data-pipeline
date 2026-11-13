"""Which tables a Postgres idmap uses: its ``tableName``, else its name.

It used to fall back to ``"idmap"`` whatever the map was called, so a second
idmap record that changed only its ``name`` (``idmap_sam``) opened the live
map's tables: ``lux counts --map idmap_sam`` reported the live map's size, and
anything run against it wrote to the live map.
"""

import logging
import sys
from argparse import Namespace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from lux_pipeline.cli import pg_tune
from lux_pipeline.storage.idmap import postgres as pgmap
from lux_pipeline.storage.idmap.postgres import IdMap

from test_idmap_indexes import Conn


@pytest.fixture
def no_db(monkeypatch):
    """IdMap.__init__ without a server: no connection, schema or statements."""
    monkeypatch.setattr(pgmap, "_connect_kwargs", lambda config, cfgs: {})
    monkeypatch.setattr(pgmap, "_connect", lambda kw: Conn())
    monkeypatch.setattr(IdMap, "_ensure_schema", lambda self: None)
    monkeypatch.setattr(IdMap, "_prepare", lambda self: None)


def make(tmp_path, **record):
    (tmp_path / "idmap_update_token.txt").write_text("__20261112__\n")
    cfgs = Namespace(internal_uri="https://lux.collections.yale.edu/data/",
                     external={}, data_dir=str(tmp_path))
    return IdMap({"all_configs": cfgs, **record})


def test_the_table_is_the_maps_name_without_a_table_name(tmp_path, no_db):
    m = make(tmp_path, name="idmap_sam")
    assert (m.table, m.yuid_table) == ("idmap_sam", "idmap_sam_yuid")


def test_the_live_idmap_keeps_its_tables(tmp_path, no_db):
    """map_idmap.json sets no tableName and is called idmap."""
    m = make(tmp_path, name="idmap")
    assert (m.table, m.yuid_table) == ("idmap", "idmap_yuid")


def test_a_table_name_wins(tmp_path, no_db):
    m = make(tmp_path, name="idmap_sam", tableName="idmap")
    assert m.table == "idmap"
    assert not m._table_from_name


def test_a_new_table_named_for_the_map_warns_when_idmap_exists(caplog):
    """An installation whose idmap record has another name and no tableName
    was using 'idmap'; it now starts empty, and says why."""
    m = object.__new__(IdMap)
    m.table, m.yuid_table, m._table_from_name = "lux_ids", "lux_ids_yuid", True
    # to_regclass(lux_ids): missing; to_regclass('idmap'): there
    m.conn = Conn(answers=[(None,), ("idmap",)])
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        m._ensure_schema()
    assert "'lux_ids'" in caplog.text
    assert '"tableName": "idmap"' in caplog.text


def test_no_warning_when_the_table_name_was_given(caplog):
    m = object.__new__(IdMap)
    m.table, m.yuid_table, m._table_from_name = "idmap_sam", "idmap_sam_yuid", False
    m.conn = Conn(answers=[(None,), ("idmap",)])
    with caplog.at_level(logging.WARNING, logger="lux_pipeline"):
        m._ensure_schema()
    assert "tableName" not in caplog.text
    # and did not look for 'idmap'
    assert not any("to_regclass('idmap')" in q for q in m.conn.executed)


def test_pg_tune_sizes_from_the_maps_name():
    cfgs = Namespace(caches={}, max_workers=4,
                     map_stores={"idmap": {"name": "idmap_sam"}})
    assert pg_tune.pipeline_settings(cfgs)[2] == "idmap_sam"
