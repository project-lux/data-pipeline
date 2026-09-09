"""The tuning script's arithmetic, and the parts of it that could do harm.

Two things are worth pinning. The sizing formulas are anchored on the box in
docs/idmap-migration.md, whose figures were arrived at by measurement rather
than by formula -- if a change to the formulas moves those, it needs to be a
decision rather than a side effect. And a couple of the recommendations could
break a server if they were wrong in the right way: replacing
shared_preload_libraries instead of appending to it silently unloads whatever
else was there.
"""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location("pg_tune", ROOT / "pg-tune.py")
pg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pg)

GB, MB = pg.GB, pg.MB


def machine(ram_gb=128, cpus=32, storage="nvme"):
    return {"cpus": {"logical": cpus, "physical": cpus // 2, "limited_by": None},
            "ram": {"total": ram_gb * GB, "limited_by": None},
            "storage": {"kind": storage, "why": "test"}}


def recs(m=None, srv=None, workers=24):
    out = {}
    for _title, group in pg.recommend(m or machine(), srv, workers):
        for r in group:
            out[r.name] = r
    return out


def server(**settings):
    """A pg_settings snapshot: name -> (setting, unit)."""
    return {"version_num": 150000, "version": "PostgreSQL 15.0",
            "settings": {n: {"setting": v, "unit": u, "context": "sighup",
                             "source": "default"}
                         for n, (v, u) in settings.items()},
            "ref_tables": {}, "total_bytes": 0, "data_dir": None}


# --- units ------------------------------------------------------------------

def test_byte_formatting_round_trips():
    for text in ("32GB", "96GB", "2GB", "64MB", "32MB"):
        assert pg.pg_bytes(pg.parse_bytes(text)) == text


def test_pg_settings_units_are_normalised():
    """shared_buffers comes back in 8kB pages, not bytes."""
    srv = server(shared_buffers=("4194304", "8kB"))
    rec = pg.Rec("shared_buffers", "32GB", "", kind="bytes")
    norm, text = pg.current_value(srv, rec)
    assert norm == 32 * GB
    assert text == "32GB"


def test_time_units_are_normalised():
    srv = server(checkpoint_timeout=("300", "s"))
    rec = pg.Rec("checkpoint_timeout", "30min", "", kind="ms")
    norm, _ = pg.current_value(srv, rec)
    assert norm == 300_000
    assert pg.wanted_value(rec) == 1_800_000


# --- the anchor -------------------------------------------------------------

def test_the_documented_box_reproduces_the_documented_figures():
    """128 GB / 32 vCPU, the box docs/idmap-migration.md was measured on."""
    r = recs(machine(ram_gb=128, cpus=32))
    assert r["shared_buffers"].value == "32GB"
    assert r["effective_cache_size"].value == "96GB"
    assert r["work_mem"].value == "32MB"
    assert r["maintenance_work_mem"].value == "2GB"
    assert r["max_wal_size"].value == "32GB"
    assert r["min_wal_size"].value == "2GB"


def test_the_production_box_scales_down():
    """80 GB / 32 vCPU, the production server.

    shared_buffers matches the doc's 20GB. effective_cache_size comes out at
    75% where the doc wrote 70% (56GB) -- the doc is inconsistent with its own
    dev-box figure, and this is a planner hint rather than an allocation, so
    anything in the 50-75% band is defensible. Pinned so a change to the
    formula is a decision rather than a side effect."""
    r = recs(machine(ram_gb=80, cpus=32))
    assert r["shared_buffers"].value == "20GB"
    assert r["effective_cache_size"].value == "60GB"


def test_max_connections_covers_four_per_worker():
    """Not a tuning knob: 48 workers x 4 exceeded a max_connections of 200."""
    r = recs(workers=48)
    assert int(r["max_connections"].value) >= 48 * 4
    assert r["max_connections"].restart is True


def test_a_tiny_box_still_gets_sane_floors():
    r = recs(machine(ram_gb=2, cpus=2))
    assert pg.parse_bytes(r["shared_buffers"].value) >= 128 * MB
    assert pg.parse_bytes(r["max_wal_size"].value) >= 4 * GB


# --- storage ----------------------------------------------------------------

def test_spinning_disks_keep_the_seek_costs():
    r = recs(machine(storage="rotational"))
    assert r["random_page_cost"].value == "4.0"
    assert r["effective_io_concurrency"].value == "2"


def test_flash_does_not():
    assert recs(machine(storage="nvme"))["random_page_cost"].value == "1.0"
    assert recs(machine(storage="ssd"))["random_page_cost"].value == "1.1"


# --- the ones that could do harm -------------------------------------------

def test_shared_preload_libraries_is_appended_to_not_replaced():
    """Replacing the list would silently unload whatever else was there."""
    srv = server(shared_preload_libraries=("pg_cron,auto_explain", ""))
    r = recs(srv=srv)
    value = r["shared_preload_libraries"].value
    assert "pg_cron" in value and "auto_explain" in value
    assert "pg_stat_statements" in value


def test_shared_preload_libraries_is_left_alone_when_already_loaded():
    srv = server(shared_preload_libraries=("pg_stat_statements", ""))
    assert "shared_preload_libraries" not in recs(srv=srv)


def test_parallel_query_is_turned_off_for_the_build():
    """N worker processes already saturate the box; per-query parallelism on
    top oversubscribes it."""
    assert recs()["max_parallel_workers_per_gather"].value == "0"


def test_wal_compression_respects_the_server_version():
    old = dict(server()); old["version_num"] = 140000
    assert recs(srv=old)["wal_compression"].value == "on"
    assert recs(srv=server())["wal_compression"].value == "zstd"


def test_maintenance_io_concurrency_is_skipped_before_pg13():
    old = dict(server()); old["version_num"] = 120000
    assert "maintenance_io_concurrency" not in recs(srv=old)


def test_statements_are_valid_shaped_sql():
    for name, rec in recs().items():
        st = rec.statement()
        assert st.startswith(f"ALTER SYSTEM SET {name} = ")
        assert st.endswith(";")
        if rec.kind in ("bytes", "ms") or rec.quote:
            assert "'" in st, f"{name} should be quoted: {st}"


def test_the_fourth_connection_is_dropped_when_the_server_is_already_async():
    """The reference queues share the idmap's connection when their session
    setting matches the server's -- see pipeline/storage/idmap/postgres.py's
    _refs_connection()."""
    off = server(synchronous_commit=("off", ""))
    on = server(synchronous_commit=("on", ""))
    assert int(recs(srv=off, workers=48)["max_connections"].value) < \
           int(recs(srv=on, workers=48)["max_connections"].value)
    # 48 x 3 + 20 = 164 -> 175; 48 x 4 + 20 = 212 -> 225
    assert recs(srv=off, workers=48)["max_connections"].value == "175"
    assert recs(srv=on, workers=48)["max_connections"].value == "225"


def test_an_uninspected_server_assumes_the_fourth_connection():
    """Over-provisioning connections is safe; under-provisioning is a build
    that dies partway through."""
    assert recs(srv=None, workers=24)["max_connections"].value == "125"
