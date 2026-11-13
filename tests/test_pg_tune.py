"""`lux pg-tune`: its arithmetic, the parts of it that could do harm, and
the command (lux_pipeline.storage.pg_tune, formerly pg-tune.py).

Two things are worth pinning. The sizing formulas are anchored on the box in
docs/idmap-migration.md, whose figures were arrived at by measurement rather
than by formula -- if a change to the formulas moves those, it needs to be a
decision rather than a side effect. And a couple of the recommendations could
break a server if they were wrong in the right way: replacing
shared_preload_libraries instead of appending to it silently unloads whatever
else was there.
"""

import sys
from argparse import Namespace
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from lux_pipeline.cli import entry
from lux_pipeline.cli import pg_tune as cli
from lux_pipeline.storage import pg_tune as pg

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


def test_shared_preload_libraries_quotes_each_library_on_its_own():
    """'a,b' in one quote is ONE library called "a,b": Postgres.app's server
    would not start after it (could not access file
    "auth_permission_dialog,pg_stat_statements")."""
    srv = server(shared_preload_libraries=("auth_permission_dialog", ""))
    stmt = recs(srv=srv)["shared_preload_libraries"].statement()
    assert stmt == ("ALTER SYSTEM SET shared_preload_libraries = "
                    "'auth_permission_dialog', 'pg_stat_statements';")
    stmt = recs(srv=server(shared_preload_libraries=("", "")))["shared_preload_libraries"].statement()
    assert stmt == "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';"


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
    setting matches the server's -- see lux_pipeline/storage/idmap/postgres.py's
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


# --- pending_changes (lux testinstall) --------------------------------------


def test_pending_changes_names_what_differs_as_alter_system_writes_it():
    m = machine()
    srv = server(max_connections=("100", None), shared_buffers=("16384", "8kB"))
    pending = {name: (cur, want) for name, cur, want in pg.pending_changes(m, srv, 24)}
    assert pending["max_connections"] == ("100", recs(m, srv)["max_connections"].value)
    want_sb = recs(m, srv)["shared_buffers"].value
    assert pending["shared_buffers"][1] == want_sb and want_sb.endswith("GB")


def test_pending_changes_leaves_out_what_is_already_set():
    m = machine()
    r = recs(m, server())
    srv = server(max_connections=(r["max_connections"].value, None))
    names = [n for n, _, _ in pg.pending_changes(m, srv, 24)]
    assert "max_connections" not in names


def test_pending_changes_needs_a_server_and_memory():
    assert pg.pending_changes(machine(), None, 24) is None
    m = machine()
    m["ram"]["total"] = 0
    assert pg.pending_changes(m, server(), 24) is None


# --- the command ------------------------------------------------------------


def run_cmd(monkeypatch, cfgs, *argv):
    from lux_pipeline import runtime
    if isinstance(cfgs, Exception):
        def get_configs(*a, **kw):
            raise cfgs
    else:
        def get_configs(*a, **kw):
            assert kw == {"instantiate": False}      # read, never instantiated
            return cfgs
    monkeypatch.setattr(runtime, "get_configs", get_configs)
    return entry.run(["--no-ui", "pg-tune", *argv])


@pytest.fixture
def no_server(monkeypatch):
    """analyse without probing this machine's database: record its arguments."""
    seen = {}

    def analyse(db, no_db=False, idmap_table="idmap", unreadable=None):
        seen.update(db=db, no_db=no_db, idmap_table=idmap_table, unreadable=unreadable)
        return machine(), None, unreadable or ("skipped (--no-db)" if no_db else "stub")
    monkeypatch.setattr(pg, "analyse", analyse)
    return seen


def test_command_sizes_from_the_configuration(monkeypatch, capsys, no_server):
    cfgs = Namespace(caches={"dbname": "lux"}, max_workers=15,
                     map_stores={"idmap": {"tableName": "idmap2"}})
    assert run_cmd(monkeypatch, cfgs) == 0
    out = capsys.readouterr().out
    assert no_server == {"db": {"dbname": "lux"}, "no_db": False, "idmap_table": "idmap2",
                         "unreadable": None}
    assert "Sized for --workers 15" in out
    assert "REINDEX (VERBOSE) TABLE CONCURRENTLY idmap2;" in out


def test_command_options_override(monkeypatch, capsys, no_server):
    cfgs = Namespace(caches={}, max_workers=15, map_stores={})
    assert run_cmd(monkeypatch, cfgs, "--workers", "40", "--no-db", "--idmap-table", "t") == 0
    assert no_server["no_db"] is True and no_server["idmap_table"] == "t"
    assert "Sized for --workers 40" in capsys.readouterr().out


def test_command_without_a_configuration_sizes_the_machine(monkeypatch, capsys, no_server):
    assert run_cmd(monkeypatch, RuntimeError("LUX_BASEPATH is not set")) == 0
    out = capsys.readouterr().out
    assert no_server["db"] is None
    assert "server       not inspected: could not read the pipeline configuration " \
           "(LUX_BASEPATH is not set)" in out
    assert "Sized for --workers 24" in out


def test_command_without_memory_exits_1(monkeypatch, capsys):
    m = machine()
    m["ram"]["total"] = 0
    monkeypatch.setattr(pg, "analyse", lambda *a, **kw: (m, None, "stub"))
    cfgs = Namespace(caches={}, max_workers=4, map_stores={})
    with pytest.raises(SystemExit) as info:
        run_cmd(monkeypatch, cfgs)
    assert info.value.code == 1
    assert "Cannot size anything without a memory figure" in capsys.readouterr().out


def test_command_is_listed():
    from lux_pipeline.cli.help import list_commands
    assert dict(list_commands())["pg-tune"].startswith("Suggest Postgres settings")


# --- --sql: the statements as files, and how to run them ----------------------

def sql_args(**kw):
    return Namespace(workers=24, all=False, idmap_table="idmap", **kw)


def statements(text):
    """The statements of a script, comments dropped; every one must end in ;
    -- a stray report line (a header, `machine`) would be glued onto the
    next statement and break it."""
    body = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    parts = [p.strip() for p in body.split(";")]
    assert parts[-1] == "", f"text after the last statement: {parts[-1]!r}"
    return [" ".join(p.split()) for p in parts[:-1]]


def test_the_files_hold_only_comments_and_statements():
    srv = server(shared_preload_libraries=("auth_permission_dialog", ""))
    srv["idmap_rels"] = {"idmap": 1, "idmap_yuid": 1}
    scripts, restart = pg.sql_scripts(machine(), srv, sql_args())
    assert set(scripts) == {pg.SETTINGS_SQL, pg.AFTER_RESTART_SQL, pg.DATABASE_SQL}
    first = statements(scripts[pg.SETTINGS_SQL])
    assert all(s.startswith("ALTER SYSTEM SET ") for s in first[:-1]) and first[-1] == "SELECT pg_reload_conf()"
    assert ("ALTER SYSTEM SET shared_preload_libraries = 'auth_permission_dialog', 'pg_stat_statements'"
            in first)
    # pg_stat_statements' own settings only after the restart that loads it
    assert not any("pg_stat_statements." in s for s in first)
    after = statements(scripts[pg.AFTER_RESTART_SQL])
    assert after[-1] == "SELECT pg_reload_conf()" and all("pg_stat_statements." in s for s in after[:-1])
    assert "shared_buffers" in restart and "shared_preload_libraries" in restart
    db = statements(scripts[pg.DATABASE_SQL])
    assert all(s.split()[0] in ("ALTER", "REINDEX", "CREATE", "SELECT") for s in db)
    assert "REINDEX (VERBOSE) TABLE CONCURRENTLY idmap" in db


def test_nothing_to_change_writes_no_settings_and_removes_an_old_one(tmp_path, monkeypatch):
    scripts, _ = pg.sql_scripts(machine(), server(), sql_args())
    paths = pg.write_sql(tmp_path / "sql", scripts)
    assert set(paths) == set(scripts)
    # later, every setting already as recommended
    monkeypatch.setattr(pg, "plan", lambda m, srv, args: ([], [], []))
    scripts, restart = pg.sql_scripts(machine(), server(), sql_args())
    assert set(scripts) == {pg.DATABASE_SQL} and restart == []
    paths = pg.write_sql(tmp_path / "sql", scripts)
    assert sorted(p.name for p in (tmp_path / "sql").iterdir()) == [pg.DATABASE_SQL]
    lines = "\n".join(pg.sql_instructions(paths, restart, {"dbname": "lux2"}, platform_name="darwin"))
    assert "1. In the pipeline's database (lux2)" in lines and "Restart" not in lines


def all_paths(d="/x"):
    return {n: f"{d}/{n}" for n in (pg.SETTINGS_SQL, pg.AFTER_RESTART_SQL, pg.DATABASE_SQL)}


def test_instructions_on_a_mac_use_postgres_apps_psql(monkeypatch):
    monkeypatch.setattr(pg.shutil, "which", lambda name: None)
    monkeypatch.setattr(pg.os.path, "exists", lambda p: p == pg.POSTGRES_APP_PSQL)
    lines = "\n".join(pg.sql_instructions(all_paths(), ["shared_buffers"], {"dbname": "lux2"},
                                          {"data_dir": "/pg/var-18"}, platform_name="darwin"))
    app = pg.POSTGRES_APP_PSQL
    assert f"1. The server settings" in lines
    assert f"{app} -v ON_ERROR_STOP=1 -d postgres -f /x/{pg.SETTINGS_SQL}" in lines
    assert "2. Restart Postgres; a reload does not apply shared_buffers:" in lines
    assert "Postgres.app: Stop, then Start" in lines and "/pg/var-18/postgresql.auto.conf" in lines
    assert f"{app} -d postgres -c 'SHOW shared_preload_libraries;'" in lines
    assert "auth_permission_dialog" in lines
    assert f"4. In the pipeline's database (lux2)" in lines
    assert f"{app} -v ON_ERROR_STOP=1 -d lux2 -f /x/{pg.DATABASE_SQL}" in lines
    assert "ONE AT A TIME" in lines


def test_instructions_on_linux_run_as_postgres_and_remote_servers_get_the_connection():
    lines = "\n".join(pg.sql_instructions(all_paths("/home/me/pg tune"), ["shared_buffers"],
                                          {"dbname": "lux"}, platform_name="linux"))
    assert f"sudo -u postgres psql -v ON_ERROR_STOP=1 -d postgres < '/home/me/pg tune/{pg.SETTINGS_SQL}'" in lines
    assert "sudo systemctl restart postgresql" in lines
    remote = "\n".join(pg.sql_instructions(all_paths(), [], {"dbname": "lux", "host": "db.example.org",
                                                              "port": 5433, "user": "admin"},
                                           platform_name="linux"))
    assert f"psql -h db.example.org -p 5433 -U admin -v ON_ERROR_STOP=1 -d lux -f /x/{pg.DATABASE_SQL}" in remote
    assert "sudo" not in remote and "-U must be a superuser" in remote


def test_command_sql_writes_the_files_and_says_how_to_run_them(monkeypatch, capsys, no_server, tmp_path):
    cfgs = Namespace(caches={}, defaults={"cache": {"dbname": "lux2"}}, max_workers=8, map_stores={})
    assert run_cmd(monkeypatch, cfgs, "--sql", str(tmp_path / "sql")) == 0
    out = capsys.readouterr().out
    # read from the cache defaults (default_cache.json), not the empty caches record
    assert no_server["db"] == {"dbname": "lux2"}
    names = sorted(p.name for p in (tmp_path / "sql").iterdir())
    assert names == [pg.SETTINGS_SQL, pg.AFTER_RESTART_SQL, pg.DATABASE_SQL]
    assert out.rstrip().endswith("and REINDEX CONCURRENTLY refuse to run inside.")
    assert f"-d lux2 -f {tmp_path / 'sql' / pg.DATABASE_SQL}" in out
