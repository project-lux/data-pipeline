"""`lux pg-maintain`: which tables each action reaches, the order the
actions run in, the guards on --rewrite, the build-in-progress refusal,
and non-Postgres stores being skipped -- against stub configurations whose
stores record what was asked of them."""

import sys
from argparse import Namespace
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.cli import pg_maintain
from lux_pipeline.cli.pg_maintain import CommandHandler


class PgCache:
    def __init__(self, label, log):
        self.label, self.log = label, log

    def optimize(self):
        self.log.append(("vacuum", self.label))

    def bloat_line(self):
        self.log.append(("bloat", self.label))
        return f"{self.label}: 1 live rows"

    def drop_time_index(self):
        self.log.append(("drop_time_index", self.label))
        return self.label.endswith("recordcache")

    def rewrite(self):
        self.log.append(("rewrite", self.label))

    def identifier_to_text(self):
        self.log.append(("identifier_text", self.label))
        return not self.label.endswith("recordcache2")


class LmdbCache:
    """Not Postgres: none of the maintenance methods."""


class IdMap:
    def __init__(self, log):
        self.log = log

    def optimize(self):
        self.log.append(("vacuum", "idmap"))

    def index_sizes(self):
        return {"idmap_pkey": 5 * 1024 ** 3, "idmap_yuid_idx": 3 * 1024 ** 3}

    def ensure_covering_indexes(self, drop_old=True):
        self.log.append(("covering", drop_old))

    def missing_summary(self):
        return 12, "__20260901__"

    @staticmethod
    def expiry_token(days, now=None):
        return f"__days{days}__"

    def expire_yuids(self, cutoff, dry_run=False):
        self.log.append(("expire", cutoff, dry_run))
        return {"yuids": 5, "members": 9, "cutoff": cutoff}


class StubConfigs:
    def __init__(self):
        self.log = []

        def caches(name, types):
            return {"name": name, **{t: PgCache(f"{name}.{t}", self.log) for t in types}}
        self.internal = {"ycba": caches("ycba", ("datacache", "recordcache", "recordcache2",
                                                "reconciledRecordcache"))}
        self.external = {"aat": caches("aat", ("datacache", "recordcache")),
                         "lcnaf": {"name": "lcnaf", "datacache": LmdbCache(),
                                   "recordcache": PgCache("lcnaf.recordcache", self.log)}}
        self.results = {"merged": caches("merged", ("recordcache", "recordcache2", "datacache"))}
        self.idmap = IdMap(self.log)
        self.map_stores = {"idmap": {"name": "idmap", "store": self.idmap},
                           "all_refs": {"name": "all_refs", "store": object()}}

    def get_idmap(self):
        return self.idmap


@pytest.fixture
def run(monkeypatch):
    refused = []
    monkeypatch.setattr(pg_maintain, "refuse_while_building",
                        lambda cfgs, cmd, force=False: refused.append((cmd, force)))

    def run(*rest, source=None, cfgs=None):
        cfgs = cfgs or StubConfigs()
        out = CommandHandler(cfgs).process(Namespace(command="pg-maintain", source=source),
                                           list(rest))
        return cfgs, out, refused
    return run


def test_vacuum_walks_the_tables_manage_data_did(run, capsys):
    cfgs, out, refused = run("--vacuum")
    # every source's datacache / recordcache / recordcache2, the results'
    # recordcache / recordcache2; reconciledRecordcache and the merged
    # datacache are not among them
    assert [x[1] for x in cfgs.log] == [
        "ycba.datacache", "ycba.recordcache", "ycba.recordcache2",
        "aat.datacache", "aat.recordcache", "lcnaf.recordcache",
        "merged.recordcache", "merged.recordcache2"]
    assert all(x[0] == "vacuum" for x in cfgs.log)
    assert "skipped, not Postgres tables: lcnaf datacache" in capsys.readouterr().out
    assert out["skipped"] == ["lcnaf datacache"]
    assert refused == [("pg-maintain", False)]


def test_optimize_is_vacuum(run):
    cfgs, out, _ = run("--optimize", source="aat")
    assert out["actions"] == ["vacuum"]
    assert cfgs.log == [("vacuum", "aat.datacache"), ("vacuum", "aat.recordcache")]


def test_source_and_cache_narrow_it(run):
    cfgs, _, _ = run("--vacuum", "--cache", "recordcache", source="internal")
    assert cfgs.log == [("vacuum", "ycba.recordcache")]
    cfgs, _, _ = run("--vacuum", "--cache", "reconciledRecordcache", source="ycba")
    assert cfgs.log == [("vacuum", "ycba.reconciledRecordcache")]


def test_maps_join_vacuum_and_bloat(run, capsys):
    cfgs, out, _ = run("--vacuum", "--bloat", "--map", "idmap,all_refs", "--cache", "datacache",
                       source="aat")
    assert cfgs.log == [("bloat", "aat.datacache"), ("vacuum", "aat.datacache"),
                        ("vacuum", "idmap")]
    text = capsys.readouterr().out
    assert "map idmap: idmap_pkey 5.0GB, idmap_yuid_idx 3.0GB" in text
    assert out["skipped"] == ["map all_refs"]


def test_actions_run_in_order(run):
    cfgs, out, _ = run("--vacuum", "--rewrite", "--bloat", "--drop-time-indexes",
                       "--identifier-text", "--idmap-indexes", "--expire-yuids", "180",
                       "--cache", "recordcache", source="aat")
    assert out["actions"] == ["bloat", "drop_time_indexes", "identifier_text", "idmap_indexes",
                              "rewrite", "expire_yuids", "vacuum"]
    assert [x[0] for x in cfgs.log] == ["bloat", "drop_time_index", "identifier_text",
                                        "covering", "rewrite", "expire", "vacuum"]


def test_expire_yuids_reports_and_deletes(run, capsys):
    cfgs, out, refused = run("--expire-yuids", "180")
    assert cfgs.log == [("expire", "__days180__", False)]
    assert out["expired"] == {"yuids": 5, "members": 9, "cutoff": "__days180__"}
    text = capsys.readouterr().out
    assert "12 YUIDs missing, the oldest since __20260901__" in text
    assert "deleted 5 YUIDs missing since before __days180__ and their 9 member rows" in text
    assert refused == [("pg-maintain", False)]


def test_expire_yuids_dry_run(run, capsys):
    cfgs, _, _ = run("--expire-yuids", "0", "--dry-run")
    assert cfgs.log == [("expire", "__days0__", True)]
    assert "would delete 5 YUIDs" in capsys.readouterr().out


def test_expire_yuids_needs_the_postgres_idmap_and_a_sane_number(run):
    cfgs = StubConfigs()
    cfgs.idmap = object()
    with pytest.raises(ValueError, match="not the Postgres one"):
        run("--expire-yuids", "30", cfgs=cfgs)
    with pytest.raises(ValueError, match="0 or more"):
        run("--expire-yuids", "-1")
    with pytest.raises(ValueError, match="--dry-run goes with --expire-yuids"):
        run("--vacuum", "--dry-run")


def test_bloat_alone_never_refuses(run, capsys):
    _, _, refused = run("--bloat")
    assert refused == []
    assert "ycba.datacache: 1 live rows" in capsys.readouterr().out


def test_force_reaches_the_refusal(run):
    _, _, refused = run("--vacuum", "--force", source="aat")
    assert refused == [("pg-maintain", True)]


def test_a_running_build_stops_it_before_anything(monkeypatch):
    def refuse(cfgs, cmd, force=False):
        raise RuntimeError("a build is running")
    monkeypatch.setattr(pg_maintain, "refuse_while_building", refuse)
    cfgs = StubConfigs()
    with pytest.raises(RuntimeError, match="a build is running"):
        CommandHandler(cfgs).process(Namespace(command="pg-maintain", source=None), ["--vacuum"])
    assert cfgs.log == []


@pytest.mark.parametrize("source,rest,match", [
    (None, ["--rewrite", "--cache", "datacache"], "name the sources"),
    ("all", ["--rewrite", "--cache", "datacache"], "name the sources"),
    ("external", ["--rewrite", "--cache", "datacache"], "name the sources"),
    ("aat", ["--rewrite"], "--rewrite needs --cache"),
])
def test_rewrite_only_takes_named_tables(run, source, rest, match):
    with pytest.raises(ValueError, match=match):
        run(*rest, source=source)


def test_rewrite_named(run):
    cfgs, _, _ = run("--rewrite", "--cache", "datacache", source="aat,ycba")
    # in the order named
    assert cfgs.log == [("rewrite", "aat.datacache"), ("rewrite", "ycba.datacache")]


def test_drop_time_indexes(run, capsys):
    cfgs, _, _ = run("--drop-time-indexes", source="aat")
    assert [x[0] for x in cfgs.log] == ["drop_time_index", "drop_time_index"]
    assert "dropped 1 insert_time index(es)" in capsys.readouterr().out


def test_idmap_indexes_and_keep_old(run):
    cfgs, _, _ = run("--idmap-indexes")
    assert cfgs.log == [("covering", True)]
    cfgs, _, _ = run("--idmap-indexes", "--keep-old-index")
    assert cfgs.log == [("covering", False)]


def test_idmap_indexes_need_the_postgres_idmap(run):
    cfgs = StubConfigs()
    cfgs.idmap = object()
    with pytest.raises(ValueError, match="not the Postgres one"):
        run("--idmap-indexes", cfgs=cfgs)


def test_nothing_to_do_and_stray_options(run):
    with pytest.raises(ValueError, match="Nothing to do"):
        run()
    with pytest.raises(ValueError, match="--keep-old-index goes with --idmap-indexes"):
        run("--vacuum", "--keep-old-index")


def test_unknown_names_fail_before_anything_runs(run):
    with pytest.raises(ValueError, match="Unknown source: nope"):
        run("--vacuum", source="aat,nope")
    with pytest.raises(ValueError, match="Unknown map: nomap"):
        run("--vacuum", "--map", "nomap")


def test_listed():
    from lux_pipeline.cli.help import list_commands
    assert dict(list_commands())["pg-maintain"].startswith("Maintain the Postgres tables")


def test_identifier_text_reaches_every_cache_with_an_identifier(run, capsys):
    cfgs, _, refused = run("--identifier-text")
    # unlike --vacuum's list: reconciledRecordcache and the results'
    # datacache have identifier columns too
    assert cfgs.log == [("identifier_text", t) for t in (
        "ycba.datacache", "ycba.recordcache", "ycba.recordcache2", "ycba.reconciledRecordcache",
        "aat.datacache", "aat.recordcache", "lcnaf.recordcache",
        "merged.datacache", "merged.recordcache", "merged.recordcache2")]
    text = capsys.readouterr().out
    assert "made 8 identifier column(s) text" in text
    assert "skipped, not Postgres tables: lcnaf datacache" in text
    assert refused == [("pg-maintain", False)]


def test_identifier_text_with_other_actions_keeps_their_tables(run):
    cfgs, _, _ = run("--identifier-text", "--vacuum", source="ycba")
    assert [x[1] for x in cfgs.log if x[0] == "identifier_text"][-1] == "ycba.reconciledRecordcache"
    assert "ycba.reconciledRecordcache" not in [x[1] for x in cfgs.log if x[0] == "vacuum"]


def test_identifier_text_narrowed_by_cache(run):
    cfgs, _, _ = run("--identifier-text", "--cache", "datacache", source="aat")
    assert cfgs.log == [("identifier_text", "aat.datacache")]
