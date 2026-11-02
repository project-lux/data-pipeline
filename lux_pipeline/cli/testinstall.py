"""Check an installation step by step: .env, the configuration, the databases, the directories, the workers.

    cd /srv/lux && lux testinstall
    lux testinstall --skip-workers           # everything but the spawn check

Each check prints one line, OK or FAIL, and the command stops at the
first failure with a non-zero exit status: .env found; LUX_BASEPATH set;
the configuration directory exists; the Config builds; Postgres answers
with the cache defaults' settings; Redis answers wherever a map record
still names it (a warning only: the pipeline no longer needs Redis);
the idmap is reachable; the globals load; every source's components build and the
first datacache answers; every data directory exists and is writable;
the validator's schema files are present; ``max_workers`` worker
processes can be spawned and report back. Last, Postgres's settings are
compared with what ``lux pg-tune`` recommends for this machine and
``max_workers``: a warning naming the settings that differ, never a
failure (``--skip-pg-tune`` leaves it out).
"""

import io
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

from lux_pipeline import runtime
from lux_pipeline.process.tasks._task_manager import TaskManager
from lux_pipeline.progress import read_status, status_path

from . import _services
from ._handler import BaseHandler

logger = logging.getLogger("lux_pipeline")

# not backup_dir: `lux backup` creates it, and its default (under data_dir)
# need not exist before the first backup
DIR_KEYS = ("data_dir", "dumps_dir", "exports_dir", "nt_dir", "indexes_dir", "log_dir", "temp_dir",
            "tests_dir")


def schema_names(validator):
    from lux_pipeline.process.validator import schema_file_names
    return schema_file_names()


class SpawnCheckManager(TaskManager):
    """Each worker reports done and returns its number; module-level so
    the spawn context can pickle it, and needs no configuration."""

    needs_configs = False

    def _distributed(self, n, cfgs, progress):
        progress(phase="testinstall", total=1, completed=1)
        return n


class Advice(Exception):
    """Raised by a check to say WARN with its message: something to act
    on, not a failure, whether or not the check is required."""


class Check:
    """Runs one check at a time: a line per check written to `out`, and
    the same as data in `results`, ``[{"label", "state": "OK" | "WARN" |
    "FAIL", "detail", "required"}]``. A check fails by raising."""

    def __init__(self, out=None, on_result=None):
        self.out = out or sys.stdout
        self.failed = False
        self.detail = ""
        self.results = []
        # called with each result as it is recorded (the web UI streams them)
        self.on_result = on_result

    def record(self, label, state, detail="", required=True, line=None):
        result = {"label": label, "state": state, "detail": detail, "required": required}
        self.results.append(result)
        if self.on_result is not None:
            self.on_result(result)
        if state == "FAIL":
            self.failed = True
        if line is None:
            line = f"{state:<5} {label}" + (f": {detail}" if detail else "")
        self.out.write(line + "\n")

    def run(self, label, fn, required=True):
        """True to carry on: always after OK, and after a failure of a
        check that is not `required` (printed as WARN)."""
        try:
            detail = fn()
        except Advice as e:
            self.record(label, "WARN", str(e), required)
            return True
        except Exception as e:
            if required:
                self.record(label, "FAIL", f"{type(e).__name__}: {e}", required)
                return False
            self.record(label, "WARN", f"{type(e).__name__}: {e}", required,
                        line=f"WARN  {label}: {type(e).__name__}: {e} (not required)")
            return True
        self.record(label, "OK", detail or "", required)
        return True


class CommandHandler(BaseHandler):
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("--skip-workers", action="store_true",
                        help="Do not spawn worker processes to check the task engine")
        ap.add_argument("--workers", type=int, default=0,
                        help="Spawn this many workers in the engine check, and size the "
                             "Postgres check for them (default: max_workers)")
        ap.add_argument("--skip-pg-tune", dest="skip_pg_tune", action="store_true",
                        help="Do not compare Postgres's settings with lux pg-tune's")

    # --- the checks ----------------------------------------------------------------

    def check_dotenv(self):
        if os.getenv("LUX_ENV_FILE"):
            return f"{os.environ['LUX_ENV_FILE']} (--env)"
        fns = runtime.find_dotenvs()
        if not fns and not os.getenv("LUX_BASEPATH"):
            raise FileNotFoundError("no ./.env or ~/.lux/.env found and LUX_BASEPATH is not set")
        return ", ".join(fns) or "LUX_BASEPATH from the environment"

    def check_basepath(self):
        bp = runtime.get_basepath()
        if not bp:
            raise ValueError("LUX_BASEPATH is not set in .env or the environment")
        self.basepath = bp
        return bp

    def check_config_dir(self):
        cc = Path(self.basepath) / "config_cache"
        if not cc.is_dir():
            raise FileNotFoundError(f"{cc} does not exist")
        if not any((cc / f"{n}.json").exists() for n in ("system", "base")):
            raise FileNotFoundError(f"{cc} has no system.json or base.json")
        return str(cc)

    def check_config(self):
        cfgs = getattr(self, "given_configs", None)
        if cfgs is None:
            runtime.reset_configs()
            cfgs = runtime.get_configs(instantiate=False, basepath=self.basepath)
        self.cfgs = cfgs
        return (f"{len(cfgs.internal)} internal, {len(cfgs.external)} external, "
                f"{len(cfgs.results)} results, {len(cfgs.map_stores)} maps, max_workers {cfgs.max_workers}")

    def check_postgres(self):
        settings = _services.postgres_settings(self.cfgs.defaults.get("cache") or {})
        return _services.probe_postgres(settings)

    def check_redis(self):
        targets = _services.redis_targets(self.cfgs.map_stores.values(), self.cfgs.defaults.get("map") or {})
        if not targets:
            return "not used by this configuration"
        return "; ".join(_services.probe_redis(host, port) for host, port in targets)

    def check_idmap(self):
        idmap = self.cfgs.get_idmap()
        n = len(idmap)
        return f"{type(idmap).__module__}.{type(idmap).__name__}, {n:,} entries"

    def check_globals(self):
        self.cfgs.cache_globals()
        return f"{len(self.cfgs.globals)} globals"

    def check_components(self):
        """Build every source and MarkLogic store, as instantiate_all()
        does, but carry on past a failure so one run names every source
        that cannot be built, not just the first."""
        cfgs = self.cfgs
        failures = []
        built = 0
        for group in ("internal", "external", "results"):
            for name in list(getattr(cfgs, group)):
                try:
                    cfgs.instantiate(name, group)
                    built += 1
                except Exception as e:
                    failures.append(f"{name}: {type(e).__name__}: {e}")
        for name in list(cfgs.marklogic_stores):
            try:
                cfgs.instantiate_ml(name)
            except Exception as e:
                failures.append(f"marklogic {name}: {type(e).__name__}: {e}")
        if failures:
            raise RuntimeError(f"{len(failures)} of {built + len(failures)} failed to build -- "
                               + "; ".join(failures))
        for group in ("internal", "external"):
            for name, cfg in getattr(cfgs, group).items():
                dc = cfg.get("datacache")
                if dc is not None and hasattr(dc, "len_estimate"):
                    return f"{built} sources built; {name} datacache has ~{dc.len_estimate():,} records"
        return f"{built} sources built"

    def check_directories(self):
        missing, unwritable = [], []
        for key in DIR_KEYS:
            path = getattr(self.cfgs, key, None)
            if not path:
                continue
            if not os.path.isdir(path):
                missing.append(f"{key}={path}")
            elif not os.access(path, os.W_OK):
                unwritable.append(f"{key}={path}")
        if missing or unwritable:
            raise FileNotFoundError("missing: " + ", ".join(missing) + ("; not writable: " + ", ".join(unwritable) if unwritable else ""))
        return ", ".join(k for k in DIR_KEYS if getattr(self.cfgs, k, None))

    def check_schemas(self):
        v = getattr(self.cfgs, "validator", None)
        if v is None:
            return "no validatorClass configured"
        if not hasattr(v, "missing"):
            return f"{type(v).__name__}"
        missing = v.missing()
        if missing:
            from lux_pipeline.process.validator import SCHEMA_BASE_URL
            raise FileNotFoundError(f"{', '.join(missing)} missing from {v.schema_dir} "
                                    f"(fetch from {SCHEMA_BASE_URL}, or rerun lux initialize)")
        v.load()
        return f"{len(schema_names(v))} schemas under {v.schema_dir}"

    def check_workers(self, n):
        prefix = os.path.join(tempfile.mkdtemp(prefix="testinstall-", dir=getattr(self.cfgs, "temp_dir", None) or None),
                              "spawn")
        start = time.time()
        m = SpawnCheckManager(None, max_workers=n)
        results = m.process("mp", prefix, None)
        if results != list(range(n)):
            raise RuntimeError(f"expected {n} workers to report, got {results}")
        for i in range(n):
            st = read_status(status_path(prefix, i))
            if not st or not st.get("done"):
                raise RuntimeError(f"worker {i} wrote no status file under {prefix}")
        return f"{n} workers spawned and reported in {time.time() - start:.1f}s"

    def check_pg_tune(self, workers=0):
        """Postgres's settings against `lux pg-tune`'s recommendations for
        this machine and `workers` (default max_workers). Settings that
        differ are an `Advice`; a server that cannot be read is an error,
        which the step reports as a warning."""
        from lux_pipeline.storage import pg_tune
        from .pg_tune import pipeline_settings
        db, max_workers, table = pipeline_settings(self.cfgs)
        workers = workers or max_workers
        m, srv, why = pg_tune.analyse(db, idmap_table=table)
        if srv is None:
            raise RuntimeError(f"server not inspected: {why}")
        pending = pg_tune.pending_changes(m, srv, workers)
        if pending is None:
            raise RuntimeError("could not size anything without a memory figure")
        if pending:
            shown = ", ".join(f"{name} ({cur} -> {want})" if cur is not None else name
                              for name, cur, want in pending[:6])
            more = f" and {len(pending) - 6} more" if len(pending) > 6 else ""
            raise Advice(f"{len(pending)} setting{'s' if len(pending) != 1 else ''} differ "
                         f"from the recommendation for {workers} workers: {shown}{more}. "
                         f"lux pg-tune prints the SQL")
        return f"{srv['version']}: every setting as recommended for {workers} workers"

    # --- the command -------------------------------------------------------------

    def checks(self, args):
        """``[(label, check, required, needs)]`` in the order they run.
        `needs` is what the check cannot run without: ``"basepath"`` (an
        earlier check found LUX_BASEPATH), ``"configs"`` (the configuration
        built), or None. A check that is not `required` reports a failure
        as WARN."""
        workers = getattr(args, "workers", 0) or 0
        steps = [
            (".env", self.check_dotenv, True, None),
            ("LUX_BASEPATH", self.check_basepath, True, None),
            ("configuration directory", self.check_config_dir, True, "basepath"),
            ("configuration builds", self.check_config, True, "basepath"),
            ("postgres reachable", self.check_postgres, True, "configs"),
            ("redis reachable", self.check_redis, False, "configs"),
            ("idmap reachable", self.check_idmap, True, "configs"),
            ("globals load", self.check_globals, True, "configs"),
            ("source components build, caches reachable", self.check_components, True, "configs"),
            ("data directories exist and are writable", self.check_directories, True, "configs"),
            ("validator schemas present", self.check_schemas, True, "configs"),
        ]
        if not getattr(args, "skip_workers", False):
            steps.append(("worker processes spawn",
                          lambda: self.check_workers(workers or self.cfgs.max_workers), True,
                          "configs"))
        if not getattr(args, "skip_pg_tune", False):
            steps.append(("postgres tuned for the build",
                          lambda: self.check_pg_tune(workers), False, "configs"))
        return steps

    def run_checks(self, args, out=None, stop_on_fail=True, cfgs=None, on_result=None):
        """Run the checks and return their results, ``[{"label", "state",
        "detail", "required"}]`` (`Check.results`), writing a line per
        check to `out` (None: nowhere).

        `stop_on_fail` stops at the first failure, as the command does;
        without it every check gets a row, and one that needs what an
        earlier failure left missing reports that instead of running.
        `cfgs`, when given, is the configuration to check (the web UI's
        own) instead of one built afresh through `lux_pipeline.runtime`.
        `on_result` is called with each result as it is recorded."""
        self.basepath = ""
        self.cfgs = None
        self.given_configs = cfgs
        check = Check(out if out is not None else io.StringIO(), on_result)
        missing = {"basepath": "LUX_BASEPATH was not found",
                   "configs": "the configuration did not build"}
        for label, fn, required, needs in self.checks(args):
            if (needs == "basepath" and not self.basepath) or \
                    (needs == "configs" and self.cfgs is None):
                ok = not required
                check.record(label, "FAIL" if required else "WARN", missing[needs], required)
            else:
                ok = check.run(label, fn, required)
            if not ok and stop_on_fail:
                break
        return check.results

    def process(self, args, rest):
        super().process(args, rest)
        results = self.run_checks(args, out=sys.stdout, stop_on_fail=True)
        if any(r["state"] == "FAIL" for r in results):
            print(f"Stopped at the first failure. Fix it and run lux testinstall again.")
            sys.exit(1)
        print("All checks passed.")
        return True
