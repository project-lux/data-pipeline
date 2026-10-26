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
processes can be spawned and report back.
"""

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

DIR_KEYS = ("data_dir", "dumps_dir", "exports_dir", "indexes_dir", "log_dir", "temp_dir", "tests_dir")


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


class Check:
    """One line per check; raising inside ``with`` is the FAIL."""

    def __init__(self, out=None):
        self.out = out or sys.stdout
        self.failed = False
        self.detail = ""

    def run(self, label, fn, required=True):
        """True to carry on: always after OK, and after a failure of a
        check that is not `required` (printed as WARN)."""
        try:
            detail = fn()
        except Exception as e:
            if required:
                self.out.write(f"FAIL  {label}: {type(e).__name__}: {e}\n")
                self.failed = True
                return False
            self.out.write(f"WARN  {label}: {type(e).__name__}: {e} (not required)\n")
            return True
        self.out.write(f"OK    {label}" + (f": {detail}" if detail else "") + "\n")
        return True


class CommandHandler(BaseHandler):
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("--skip-workers", action="store_true",
                        help="Do not spawn worker processes to check the task engine")
        ap.add_argument("--workers", type=int, default=0,
                        help="Spawn this many workers in the engine check (default: max_workers)")

    # --- the checks ----------------------------------------------------------------

    def check_dotenv(self):
        from dotenv import find_dotenv
        fn = find_dotenv(usecwd=True)
        if not fn and not os.getenv("LUX_BASEPATH"):
            raise FileNotFoundError("no .env found from the current directory upwards and LUX_BASEPATH is not set")
        return fn or "LUX_BASEPATH from the environment"

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
        cfgs = self.cfgs
        cfgs.instantiate_all()
        for group in ("internal", "external"):
            for name, cfg in getattr(cfgs, group).items():
                dc = cfg.get("datacache")
                if dc is not None and hasattr(dc, "len_estimate"):
                    return f"every source built; {name} datacache has ~{dc.len_estimate():,} records"
        return "every source built"

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

    # --- the command -------------------------------------------------------------

    def process(self, args, rest):
        super().process(args, rest)
        self.basepath = ""
        self.cfgs = None
        check = Check()
        steps = [
            (".env", self.check_dotenv),
            ("LUX_BASEPATH", self.check_basepath),
            ("configuration directory", self.check_config_dir),
            ("configuration builds", self.check_config),
            ("postgres reachable", self.check_postgres),
            ("redis reachable", self.check_redis, False),
            ("idmap reachable", self.check_idmap),
            ("globals load", self.check_globals),
            ("source components build, caches reachable", self.check_components),
            ("data directories exist and are writable", self.check_directories),
            ("validator schemas present", self.check_schemas),
        ]
        if not getattr(args, "skip_workers", False):
            steps.append(("worker processes spawn",
                          lambda: self.check_workers(args.workers or self.cfgs.max_workers)))
        for label, fn, *rest in steps:
            if not check.run(label, fn, *rest):
                print(f"Stopped at the first failure. Fix it and run lux testinstall again.")
                sys.exit(1)
        print("All checks passed.")
        return True
