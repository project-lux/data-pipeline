"""Create or complete an installation: the data directories, a configuration, the .env, the schemas, the database.

    lux initialize /srv/lux                       # the sample configuration shipped in docs/sample_config
    lux initialize /srv/lux --configs ~/pipeline-configs   # a local pipeline-configs checkout
    lux initialize /srv/lux --clone               # git clone project-lux/pipeline-configs into it first
    lux initialize /srv/lux                       # again, later: finishes whatever is not done yet

Every step checks its own state, so the command can be run again after a
failure (Postgres not up yet, no network for the schemas, a wrong
setting) and carries on from where it got to; nothing already in place
is overwritten, and your edits to the installed records are kept.

With the shipped samples, ``<dir>/config_cache/`` holds a copy of every
sample record (missing ones are added on a rerun), ``base_dir`` is set
to ``<dir>``, and ``.env`` points ``LUX_BASEPATH`` at ``<dir>``. With a
pipeline-configs checkout, its ``configs/`` tree is copied (or cloned) to
``<dir>/pipeline-configs/`` the first time and reused after,
``system.json``'s ``base_dir`` is set to ``<dir>``, each sub-config
store's relative ``base_dir`` becomes ``../subs`` (what item 35 resolves
against the configuration directory), and ``.env`` points at
``<dir>/pipeline-configs/configs/system``. A rerun with no flags finds
whichever is installed. Every ``*_dir`` the base record names is created
under ``<dir>``, plus ``logs/flags``. The sample ``caches.json``
placeholders (``USERNAME``, ``DBNAME``) become the current user and
``PASSWORD`` empty, the local-socket defaults of a development machine.
The Linked Art JSON schemas the validator reads are fetched into
``<data_dir>/schema/`` unless ``--no-schemas`` is given; only missing
files are fetched.

Postgres and Redis are then probed with the installed settings and
reported; only Postgres is required (Redis is probed because some map
records still name it, and reported as not required). When Postgres answers and the role may create databases, you
are asked for the name of the database (default ``lux``, or the one
already configured) and whether to create it (``--dbname`` to give the
name up front; ``--create-db`` answers yes without asking, for a script;
``--no-create-db`` never asks): the cache defaults are pointed at it, the
idmap record is switched to the Postgres store, a dated
``idmap_update_token.txt`` is written, and the idmap's empty tables are
created. Each of those is skipped when already done, and a database
already configured is only verified. Finish with ``lux testinstall``.
"""

import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import requests

from lux_pipeline.config import Config
from lux_pipeline.process.validator import SCHEMA_BASE_URL, schema_file_names

from . import _services
from ._handler import BaseHandler

logger = logging.getLogger("lux_pipeline")
USER_AGENT = "lux-pipeline/0.1 (+https://github.com/project-lux/data-pipeline)"

SAMPLES = Path(__file__).resolve().parent.parent.parent / "docs" / "sample_config"
PIPELINE_CONFIGS_URL = "https://github.com/project-lux/pipeline-configs.git"
POSTGRES_IDMAP = "storage.idmap.postgres.IdMap"
DIR_KEYS = ("data_dir", "dumps_dir", "exports_dir", "nt_dir", "indexes_dir", "log_dir", "temp_dir",
            "tests_dir")


def read_json(path):
    with open(path) as fh:
        return json.load(fh)


def write_json(path, data):
    with open(path, "w") as fh:
        json.dump(data, fh, indent=4, ensure_ascii=False)
        fh.write("\n")


def find_base_record(config_cache):
    """``(path, record)`` of the ``system`` or ``base`` record in a config_cache."""
    for name in ("system", "base"):
        p = config_cache / f"{name}.json"
        if p.exists():
            return p, read_json(p)
    raise ValueError(f"No system.json or base.json in {config_cache}")


def make_directories(root, base):
    """Every ``*_dir`` the base record names, resolved under `root` when
    relative, plus ``logs/flags`` beside the log dir; ``(created,
    existing)`` paths."""
    created, existing = [], []
    for key in DIR_KEYS:
        value = base.get(key)
        if not value:
            continue
        p = Path(value) if os.path.isabs(value) else root / value
        (existing if p.is_dir() else created).append(p)
        p.mkdir(parents=True, exist_ok=True)
        if key == "log_dir":
            (p / "flags").mkdir(exist_ok=True)
    return created, existing


def dotenv_basepath(env_file):
    """The LUX_BASEPATH an existing .env sets, or ``""``."""
    try:
        for line in Path(env_file).read_text().splitlines():
            if line.strip().startswith("LUX_BASEPATH"):
                value = line.split("=", 1)[1].strip().strip("'\"")
                return value
    except OSError:
        pass
    return ""


def record_file(cfgs, predicate):
    """The file of the first configuration record `predicate` accepts,
    in the main directory or any sub-config store; ``(path, record)`` or
    ``(None, None)``."""
    dirs = [Path(cfgs.config_dir) / "config_cache"]
    for store in cfgs.subconfig_stores.values():
        dc = store.get("datacache")
        if dc is not None and getattr(dc, "directory", None):
            dirs.append(Path(dc.directory))
    for d in dirs:
        for fn in sorted(d.glob("*.json")):
            try:
                rec = read_json(fn)
            except ValueError:
                continue
            if isinstance(rec, dict) and predicate(rec):
                return fn, rec
    return None, None


def probe_services(cfgs):
    """``[(label, ok, detail)]`` for Postgres (the cache defaults) and,
    wherever a map record names it, Redis."""
    out = []
    pg = _services.postgres_settings(cfgs.defaults.get("cache") or {})
    try:
        out.append(("postgres", True, _services.probe_postgres(pg)))
    except Exception as e:
        out.append(("postgres", False, f"{_services.describe_postgres(pg)}: {type(e).__name__}: {str(e).strip()}"))
    targets = _services.redis_targets(cfgs.map_stores.values(), cfgs.defaults.get("map") or {})
    if not targets:
        out.append(("redis", True, "not used by this configuration"))
    for host, port in targets:
        try:
            out.append(("redis", True, _services.probe_redis(host, port)))
        except Exception as e:
            out.append(("redis", False, f"{host}:{port}: {type(e).__name__}: {str(e).strip()}"))
    return out


def ask_name(question, default):
    """A name from the keyboard, `default` when empty or when no person
    is there."""
    if not sys.stdin.isatty():
        return default
    answer = input(f"{question} [{default}]: ").strip()
    return answer or default


def ask(question, assume=None):
    """``assume`` True/False decides without asking; otherwise ask when a
    person is at the keyboard, and answer no when not."""
    if assume is not None:
        return assume
    if not sys.stdin.isatty():
        return False
    return input(f"{question} [y/N] ").strip().lower() in ("y", "yes")


DEFAULT_DBNAME = "lux"


def setup_database(basepath, cfgs, name=None, assume=None, out=print):
    """Create (or adopt) Postgres database `name` for this installation and
    configure an empty idmap in it, asking first; every part is skipped
    when already done, so a rerun only completes and verifies. With no
    `name`, a person at the keyboard is asked for one (default ``lux``,
    or the database already configured); a script gets ``lux``."""
    pg = _services.postgres_settings(cfgs.defaults.get("cache") or {})

    cache_fn, cache_rec = record_file(cfgs, lambda r: r.get("type") == "caches" or
                                      (r.get("type") == "default" and r.get("name") == "cache"))
    idmap_name = getattr(cfgs, "idmap_name", "idmap")
    map_fn, map_rec = record_file(cfgs, lambda r: r.get("type") == "map" and r.get("name") == idmap_name)
    token_fn = Path(cfgs.data_dir) / "idmap_update_token.txt"
    if cache_fn is None:
        raise ValueError("no cache defaults record (caches.json or default_cache.json) to point at the database")
    if map_fn is None:
        raise ValueError(f"no map record named {idmap_name!r} to configure")

    if not name:
        # the token is written only when a database was set up here (or by
        # a real build), so with it the cache defaults name that database
        configured = cache_rec.get("dbname") if (map_rec.get("storeClass") == POSTGRES_IDMAP
                                                 and token_fn.exists()) else None
        default = configured or DEFAULT_DBNAME
        name = default if assume is not None else ask_name("Name of the Postgres database for the pipeline", default)
    exists, can_create = _services.postgres_database_status(pg, name)

    todo = []
    if not exists:
        todo.append("create the database")
    if cache_rec.get("dbname") != name:
        todo.append("point the cache defaults at it")
    if map_rec.get("storeClass") != POSTGRES_IDMAP:
        todo.append("switch the idmap to Postgres")
    if not token_fn.exists():
        todo.append("write the update token")

    if not exists and not can_create:
        out(f"  database: '{name}' does not exist and {pg['user']} may not create databases; "
            f"create it, or set dbname in {cache_fn.name} to one that exists, and rerun")
        return False
    if todo:
        verb = "Use the existing" if exists else "Create a"
        if not ask(f"{verb} Postgres database '{name}' for the pipeline and set up an empty idmap in it?", assume):
            out(f"  database: skipped (rerun with --create-db to {'adopt' if exists else 'create'} '{name}')")
            return False
        if not exists:
            _services.create_database(pg, name)
            out(f"  database: created '{name}'")
        else:
            out(f"  database: using existing '{name}'")
    else:
        out(f"  database: '{name}' already configured")

    if cache_rec.get("dbname") != name:
        cache_rec["dbname"] = name
        cache_rec["user"] = pg["user"]
        cache_rec["password"] = pg["password"]
        write_json(cache_fn, cache_rec)
        out(f"  cache defaults: {cache_fn.name} -> dbname {name}")
    if map_rec.get("storeClass") != POSTGRES_IDMAP:
        map_rec["storeClass"] = POSTGRES_IDMAP
        write_json(map_fn, map_rec)
        out(f"  idmap: {map_fn.name} -> {POSTGRES_IDMAP}")
    if not token_fn.exists():
        token_fn.parent.mkdir(parents=True, exist_ok=True)
        token_fn.write_text(f"__{date.today().strftime('%Y%m%d')}__\n")
        out(f"  update token: {token_fn}")

    # the empty tables: building the idmap creates them (IF NOT EXISTS), and
    # on a rerun this is the verification
    fresh = Config(basepath=str(basepath))
    idmap = fresh.get_idmap()
    out(f"  idmap tables: {'verified' if not todo else 'created'} in '{name}' "
        f"({type(idmap).__name__}, {len(idmap):,} entries)")
    return True


def fetch_schemas(data_dir):
    """Download the validator's schema files into ``<data_dir>/schema/``;
    ``(fetched, failed)`` file names. Files already present are kept."""
    sdir = Path(data_dir) / "schema"
    sdir.mkdir(parents=True, exist_ok=True)
    fetched, failed = [], []
    for fn in schema_file_names():
        target = sdir / fn
        if target.exists():
            continue
        try:
            r = requests.get(SCHEMA_BASE_URL + fn, timeout=(10, 60), headers={"User-Agent": USER_AGENT})
            r.raise_for_status()
            json.loads(r.text)
            target.write_text(r.text)
            fetched.append(fn)
        except Exception as e:
            logger.warning(f"could not fetch {SCHEMA_BASE_URL}{fn}: {e}")
            failed.append(fn)
    return fetched, failed


class CommandHandler(BaseHandler):
    needs_configs = False

    def add_args(self, ap):
        ap.add_argument("--no-schemas", action="store_true",
                        help="Do not fetch the Linked Art JSON schemas the validator reads")
        ap.add_argument("--dbname",
                        help="The Postgres database to create or adopt for the pipeline "
                             "(default: asked, lux; a script gets lux)")
        ap.add_argument("--create-db", action="store_true",
                        help="Create the database and the idmap without asking (for scripts)")
        ap.add_argument("--no-create-db", action="store_true",
                        help="Never offer to create the database")
        ap.add_argument("directory", nargs="?", default=".",
                        help="Where the installation lives (default: the current directory)")
        ap.add_argument("--configs", metavar="PATH",
                        help="A pipeline-configs checkout (or any directory with configs/system/config_cache) "
                             "to install from instead of the shipped samples")
        ap.add_argument("--clone", nargs="?", const=PIPELINE_CONFIGS_URL, metavar="URL",
                        help=f"git clone pipeline-configs into the directory first (default {PIPELINE_CONFIGS_URL})")

    # --- the shipped samples ---------------------------------------------------

    def install_samples(self, root, out=print):
        """``<root>/config_cache`` from docs/sample_config: created, or
        completed with any sample missing from it; records already there
        are kept as they are (your edits survive a rerun)."""
        cc = root / "config_cache"
        fresh = not cc.exists()
        cc.mkdir(parents=True, exist_ok=True)
        added = []
        for fn in sorted(SAMPLES.glob("*.json")):
            if not (cc / fn.name).exists():
                shutil.copy(fn, cc / fn.name)
                added.append(fn.name)
        if fresh:
            out(f"  configuration: created {cc} from the shipped samples ({len(added)} records)")
        elif added:
            out(f"  configuration: {cc} exists; added the missing samples {', '.join(added)}")
        else:
            out(f"  configuration: {cc} exists, kept")
        path, base = find_base_record(cc)
        if base.get("base_dir") != str(root):
            base["base_dir"] = str(root)
            write_json(path, base)
            out(f"  base_dir: set to {root} in {path.name}")
        caches = cc / "caches.json"
        if caches.exists():
            rec = read_json(caches)
            resolved = {k: v for k, v in _services.postgres_settings(rec).items() if k in ("user", "password", "dbname")}
            if any(rec.get(k) != v for k, v in resolved.items()):
                rec.update(resolved)
                write_json(caches, rec)
                out(f"  cache defaults: placeholders in caches.json -> user {rec['user']}, dbname {rec['dbname']}")
        return root, base

    # --- a pipeline-configs checkout -------------------------------------------

    def checkout_configs(self, root):
        """The installed checkout's ``configs`` directory, or None."""
        configs = root / "pipeline-configs" / "configs"
        return configs if (configs / "system" / "config_cache").is_dir() else None

    def install_checkout(self, root, source, out=print):
        source = Path(source).resolve()
        configs = source / "configs" if (source / "configs").is_dir() else source
        if not (configs / "system" / "config_cache").is_dir():
            raise ValueError(f"{source} has no configs/system/config_cache; not a pipeline-configs checkout")
        installed = self.checkout_configs(root)
        if installed is not None:
            out(f"  configuration: {installed.parent} exists, kept (not re-copied from {source})")
        elif configs.resolve() == (root / "pipeline-configs" / "configs").resolve():
            installed = configs
            out(f"  configuration: using the checkout at {installed.parent}")
        else:
            installed = root / "pipeline-configs" / "configs"
            shutil.copytree(configs, installed)
            out(f"  configuration: copied {configs} to {installed}")
        return self.wire_checkout(root, installed, out)

    def clone(self, root, url, out=print):
        installed = self.checkout_configs(root)
        if installed is not None:
            out(f"  configuration: {installed.parent} exists, kept (not cloned again)")
        else:
            target = root / "pipeline-configs"
            root.mkdir(parents=True, exist_ok=True)
            result = subprocess.run(["git", "clone", url, str(target)], capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"git clone of {url} failed: {result.stderr.strip() or result.stdout.strip()}")
            installed = target / "configs"
            out(f"  configuration: cloned {url} to {target}")
        return self.wire_checkout(root, installed, out)

    def wire_checkout(self, root, configs, out=print):
        """Point the checkout's records at this installation: the base
        record's base_dir, and each sub-config store's relative base_dir
        made relative to the system directory. Only what differs is written."""
        system_dir = configs / "system"
        cc = system_dir / "config_cache"
        path, base = find_base_record(cc)
        changed = []
        if base.get("base_dir") != str(root):
            base["base_dir"] = str(root)
            write_json(path, base)
            changed.append(f"base_dir -> {root}")
        for fn in sorted(cc.glob("*.json")):
            rec = read_json(fn)
            if rec.get("type") != "config":
                continue
            bd = rec.get("base_dir")
            if bd and not os.path.isabs(bd) and bd != "../subs" and (configs / "subs").is_dir():
                rec["base_dir"] = "../subs"
                write_json(fn, rec)
                changed.append(f"{fn.name} base_dir -> ../subs")
        out("  wiring: " + ("; ".join(changed) if changed else "already pointed at this installation"))
        return system_dir, base

    # --- the steps -----------------------------------------------------------------

    def ensure_configuration(self, root, args, out=print):
        if args.clone:
            return self.clone(root, args.clone, out)
        if args.configs:
            return self.install_checkout(root, args.configs, out)
        installed = self.checkout_configs(root)
        if installed is not None and not (root / "config_cache").is_dir():
            return self.wire_checkout(root, installed, out)
        if installed is not None:
            out(f"  note: both {installed.parent} and {root / 'config_cache'} exist; .env decides which is used")
        return self.install_samples(root, out)

    def ensure_dotenv(self, root, basepath, out=print):
        env = root / ".env"
        wanted = f"LUX_BASEPATH={basepath}\n"
        if env.exists():
            current = dotenv_basepath(env)
            if current == str(basepath):
                out(f"  .env: kept (LUX_BASEPATH={basepath})")
                return
            if current and (Path(current) / "config_cache").is_dir():
                out(f"  .env: kept; it points at {current}, not {basepath} -- edit it to switch")
                return
            out(f"  .env: rewritten (was LUX_BASEPATH={current or '<unset>'}, which has no config_cache)")
        else:
            out(f"  .env: written (LUX_BASEPATH={basepath})")
        env.write_text(wanted)

    # --- the command -------------------------------------------------------------

    def process(self, args, rest):
        super().process(args, rest)
        root = Path(args.directory).resolve()
        root.mkdir(parents=True, exist_ok=True)
        print(f"Installation at {root}")
        basepath, base = self.ensure_configuration(root, args)
        created, existing = make_directories(root, base)
        print(f"  directories: {len(created)} created, {len(existing)} existing"
              + (" (" + ", ".join(str(p.relative_to(root)) if str(p).startswith(str(root)) else str(p)
                                  for p in created) + ")" if created else ""))
        self.ensure_dotenv(root, basepath)
        data_dir = base.get("data_dir")
        if data_dir and not getattr(args, "no_schemas", False):
            data_dir = Path(data_dir) if os.path.isabs(data_dir) else root / data_dir
            present = len(schema_file_names()) - len([f for f in schema_file_names() if not (data_dir / "schema" / f).exists()])
            fetched, failed = fetch_schemas(data_dir)
            print(f"  schemas: {present} present, {len(fetched)} fetched into {data_dir / 'schema'}"
                  + (f"; FAILED: {', '.join(failed)} (rerun when {SCHEMA_BASE_URL} is reachable)" if failed else ""))
        print("Services, with the installed settings:")
        cfgs = Config(basepath=str(basepath))
        problems = []
        pg_ok = False
        for label, ok, detail in probe_services(cfgs):
            if ok:
                print(f"  {label}: OK: {detail}")
            elif label == "redis":
                print(f"  {label}: not reachable: {detail} (not required; a map record still names it)")
            else:
                print(f"  {label}: NOT REACHABLE: {detail}")
                problems.append(label)
            if ok and label == "postgres":
                pg_ok = True
        if pg_ok and not getattr(args, "no_create_db", False):
            assume = True if getattr(args, "create_db", False) else None
            try:
                setup_database(basepath, cfgs, args.dbname, assume)
            except Exception as e:
                print(f"  database: FAILED: {type(e).__name__}: {e}")
                problems.append("the database")
        print("Next:")
        if problems:
            print(f"  start or configure {' and '.join(problems)} (the cache defaults and the map records in "
                  f"{basepath}/config_cache), then run lux initialize {root} again: it carries on from here")
        print(f"  cd {root} && lux testinstall")
        return basepath
