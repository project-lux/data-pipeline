"""Create a new installation: the data directories, a configuration, and the .env that points at it.

    lux initialize /srv/lux                       # the sample configuration shipped in docs/sample_config
    lux initialize /srv/lux --configs ~/pipeline-configs   # a local pipeline-configs checkout
    lux initialize /srv/lux --clone               # git clone project-lux/pipeline-configs into it first

With the shipped samples, ``<dir>/config_cache/`` holds a copy of every
sample record with ``base_dir`` set to ``<dir>``, and ``.env`` points
``LUX_BASEPATH`` at ``<dir>``. With a pipeline-configs checkout, its
``configs/`` tree is copied (or cloned) to ``<dir>/pipeline-configs/``,
``system.json``'s ``base_dir`` is set to ``<dir>``, each sub-config store's
relative ``base_dir`` becomes ``../subs`` (what item 35 resolves against
the configuration directory), and ``.env`` points at
``<dir>/pipeline-configs/configs/system``. Either way every ``*_dir`` the
base record names is created under ``<dir>``, plus ``logs/flags``.
The Linked Art JSON schemas the validator reads are fetched into
``<data_dir>/schema/`` unless ``--no-schemas`` is given (a failure there
is reported, not fatal). Refuses to touch a directory that already holds
an installation. Finish by editing the database settings and running
``lux testinstall``.
"""

import json
import logging
import os
import shutil
import subprocess
from pathlib import Path

import requests

from lux_pipeline.process.validator import SCHEMA_BASE_URL, schema_file_names

from ._handler import BaseHandler

logger = logging.getLogger("lux_pipeline")
USER_AGENT = "lux-pipeline/0.1 (+https://github.com/project-lux/data-pipeline)"

SAMPLES = Path(__file__).resolve().parent.parent.parent / "docs" / "sample_config"
PIPELINE_CONFIGS_URL = "https://github.com/project-lux/pipeline-configs.git"
DIR_KEYS = ("data_dir", "dumps_dir", "exports_dir", "indexes_dir", "log_dir", "temp_dir", "tests_dir")


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
    relative, plus ``logs/flags`` beside the log dir; returns the paths."""
    made = []
    for key in DIR_KEYS:
        value = base.get(key)
        if not value:
            continue
        p = Path(value) if os.path.isabs(value) else root / value
        p.mkdir(parents=True, exist_ok=True)
        made.append(p)
        if key == "log_dir":
            (p / "flags").mkdir(exist_ok=True)
    return made


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
        ap.add_argument("directory", nargs="?", default=".",
                        help="Where the installation lives (default: the current directory)")
        ap.add_argument("--configs", metavar="PATH",
                        help="A pipeline-configs checkout (or any directory with configs/system/config_cache) "
                             "to install from instead of the shipped samples")
        ap.add_argument("--clone", nargs="?", const=PIPELINE_CONFIGS_URL, metavar="URL",
                        help=f"git clone pipeline-configs into the directory first (default {PIPELINE_CONFIGS_URL})")

    # --- the shipped samples ---------------------------------------------------

    def install_samples(self, root):
        cc = root / "config_cache"
        if cc.exists():
            raise ValueError(f"{cc} already exists; refusing to overwrite an installation")
        cc.mkdir(parents=True)
        for fn in sorted(SAMPLES.glob("*.json")):
            shutil.copy(fn, cc / fn.name)
        path, base = find_base_record(cc)
        base["base_dir"] = str(root)
        write_json(path, base)
        return root, base

    # --- a pipeline-configs checkout -------------------------------------------

    def install_checkout(self, root, source):
        source = Path(source).resolve()
        configs = source / "configs" if (source / "configs").is_dir() else source
        system_cc = configs / "system" / "config_cache"
        if not system_cc.is_dir():
            raise ValueError(f"{source} has no configs/system/config_cache; not a pipeline-configs checkout")
        target = root / "pipeline-configs"
        if target.exists() and target.resolve() != source.parent.resolve() and target.resolve() != source.resolve():
            raise ValueError(f"{target} already exists; refusing to overwrite an installation")
        if source.resolve() != target.resolve():
            shutil.copytree(configs, target / "configs")
        return self.wire_checkout(root, target / "configs")

    def clone(self, root, url):
        target = root / "pipeline-configs"
        if target.exists():
            raise ValueError(f"{target} already exists; refusing to overwrite an installation")
        root.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(["git", "clone", url, str(target)], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"git clone of {url} failed: {result.stderr.strip() or result.stdout.strip()}")
        return self.wire_checkout(root, target / "configs")

    def wire_checkout(self, root, configs):
        """Point the checkout's records at this installation: the base
        record's base_dir, and each sub-config store's relative base_dir
        made relative to the system directory."""
        system_dir = configs / "system"
        cc = system_dir / "config_cache"
        path, base = find_base_record(cc)
        base["base_dir"] = str(root)
        write_json(path, base)
        for fn in sorted(cc.glob("*.json")):
            rec = read_json(fn)
            if rec.get("type") != "config":
                continue
            bd = rec.get("base_dir")
            if bd and not os.path.isabs(bd) and (configs / "subs").is_dir():
                rec["base_dir"] = "../subs"
                write_json(fn, rec)
        return system_dir, base

    # --- the command -------------------------------------------------------------

    def process(self, args, rest):
        super().process(args, rest)
        root = Path(args.directory).resolve()
        if (root / ".env").exists():
            raise ValueError(f"{root / '.env'} already exists; refusing to overwrite an installation")
        root.mkdir(parents=True, exist_ok=True)
        if args.clone:
            basepath, base = self.clone(root, args.clone)
        elif args.configs:
            basepath, base = self.install_checkout(root, args.configs)
        else:
            basepath, base = self.install_samples(root)
        made = make_directories(root, base)
        (root / ".env").write_text(f"LUX_BASEPATH={basepath}\n")

        print(f"Installed at {root}")
        print(f"  configuration: {basepath}/config_cache")
        print(f"  .env: LUX_BASEPATH={basepath}")
        for p in made:
            print(f"  {p}")
        data_dir = base.get("data_dir")
        if data_dir and not getattr(args, "no_schemas", False):
            data_dir = Path(data_dir) if os.path.isabs(data_dir) else root / data_dir
            fetched, failed = fetch_schemas(data_dir)
            print(f"  schemas: {len(fetched)} fetched into {data_dir / 'schema'}"
                  + (f"; FAILED: {', '.join(failed)} (fetch them from {SCHEMA_BASE_URL})" if failed else ""))
        print("Next:")
        print(f"  edit the database settings (the cache defaults and the idmap record) in {basepath}/config_cache")
        print(f"  cd {root} && lux testinstall")
        return basepath
