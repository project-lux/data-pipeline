"""`ArtifactCache`: the artifacts directory and its manifest (docs/artifacts.md).

``artifacts_dir`` holds every artifact's files and ``manifest.json``, one
entry per artifact made at least once::

    {"getty_replacement_uris": {
        "generator": "lux_pipeline.artifacts.getty.ReplacementUris",
        "generated_at": "2026-11-18T14:02:11+00:00", "seconds": 4.2,
        "files": [{"path": "getty_replacement_uris.json", "size": 8226003,
                   "md5": "...", "rows": 40606}],
        "code": {"commit": "...", "dirty": false, "version": "0.1.0"},
        "config": {"commit": "...", "dirty": false, "path": "...",
                   "record_sha256": "..."}}}

`ArtifactCache` is the only thing that writes there, and every write
(`set`, which `make` ends with) updates the artifact's entry in the same
call, so the manifest cannot drift from the files. Readers go through
`path`, or `Config.artifact_path`, which adds the fallback to a file still
in ``data_dir`` and a warning when an artifact is stale.

An artifact is stale (`stale`) when it was never made, a file is missing,
it is older than its record's ``maxAgeDays``, or its record or generator
class changed since it was made. A new code commit alone does not make it
stale.
"""

import datetime
import fcntl
import functools
import hashlib
import importlib.metadata
import json
import logging
import os
import shutil
import subprocess
import tempfile
import time

from lux_pipeline.config import importObject
from lux_pipeline.progress import null_progress

from .base import ArtifactError

logger = logging.getLogger("lux_pipeline")

MANIFEST = "manifest.json"
DEFAULT_MAX_SHRINK = 0.5
# the repository lux_pipeline was imported from (lux_pipeline/artifacts/cache.py)
CODE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_warned = set()


def warn_once(message):
    """Log `message` as a warning once per process: several mappers in each
    of many workers read the same artifact."""
    if message not in _warned:
        _warned.add(message)
        logger.warning(message)


def file_problem(rel):
    """Why `rel` cannot be one of an artifact's files (absolute, empty, or
    leaving the artifacts directory), or None."""
    if not isinstance(rel, str) or not rel.strip():
        return f"{rel!r} is not a file name"
    if os.path.isabs(rel):
        return f"{rel} is absolute; files are relative to artifacts_dir"
    norm = os.path.normpath(rel)
    if norm == "." or norm.startswith("..") or norm == MANIFEST or rel.endswith("/"):
        return f"{rel} is not a file inside artifacts_dir"
    return None


def record_sha256(record):
    """SHA-256 of a configuration record as read: what the manifest compares
    to tell that the record changed since the artifact was made."""
    data = {k: v for k, v in record.items() if k != "all_configs"}
    text = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def md5_file(path):
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _git(directory, *args):
    """``git -C directory args`` output, stripped; None when git fails or
    `directory` is not in a checkout."""
    try:
        out = subprocess.run(["git", "-C", directory, *args], capture_output=True, text=True,
                             timeout=30)
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    return out.stdout.strip()


@functools.lru_cache(maxsize=1)
def code_version():
    """``{"commit", "dirty", "version"}`` of the code making an artifact:
    the commit of the checkout lux_pipeline was imported from (None when it
    was not imported from the top of one: an installed copy), whether a
    tracked file differs from it, and the installed package's version."""
    commit = dirty = None
    top = _git(CODE_ROOT, "rev-parse", "--show-toplevel")
    if top and os.path.realpath(top) == os.path.realpath(CODE_ROOT):
        commit = _git(CODE_ROOT, "rev-parse", "HEAD")
        status = _git(CODE_ROOT, "status", "--porcelain", "--untracked-files=no")
        dirty = None if status is None else bool(status)
    try:
        version = importlib.metadata.version("lux-pipeline")
    except importlib.metadata.PackageNotFoundError:
        version = None
    return {"commit": commit, "dirty": dirty, "version": version}


def config_version(path, record):
    """``{"commit", "dirty", "path", "record_sha256"}`` for an artifact's
    record read from `path`: the commit of the checkout the file is in and
    whether that one file differs from it (None for both outside a
    checkout, as the build machine's configuration copies are)."""
    commit = dirty = None
    if path:
        directory = os.path.dirname(path)
        commit = _git(directory, "rev-parse", "HEAD")
        if commit:
            status = _git(directory, "status", "--porcelain", "--", os.path.basename(path))
            dirty = None if status is None else bool(status)
    return {"commit": commit, "dirty": dirty, "path": path, "record_sha256": record_sha256(record)}


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def _parse_time(text):
    try:
        t = datetime.datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    return t if t.tzinfo else t.replace(tzinfo=datetime.timezone.utc)


def _move(src, dest):
    """Move `src` to `dest`, replacing it atomically: a rename on the same
    filesystem, else a copy beside `dest` then a rename."""
    try:
        os.replace(src, dest)
    except OSError:
        tmp = f"{dest}.tmp-{os.getpid()}"
        shutil.copyfile(src, tmp)
        os.replace(tmp, dest)
        os.remove(src)


class ArtifactCache:
    """The artifacts of configuration `cfgs`: its ``artifacts`` records and
    the files and manifest in its ``artifacts_dir``. Building one reads and
    creates nothing."""

    def __init__(self, cfgs, directory=None):
        self.configs = cfgs
        self.directory = directory or cfgs.artifacts_dir
        self.manifest_path = os.path.join(self.directory, MANIFEST)

    # --- records and the manifest -----------------------------------------------------

    @property
    def records(self):
        return getattr(self.configs, "artifacts", {}) or {}

    def record(self, name):
        rec = self.records.get(name)
        if rec is None:
            raise ArtifactError(f"no artifact record {name!r} in the configuration")
        return rec

    def entries(self, strict=False):
        """The manifest: ``{name: entry}``; ``{}`` before anything was made.
        A manifest that is not JSON is an `ArtifactError` when `strict`,
        else a warning and ``{}``."""
        try:
            with open(self.manifest_path, encoding="utf-8") as fh:
                data = json.load(fh)
        except FileNotFoundError:
            return {}
        except (OSError, ValueError) as e:
            if strict:
                raise ArtifactError(f"{self.manifest_path} cannot be read: {e}")
            warn_once(f"{self.manifest_path} cannot be read ({e}); every artifact counts as never made")
            return {}
        if not isinstance(data, dict):
            if strict:
                raise ArtifactError(f"{self.manifest_path} is not a JSON object")
            return {}
        return data

    def entry(self, name):
        return self.entries().get(name)

    def files(self, name):
        """The record's files (relative paths)."""
        files = self.record(name).get("files") or []
        return [files] if isinstance(files, str) else list(files)

    # --- reading -------------------------------------------------------------------

    def file_name(self, name, file=None):
        """`file`, checked to be one of artifact `name`'s files; with no
        `file`, its only one."""
        files = self.files(name)
        if file is None:
            if len(files) != 1:
                raise ArtifactError(f"artifact {name} has {len(files)} files; say which")
            return files[0]
        if file not in files:
            raise ArtifactError(f"{file} is not one of artifact {name}'s files ({', '.join(files)})")
        return file

    def path(self, name, file=None):
        """The path of artifact `name`'s file (`file` for one with several),
        None when it has not been made."""
        path = os.path.join(self.directory, self.file_name(name, file))
        return path if os.path.isfile(path) else None

    def stale(self, name, entry=None, now=None):
        """None when artifact `name` is current, else why it is not."""
        rec = self.record(name)
        entry = self.entry(name) if entry is None else entry
        if not entry:
            return "never made"
        missing = [f for f in self.files(name) if not os.path.isfile(os.path.join(self.directory, f))]
        if missing:
            return f"{', '.join(missing)} missing"
        generator = rec.get("generatorClass")
        if generator != entry.get("generator"):
            return f"its generator is now {generator}, not {entry.get('generator')}"
        if record_sha256(rec) != (entry.get("config") or {}).get("record_sha256"):
            return "its configuration record changed since it was made"
        max_age = rec.get("maxAgeDays")
        made = _parse_time(entry.get("generated_at"))
        if made is None:
            return "the manifest does not say when it was made"
        if max_age is not None:
            age = ((now or _now()) - made).total_seconds() / 86400
            if age > max_age:
                return f"made {age:.0f} days ago, more than its maxAgeDays of {max_age}"
        return None

    def status(self, now=None):
        """A row per configured artifact, then per manifest entry with no
        record: ``{"name", "configured", "files", "rows", "generated_at",
        "age_days", "max_age_days", "stale"}`` (`stale` is None when
        current)."""
        entries = self.entries()
        now = now or _now()
        rows = []
        for name in sorted(set(self.records) | set(entries)):
            entry = entries.get(name)
            rec = self.records.get(name)
            made = _parse_time((entry or {}).get("generated_at"))
            counts = [f.get("rows") for f in (entry or {}).get("files", [])]
            row = {
                "name": name, "configured": rec is not None,
                "files": self.files(name) if rec is not None
                else [f.get("path") for f in (entry or {}).get("files", [])],
                "rows": sum(c for c in counts if c is not None) if any(c is not None for c in counts)
                else None,
                "generated_at": (entry or {}).get("generated_at"),
                "age_days": round((now - made).total_seconds() / 86400, 1) if made else None,
                "max_age_days": (rec or {}).get("maxAgeDays"),
            }
            if rec is None:
                row["stale"] = "no configuration record"
            else:
                try:
                    row["stale"] = self.stale(name, entry=entry or {}, now=now)
                except ArtifactError as e:
                    row["stale"] = str(e)
            rows.append(row)
        return rows

    def check(self, name=None):
        """``[{"artifact", "path", "problem"}]``: every file of the manifest's
        entries (or `name`'s) whose size or md5 no longer matches, or that is
        missing. ``[]`` when all match."""
        entries = self.entries(strict=True)
        names = [name] if name else sorted(entries)
        problems = []
        for n in names:
            entry = entries.get(n)
            if entry is None:
                problems.append({"artifact": n, "path": None, "problem": "not in the manifest"})
                continue
            for f in entry.get("files", []):
                path = os.path.join(self.directory, f.get("path", ""))
                if not os.path.isfile(path):
                    problem = "missing"
                elif os.path.getsize(path) != f.get("size"):
                    problem = f"size {os.path.getsize(path):,}, the manifest says {f.get('size'):,}"
                elif md5_file(path) != f.get("md5"):
                    problem = "md5 differs from the manifest's"
                else:
                    continue
                problems.append({"artifact": n, "path": f.get("path"), "problem": problem})
        return problems

    # --- writing -------------------------------------------------------------------

    def set(self, name, files, generator=None, seconds=None, force=False):
        """Publish `files`, ``{relative path: (path written, row count or
        None)}``, as artifact `name`: check them (every file the record
        lists, and no row count down by more than its ``maxShrink`` unless
        `force`), move each into place and update the artifact's manifest
        entry. Returns the entry. Raises `ArtifactError`, leaving the
        previous files and entry, when a check fails."""
        rec = self.record(name)
        want = self.files(name)
        if set(files) != set(want):
            raise ArtifactError(f"artifact {name} has the files {', '.join(want)}, "
                                f"not {', '.join(sorted(files))}")
        for rel in want:
            problem = file_problem(rel)
            if problem:
                raise ArtifactError(f"artifact {name}: {problem}")
            if not os.path.isfile(files[rel][0]):
                raise ArtifactError(f"artifact {name}: {rel} was not written ({files[rel][0]})")

        old = {f.get("path"): f for f in (self.entry(name) or {}).get("files", [])}
        shrink = rec.get("maxShrink")
        shrink = DEFAULT_MAX_SHRINK if shrink is None else shrink
        for rel in want:
            rows = files[rel][1]
            before = old.get(rel, {}).get("rows")
            if not force and rows is not None and before and rows < before * (1 - shrink):
                raise ArtifactError(
                    f"artifact {name}: {rel} has {rows:,} rows, down from {before:,}, more than "
                    f"its maxShrink of {shrink:.0%}; --force replaces it anyway")

        described = [{"path": rel, "size": os.path.getsize(files[rel][0]),
                      "md5": md5_file(files[rel][0]), "rows": files[rel][1]} for rel in want]
        os.makedirs(self.directory, exist_ok=True)
        for rel in want:
            dest = os.path.join(self.directory, rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            _move(files[rel][0], dest)

        origin = (getattr(self.configs, "record_origins", {}) or {}).get(name) or {}
        entry = {
            "generator": generator or rec.get("generatorClass"),
            "generated_at": _now().isoformat(timespec="seconds"),
            "seconds": None if seconds is None else round(seconds, 1),
            "files": described,
            "code": code_version(),
            "config": config_version(origin.get("path"), rec),
        }
        self._write_entry(name, entry)
        logger.info(f"artifact {name}: " + ", ".join(
            f"{f['path']} ({f['rows']:,} rows)" if f["rows"] is not None else f["path"]
            for f in described))
        return entry

    def _write_entry(self, name, entry):
        """Replace `name`'s manifest entry: under an exclusive lock, read the
        manifest, write it to a temporary file and rename that into place, so
        two artifacts made at once keep both their entries."""
        os.makedirs(self.directory, exist_ok=True)
        with open(self.manifest_path + ".lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                manifest = self.entries(strict=True)
                manifest[name] = entry
                tmp = f"{self.manifest_path}.tmp-{os.getpid()}"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump(manifest, fh, indent=2, sort_keys=True, ensure_ascii=False)
                    fh.write("\n")
                os.replace(tmp, self.manifest_path)
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def generator(self, name, progress=null_progress):
        """Artifact `name`'s generator, built (nothing opened). Refuses when
        a source it needs is not configured."""
        rec = self.record(name)
        path = rec.get("generatorClass")
        if not path:
            raise ArtifactError(f"artifact {name} has no generatorClass")
        try:
            cls = importObject(path)
        except Exception as e:
            raise ArtifactError(f"artifact {name}: generatorClass {path}: {type(e).__name__}: {e}")
        cfgs = self.configs
        have = set(getattr(cfgs, "internal", {})) | set(getattr(cfgs, "external", {})) \
            | set(getattr(cfgs, "results", {}))
        missing = [s for s in getattr(cls, "needs_sources", ()) if s not in have]
        if missing:
            raise ArtifactError(f"artifact {name} is made from the source{'s' if len(missing) > 1 else ''} "
                                f"{', '.join(missing)}, which this configuration does not have")
        return cls(rec, cfgs, progress=progress)

    def make(self, name, force=False, progress=null_progress):
        """Make artifact `name` again: run its generator into a temporary
        directory in ``artifacts_dir`` and `set` what it wrote. Returns the
        manifest entry. The temporary directory goes, whatever happens."""
        gen = self.generator(name, progress=progress)
        os.makedirs(self.directory, exist_ok=True)
        tmp = tempfile.mkdtemp(prefix=f".tmp-{name}-{os.getpid()}-", dir=self.directory)
        try:
            start = time.time()
            logger.info(f"making artifact {name} with {type(gen).__name__}")
            rows = gen.generate(tmp) or {}
            files = {rel: (os.path.join(tmp, rel), rows.get(rel)) for rel in self.files(name)}
            return self.set(name, files, generator=self.record(name).get("generatorClass"),
                            seconds=time.time() - start, force=force)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
