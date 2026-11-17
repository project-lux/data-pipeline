"""Fetching a source's files with rsync, for `BaseDownloader`: a mirror
that serves its dumps over rsync (Wikimedia's, many others) is copied, or
brought up to date, into the source's ``dumps_dir``, and the transfer is
reported to the progress bar like an HTTP download.

A source names what to copy in its ``rsync`` key: one entry or a list,
each a source string or an object::

    "rsync": {
        "source": "rsync://mirror.example.org/dumps/commonswiki/latest/",
        "path": "commonswiki",
        "include": ["*-pages-articles*.xml*.bz2"],
        "exclude": [],
        "options": ["--bwlimit=20m"],
        "delete": false,
        "type": "records"
    }

``source``
    Anything rsync accepts: ``rsync://host/module/path``,
    ``host::module/path``, ``user@host:path`` (over ssh, which must not
    need a password) or a local path. As with rsync itself, a directory
    ending in ``/`` copies its contents, without it the directory itself.
``path``
    Where the files go, relative to the source's ``dumps_dir`` (default:
    the ``dumps_dir`` itself), as an ``input_files`` path is.
``include`` / ``exclude``
    rsync patterns. With ``include``, only the files matching one are
    copied, in any subdirectory (empty directories are pruned).
``options``
    Further rsync arguments, as a list.
``delete``
    Delete files under ``path`` that the source no longer has (default
    false).
``type``
    The ``lux download --type`` the entry belongs to (default
    ``records``).

Every transfer is ``-rtL --partial --delay-updates``: times are kept, so
a second run only fetches what changed; symbolic links are followed, so a
mirror's ``latest/`` directory of links into a dated one arrives as the
files themselves; an interrupted file is resumed; and new files stay in
rsync's ``.~tmp~`` directory until the whole transfer is done, so a
loader never reads a file that is still arriving.
``lux download --no-overwrite`` adds ``--ignore-existing``.

The bar's total is the size of what a dry run (``--dry-run
--out-format``) says will be sent, and it advances with rsync's
``--progress`` lines. Only options that rsync 2.6.9 and openrsync (macOS)
understand are used; ``--info=progress2`` is newer.
"""

import logging
import os
import re
import subprocess
import threading
from pathlib import Path

logger = logging.getLogger("lux_pipeline")

RSYNC = "rsync"
BASE_OPTIONS = ("-rtL", "--partial", "--delay-updates")
# rsync's own working directory under the destination (--delay-updates)
TEMP_DIR = ".~tmp~"
# a progress line: bytes so far in this file (digits, maybe grouped), percent
_PROGRESS = re.compile(r"^\s*(\d[\d,.']*)\s+(\d{1,3})%")
# the end of a file: "(xfr#3, to-chk=4/9)" (rsync 3) or "(xfer#3, ...)"
_DONE = re.compile(r"\(xfe?r#\d+")
_PLANNED = re.compile(r"^(\d+) (.+)$")
# exit 24: some source files vanished during the transfer; the rest arrived
VANISHED = 24
SPEC_KEYS = {"source", "path", "include", "exclude", "options", "delete", "type"}


def rsync_specs(config):
    """The source's ``rsync`` entries as dicts with a ``source``; a bad
    entry raises ValueError naming the source."""
    raw = config.get("rsync")
    if not raw:
        return []
    name = config.get("name", "")
    out = []
    for item in raw if isinstance(raw, list) else [raw]:
        if isinstance(item, str):
            item = {"source": item}
        if not isinstance(item, dict) or not item.get("source"):
            raise ValueError(f"{name}: each rsync entry needs a source")
        unknown = set(item) - SPEC_KEYS
        if unknown:
            raise ValueError(f"{name}: unknown rsync keys {sorted(unknown)}")
        for key in ("include", "exclude", "options"):
            value = item.get(key, [])
            if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
                raise ValueError(f"{name}: rsync {key} should be a list of strings")
        out.append(item)
    return out


def command(spec, dest, overwrite=True, dry_run=False, binary=RSYNC):
    """The rsync command line for `spec` into the directory `dest`."""
    cmd = [binary, *BASE_OPTIONS]
    if dry_run:
        cmd += ["--dry-run", "--out-format=%l %n"]
    else:
        cmd += ["--progress", "--out-format=%n"]
    if not overwrite:
        cmd.append("--ignore-existing")
    if spec.get("delete"):
        cmd.append("--delete")
    includes = spec.get("include") or []
    if includes:
        cmd += ["--include=*/", *(f"--include={p}" for p in includes)]
    cmd += [f"--exclude={p}" for p in spec.get("exclude") or []]
    if includes:
        cmd += ["--exclude=*", "--prune-empty-dirs"]
    cmd += list(spec.get("options") or [])
    cmd += [spec["source"], dest.rstrip("/") + "/"]
    return cmd


def _run(cmd, **kw):
    try:
        return subprocess.run(cmd, stdin=subprocess.DEVNULL, capture_output=True, text=True,
                              errors="replace", **kw)
    except FileNotFoundError:
        raise RuntimeError(f"{cmd[0]} is not installed (or not on the PATH)")


def check(result, label, cmd):
    """Raise for a failed rsync; exit 24 (files vanished) is a warning."""
    if result.returncode == 0:
        return
    tail = " | ".join((result.stderr or "").strip().splitlines()[-5:])
    if result.returncode == VANISHED:
        logger.warning(f"{label}: some source files vanished during the transfer: {tail}")
        return
    raise RuntimeError(f"{label}: rsync exited {result.returncode}: {tail or ' '.join(cmd)}")


def plan(spec, dest, overwrite=True, label="", binary=RSYNC):
    """``[(name, size)]``: the files a transfer would send now."""
    cmd = command(spec, dest, overwrite, dry_run=True, binary=binary)
    result = _run(cmd)
    check(result, label, cmd)
    files = []
    for line in result.stdout.splitlines():
        m = _PLANNED.match(line)
        if m and not m.group(2).endswith("/"):
            files.append((m.group(2), int(m.group(1))))
    return files


class ProgressReader:
    """Turns rsync ``--progress`` output into bar steps: `feed` takes one
    line (rsync ends progress updates with ``\\r``) and returns the bytes
    to advance by. File names (the ``--out-format`` lines) go to
    `on_name`; finished files are counted in ``files``."""

    def __init__(self, on_name=None):
        self.on_name = on_name
        self.current = 0
        self.files = 0
        self.bytes = 0

    def feed(self, line):
        m = _PROGRESS.match(line)
        if m is None:
            name = line.strip()
            if name and not name.endswith("/"):
                self.current = 0
                if self.on_name is not None:
                    self.on_name(name)
            return 0
        done = int(re.sub(r"\D", "", m.group(1)))
        if done < self.current:
            # a new file whose name line has not been seen
            self.current = 0
        step = done - self.current
        self.current = done
        self.bytes += step
        if _DONE.search(line):
            self.files += 1
            self.current = 0
        return step


def lines(stream, size=65536):
    """The lines of a binary stream, split at ``\\n`` or ``\\r``, decoded."""
    buf = b""
    while True:
        chunk = stream.read1(size) if hasattr(stream, "read1") else stream.read(size)
        if not chunk:
            break
        buf += chunk
        parts = re.split(rb"[\r\n]", buf)
        buf = parts.pop()
        for p in parts:
            if p:
                yield p.decode("utf-8", "replace")
    if buf:
        yield buf.decode("utf-8", "replace")


def transfer(spec, dest, progress, label, overwrite=True, binary=RSYNC):
    """Run the transfer, reporting to `progress`; returns
    ``(files, bytes)`` sent. Raises on failure, and stops rsync when
    interrupted."""
    Path(dest).mkdir(parents=True, exist_ok=True)
    planned = plan(spec, dest, overwrite, label, binary)
    total = sum(size for _, size in planned)
    if not planned and not spec.get("delete"):
        logger.info(f"{label}: up to date with {spec['source']}")
        progress(description=f"{label} (up to date)")
        return 0, 0
    logger.info(f"{label}: {len(planned):,} file(s), {total:,} bytes to fetch from {spec['source']}")
    if total:
        progress(total=total, completed=0, description=label)
    else:
        progress(description=label)

    cmd = command(spec, dest, overwrite, binary=binary)
    reader = ProgressReader(on_name=lambda name: progress(description=f"{label}: {name}"))
    errors = []
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    except FileNotFoundError:
        raise RuntimeError(f"{binary} is not installed (or not on the PATH)")
    drain = threading.Thread(target=lambda: errors.extend(lines(proc.stderr)), daemon=True)
    drain.start()
    try:
        for line in lines(proc.stdout):
            step = reader.feed(line)
            if step:
                progress(advance=step)
        proc.wait()
    except BaseException:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        raise
    finally:
        drain.join(timeout=5)
    check(subprocess.CompletedProcess(cmd, proc.returncode, "", "\n".join(errors)), label, cmd)
    logger.info(f"Finished rsync of {label}: {reader.files:,} file(s), {reader.bytes:,} bytes")
    return reader.files, reader.bytes
