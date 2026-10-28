"""Back up and restore any store the configuration names: caches, maps and
the LMDB indexes, on whichever backend they live.

`backup(store, dest_dir, stem)` writes one store under `dest_dir` and
returns its manifest entry; `restore(store, src_dir, entry)` loads an entry
back into a store, replacing what it holds. A store class offers its own
exact copy by defining ``backup(dest_dir, stem, progress)`` and
``restore(src_dir, entry, progress)``; the Postgres caches and maps copy
their tables with COPY (``pg-copy``) and LMDB copies its environment
(``lmdb-copy``). Everything else, and everything when ``portable=True``,
goes through the portable format here:

* ``jsonl``: gzipped JSON Lines, one ``{"k": key, "v": value}`` per key,
  the value as the store hands it back (a string, a list for a set-valued
  map, the record with its metadata for a cache). Restores into any store
  of the same kind; a cache's ``insert_time`` becomes the time of the
  restore.
* ``idmap-jsonl``: the identity map, which is more than its keys -- two
  files, the members (``{"k": uri, "v": yuid}``) and the YUID registry
  (``{"yuid", "token", "minted"}``), so the update tokens and minted YUIDs
  without members survive. A class supports it by having
  ``iter_members()``, ``iter_yuid_registry()`` and ``load_portable()``.

A store that cannot be backed up says why through ``backup_refused()``
(the subsidiary idmap, a partial view of its master; the Redis idmap,
which `migrate-idmap.py` moves). `BackupRefused` carries the reason.

Native entries restore only into the class that wrote them; the manifest
records it. A backup is a snapshot only per Postgres store (each one is a
single transaction; the idmap's two tables are copied in one); Redis,
filesystem and LMDB-portable iteration are not snapshots, so take a backup
when no phase is running. Everything streams: nothing collects a store in
memory.

`write_manifest` / `read_manifest` keep ``manifest.json``, written last so
a backup that did not finish has none. `verify` re-reads an entry's files
and checks their row counts against the manifest, and samples keys against
the store.

No ``print`` and no ``rich`` here: ``progress`` (lux_pipeline.progress) is
called once per chunk and the commands report.
"""

import datetime
import gzip
import inspect
import io
import json
import logging
import os
import random
import socket
import subprocess
import time
import uuid

from lux_pipeline.progress import null_progress

logger = logging.getLogger("lux_pipeline")

MANIFEST = "manifest.json"
MANIFEST_VERSION = 1

JSONL = "jsonl"
IDMAP_JSONL = "idmap-jsonl"
PG_COPY = "pg-copy"
LMDB_COPY = "lmdb-copy"
NATIVE_FORMATS = (PG_COPY, LMDB_COPY)

KINDS = ("cache", "map", "index")

# Keys per chunk: one progress call, and for a restore one write batch.
CHUNK = 10_000

# gzip level for every file written here. The build machine is bound by
# random reads, not CPU or sequential writes, so the fast level: the backup
# of a data cache is dominated by the size of the table either way.
GZIP_LEVEL = 1

# The cache record fields the portable format carries. `source` is added by
# the caches' readers and is not stored.
CACHE_FIELDS = ("identifier", "yuid", "format", "valid", "change",
                "insert_time", "record_time", "refresh_time")


class BackupRefused(RuntimeError):
    """This store cannot be backed up or restored; the message says why."""


# --------------------------------------------------------------- helpers


def class_path(store):
    """The class as the configuration names it: ``storage.cache.postgres.DataCache``."""
    mod = type(store).__module__
    if mod.startswith("lux_pipeline."):
        mod = mod[len("lux_pipeline."):]
    return f"{mod}.{type(store).__qualname__}"


def refused(store):
    """Why `store` cannot be backed up, or None."""
    fn = getattr(store, "backup_refused", None)
    return fn() if fn is not None else None


def is_portable_idmap(store):
    return all(hasattr(store, m) for m in ("iter_members", "iter_yuid_registry", "load_portable"))


def _jsonable(obj):
    if isinstance(obj, (set, frozenset)):
        return sorted(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8")
    raise TypeError(f"Cannot write {type(obj).__name__} in a backup")


def dumps(obj):
    return json.dumps(obj, default=_jsonable, ensure_ascii=False, separators=(",", ":"))


def _chunks(it, n):
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def file_rows(path):
    """Lines in a gzipped file: the rows of a COPY file or a JSON Lines one."""
    n = 0
    with gzip.open(path, "rb") as fh:
        for _ in fh:
            n += 1
    return n


class CountingWriter(io.RawIOBase):
    """A binary file wrapper that reports the lines written through it to
    `progress`, one call per write -- COPY TO hands over a buffer at a
    time, and every row ends in a newline (text format escapes the ones
    inside a value)."""

    def __init__(self, fh, progress=null_progress):
        self.fh = fh
        self.progress = progress
        self.lines = 0

    def writable(self):
        return True

    def write(self, b):
        if isinstance(b, str):
            b = b.encode("utf-8")
        n = b.count(b"\n")
        self.lines += n
        if n:
            self.progress(advance=n)
        return self.fh.write(b)


class CountingReader(io.RawIOBase):
    """A binary file wrapper for COPY FROM that reports lines read."""

    def __init__(self, fh, progress=null_progress):
        self.fh = fh
        self.progress = progress

    def readable(self):
        return True

    def read(self, size=-1):
        b = self.fh.read(size)
        n = b.count(b"\n")
        if n:
            self.progress(advance=n)
        return b

    def readline(self, size=-1):
        b = self.fh.readline(size)
        if b.endswith(b"\n"):
            self.progress(advance=1)
        return b


class LineReader(io.RawIOBase):
    """A readable binary stream over an iterator of text lines, for COPY
    FROM out of a generator without building the file in memory."""

    def __init__(self, lines):
        self.lines = iter(lines)
        self.buf = b""

    def readable(self):
        return True

    def _fill(self, size):
        while size < 0 or len(self.buf) < size:
            try:
                self.buf += next(self.lines).encode("utf-8")
            except StopIteration:
                break

    def read(self, size=-1):
        self._fill(size)
        if size < 0:
            out, self.buf = self.buf, b""
        else:
            out, self.buf = self.buf[:size], self.buf[size:]
        return out

    def readline(self, size=-1):
        while b"\n" not in self.buf:
            before = len(self.buf)
            self._fill(len(self.buf) + 1)
            if len(self.buf) == before:
                break
        i = self.buf.find(b"\n")
        end = len(self.buf) if i < 0 else i + 1
        out, self.buf = self.buf[:end], self.buf[end:]
        return out


def copy_text(value):
    """One field in COPY text format: ``\\N`` for None, and the four
    characters that would shift a column escaped."""
    if value is None:
        return "\\N"
    if isinstance(value, (datetime.datetime, datetime.date)):
        value = value.isoformat()
    s = str(value)
    return (s.replace("\\", "\\\\").replace("\t", "\\t")
             .replace("\n", "\\n").replace("\r", "\\r"))


# --------------------------------------------------------------- postgres


def pg_columns(cur, table):
    cur.execute("SELECT attname FROM pg_attribute WHERE attrelid = %s::regclass "
                "AND attnum > 0 AND NOT attisdropped ORDER BY attnum", (table,))
    return [r[0] for r in cur.fetchall()]


def pg_size(conn, tables):
    """Bytes the tables' rows occupy on disk, TOAST included and indexes
    not (COPY writes no index): about what their COPY files need before
    compression."""
    with conn.cursor() as cur:
        cur.execute("SELECT coalesce(sum(pg_table_size(t::regclass)), 0) "
                    "FROM unnest(%s::text[]) t", (list(tables),))
        return int(cur.fetchone()[0])


def pg_copy_out(conn, tables, dest_dir, stem, progress=null_progress):
    """COPY each of `tables` to ``<stem>.<table>.copy.gz`` in `dest_dir`,
    all in one read-only REPEATABLE READ transaction on `conn` (a
    connection of the caller's own, not in autocommit), so tables copied
    together are consistent with each other. Returns the entry fields."""
    import psycopg2.extensions
    conn.autocommit = False
    conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
                     readonly=True)
    specs = []
    try:
        with conn.cursor() as cur:
            for table in tables:
                cols = pg_columns(cur, table)
                fn = f"{stem}.{table}.copy.gz"
                with gzip.open(os.path.join(dest_dir, fn), "wb", compresslevel=GZIP_LEVEL) as gz:
                    out = CountingWriter(gz, progress)
                    cur.copy_expert(f"COPY {table} ({', '.join(cols)}) TO STDOUT", out)
                specs.append({"table": table, "file": fn, "columns": cols, "rows": out.lines})
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return {"format": PG_COPY, "files": [s["file"] for s in specs],
            "rows": sum(s["rows"] for s in specs), "extra": {"tables": specs}}


def _drop_indexes(cur, table):
    """Drop `table`'s indexes and its primary-key / unique constraints,
    returning what `_recreate_indexes` needs to put them back exactly."""
    cur.execute("SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conrelid = %s::regclass AND contype IN ('p', 'u') ORDER BY conname",
                (table,))
    constraints = cur.fetchall()
    cur.execute("SELECT i.relname, pg_get_indexdef(i.oid) FROM pg_index x "
                "JOIN pg_class i ON i.oid = x.indexrelid "
                "WHERE x.indrelid = %s::regclass AND NOT EXISTS "
                "(SELECT 1 FROM pg_constraint c WHERE c.conindid = i.oid) ORDER BY i.relname",
                (table,))
    indexes = cur.fetchall()
    for name, _ in indexes:
        cur.execute(f'DROP INDEX "{name}"')
    for name, _ in constraints:
        cur.execute(f'ALTER TABLE {table} DROP CONSTRAINT "{name}"')
    return table, constraints, indexes


def _recreate_indexes(cur, saved):
    table, constraints, indexes = saved
    for name, defn in constraints:
        cur.execute(f'ALTER TABLE {table} ADD CONSTRAINT "{name}" {defn}')
    for _name, defn in indexes:
        cur.execute(defn)


def pg_load(conn, loads):
    """Replace the contents of tables in one transaction on `conn` (the
    caller's own connection): drop their indexes and key constraints,
    TRUNCATE them together, COPY each from its stream, then rebuild the
    indexes and constraints as they were. A COPY into a bare table and an
    index build afterwards is several times faster than COPY into an
    indexed one at idmap scale, and DDL is transactional in Postgres: any
    failure -- a bad row, a duplicate key on the rebuild -- rolls back to
    the previous contents and indexes.

    `loads` is ``[(table, columns, binary stream)]``. Returns rows loaded.
    """
    conn.autocommit = False
    total = 0
    try:
        with conn.cursor() as cur:
            saved = [_drop_indexes(cur, table) for table, _, _ in loads]
            cur.execute(f"TRUNCATE {', '.join(t for t, _, _ in loads)}")
            for table, cols, stream in loads:
                cur.copy_expert(f"COPY {table} ({', '.join(cols)}) FROM STDIN", stream)
                total += cur.rowcount
            for s in saved:
                _recreate_indexes(cur, s)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return total


def pg_copy_in(conn, tables, src_dir, entry, progress=null_progress):
    """Restore a `pg_copy_out` entry into `tables` -- the store's own
    table names, in the order it wrote them, which need not be the names
    in the backup (a scratch ``tableName``). The columns must match."""
    specs = entry["extra"]["tables"]
    if len(specs) != len(tables):
        raise ValueError(f"backup has {len(specs)} tables, the store {len(tables)}")
    # checked in the transaction the load then runs in; on a mismatch the
    # caller closes the connection, which ends it
    conn.autocommit = False
    with conn.cursor() as cur:
        for table, spec in zip(tables, specs):
            have = pg_columns(cur, table)
            if sorted(have) != sorted(spec["columns"]):
                raise ValueError(f"{table} has columns {have}; the backup of {spec['table']} "
                                 f"has {spec['columns']}")
    handles = [gzip.open(os.path.join(src_dir, s["file"]), "rb") for s in specs]
    try:
        loads = [(table, spec["columns"], CountingReader(fh, progress))
                 for table, spec, fh in zip(tables, specs, handles)]
        return pg_load(conn, loads)
    finally:
        for fh in handles:
            fh.close()


# --------------------------------------------------------------- portable


def iter_items(store):
    """``(key, value)`` for every key in `store`, streamed: the store's own
    ``iter_items()`` when it has one, LMDB's ``items()``, else each key of
    ``iter_keys()`` looked up."""
    fn = getattr(store, "iter_items", None)
    if fn is not None:
        yield from fn()
        return
    if hasattr(store, "env") and hasattr(store, "items"):
        yield from store.items()
        return
    for k in store.iter_keys():
        yield k, store.get(k)


def cache_value(rec):
    """A cache record as the portable format keeps it: the data and the
    metadata columns, nothing the reader added."""
    out = {"data": rec["data"]}
    for f in CACHE_FIELDS:
        if rec.get(f) not in (None, ""):
            out[f] = rec[f]
    return out


def write_jsonl(path, items, progress=null_progress):
    """``{"k", "v"}`` lines for `items` to gzipped `path`; the row count."""
    n = 0
    with gzip.open(path, "wt", compresslevel=GZIP_LEVEL, encoding="utf-8") as fh:
        for chunk in _chunks(items, CHUNK):
            for k, v in chunk:
                fh.write(dumps({"k": k, "v": v}))
                fh.write("\n")
            n += len(chunk)
            progress(advance=len(chunk))
    return n


def read_jsonl(path):
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            yield json.loads(line)


def backup_portable(store, kind, dest_dir, stem, progress=null_progress):
    if kind == "map" and is_portable_idmap(store):
        return backup_idmap_portable(store, dest_dir, stem, progress)
    items = iter_items(store)
    if kind == "cache":
        items = ((str(k), cache_value(v)) for k, v in items if v is not None)
    fn = f"{stem}.jsonl.gz"
    n = write_jsonl(os.path.join(dest_dir, fn), items, progress)
    return {"format": JSONL, "files": [fn], "rows": n, "extra": {}}


def backup_idmap_portable(store, dest_dir, stem, progress=null_progress):
    members = f"{stem}.members.jsonl.gz"
    yuids = f"{stem}.yuids.jsonl.gz"
    n = write_jsonl(os.path.join(dest_dir, members), store.iter_members(), progress)
    m = 0
    with gzip.open(os.path.join(dest_dir, yuids), "wt", compresslevel=GZIP_LEVEL,
                   encoding="utf-8") as fh:
        for chunk in _chunks(store.iter_yuid_registry(), CHUNK):
            for yuid, token, minted in chunk:
                fh.write(dumps({"yuid": yuid, "token": token, "minted": minted}))
                fh.write("\n")
            m += len(chunk)
            progress(advance=len(chunk))
    return {"format": IDMAP_JSONL, "files": [members, yuids], "rows": n,
            "extra": {"members": n, "yuids": m}}


def _setter_kwargs(store):
    """The keyword arguments the cache's ``set()`` accepts."""
    try:
        return set(inspect.signature(store.set).parameters)
    except (TypeError, ValueError):
        return set()


def _put_cache(store, accepted, k, v):
    kw = {f: v[f] for f in ("identifier", "yuid", "format", "valid", "change",
                             "record_time", "refresh_time") if f in v and f in accepted}
    if "identifier" not in kw and "yuid" not in kw:
        kw[getattr(store, "key", "identifier")] = k
    store.set(v["data"], **kw)


def _put_map(store, k, v):
    if isinstance(v, list):
        # a set-valued map: one member at a time, as the map is built
        for member in v:
            store.set(k, member)
    else:
        store.set(k, v)


def restore_portable(store, kind, src_dir, entry, progress=null_progress):
    """Empty `store` and load a ``jsonl`` / ``idmap-jsonl`` entry into it,
    a chunk at a time. Not one transaction: a failure part way leaves the
    store part-loaded (restore again)."""
    if entry["format"] == IDMAP_JSONL:
        if not is_portable_idmap(store):
            raise BackupRefused(f"{class_path(store)} cannot load a portable idmap backup")
        members = (tuple((r["k"], r["v"])) for r in read_jsonl(os.path.join(src_dir, entry["files"][0])))
        registry = ((r["yuid"], r.get("token"), r.get("minted"))
                    for r in read_jsonl(os.path.join(src_dir, entry["files"][1])))
        return store.load_portable(members, registry, progress=progress)

    store.clear()
    rows = read_jsonl(os.path.join(src_dir, entry["files"][0]))
    n = 0
    defer = getattr(store, "defer_commits", None) if kind == "cache" else None
    if defer is not None:
        defer(every=CHUNK)
    accepted = _setter_kwargs(store) if kind == "cache" else set()
    for chunk in _chunks(rows, CHUNK):
        if kind == "index":
            # an index value is one value, a list included: never per member
            if hasattr(store, "update"):
                store.update({r["k"]: r["v"] for r in chunk})
            else:
                for r in chunk:
                    store[r["k"]] = r["v"]
        else:
            for r in chunk:
                if kind == "cache":
                    _put_cache(store, accepted, r["k"], r["v"])
                else:
                    _put_map(store, r["k"], r["v"])
            checkpoint = getattr(store, "checkpoint", None)
            if checkpoint is not None:
                checkpoint()
        n += len(chunk)
        progress(advance=len(chunk))
    if defer is not None:
        store.flush()
        store.resume_commits()
    if kind == "index":
        sync = getattr(store, "sync", None)
        if sync is not None:
            sync()
    return n


# --------------------------------------------------------------- protocol


def backup(store, kind, name, dest_dir, stem=None, portable=False, progress=null_progress,
           **labels):
    """Back up `store` under `dest_dir` and return its manifest entry.

    `kind` is cache, map or index and `name` the source or map name, as
    the commands take them; `labels` (``cache=``, ``index=``) go into the
    entry as they are. `stem` prefixes every file (default `name`). The
    store's own ``backup()`` unless `portable` or it has none.
    """
    if kind not in KINDS:
        raise ValueError(f"kind is one of {', '.join(KINDS)}, not {kind}")
    why = refused(store)
    if why:
        raise BackupRefused(why)
    os.makedirs(dest_dir, exist_ok=True)
    stem = stem or name
    start = time.monotonic()
    native = None if portable else getattr(store, "backup", None)
    if native is not None:
        got = native(dest_dir, stem, progress=progress)
    else:
        got = backup_portable(store, kind, dest_dir, stem, progress)
    entry = {"name": name, "kind": kind, **labels, "store_class": class_path(store)}
    entry.update(got)
    entry["bytes"] = sum(_size(os.path.join(dest_dir, f)) for f in entry["files"])
    entry["seconds"] = round(time.monotonic() - start, 3)
    progress(done=True)
    return entry


def _size(path):
    if os.path.isdir(path):
        return sum(os.path.getsize(os.path.join(path, f)) for f in os.listdir(path))
    return os.path.getsize(path)


def check_restorable(store, entry):
    """Raise unless `entry` can be restored into `store`: not refused, and
    a native entry only into the class that wrote it."""
    why = refused(store)
    if why:
        raise BackupRefused(why)
    if entry["format"] in NATIVE_FORMATS:
        if entry.get("store_class") != class_path(store):
            raise BackupRefused(
                f"{entry['name']} was backed up natively ({entry['format']}) from "
                f"{entry.get('store_class')}, and is configured as {class_path(store)}: "
                f"restore it from a portable backup (lux backup --portable)")
        if getattr(store, "restore", None) is None:
            raise BackupRefused(f"{class_path(store)} cannot restore {entry['format']}")
    elif entry["format"] == IDMAP_JSONL and not is_portable_idmap(store):
        raise BackupRefused(f"{class_path(store)} cannot load a portable idmap backup")


def restore(store, src_dir, entry, progress=null_progress):
    """Replace what `store` holds with `entry` from the backup in
    `src_dir`; the rows loaded."""
    check_restorable(store, entry)
    if entry["format"] in NATIVE_FORMATS:
        n = store.restore(src_dir, entry, progress=progress)
    else:
        n = restore_portable(store, entry["kind"], src_dir, entry, progress)
    progress(done=True)
    return n


# --------------------------------------------------------------- manifest


def _git_sha():
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=here, capture_output=True,
                              text=True, timeout=5).stdout.strip() or None
    except Exception:
        return None


def new_manifest(label):
    return {"version": MANIFEST_VERSION,
            "created": datetime.datetime.now().isoformat(timespec="seconds"),
            "host": socket.gethostname(), "git": _git_sha(), "label": label, "entries": []}


def write_manifest(backup_dir, manifest):
    """Write ``manifest.json`` atomically; the last thing a backup does."""
    path = os.path.join(backup_dir, MANIFEST)
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")
    os.replace(tmp, path)
    return path


def read_manifest(backup_dir):
    """The manifest of the backup in `backup_dir`; ``ValueError`` when it
    has none (the backup did not finish) or a version this code does not
    know."""
    path = os.path.join(backup_dir, MANIFEST)
    if not os.path.exists(path):
        raise ValueError(f"No {MANIFEST} in {backup_dir}: not a backup, or one that did not finish")
    with open(path) as fh:
        manifest = json.load(fh)
    if manifest.get("version") != MANIFEST_VERSION:
        raise ValueError(f"{path} is manifest version {manifest.get('version')}, "
                         f"not {MANIFEST_VERSION}")
    return manifest


# --------------------------------------------------------------- verify


def verify(store, src_dir, entry, sample=100, rng=None):
    """Check an entry against its files and its store. Returns a list of
    problems, empty when it is sound.

    The files: every one there, and their rows (lines; for an LMDB copy,
    the entries of the copied environment) equal to the manifest's. The
    store: up to `sample` keys from the portable files looked up and
    compared (native files are not parsed back, so only their counts are
    checked). Nothing here counts the live store: on a large Postgres table
    that is a full scan, and the manifest's rows came from the copy itself.
    """
    problems = []
    for f in entry["files"]:
        if not os.path.exists(os.path.join(src_dir, f)):
            problems.append(f"{f} is missing")
    if problems:
        return problems
    fmt = entry["format"]
    if fmt == PG_COPY:
        for spec in entry["extra"]["tables"]:
            n = file_rows(os.path.join(src_dir, spec["file"]))
            if n != spec["rows"]:
                problems.append(f"{spec['file']}: {n:,} rows, manifest says {spec['rows']:,}")
    elif fmt == LMDB_COPY:
        n = lmdb_entries(os.path.join(src_dir, entry["files"][0]))
        if n != entry["rows"]:
            problems.append(f"{entry['files'][0]}: {n:,} entries, manifest says {entry['rows']:,}")
    elif fmt == IDMAP_JSONL:
        for f, want in zip(entry["files"], (entry["extra"]["members"], entry["extra"]["yuids"])):
            n = file_rows(os.path.join(src_dir, f))
            if n != want:
                problems.append(f"{f}: {n:,} rows, manifest says {want:,}")
        if store is not None and not problems:
            rows = ((r["k"], r["v"]) for r in read_jsonl(os.path.join(src_dir, entry["files"][0])))
            problems.extend(_sample(store, "map", rows, entry["extra"]["members"], sample, rng))
    else:
        n = file_rows(os.path.join(src_dir, entry["files"][0]))
        if n != entry["rows"]:
            problems.append(f"{entry['files'][0]}: {n:,} rows, manifest says {entry['rows']:,}")
        elif store is not None:
            rows = ((r["k"], r["v"]) for r in read_jsonl(os.path.join(src_dir, entry["files"][0])))
            problems.extend(_sample(store, entry["kind"], rows, n, sample, rng))
    return problems


def _sample(store, kind, rows, n, sample, rng):
    """Compare up to `sample` of the `n` backed-up rows with the store."""
    rng = rng or random.Random()
    if n == 0 or sample <= 0:
        return []
    picks = set(rng.sample(range(n), min(sample, n)))
    problems = []
    for i, (k, v) in enumerate(rows):
        if i not in picks:
            continue
        got = store.get(k)
        if kind == "cache":
            same = got is not None and json.loads(dumps(got["data"])) == v["data"]
        else:
            same = json.loads(dumps(got)) == v
        if not same:
            problems.append(f"{k}: the store no longer matches the backup")
    return problems


def lmdb_entries(path):
    import lmdb
    env = lmdb.open(path, readonly=True, lock=False, max_dbs=1)
    try:
        return env.stat()["entries"]
    finally:
        env.close()
