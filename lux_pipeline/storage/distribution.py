"""LMDB distribution stores: every record of an export in one LMDB, with an
index of the records by class. What ``lux export --format lmdb`` builds.

A store is an LMDB environment (a directory) with three databases:

``data``
    key -> the record's JSON, zlib-compressed (level `ZLIB_LEVEL`). The
    record is whatever the export exports: the merged record for ``--type
    merged``, a cache's record for ``--cache``.
``index`` (dupsort)
    class -> blocks of keys. Every key whose record has that ``type``, in
    key order, packed several to a value: walking one class's duplicates
    (``cursor.set_key(b"Person")``, then ``iternext_dup()``) yields every
    Person's key in order without touching any other record. A record
    without a string ``type`` is in ``data`` but not in the index.
``meta``
    ``b"store"`` -> JSON describing the store: ``format``, ``keys``,
    ``compression``, ``index`` (how a block is packed), ``records``,
    ``classes`` (records per class), ``unindexed``, ``sources``,
    ``created``. A reader should check ``keys`` before decoding.

Keys come in two forms (`KEY_FORMATS`):

``uuid``
    The 16 bytes of the record's UUID (``uuid.UUID(...).bytes``), taken
    from its identifier's last path segment. The default for the merged
    records, whose identifiers are UUIDs. An index block is up to
    `UUID_BLOCK` keys concatenated, 16 bytes each.
``text``
    ``<source>:<identifier>`` in UTF-8, the identifier as the cache has
    it (a record cache's with its ``##qua<type>``). The default for
    ``--cache`` and ``--type external``, and the layout ``lux load --type
    lmdb`` (the loader's ``lmdb/json.z``) reads, one source's prefix at a
    time. An index block is keys joined by NUL bytes, at most
    `TEXT_BLOCK_BYTES` long; NUL sorts below every other byte, so blocks
    stay in key order. A key longer than that cannot be stored (LMDB
    limits keys, and dupsort values, to 511 bytes) and is skipped and
    counted.

LMDB loads quickly only in key order: an insert anywhere else splits and
rewrites B-tree pages, and a store built that way is both slow to build and
larger. The records do not come out of the database in key order (export
slices are hash partitions), so a store is built in two steps:

1. Each worker reads its slice, compresses every record, and writes sorted
   *runs* (`RunWriter`): up to `RUN_BYTES` of records held in memory,
   sorted by key and written to ``<runs_dir>/w<worker>-<n>.run``. When it
   has written them all it leaves ``w<worker>.done`` (its statistics), so
   a merge can tell a finished slice from one that failed or never ran
   (`missing_workers`).
2. `build_store` merges every run in key order and appends to the store,
   one write transaction per `COMMIT_EVERY` records, building the index
   blocks as it goes. Every run is open at once and the merge takes a few
   records from each in turn, so each is read through a large buffer
   (`read_buffer`): with the default 8 kB, a merge of a few hundred runs
   is a stream of small scattered reads, and a disk limited in operations
   per second, not bytes, caps it at half the speed it manages from
   memory. It is written to ``<path>.tmp`` and moved into place
   when complete, so a failed build never replaces a good store.

`DistributionStore` reads one.
"""

import heapq
import json
import logging
import os
import shutil
import struct
import time
import uuid
import zlib

import lmdb
import ujson

logger = logging.getLogger("lux_pipeline")

FORMAT_VERSION = 1
KEY_FORMATS = ("uuid", "text")
DATA_DB = b"data"
INDEX_DB = b"index"
META_DB = b"meta"
META_KEY = b"store"

ZLIB_LEVEL = 1
# 30 x 16 = 480 bytes, under LMDB's 511-byte limit on a dupsort value
UUID_BLOCK = 30
TEXT_BLOCK_BYTES = 500
TEXT_SEPARATOR = b"\x00"
# records held in memory per run before it is sorted and written
RUN_BYTES = int(os.getenv("LUX_LMDB_RUN_MB", "256")) * 1024 * 1024
# the merge's read buffers, all runs together (LUX_LMDB_READ_MB), shared out
# between the runs, each between these two
READ_MB = 1024
MIN_READ_BUFFER = 1024 * 1024
MAX_READ_BUFFER = 16 * 1024 * 1024
COMMIT_EVERY = 10000

_FRAME = struct.Struct(">HHI")      # key length, class length, value length


# --- keys --------------------------------------------------------------------


def uuid_key(identifier):
    """The 16-byte key for an identifier that is (or ends in) a UUID;
    ``ValueError`` for one that is not."""
    text = str(identifier).rstrip("/").rsplit("/", 1)[-1]
    try:
        return uuid.UUID(text).bytes
    except ValueError:
        raise ValueError(f"not a UUID, so it cannot have a uuid key: {identifier!r} "
                         f"(use text keys)") from None


def text_key(source, identifier):
    return f"{source}:{identifier}".encode("utf-8")


def make_key(key_format, source, identifier):
    if key_format == "uuid":
        return uuid_key(identifier)
    if key_format == "text":
        return text_key(source, identifier)
    raise ValueError(f"unknown key format {key_format!r} (one of {', '.join(KEY_FORMATS)})")


def decode_key(key_format, key):
    """A stored key back to text: the UUID string, or ``source:id``."""
    key = bytes(key)
    return str(uuid.UUID(bytes=key)) if key_format == "uuid" else key.decode("utf-8")


def split_block(key_format, block):
    """The keys packed in one index value."""
    block = bytes(block)
    if key_format == "uuid":
        return [block[i:i + 16] for i in range(0, len(block), 16)]
    return block.split(TEXT_SEPARATOR)


def record_class(text):
    """The record's ``type``, when it is a string; else None."""
    try:
        cls = ujson.loads(text).get("type")
    except (ValueError, AttributeError):
        return None
    return cls if isinstance(cls, str) and cls else None


# --- step 1: sorted runs -------------------------------------------------------


def worker_runs(runs_dir, worker):
    return sorted(f for f in _list(runs_dir) if f.startswith(f"w{worker:04d}-"))


def done_marker(runs_dir, worker):
    return os.path.join(runs_dir, f"w{worker:04d}.done")


def missing_workers(runs_dir, workers):
    """The workers of ``range(workers)`` that have not finished their runs."""
    return [n for n in range(workers) if not os.path.exists(done_marker(runs_dir, n))]


def _list(d):
    try:
        return os.listdir(d)
    except FileNotFoundError:
        return []


class RunWriter:
    """Compress records and write them out as sorted runs. Removes this
    worker's runs from an earlier attempt when it starts, so rerunning a
    slice never merges it twice. Use as a context manager, or call
    `close`."""

    def __init__(self, runs_dir, worker, key_format, run_bytes=None, level=ZLIB_LEVEL):
        if key_format not in KEY_FORMATS:
            raise ValueError(f"unknown key format {key_format!r}")
        self.runs_dir = runs_dir
        self.worker = worker
        self.key_format = key_format
        self.run_bytes = run_bytes or RUN_BYTES
        self.level = level
        os.makedirs(runs_dir, exist_ok=True)
        for f in worker_runs(runs_dir, worker):
            os.remove(os.path.join(runs_dir, f))
        if os.path.exists(done_marker(runs_dir, worker)):
            os.remove(done_marker(runs_dir, worker))
        self.pending = []
        self.pending_bytes = 0
        self.runs = []
        self.stats = {"records": 0, "too_long": 0, "bytes": 0}

    def add(self, source, identifier, text):
        """One record: its source name, identifier and JSON text. Returns
        False for a text key too long to store."""
        key = make_key(self.key_format, source, identifier)
        if len(key) > TEXT_BLOCK_BYTES:
            self.stats["too_long"] += 1
            return False
        if not isinstance(text, (str, bytes)):
            text = ujson.dumps(text, escape_forward_slashes=False)
        cls = record_class(text)
        raw = text.encode("utf-8") if isinstance(text, str) else text
        value = zlib.compress(raw, self.level)
        cls_b = cls.encode("utf-8") if cls else b""
        self.pending.append((key, cls_b, value))
        self.pending_bytes += len(key) + len(cls_b) + len(value) + _FRAME.size
        self.stats["records"] += 1
        if self.pending_bytes >= self.run_bytes:
            self.spill()
        return True

    def spill(self):
        if not self.pending:
            return
        self.pending.sort(key=lambda r: r[0])
        fn = os.path.join(self.runs_dir, f"w{self.worker:04d}-{len(self.runs):05d}.run")
        tmp = fn + ".tmp"
        with open(tmp, "wb") as fh:
            for key, cls, value in self.pending:
                fh.write(_FRAME.pack(len(key), len(cls), len(value)))
                fh.write(key)
                fh.write(cls)
                fh.write(value)
        os.replace(tmp, fn)
        self.runs.append(fn)
        self.stats["bytes"] += self.pending_bytes
        self.pending = []
        self.pending_bytes = 0

    def close(self):
        self.spill()
        self.stats["runs"] = len(self.runs)
        with open(done_marker(self.runs_dir, self.worker), "w") as fh:
            json.dump(self.stats, fh)
        return self.stats

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if exc[0] is None:
            self.close()


def read_buffer(runs):
    """The read buffer for each of `runs` run files merged at once: the
    budget (``LUX_LMDB_READ_MB``, else `READ_MB`) shared between them,
    no less than `MIN_READ_BUFFER` and no more than `MAX_READ_BUFFER`."""
    total = int(float(os.getenv("LUX_LMDB_READ_MB") or READ_MB) * 1024 * 1024)
    return max(MIN_READ_BUFFER, min(MAX_READ_BUFFER, total // max(runs, 1)))


def read_run(path, buffering=-1):
    """``(key, class, value)`` from a run file, in its (sorted) order.
    `buffering` is the read buffer in bytes (-1: Python's default)."""
    with open(path, "rb", buffering=buffering) as fh:
        if hasattr(os, "posix_fadvise"):
            # Linux: a larger read-ahead window for a file read start to end
            try:
                os.posix_fadvise(fh.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
            except OSError:
                pass
        while True:
            head = fh.read(_FRAME.size)
            if not head:
                return
            kl, cl, vl = _FRAME.unpack(head)
            body = fh.read(kl + cl + vl)
            yield body[:kl], body[kl:kl + cl], body[kl + cl:]


# --- step 2: the store ---------------------------------------------------------


def _map_size(run_bytes):
    """A ceiling for the store's size. Values over about 2kB go to overflow
    pages, rounded up to whole 4kB pages, so up to twice what the runs
    hold, plus the index; ``LUX_LMDB_MAP_GB`` overrides."""
    gb = os.getenv("LUX_LMDB_MAP_GB")
    if gb:
        return int(float(gb) * 1024 ** 3)
    return int(run_bytes * 2.5) + 256 * 1024 * 1024


class _Index:
    """Per-class pending keys, written out a block at a time."""

    def __init__(self, txn_ref, db, key_format):
        self.txn_ref = txn_ref
        self.db = db
        self.key_format = key_format
        self.pending = {}
        self.blocks = 0

    def add(self, cls, key):
        keys = self.pending.setdefault(cls, [])
        if self.key_format == "text" and keys:
            size = sum(len(k) for k in keys) + len(keys) + len(key)
            if size > TEXT_BLOCK_BYTES:
                self.flush(cls)
                keys = self.pending.setdefault(cls, [])
        keys.append(key)
        if self.key_format == "uuid" and len(keys) == UUID_BLOCK:
            self.flush(cls)

    def flush(self, cls):
        keys = self.pending.pop(cls, None)
        if keys:
            sep = b"" if self.key_format == "uuid" else TEXT_SEPARATOR
            self.txn_ref[0].put(cls, sep.join(keys), db=self.db, dupdata=True)
            self.blocks += 1

    def flush_all(self):
        for cls in list(self.pending):
            self.flush(cls)


def build_store(path, runs_dir, key_format, meta=None, remove_runs=True):
    """Merge every run in `runs_dir` into the store at `path` (a
    directory, replaced only once the new one is complete). `meta` adds to
    the store's description. Returns its statistics."""
    if key_format not in KEY_FORMATS:
        raise ValueError(f"unknown key format {key_format!r}")
    runs = sorted(os.path.join(runs_dir, f) for f in _list(runs_dir) if f.endswith(".run"))
    run_bytes = sum(os.path.getsize(r) for r in runs)
    buffering = read_buffer(len(runs))
    logger.info(f"Merging {len(runs):,} runs ({run_bytes / 1024 ** 3:.1f} GB), reading "
                f"{buffering / 1024 ** 2:.0f} MB at a time from each")
    tmp = f"{path}.tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    start = last = time.time()
    env = lmdb.open(tmp, map_size=_map_size(run_bytes), max_dbs=3, metasync=False,
                    sync=False, map_async=True)
    stats = {"records": 0, "duplicates": 0, "unindexed": 0, "classes": {}}
    try:
        data = env.open_db(DATA_DB)
        index_db = env.open_db(INDEX_DB, dupsort=True)
        meta_db = env.open_db(META_DB)
        txn = [env.begin(write=True)]
        index = _Index(txn, index_db, key_format)
        in_batch = 0
        for key, cls, value in heapq.merge(*(read_run(r, buffering) for r in runs),
                                           key=lambda r: r[0]):
            # in key order, so append is always true unless the key repeats
            if not txn[0].put(key, value, db=data, append=True):
                stats["duplicates"] += 1
                continue
            stats["records"] += 1
            if cls:
                index.add(cls, key)
                c = cls.decode("utf-8")
                stats["classes"][c] = stats["classes"].get(c, 0) + 1
            else:
                stats["unindexed"] += 1
            in_batch += 1
            if in_batch >= COMMIT_EVERY:
                txn[0].commit()
                txn[0] = env.begin(write=True)
                in_batch = 0
                if not stats["records"] % 1000000:
                    # the rate since the start, and over the last million:
                    # the overall one hides a step change for a long time
                    now = time.time()
                    el = now - start
                    logger.info(f"{stats['records']:,} records in {el:.0f}s "
                                f"= {stats['records'] / el:,.0f}/s "
                                f"(last million {1000000 / max(now - last, 1e-9):,.0f}/s)")
                    last = now
        index.flush_all()
        stats["index_blocks"] = index.blocks
        description = {
            "format": FORMAT_VERSION, "keys": key_format, "compression": "zlib",
            "zlib_level": ZLIB_LEVEL,
            "index": ({"block_keys": UUID_BLOCK, "key_bytes": 16} if key_format == "uuid"
                      else {"block_bytes": TEXT_BLOCK_BYTES, "separator": "\\u0000"}),
            "records": stats["records"], "classes": stats["classes"],
            "unindexed": stats["unindexed"],
            "created": time.strftime("%Y-%m-%dT%H:%M:%S"),
            **(meta or {}),
        }
        txn[0].put(META_KEY, json.dumps(description).encode("utf-8"), db=meta_db)
        txn[0].commit()
        env.sync(True)
    except lmdb.MapFullError:
        env.close()
        shutil.rmtree(tmp, ignore_errors=True)
        raise RuntimeError(f"the store outgrew its {_map_size(run_bytes) / 1024 ** 3:.1f} GB "
                           f"map; set LUX_LMDB_MAP_GB higher and run the merge again "
                           f"(lux export ... --merge-only)") from None
    except BaseException:
        env.close()
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    env.close()
    shutil.rmtree(path, ignore_errors=True)
    os.replace(tmp, path)
    if remove_runs:
        shutil.rmtree(runs_dir, ignore_errors=True)
    stats.update(path=path, runs=len(runs), seconds=round(time.time() - start, 1))
    if stats["duplicates"]:
        logger.warning(f"{path}: {stats['duplicates']:,} duplicate keys, the first kept")
    logger.info(f"{path}: {stats['records']:,} records, {len(stats['classes'])} classes, "
                f"{stats['index_blocks']:,} index blocks in {stats['seconds']}s")
    return stats


# --- reading -------------------------------------------------------------------


class DistributionStore:
    """A built store, read-only.

        with DistributionStore(path) as st:
            st.meta["records"]
            for key in st.keys("Person"): ...          # in key order
            for key, record in st.records("Concept"): ...
            st.get("4f0c...-...")                       # uuid or source:id text
    """

    def __init__(self, path):
        self.env = lmdb.open(path, max_dbs=3, readonly=True, lock=False, readahead=False)
        self.data = self.env.open_db(DATA_DB)
        self.index = self.env.open_db(INDEX_DB, dupsort=True)
        meta_db = self.env.open_db(META_DB)
        with self.env.begin() as txn:
            raw = txn.get(META_KEY, db=meta_db)
        self.meta = json.loads(raw) if raw else {}
        self.key_format = self.meta.get("keys", "uuid")

    def _key(self, key):
        if isinstance(key, bytes):
            return key
        return uuid.UUID(key).bytes if self.key_format == "uuid" else key.encode("utf-8")

    def get(self, key):
        """The record (parsed) for a key (bytes, or its text form), or None."""
        with self.env.begin() as txn:
            value = txn.get(self._key(key), db=self.data)
        return None if value is None else ujson.loads(zlib.decompress(value))

    def classes(self):
        return dict(self.meta.get("classes", {}))

    def keys(self, cls):
        """Every key of class `cls`, in key order, as bytes."""
        with self.env.begin(buffers=True) as txn:
            cur = txn.cursor(db=self.index)
            if cur.set_key(cls.encode("utf-8")):
                for block in cur.iternext_dup():
                    yield from split_block(self.key_format, block)

    def records(self, cls):
        """``(key text, record)`` for every record of class `cls`."""
        with self.env.begin(buffers=True) as txn:
            for key in self.keys(cls):
                value = txn.get(key, db=self.data)
                yield decode_key(self.key_format, key), ujson.loads(zlib.decompress(value))

    def close(self):
        self.env.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
