# Index and equivalence stores on LMDB.
#
# This used to subclass `lmdbm.Lmdb`, a MutableMapping that opens a fresh
# transaction for every single operation. That is the wrong shape for how the
# pipeline uses these stores: the reconcilers open them read-only in 24 worker
# processes and then do tens of millions of point lookups against a database
# that cannot change underneath them, so the per-call transaction was pure
# overhead -- a reader-table slot, a meta-page read and two Python objects per
# get. `lmdbm` is no longer imported anywhere; `lmdb` is used directly.
#
# What is different here:
#
#   * One read transaction per instance, created lazily and reused. Opened
#     read-only the data is immutable for the life of the process, so that
#     snapshot is always current and can live as long as the object. A
#     writable open drops it after each write, because an LMDB read
#     transaction is a point-in-time snapshot and genuinely cannot see writes
#     that came after it.
#   * `__contains__` asks a cursor's set_key(), which answers from the B-tree
#     without copying the value out of the map.
#   * `get()` returns None for a miss, so callers can stop paying for the
#     `if k in idx: idx[k]` double lookup.
#   * `clear()` is one MDB_DROP. Inherited from MutableMapping it was
#     `popitem()` in a loop -- a read transaction to find a key, another to
#     fetch its value and a write to delete it, per entry -- and every index
#     loader calls it on an index with millions of entries before rebuilding.
#   * `update()` sorts by encoded key and writes in bounded putmulti chunks,
#     so a bulk load walks up the B-tree in order instead of scattering page
#     splits through the file, and no single transaction has to hold the whole
#     input as dirty pages.
#   * Failing to grow the map raises instead of calling sys.exit(), which
#     `lmdbm` did -- taking the worker down with a bare exit code in the middle
#     of a build.
#
# The on-disk format is unchanged: the unnamed main database, with keys and
# values encoded exactly as the subclasses below already encoded them. Existing
# .lmdb files open with this code and files written by it open with the old
# code.

from pathlib import Path

import lmdb
import ujson as json

### Storage Layer

# LMDB's own ceiling is 511 bytes (env.max_key_size()). This stays at 500
# because the index loaders check their names against 500 before writing, and
# moving it would change which names get dropped.
MAX_KEY_BYTES = 500

# Pairs per write transaction in update(). Large enough that the per-commit
# cost is noise against the sort, small enough that a 40M-entry bulk load
# never asks one transaction to hold the lot as dirty pages.
WRITE_CHUNK = 100_000

# Doublings of map_size to try before giving up on a MapFullError.
GROW_ATTEMPTS = 12


def remove_lmdb(path, missing_ok=True):
    """Delete an LMDB directory, as the "n" open flag needs."""
    base = Path(path)
    for name in ("data.mdb", "lock.mdb"):
        try:
            (base / name).unlink()
        except FileNotFoundError:
            if not missing_ok:
                raise
    try:
        base.rmdir()
    except FileNotFoundError:
        if not missing_ok:
            raise


class Lmdb(object):
    """Dict-like LMDB store. Subclasses supply the key/value codecs.

    Not thread-safe: the reused read transaction and its cursor are per
    instance, not per thread. Every caller in this pipeline is a process, so
    that is the trade being made deliberately. Open one instance per process.
    """

    def __init__(self, env, autogrow=True, writable=False):
        self.env = env
        self.autogrow = autogrow
        self.writable = writable
        self._rtxn = None
        self._rcursor = None
        self._db = None

    @classmethod
    def open(cls, file, flag="r", mode=0o755, map_size=2**20, autogrow=True, **kwargs):
        """Open the database `file`.

        `flag`: r (read only, existing), w (read and write, existing),
                c (read, write, create if not exists), n (overwrite existing)
        `map_size`: initial size. For a read-only open this may be smaller
                than the file -- LMDB raises the mapping to at least what the
                meta page says is in use -- which is why the 1MB default has
                always worked for readers.
        `autogrow`: double map_size and retry on MapFullError. Leave this off
                for multi-process write access; resizing is not coordinated
                between processes.
        `**kwargs`: passed through to lmdb.open (readahead, writemap, ...).
        """
        if flag == "r":
            writable = False
            create = False
            readonly = True
        elif flag == "w":
            writable = True
            create = False
            readonly = False
        elif flag == "c":
            writable = True
            create = True
            readonly = False
        elif flag == "n":
            remove_lmdb(file)
            writable = True
            create = True
            readonly = False
        else:
            raise ValueError(f"Invalid flag {flag!r}: expected one of r, w, c, n")

        env = lmdb.open(
            file,
            map_size=map_size,
            max_dbs=1,
            readonly=readonly,
            create=create,
            mode=mode,
            **kwargs,
        )
        return cls(env, autogrow=autogrow, writable=writable)

    # --- codecs: the subclasses below are the whole of the format ---

    def _pre_key(self, key):
        if isinstance(key, bytes):
            return key
        elif isinstance(key, str):
            return key.encode("utf-8")
        raise TypeError(key)

    def _post_key(self, key):
        return key

    def _pre_value(self, value):
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode("utf-8")
        raise TypeError(value)

    def _post_value(self, value):
        return value

    # --- transaction reuse ---

    def _reader(self):
        """The instance's read transaction, opened on first use.

        Read-only: this is the point of the module. The snapshot is taken once
        and every subsequent lookup is a B-tree walk in already-mapped memory.
        """
        if self._rtxn is None:
            self._rtxn = self.env.begin(write=False)
        return self._rtxn

    def _point_cursor(self):
        """A cursor kept for point lookups only. Iteration uses its own
        transaction and cursor, so a scan can never move this one out from
        under a `in` test."""
        if self._rcursor is None:
            self._rcursor = self._reader().cursor()
        return self._rcursor

    def _drop_reader(self):
        """Discard the read snapshot. Required after any write -- the snapshot
        predates it and cannot see it -- and before set_mapsize, which
        replaces the mapping underneath anything holding a pointer into it.
        Aborting also frees the reader-table slot and unpins the pages the
        transaction was keeping alive, which is what lets a writer reclaim
        free space."""
        if self._rcursor is not None:
            self._rcursor.close()
            self._rcursor = None
        if self._rtxn is not None:
            self._rtxn.abort()
            self._rtxn = None

    def _write(self, fn):
        """Run `fn(txn)` in a write transaction, growing the map and retrying
        if it turns out to be too small.

        Every `fn` passed here is an idempotent overwrite or delete, so a retry
        after growing re-applies the same work to the same effect.
        """
        if not self.writable:
            # lmdb's own error here is a bare "Permission denied", which sends
            # people looking at file modes rather than at the open flag.
            raise lmdb.ReadonlyError(
                f"{self.env.path()} was opened read-only (flag 'r'); "
                f"reopen with 'w' or 'c' to write to it"
            )
        for _attempt in range(GROW_ATTEMPTS):
            try:
                with self.env.begin(write=True) as txn:
                    result = fn(txn)
                self._drop_reader()
                return result
            except lmdb.MapFullError:
                self._drop_reader()
                if not self.autogrow:
                    raise
                # the setter drops the reader again; harmless and keeps the
                # invariant in one place
                self.map_size = self.map_size * 2
        raise lmdb.MapFullError(
            f"Could not grow {self.env.path()} in {GROW_ATTEMPTS} doublings "
            f"(map_size is now {self.map_size} bytes). Is there disk space?"
        )

    def _main_db(self):
        # The unnamed main database -- the one lmdbm used, which is what keeps
        # existing files readable. Only drop() needs an explicit handle.
        if self._db is None:
            self._drop_reader()
            self._db = self.env.open_db()
        return self._db

    @property
    def map_size(self):
        return self.env.info()["map_size"]

    @map_size.setter
    def map_size(self, value):
        self._drop_reader()
        self.env.set_mapsize(value)

    # --- reads ---

    def _lookup_key(self, key):
        """Encode a key for a lookup that is allowed to answer "absent".

        Returns None when the key cannot be in the store at all. An over-long
        key is reported rather than silently treated as a miss: a caller that
        reads a miss here may go on to mint a duplicate identity. An empty or
        whitespace key is a plain absence -- LMDB rejects a zero-length key
        outright, so it has to be caught before it reaches the transaction.
        """
        if not str(key).strip():
            return None
        try:
            return self._pre_key(key)
        except ValueError as e:
            print(f"\n*** LMDB lookup got {e} for '{str(key)[:100]}' ***")
            return None

    def __getitem__(self, key):
        value = self._reader().get(self._pre_key(key))
        if value is None:
            raise KeyError(key)
        return self._post_value(value)

    def get(self, key, default=None):
        """One lookup, `default` for a miss.

        This is what `if k in idx: idx[k]` should be: that pattern cost two
        lookups in two transactions, and three when the value did not unpack.
        """
        k = self._lookup_key(key)
        if k is None:
            return default
        value = self._reader().get(k)
        if value is None:
            return default
        return self._post_value(value)

    def __contains__(self, key):
        k = self._lookup_key(key)
        if k is None:
            return False
        # set_key positions the cursor and answers from the B-tree; unlike
        # get() it never copies the value out of the map.
        return self._point_cursor().set_key(k)

    def __len__(self):
        return self._reader().stat()["entries"]

    def __bool__(self):
        return self.__len__() > 0

    def keys(self):
        # A full scan gets its own transaction: it is one per scan rather than
        # one per record, and keeping it off the shared snapshot means a scan
        # neither disturbs nor is disturbed by the point-lookup cursor.
        with self.env.begin() as txn:
            for key in txn.cursor().iternext(keys=True, values=False):
                yield self._post_key(key)

    def values(self):
        with self.env.begin() as txn:
            for value in txn.cursor().iternext(keys=False, values=True):
                yield self._post_value(value)

    def items(self):
        with self.env.begin() as txn:
            for key, value in txn.cursor().iternext(keys=True, values=True):
                yield (self._post_key(key), self._post_value(value))

    def __iter__(self):
        return self.keys()

    # --- writes ---

    def __setitem__(self, key, value):
        # Encode before opening the transaction: the codecs below reject bad
        # values (a tab inside a TabLmdb member), and that must raise without
        # having started a write.
        k = self._pre_key(key)
        v = self._pre_value(value)
        self._write(lambda txn: txn.put(k, v))

    def __delitem__(self, key):
        # Deleting an absent key is not an error, as it was not before.
        k = self._pre_key(key)
        self._write(lambda txn: txn.delete(k))

    def pop(self, key, default=None):
        k = self._pre_key(key)
        value = self._write(lambda txn: txn.pop(k))
        if value is None:
            return default
        return self._post_value(value)

    def update(self, other=(), **kwds):
        """Bulk write, sorted by encoded key and committed in chunks.

        putmulti in ascending key order fills B-tree pages instead of
        splitting them repeatedly, which is most of the cost of building one
        of these indexes from a full source scan. UTF-8 preserves code-point
        order, so sorting the encoded keys is the same order the callers'
        `dict(sorted(...))` was reaching for, and is the order LMDB wants.

        Everything is encoded before anything is written, so a value the codec
        rejects still fails with the store untouched.

        LMDB's MDB_APPEND would be faster again, but py-lmdb documents that an
        out-of-order key under it corrupts the database rather than raising.
        That is not a trade worth taking for an index rebuilt from a 40M-record
        scan.
        """
        pre_k, pre_v = self._pre_key, self._pre_value
        if hasattr(other, "keys"):
            items = [(pre_k(k), pre_v(other[k])) for k in other.keys()]
        else:
            items = [(pre_k(k), pre_v(v)) for k, v in other]
        if kwds:
            items.extend((pre_k(k), pre_v(v)) for k, v in kwds.items())
        items.sort(key=lambda kv: kv[0])

        for start in range(0, len(items), WRITE_CHUNK):
            chunk = items[start : start + WRITE_CHUNK]
            self._write(lambda txn, c=chunk: self._putmulti(txn, c))

    @staticmethod
    def _putmulti(txn, chunk):
        with txn.cursor() as curs:
            curs.putmulti(chunk)

    def clear(self):
        """Empty the store in one operation.

        MutableMapping's inherited clear() was popitem() in a loop: three
        transactions per entry, over indexes with millions of entries, every
        time a loader started. MDB_DROP frees the whole B-tree at once and
        returns the pages for reuse.
        """
        db = self._main_db()
        self._write(lambda txn: txn.drop(db, delete=False))

    # --- lifecycle ---

    def commit(self):
        # Writes commit as they are made; this exists because callers on the
        # generic cache interface call it.
        pass

    def sync(self):
        self.env.sync()

    def close(self):
        self._drop_reader()
        self.env.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class StringLmdb(Lmdb):
    # key is string, value is string
    def _pre_key(self, value):
        val = value.encode("utf-8")
        if len(val) > MAX_KEY_BYTES:
            raise ValueError(f"LMDB cannot have keys longer than {MAX_KEY_BYTES} bytes")
        return val

    def _post_key(self, value):
        return value.decode("utf-8")

    def _pre_value(self, value):
        return value.encode("utf-8")

    def _post_value(self, value):
        return value.decode("utf-8")


class TabLmdb(StringLmdb):
    # key is string, value is tab separated __strings__
    # eg URI\tURI\tURI or URI\tType
    # NB: a single-member list/set round-trips back as a bare str (callers
    # must handle both), and tab is the separator -- so members containing
    # a literal tab would silently split into phantom members on read.
    # Reject them at write time instead.

    def _pre_value(self, value):
        if type(value) == str:
            if "\t" in value:
                raise ValueError(f"TabLmdb member contains a tab: {value!r}")
            return value.encode("utf-8")
        elif type(value) in [list, set]:
            try:
                for v in value:
                    if "\t" in v:
                        raise ValueError(f"TabLmdb member contains a tab: {v!r}")
                return "\t".join(value).encode("utf-8")
            except ValueError:
                raise
            except Exception:
                # a non-str member: `"\t" in v` is a TypeError, and the caller
                # wants the same ValueError every other bad value gets
                raise ValueError(f"TabLmdb cannot accept {value}")
        elif type(value) == bytes:
            return value
        else:
            # Don't accept ints/floats/dicts
            raise ValueError(f"TabLmdb cannot accept {type(value)}: {value}")

    def _post_value(self, value):
        value = value.decode("utf-8")
        if "\t" in value:
            value = value.split("\t")
        return value


class JsonLmdb(StringLmdb):
    # key is string, value is arbitrary JSON
    # but adds the cost of dumps/loads if really just strings

    def _pre_value(self, value):
        return json.dumps(value).encode("utf-8")

    def _post_value(self, value):
        return json.loads(value.decode("utf-8"))
