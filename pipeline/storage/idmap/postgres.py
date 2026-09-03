"""Identity map on postgres, with an optional read-only LMDB snapshot.

Same interface as storage.idmap.redis.IdMap, so it drops in via storeClass in
map_idmap.json and nothing else has to change.

Two differences of substance:

*   The reverse direction is not stored. A YUID's members are the rows that
    carry it -- `SELECT uri FROM idmap WHERE yuid = $1` -- so the forward
    pointer and the member set cannot disagree, and the union that redis needs
    a fifty-retry WATCH/MULTI loop for is one UPDATE.

*   Lookups can be served from a frozen LMDB snapshot of the whole map instead
    of the database. That is only safe while nothing is writing, which is
    exactly the phases that already call enable_memory_cache() -- so the
    snapshot has an explicit on switch, and writing with it on is an error
    rather than a stale read.

The connection is this store's own, in autocommit. Identity is low-volume
next to the record caches and has to be visible to the other workers
immediately, so it deliberately does not join their deferred batches; it also
means a lookup here never leaves a transaction open holding an ACCESS SHARE
lock.
"""

import os
import random
import time
import uuid
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from pipeline.storage.uricache import URICache, _MISSING

# Two workers merging overlapping classes can be picked as a deadlock victim.
# Same treatment as the record caches: it is transient, so retry.
DEADLOCK_RETRIES = 5
DEADLOCK_BACKOFF = 0.05

# LMDB refuses keys over 511 bytes; a handful of URIs are longer than that.
# They simply aren't in the snapshot and fall through to postgres.
MAX_SNAPSHOT_KEY = 500

# A snapshot normally holds every type and says so with "*". These are the
# fallback for a file that predates that marker, and what --types narrows to
# if you want a smaller file.
#
# Type is not a correctness question here. The snapshot is rebuilt from scratch
# each run, after identity resolution, and read only by phases where writing
# raises -- so nothing in it can go stale inside its own lifetime, however
# volatile the entity. Narrowing it only lowers the hit rate: scoped to objects
# and works it served 7.9% of the merge read path, because a record's
# references are people and concepts whatever the record is.
SNAPSHOT_TYPES = ("HumanMadeObject", "DigitalObject", "VisualItem", "LinguisticObject")

SCHEMA = """
CREATE TABLE IF NOT EXISTS {idmap} (
    uri   TEXT PRIMARY KEY,
    yuid  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS {idmap}_yuid_idx ON {idmap} (yuid);
CREATE TABLE IF NOT EXISTS {yuids} (
    yuid    TEXT PRIMARY KEY,
    token   TEXT,
    minted  TIMESTAMP DEFAULT now()
);
CREATE INDEX IF NOT EXISTS {yuids}_token_idx ON {yuids} (token);
"""


class IdMap(object):
    def __init__(self, config):
        self.configs = config["all_configs"]
        self.table = config.get("tableName", "idmap")
        self.yuid_table = f"{self.table}_yuid"

        self.prefix_map_out = {"yuid": self.configs.internal_uri}
        for cf in self.configs.external.values():
            self.prefix_map_out[cf["name"]] = cf["namespace"]
        self.prefix_map_in = {v: k for (k, v) in self.prefix_map_out.items()}

        self.memory_cache_enabled = False
        self.memory_cache = URICache(capacity=config.get("memoryCacheSize", 200000))
        self.clean_on_remove = False

        with open(os.path.join(self.configs.data_dir, "idmap_update_token.txt")) as fh:
            token = fh.read().strip()
        if not token.startswith("__") or not token.endswith("__"):
            print("Idmap Update Token is badly formed, should be 8 character date with leading/trailing __")
            raise ValueError("update token")
        self.update_token = token

        # Connection details come from the caches config, because that is the
        # postgres the pipeline already talks to and the identity map belongs
        # in it. They deliberately do NOT come from this map's own config:
        # instantiate_map() passes map_idmap.json, whose host/port/db are
        # redis's (localhost:6379), so reading them here would point the
        # backend at redis' port. Override with pgHost/pgPort/pgUser/pgDbname
        # in the map config if the map really does live somewhere else.
        db = dict(getattr(self.configs, "caches", {}) or {})
        kw = {"user": config.get("pgUser") or db.get("user") or os.getenv("USER"),
              "dbname": config.get("pgDbname") or db.get("dbname") or os.getenv("USER"),
              "keepalives": 1, "keepalives_idle": 30}
        host = config.get("pgHost", db.get("host", ""))
        if host:
            kw["host"] = host
            kw["port"] = int(config.get("pgPort") or db.get("port", 5432))
            if db.get("password"):
                kw["password"] = db["password"]
        self.conn = psycopg2.connect(**kw)
        # Every statement its own transaction: identity has to be visible to
        # the other workers the moment it is assigned, and a read must not
        # leave a snapshot or a lock behind.
        self.conn.autocommit = True
        self._ensure_schema()
        # One cursor, reused: these calls run millions of times per build and
        # a fresh cursor per lookup is pure overhead in autocommit
        self._hot = self.conn.cursor()
        self._prepare()

        self.snapshot = None
        self.snapshot_txn = None
        self.snapshot_enabled = False
        self.snapshot_path = config.get("snapshotPath", "")
        self.snapshot_types = set(SNAPSHOT_TYPES)
        self.snapshot_slugs = set()
        self._route_all = False
        self._route_suffixes = ()
        self._route_prefixes = ()
        # hits / minted since the snapshot was taken / sent to postgres by type
        self.snapshot_stats = {"hit": 0, "miss": 0, "routed": 0}

    # ------------------------------------------------------------------ setup

    def _ensure_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(SCHEMA.format(idmap=self.table, yuids=self.yuid_table))

    def _cursor(self):
        return self.conn.cursor()

    # Single-row statements, prepared once per connection. Without this
    # psycopg2 hands postgres the full text to parse and plan on every call:
    # 34us against 14us for the same lookup, and identity does several per
    # record across tens of millions of records.
    PREPARED = {
        "fwd": "SELECT yuid FROM {t} WHERE uri = $1",
        "rev": "SELECT uri FROM {t} WHERE yuid = $1",
        "has_uri": "SELECT 1 FROM {t} WHERE uri = $1",
        "has_yuid": "SELECT 1 FROM {y} WHERE yuid = $1",
        "member_count": "SELECT count(*) FROM {t} WHERE yuid = $1",
        "token_is": "SELECT 1 FROM {y} WHERE yuid = $1 AND token = $2",
        "token_set": "INSERT INTO {y} (yuid, token) VALUES ($1, $2) "
                     "ON CONFLICT (yuid) DO UPDATE SET token = EXCLUDED.token "
                     "WHERE {y}.token IS DISTINCT FROM EXCLUDED.token",
        "del_uri": "DELETE FROM {t} WHERE uri = $1",
    }

    def _prepare(self):
        self._stmt = {}
        for name, sql in self.PREPARED.items():
            stmt = f"idmap_{name}_{id(self) & 0xffff:x}"
            try:
                self._hot.execute(f"PREPARE {stmt} AS "
                                  + sql.format(t=self.table, y=self.yuid_table))
                self._stmt[name] = stmt
            except psycopg2.Error as e:
                # Not fatal: the callers fall back to plain SQL, just slower
                print(f"idmap: could not prepare {name}: {str(e).strip()[:80]}")
                self.conn.rollback()

    def _run(self, name, params):
        """EXECUTE a prepared statement, returning the cursor to fetch from."""
        placeholders = ", ".join(["%s"] * len(params))
        self._hot.execute(f"EXECUTE {self._stmt[name]} ({placeholders})", params)
        return self._hot

    @contextmanager
    def _streaming(self, name):
        """A server-side cursor, which needs a transaction to live in -- this
        connection is otherwise in autocommit, where there isn't one."""
        self.conn.autocommit = False
        try:
            cur = self.conn.cursor(name=name)
            cur.itersize = 10000
            try:
                yield cur
            finally:
                cur.close()
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = True

    def shutdown(self):
        if getattr(self, "_hot", None) is not None:
            try:
                self._hot.close()
            except psycopg2.Error:
                pass
            self._hot = None
        if self.snapshot_txn is not None:
            self.snapshot_txn.abort()
            self.snapshot_txn = None
        if self.snapshot is not None:
            self.snapshot.close()
            self.snapshot = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    # --------------------------------------------------- prefix compression
    # Identical to the redis backend: callers pass and receive full URIs, and
    # what is stored is the CURIE form. Keeping the same on-disk shape means a
    # fingerprint or an export is comparable across the two.

    def _manage_key_in(self, key):
        if key.startswith("http"):
            for (k, v) in self.prefix_map_in.items():
                if key.startswith(k):
                    key = key.replace(k, f"{v}:")
                    break
        return key

    def _manage_key_out(self, key):
        if not key.startswith("http"):
            for (k, v) in self.prefix_map_out.items():
                if key.startswith(f"{k}:"):
                    key = key.replace(f"{k}:", v)
                    break
        return key

    _manage_value_in = _manage_key_in
    _manage_value_out = _manage_key_out

    # --------------------------------------------------------------- caching

    def enable_memory_cache(self):
        self.memory_cache_enabled = True

    def disable_memory_cache(self):
        self.memory_cache_enabled = False

    def enable_snapshot(self):
        """Serve lookups from the LMDB snapshot instead of the database.

        Read-only mode: any write while this is on raises, rather than leaving
        a stale entry to be read by the next worker. Turn it on for the phases
        that only read the map -- merge, export -- and off before anything
        assigns identity.

        The snapshot carries the update token it was built from and is refused
        if that doesn't match this build, so last build's file cannot quietly
        answer this build's questions."""
        if self.snapshot is not None:
            self.snapshot_enabled = True
            return True
        if not self.snapshot_path:
            print("idmap: no snapshotPath configured")
            return False
        if not os.path.exists(self.snapshot_path):
            print(f"idmap: no snapshot at {self.snapshot_path}")
            return False
        try:
            import lmdb
        except ImportError:
            print("idmap: lmdb not installed, staying on postgres")
            return False
        env = lmdb.open(self.snapshot_path, readonly=True, subdir=True,
                        lock=False, max_readers=256)
        with env.begin(buffers=False) as txn:
            token = txn.get(b"__token__")
            types = txn.get(b"__types__")
            slugs = txn.get(b"__slugs__")
        token = token.decode("utf-8") if token else None
        if token != self.update_token:
            env.close()
            print(f"idmap: snapshot at {self.snapshot_path} was built for "
                  f"{token}, this build is {self.update_token} -- refusing it; "
                  f"rebuild with make-idmap-snapshot.py")
            return False
        # The file declares which types it holds, so changing the selection is
        # a rebuild rather than a code change -- and the reader can never
        # route a lookup to a file that was not built to answer it.
        self._route_all = types == b"*"
        if self._route_all:
            self.snapshot_types = {"*"}
            self.snapshot_slugs = {"*"}
        else:
            self.snapshot_types = set(types.decode("utf-8").split("\t")) if types else set(SNAPSHOT_TYPES)
            self.snapshot_slugs = set(slugs.decode("utf-8").split("\t")) if slugs else set()
        self._route_suffixes = tuple(f"##qua{t}" for t in sorted(self.snapshot_types))
        self._route_prefixes = tuple(f"yuid:{s}/" for s in sorted(self.snapshot_slugs))
        self.snapshot = env
        # One long-lived read transaction rather than one per lookup. The file
        # is immutable and has no writer, so there is nothing to miss by
        # holding it, and env.begin() per call was ~50us -- more than the
        # postgres query it was meant to avoid.
        self.snapshot_txn = env.begin(buffers=False)
        self.snapshot_enabled = True
        print(f"idmap: reading from snapshot {self.snapshot_path} ({token}), "
              f"types: {'all' if self._route_all else ', '.join(sorted(self.snapshot_types))}")
        return True

    def disable_snapshot(self):
        self.snapshot_enabled = False
        if self.snapshot_txn is not None:
            self.snapshot_txn.abort()
            self.snapshot_txn = None

    def _no_writes_while_reading(self, op):
        if self.snapshot_enabled:
            raise RuntimeError(
                f"idmap.{op}() called while the LMDB snapshot is enabled. The "
                f"snapshot is a frozen copy, so a write now would be invisible "
                f"to every reader using it. Call disable_snapshot() first.")

    def _in_snapshot(self, ikey):
        """Whether this key is one the snapshot was built to answer.

        Decided from the key alone, both directions: an external identifier
        carries its type (wd:Q42##quaPerson) and a YUID carries the slug that
        mint() built it from (yuid:object/<uuid>). So a lookup for a type the
        file does not hold goes straight to postgres instead of probing LMDB
        and missing -- the routing costs nothing and there is no wrong guess
        to pay for."""
        # One endswith/startswith against a precomputed tuple, both C-level
        # and allocation-free. Splitting the key per lookup instead cost more
        # than the tier saved on a workload where most keys are out of scope:
        # this check runs on every key, hit or miss.
        if len(ikey) > MAX_SNAPSHOT_KEY:
            return False
        if self._route_all:
            # A whole-map snapshot has nothing to route around, so the check
            # that runs on every key costs one length comparison
            return True
        if ikey.startswith("yuid:"):
            return ikey.startswith(self._route_prefixes)
        return ikey.endswith(self._route_suffixes)

    def _snapshot_get(self, ikey):
        if not self.snapshot_enabled or self.snapshot is None:
            return _MISSING
        if not self._in_snapshot(ikey):
            self.snapshot_stats["routed"] += 1
            return _MISSING
        val = self.snapshot_txn.get(ikey.encode("utf-8"))
        if val is None:
            # In scope but absent: minted after the snapshot was taken. Rare,
            # and postgres still has the answer -- watch this number, because
            # a large one means the snapshot is too old to be earning its keep.
            self.snapshot_stats["miss"] += 1
            return _MISSING
        self.snapshot_stats["hit"] += 1
        val = val.decode("utf-8")
        if ikey.startswith("yuid:"):
            return {self._manage_value_out(v) for v in val.split("\t") if v}
        return self._manage_value_out(val)

    def _snapshot_get_many(self, ikeys):
        """Probe the snapshot for a whole batch in one read transaction.

        get_multi used to call _snapshot_get per key, which opened an LMDB
        transaction each time -- enough overhead to make the tier slower than
        not having one (137 us/key against 73 us/key straight to postgres on a
        merge-shaped workload). One transaction for the batch is the whole
        point of a memory-mapped store."""
        out = {}
        if not self.snapshot_enabled or self.snapshot is None:
            return out
        in_scope = [k for k in ikeys if self._in_snapshot(k)]
        self.snapshot_stats["routed"] += len(ikeys) - len(in_scope)
        if not in_scope:
            return out
        get = self.snapshot_txn.get
        for ikey in in_scope:
            val = get(ikey.encode("utf-8"))
            if val is None:
                self.snapshot_stats["miss"] += 1
                continue
            self.snapshot_stats["hit"] += 1
            val = val.decode("utf-8")
            if ikey.startswith("yuid:"):
                out[ikey] = {self._manage_value_out(v) for v in val.split("\t") if v}
            else:
                out[ikey] = self._manage_value_out(val)
        return out

    def snapshot_report(self):
        s = self.snapshot_stats
        total = sum(s.values())
        if not total:
            return "idmap snapshot: unused"
        return (f"idmap snapshot: {s['hit']:,} served ({s['hit'] / total * 100:.1f}%), "
                f"{s['routed']:,} routed to postgres by type, "
                f"{s['miss']:,} in scope but absent (minted since the snapshot)")

    # ------------------------------------------------------------------ reads

    def _check_qua(self, key, typ):
        if typ in self.configs.ok_record_types:
            return self.configs.make_qua(key, typ)
        elif typ:
            raise ValueError(typ)
        elif not self.configs.is_qua(key) and self.prefix_map_out["yuid"] not in key:
            raise ValueError(f"Need a type: {key}")
        return key

    def get(self, key, typ=""):
        key = self._check_qua(key, typ)
        ikey = self._manage_key_in(key)

        if self.memory_cache_enabled:
            maybe = self.memory_cache[ikey]
            if maybe is not self.memory_cache.missing:
                return maybe

        out = self._snapshot_get(ikey)
        if out is _MISSING:
            out = self._db_get(ikey)
        if out is None:
            return None
        if self.memory_cache_enabled:
            self.memory_cache[ikey] = out
        return out

    def _db_get(self, ikey):
        try:
            if ikey.startswith("yuid:"):
                rows = self._run("rev", (ikey,)).fetchall()
                if not rows:
                    return None
                return {self._manage_value_out(r[0]) for r in rows}
            row = self._run("fwd", (ikey,)).fetchone()
            return self._manage_value_out(row[0]) if row else None
        except Exception as e:
            print(f"idmap lookup failed for {ikey}: {e}")
            self.conn.rollback()
            return None

    def get_multi(self, keys, chunk=1000):
        """Resolve many keys in one round trip each way. Same semantics as
        get(); this is the call that makes the database version cheap, so the
        hot loops should prefer it."""
        out = {}
        need_str, need_set = [], []
        candidates = []
        for key in keys:
            if not self.configs.is_qua(key) and self.prefix_map_out["yuid"] not in key:
                raise ValueError(f"Need a type: {key}")
            ikey = self._manage_key_in(key)
            if self.memory_cache_enabled:
                maybe = self.memory_cache[ikey]
                if maybe is not self.memory_cache.missing:
                    out[key] = maybe
                    continue
            candidates.append((key, ikey))

        if self.snapshot_enabled and candidates:
            found = self._snapshot_get_many([ik for _, ik in candidates])
            rest = []
            for key, ikey in candidates:
                if ikey in found:
                    out[key] = found[ikey]
                    if self.memory_cache_enabled:
                        self.memory_cache[ikey] = found[ikey]
                else:
                    rest.append((key, ikey))
            candidates = rest

        for key, ikey in candidates:
            (need_set if ikey.startswith("yuid:") else need_str).append((key, ikey))

        for i in range(0, len(need_str), chunk):
            batch = need_str[i:i + chunk]
            found = {}
            try:
                with self._cursor() as cur:
                    cur.execute(f"SELECT uri, yuid FROM {self.table} WHERE uri = ANY(%s)",
                                ([ik for _, ik in batch],))
                    found = {r[0]: r[1] for r in cur.fetchall()}
            except Exception as e:
                print(f"idmap batch lookup failed ({len(batch)} keys): {e}")
            for key, ikey in batch:
                raw = found.get(ikey)
                v = self._manage_value_out(raw) if raw else None
                out[key] = v
                # Don't cache misses -- they ride along free in the next batch
                # but would evict hits.
                if self.memory_cache_enabled and v is not None:
                    self.memory_cache[ikey] = v

        for i in range(0, len(need_set), chunk):
            batch = need_set[i:i + chunk]
            members = {}
            try:
                with self._cursor() as cur:
                    cur.execute(f"SELECT yuid, uri FROM {self.table} WHERE yuid = ANY(%s)",
                                ([ik for _, ik in batch],))
                    for yuid, uri in cur.fetchall():
                        members.setdefault(yuid, set()).add(self._manage_value_out(uri))
            except Exception as e:
                print(f"idmap batch member lookup failed ({len(batch)} keys): {e}")
            for key, ikey in batch:
                v = members.get(ikey)
                out[key] = v
                if self.memory_cache_enabled and v is not None:
                    self.memory_cache[ikey] = v
        return out

    def has_item(self, key):
        ikey = self._manage_key_in(key)
        # A YUID exists if it is registered, even with no members yet: set()
        # checks this before assigning to it.
        which = "has_yuid" if ikey.startswith("yuid:") else "has_uri"
        return self._run(which, (ikey,)).fetchone() is not None

    def count(self, key):
        """Members of a YUID."""
        ikey = self._manage_key_in(key)
        return self._run("member_count", (ikey,)).fetchone()[0]

    def iter_keys(self, **kw):
        """Every key, both directions, to match the redis backend's view."""
        stamp = f"{time.time()}".replace(".", "_")
        with self._streaming(f"idmap_iter_{stamp}") as cur:
            cur.execute(f"SELECT uri FROM {self.table}")
            for (uri,) in cur:
                yield self._manage_key_out(uri)
        with self._streaming(f"idmap_iter_y_{stamp}") as cur:
            cur.execute(f"SELECT yuid FROM {self.yuid_table}")
            for (yuid,) in cur:
                yield self._manage_key_out(yuid)

    def keys(self, **kw):
        return list(self.iter_keys(**kw))

    def __len__(self):
        """Estimated, not exact -- and deliberately so.

        Redis answers this with DBSIZE in O(1), so callers assume it costs
        nothing. base Mapper.__init__ calls it once per source purely to ask
        whether the map has anything in it at all, and instantiate_all() builds
        a mapper per source: taken literally that is ~30 full counts of a 97M
        row map per process, 7.5s each, 3.7 minutes of startup before any work
        begins. The catalog estimate costs 3ms.

        reltuples is maintained by ANALYZE and autovacuum and drifts between
        them, which is fine for every caller here (a truthiness test and a
        report). It reads -1 on a table that has never been analysed, so a
        non-positive estimate falls back to an existence check rather than
        claiming the map is empty."""
        with self._cursor() as cur:
            cur.execute("SELECT coalesce(sum(reltuples), 0)::bigint FROM pg_class "
                        "WHERE oid IN (to_regclass(%s), to_regclass(%s))",
                        (self.table, self.yuid_table))
            row = cur.fetchone()
            n = int(row[0]) if row and row[0] is not None else 0
            if n > 0:
                return n
            cur.execute(f"SELECT 1 FROM {self.table} LIMIT 1")
            return 1 if cur.fetchone() else 0

    # ----------------------------------------------------------------- writes

    def mint(self, key, slug, typ=""):
        """Assign a new YUID to a key, or adopt the one another worker just
        assigned.

        The INSERT decides the race: whoever gets the row wins, and the value
        that comes back is the YUID to use. In redis this was an SADD followed
        by a separate SET, with a window in between."""
        self._no_writes_while_reading("mint")
        if typ in self.configs.ok_record_types:
            key = self.configs.make_qua(key, typ)
        elif typ:
            raise ValueError(f"Unknown type: {typ}")
        elif not self.configs.is_qua(key):
            raise ValueError(f"Need a type: {key}")

        uu = str(uuid.uuid4())
        base = self.prefix_map_out["yuid"]
        value = f"{base}{slug}/{uu}" if slug else f"{base}{uu}"
        ikey = self._manage_key_in(key)
        ivalue = self._manage_value_in(value)

        with self._cursor() as cur:
            cur.execute(f"INSERT INTO {self.yuid_table} (yuid, token) VALUES (%s, %s) "
                        f"ON CONFLICT (yuid) DO NOTHING", (ivalue, self.update_token))
            cur.execute(
                f"INSERT INTO {self.table} (uri, yuid) VALUES (%s, %s) "
                f"ON CONFLICT (uri) DO UPDATE SET yuid = {self.table}.yuid "
                f"RETURNING yuid", (ikey, ivalue))
            got = cur.fetchone()[0]
        if got != ivalue:
            # Someone else minted for this key first; theirs is the identity.
            # Drop the registry row we speculatively created.
            with self._cursor() as cur:
                cur.execute(f"DELETE FROM {self.yuid_table} WHERE yuid = %s "
                            f"AND NOT EXISTS (SELECT 1 FROM {self.table} WHERE yuid = %s)",
                            (ivalue, ivalue))
            value = self._manage_value_out(got)
        if self.memory_cache_enabled:
            self.memory_cache[ikey] = value
        return value

    def set(self, key, value, typ=""):
        """Assign key to an existing YUID, merging whatever class it was in.

        The merge is the whole point: if key already belongs to another YUID,
        every member of that class moves too, so the two become one. That is a
        single UPDATE here."""
        self._no_writes_while_reading("set")
        if typ in self.configs.ok_record_types:
            key = self.configs.make_qua(key, typ)
        elif typ:
            raise ValueError(typ)
        elif not self.configs.is_qua(key):
            raise ValueError(f"Need a type: {key}")

        if value not in self:
            raise ValueError(f"Unknown YUID {value}")
        ikey = self._manage_key_in(key)
        ivalue = self._manage_value_in(value)

        for attempt in range(DEADLOCK_RETRIES):
            try:
                return self._set_once(ikey, ivalue, key, value)
            except psycopg2.extensions.TransactionRollbackError as e:
                # Two workers merging overlapping classes; one is chosen as
                # the victim. Transient -- run it again.
                if attempt == DEADLOCK_RETRIES - 1:
                    print(f"idmap.set({key}) deadlocked {DEADLOCK_RETRIES} times: {e}")
                    raise
                time.sleep(DEADLOCK_BACKOFF * (attempt + 1) * (0.5 + random.random()))

    def _set_once(self, ikey, ivalue, key, value):
        self.conn.autocommit = False
        try:
            with self._cursor() as cur:
                cur.execute(f"SELECT yuid FROM {self.table} WHERE uri = %s FOR UPDATE", (ikey,))
                row = cur.fetchone()
                old = row[0] if row else None

                if old == ivalue:
                    self.conn.rollback()
                    return

                if old is not None:
                    # Serialise the two classes in a canonical order so two
                    # workers merging the same pair queue instead of deadlock
                    lo, hi = sorted([old, ivalue])
                    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s), hashtext(%s))", (lo, hi))
                    print(f"key: {key} old: {old} new value: {value}")
                    cur.execute(f"SELECT uri FROM {self.table} WHERE yuid = %s", (old,))
                    moved = [r[0] for r in cur.fetchall()]
                    cur.execute(f"UPDATE {self.table} SET yuid = %s WHERE yuid = %s", (ivalue, old))
                    cur.execute(f"DELETE FROM {self.yuid_table} WHERE yuid = %s", (old,))
                else:
                    moved = []

                cur.execute(f"INSERT INTO {self.table} (uri, yuid) VALUES (%s, %s) "
                            f"ON CONFLICT (uri) DO UPDATE SET yuid = EXCLUDED.yuid",
                            (ikey, ivalue))
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.conn.autocommit = True

        if self.memory_cache_enabled:
            for m in moved:
                del self.memory_cache[m]
            if old:
                del self.memory_cache[old]
            del self.memory_cache[ivalue]
            self.memory_cache[ikey] = value
        return True

    def assign_bulk(self, items):
        """Assign whole clusters at once: the identity phase's write path.

        `items` is an iterable of (yuid, members, prior) where members are full
        member URIs and prior maps a member to the YUID it is leaving, if any.
        Every member is pointed at yuid, and yuid is stamped with the current
        update token.

        Detaching a member from its old YUID needs no work here: membership is
        derived from the yuid column, so re-pointing the row *is* the removal.
        On redis it takes an SREM against a second structure that can disagree
        with the first."""
        self._no_writes_while_reading("assign_bulk")
        stats = {"set": 0, "moved": 0, "clusters": 0}
        # Dict rather than list: two rows for one uri in a single
        # execute_values would fail with "cannot affect row a second time",
        # and a duplicate here means a clustering bug, not a load that should
        # die halfway through.
        rows = {}
        yuids = {}
        for yuid, members, prior in items:
            iyuid = self._manage_value_in(yuid)
            yuids[iyuid] = self.update_token
            stats["clusters"] += 1
            for m in members:
                rows[self._manage_key_in(m)] = iyuid
                stats["set"] += 1
                old = (prior or {}).get(m)
                if old and old != yuid:
                    stats["moved"] += 1
        if not rows:
            return stats
        with self._cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {self.yuid_table} (yuid, token) VALUES %s "
                f"ON CONFLICT (yuid) DO UPDATE SET token = EXCLUDED.token "
                f"WHERE {self.yuid_table}.token IS DISTINCT FROM EXCLUDED.token",
                list(yuids.items()), page_size=1000)
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {self.table} (uri, yuid) VALUES %s "
                f"ON CONFLICT (uri) DO UPDATE SET yuid = EXCLUDED.yuid",
                list(rows.items()), page_size=1000)
        if self.memory_cache_enabled:
            for uri in rows:
                del self.memory_cache[uri]
        return stats

    def delete_empty_yuids(self, yuids):
        """Drop the YUIDs among `yuids` that have no members left.

        The redis version has to read each set and check whether anything but
        an update token survives; here the members are rows, so "no members"
        is a NOT EXISTS and the whole sweep is one statement per batch."""
        self._no_writes_while_reading("delete_empty_yuids")
        ikeys = [self._manage_value_in(y) for y in yuids]
        dead = 0
        for i in range(0, len(ikeys), 1000):
            batch = ikeys[i:i + 1000]
            with self._cursor() as cur:
                cur.execute(
                    f"DELETE FROM {self.yuid_table} y WHERE y.yuid = ANY(%s) "
                    f"AND NOT EXISTS (SELECT 1 FROM {self.table} m WHERE m.yuid = y.yuid)",
                    (batch,))
                dead += cur.rowcount
        return dead

    def _add(self, key, *values):
        """Put values into a YUID's class directly. Kept for the callers that
        use it; prefer set(), which handles the merge."""
        self._no_writes_while_reading("_add")
        ikey = self._manage_key_in(key)
        ivalues = [self._manage_value_in(v) for v in values]
        with self._cursor() as cur:
            if ikey.startswith("yuid:"):
                cur.execute(f"INSERT INTO {self.yuid_table} (yuid) VALUES (%s) "
                            f"ON CONFLICT (yuid) DO NOTHING", (ikey,))
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO {self.table} (uri, yuid) VALUES %s "
                    f"ON CONFLICT (uri) DO UPDATE SET yuid = EXCLUDED.yuid",
                    [(v, ikey) for v in ivalues])
            else:
                raise ValueError(f"_add expects a YUID, got {key}")
        return len(ivalues)

    def _remove(self, key, value):
        """Take one member out of a class. The update token is a column now,
        so a token is never a member and this only ever removes real ones."""
        self._no_writes_while_reading("_remove")
        ikey = self._manage_key_in(key)
        ivalue = self._manage_value_in(value)
        if ivalue.startswith("__"):
            # Tokens are a column here, not members, so there is nothing to
            # remove -- but the caller removing one is cleaning up a YUID whose
            # last real member has gone (manage-data.py --delete). Redis
            # dropped the set when it emptied; do the same to the registry row.
            with self._cursor() as cur:
                cur.execute(f"DELETE FROM {self.yuid_table} WHERE yuid = %s AND NOT EXISTS "
                            f"(SELECT 1 FROM {self.table} WHERE yuid = %s)", (ikey, ikey))
            return
        with self._cursor() as cur:
            cur.execute(f"DELETE FROM {self.table} WHERE uri = %s AND yuid = %s", (ivalue, ikey))
            if self.clean_on_remove:
                cur.execute(f"DELETE FROM {self.yuid_table} WHERE yuid = %s AND NOT EXISTS "
                            f"(SELECT 1 FROM {self.table} WHERE yuid = %s)", (ikey, ikey))
        if self.memory_cache_enabled:
            del self.memory_cache[ivalue]
            del self.memory_cache[ikey]

    def delete(self, key, typ=""):
        self._no_writes_while_reading("delete")
        if typ in self.configs.ok_record_types:
            key = self.configs.make_qua(key, typ)
        elif typ:
            raise ValueError(typ)
        elif not self.configs.is_qua(key):
            raise ValueError(f"Need a type: {key}")
        ikey = self._manage_key_in(key)
        if ikey.startswith("yuid:"):
            raise ValueError(f"{key} is a YUID and cannot be manually deleted")
        self._run("del_uri", (ikey,))
        if self.memory_cache_enabled:
            del self.memory_cache[ikey]
        return None

    def _force_delete(self, key, typ=""):
        if typ in self.configs.ok_record_types:
            key = self.configs.make_qua(key, typ)
        return self.delete(key)

    def delete_yuid(self, yuid):
        """Drop a YUID that has no members left. Refuses while any remain."""
        self._no_writes_while_reading("delete_yuid")
        ikey = self._manage_key_in(yuid)
        with self._cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {self.table} WHERE yuid = %s", (ikey,))
            n = cur.fetchone()[0]
            if n:
                raise ValueError(f"delete_yuid({yuid}): {n} real members remain")
            cur.execute(f"DELETE FROM {self.yuid_table} WHERE yuid = %s", (ikey,))
            return cur.rowcount > 0

    # ---------------------------------------------------------- update tokens

    def has_update_token(self, key):
        ikey = self._manage_key_in(key)
        return self._run("token_is", (ikey, self.update_token)).fetchone() is not None

    def add_update_token(self, key):
        """Mark a YUID as seen in this build. A column, so setting it replaces
        the previous build's token instead of having to find and remove it."""
        self._no_writes_while_reading("add_update_token")
        ikey = self._manage_key_in(key)
        # The WHERE in the prepared statement matters more than it looks: this
        # is called for every identity a build touches, and after a full build
        # essentially every yuid already carries the current token (45,513,255
        # of 45,513,275 in the production map). Without it each of those is an
        # UPDATE that writes a new row version and leaves a dead one -- tens of
        # millions of pointless writes and the bloat to match.
        self._run("token_set", (ikey, self.update_token))

    # -------------------------------------------------------------- lifecycle

    def commit(self):
        # autocommit: every write has already landed
        pass

    def clear(self):
        self._no_writes_while_reading("clear")
        with self._cursor() as cur:
            cur.execute(f"TRUNCATE {self.table}, {self.yuid_table}")
        self.memory_cache.clear()

    def _export_state(self):
        state = {}
        with self._streaming("idmap_export") as cur:
            cur.execute(f"SELECT uri, yuid FROM {self.table}")
            for uri, yuid in cur:
                state[self._manage_key_out(uri)] = self._manage_value_out(yuid)
        return state

    def _import_state(self, state):
        self.clear()
        with self._cursor() as cur:
            rows = [(self._manage_key_in(k), self._manage_value_in(v)) for (k, v) in state.items()]
            psycopg2.extras.execute_values(
                cur, f"INSERT INTO {self.yuid_table} (yuid) VALUES %s ON CONFLICT DO NOTHING",
                [(v,) for v in {r[1] for r in rows}])
            psycopg2.extras.execute_values(
                cur, f"INSERT INTO {self.table} (uri, yuid) VALUES %s "
                     f"ON CONFLICT (uri) DO UPDATE SET yuid = EXCLUDED.yuid", rows)

    # ------------------------------------------------------------ dict facade

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __delitem__(self, key):
        return self.delete(key)

    def __contains__(self, key):
        return self.has_item(key)
