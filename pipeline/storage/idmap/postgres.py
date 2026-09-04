"""Identity map on postgres.

Same interface as storage.idmap.redis.IdMap, so it drops in via storeClass in
map_idmap.json and nothing else has to change.

The difference of substance: the reverse direction is not stored. A YUID's
members are the rows that carry it -- `SELECT uri FROM idmap WHERE yuid = $1`
-- so the forward pointer and the member set cannot disagree, and the union
that redis needs a fifty-retry WATCH/MULTI loop for is one UPDATE.

An LMDB read tier used to sit in front of this. It was removed after measuring
it: with shared_buffers sized for the machine, postgres serves the map's hot
indexes from RAM, and routing lookups onto a memory-mapped file the OS had to
fault in cost 7.5 minutes a slice on top of a 5.4 minute build. The tier and a
correctly configured buffer pool solve the same problem, and the buffer pool
wins. See docs/idmap-migration.md.

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

from pipeline.storage.uricache import URICache

# Two workers merging overlapping classes can be picked as a deadlock victim.
# Same treatment as the record caches: it is transient, so retry.
DEADLOCK_RETRIES = 5
DEADLOCK_BACKOFF = 0.05

# One connection per process per target, shared by every map store in this
# module. Each worker already opens two for the record caches (PoolManager);
# a connection per map on top of that would be five a worker, or 240 at 48
# workers against a max_connections of 200. Sharing is safe here because every
# statement these stores issue runs in autocommit -- there is no transaction to
# interleave -- and streaming iteration opens a connection of its own.
#
# Not thread-safe, which matches the pipeline: one thread per process.
_SHARED_CONNECTIONS = {}


def _connect_kwargs(config, configs):
    """Where to find postgres.

    Deliberately not from the map's own config: instantiate_map() passes
    map_*.json, whose host/port are redis's, so reading them here would point
    at redis' port. The caches config is the postgres the pipeline already
    talks to. pgHost/pgPort/pgUser/pgDbname override per map."""
    db = dict(getattr(configs, "caches", {}) or {})
    kw = {"user": config.get("pgUser") or db.get("user") or os.getenv("USER"),
          "dbname": config.get("pgDbname") or db.get("dbname") or os.getenv("USER"),
          "keepalives": 1, "keepalives_idle": 30}
    host = config.get("pgHost", db.get("host", ""))
    if host:
        kw["host"] = host
        kw["port"] = int(config.get("pgPort") or db.get("port", 5432))
        if db.get("password"):
            kw["password"] = db["password"]
    return kw


def _connect(kw):
    key = tuple(sorted(kw.items()))
    conn = _SHARED_CONNECTIONS.get(key)
    if conn is None or conn.closed:
        conn = psycopg2.connect(**kw)
        # Every statement its own transaction: writes have to be visible to the
        # other workers immediately, and a read must not leave a transaction or
        # a lock behind.
        conn.autocommit = True
        _SHARED_CONNECTIONS[key] = conn
    return conn


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

        self._conn_kw = _connect_kwargs(config, self.configs)
        self.conn = _connect(self._conn_kw)
        self._ensure_schema()
        # One cursor, reused: these calls run millions of times per build and
        # a fresh cursor per lookup is pure overhead in autocommit
        self._hot = self.conn.cursor()
        self._prepare()


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
        """A server-side cursor on a connection of its own.

        Named cursors need a transaction to live in, and the shared connection
        is in autocommit -- flipping it would pull the other map stores'
        statements into this transaction. Iteration here is rare and
        long-lived, so a dedicated connection is the right shape anyway."""
        conn = psycopg2.connect(**self._conn_kw)
        try:
            cur = conn.cursor(name=name)
            cur.itersize = 10000
            try:
                yield cur
            finally:
                cur.close()
            conn.commit()
        finally:
            conn.close()

    def shutdown(self):
        """Release this store's own resources.

        The connection is shared with the other map stores in this process, so
        it is deliberately left open -- process exit closes it."""
        if getattr(self, "_hot", None) is not None:
            try:
                self._hot.close()
            except psycopg2.Error:
                pass
            self._hot = None
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
        for key in keys:
            if not self.configs.is_qua(key) and self.prefix_map_out["yuid"] not in key:
                raise ValueError(f"Need a type: {key}")
            ikey = self._manage_key_in(key)
            if self.memory_cache_enabled:
                maybe = self.memory_cache[ikey]
                if maybe is not self.memory_cache.missing:
                    out[key] = maybe
                    continue
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


# A queue table, not a store: every reference is inserted, updated by later
# merges, then deleted when claimed. Postgres leaves a dead tuple behind each
# time, and the claim is a LIMIT scan -- so once the queue drains, that scan
# walks the whole corpse pile to find nothing. Measured on a 400k-reference
# queue drained with autovacuum off: a poll costs 1.1ms bloated against 0.1ms
# after a VACUUM, and it scales with the bloat, not the backlog.
#
# So: tell autovacuum this table is not a normal one. Vacuum at 2% dead rather
# than 20%, don't wait for a large absolute threshold, and don't throttle
# (cost_delay 0). fillfactor leaves room on the page for merge_refs' updates
# to stay HOT, which keeps them out of the index entirely.
#
# Applied in the CREATE, not as an ALTER on every instantiation: 24 workers
# issuing DDL against the same table at startup is its own problem. Existing
# tables need the ALTER once -- see docs/idmap-migration.md.
REF_STORAGE = """fillfactor = 70,
    autovacuum_enabled = true,
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 5000,
    autovacuum_vacuum_cost_delay = 0,
    autovacuum_analyze_scale_factor = 0.05"""

REF_SCHEMA = """
CREATE TABLE IF NOT EXISTS {table} (
    uri    TEXT PRIMARY KEY,
    dist   INTEGER,
    ctype  TEXT
) WITH (""" + REF_STORAGE + """);
"""


class ReferenceMap(object):
    """The reference queue and distance map, on postgres.

    Same interface as storage.idmap.redis.ReferenceMap: a URI maps to a small
    record of fields -- `dist` (how far this reference is from a record being
    processed) and `type` -- and workers claim references off it to process.
    Two instances are configured, all_refs and done_refs.

    The two operations that made the redis version interesting both collapse
    into single statements here:

    *   MERGE_REF_LUA -- dist becomes min(existing, new), type is set only if
        not already set -- is one INSERT ... ON CONFLICT with least() and
        coalesce(). Atomic per row, and a whole record's references go in one
        statement instead of one script call each.

    *   POP_REF_LUA -- read-and-delete so exactly one worker gets a reference
        -- is DELETE ... FOR UPDATE SKIP LOCKED RETURNING, the standard
        postgres work queue. No SCAN cursor to carry between calls, no window
        for a concurrent merge to slip a shorter distance into a row that is
        about to vanish, and workers step over each other's claims rather than
        queueing behind them.

    Reconcile is the one phase where many workers write the *same* rows, which
    is why every statement here commits on its own: locks live microseconds, so
    contention degrades to a brief wait instead of a deadlock. Same reasoning
    as the comment in run-reconcile.py about not deferring commits.
    """

    def __init__(self, config):
        self.configs = config["all_configs"]
        self.table = config.get("tableName") or config["name"]
        self._conn_kw = _connect_kwargs(config, self.configs)
        self.conn = _connect(self._conn_kw)
        with self.conn.cursor() as cur:
            cur.execute(REF_SCHEMA.format(table=self.table))

    def shutdown(self):
        # shared connection: left open deliberately, see _connect()
        self.conn = None

    # The redis backend leaves its prefix maps empty for this class, so these
    # are identity functions there too. Kept so callers that reach for them
    # keep working.
    def _manage_key_in(self, key):
        return key

    _manage_key_out = _manage_key_in

    def _manage_value_in(self, value):
        return str(value) if isinstance(value, int) else value

    def _manage_value_out(self, value):
        if isinstance(value, str) and value.isnumeric():
            return int(value)
        return value

    @staticmethod
    def _fields(dist, ctype):
        """What a caller sees: dist as an int, type as a string.

        Matches the redis backend, where _manage_value_out turns a numeric
        string back into an int and everything else stays a string."""
        out = {}
        if dist is not None:
            out["dist"] = int(dist)
        if ctype is not None:
            out["type"] = ctype
        return out

    # ------------------------------------------------------------- the merge

    def merge_refs(self, items, chunk=1000):
        """Record many (key, dist, ctype) references at once.

        least() is the Lua's `if existing > new`; coalesce() is its HSETNX.
        An absent type is stored as the empty string rather than NULL on
        purpose: redis HSETNXs `ctype or ""`, so a reference first seen without
        a type keeps the empty one, and coalesce() only reproduces that if the
        stored value is non-NULL.

        Rows repeated within a batch are folded first -- ON CONFLICT DO UPDATE
        cannot touch the same row twice in one statement, and one record can
        reference the same URI at two distances.

        Then sorted, which is what stops 24 workers deadlocking. ON CONFLICT
        takes a row lock on each conflicting row in the order the VALUES list
        gives them, so two workers whose batches overlap in opposite orders
        each hold a row the other wants. Reference walks produce arbitrary
        order, so this is not rare: unsorted, 575 of 600 overlapping merges
        deadlocked in a 24-worker test. Sorted, every transaction takes its
        locks in the same global order and a cycle cannot form."""
        items = list(items)
        if not items:
            return
        folded = {}
        for (key, dist, ctype) in items:
            d = int(dist)
            prev = folded.get(key)
            if prev is None:
                folded[key] = (d, ctype or "")
            else:
                folded[key] = (min(prev[0], d), prev[1] or ctype or "")
        rows = sorted((k, d, c) for k, (d, c) in folded.items())
        sql = (f"INSERT INTO {self.table} (uri, dist, ctype) VALUES %s "
               f"ON CONFLICT (uri) DO UPDATE SET "
               f"dist = least({self.table}.dist, EXCLUDED.dist), "
               f"ctype = coalesce({self.table}.ctype, EXCLUDED.ctype)")
        for i in range(0, len(rows), chunk):
            page = rows[i:i + chunk]
            # Sorting removes the cycles this statement can cause on its own;
            # a concurrent set() or delete_multi() can still make one, so a
            # deadlock is retried rather than thrown at the phase. Safe to
            # replay: least()/coalesce() make the batch idempotent.
            for attempt in range(DEADLOCK_RETRIES):
                try:
                    with self.conn.cursor() as cur:
                        psycopg2.extras.execute_values(cur, sql, page, page_size=chunk)
                    break
                except psycopg2.errors.DeadlockDetected:
                    if attempt == DEADLOCK_RETRIES - 1:
                        print(f"{self.table}: deadlocked {DEADLOCK_RETRIES} times "
                              f"merging {len(page)} references")
                        raise
                    time.sleep(DEADLOCK_BACKOFF * (attempt + 1) * (0.5 + random.random()))

    def merge_ref(self, key, dist, ctype=""):
        self.merge_refs([(key, dist, ctype)])

    # ------------------------------------------------------------- the claim

    def popitems(self, count=100):
        """Claim up to `count` references. Exactly one worker gets each.

        Returns [(key, {field: value}), ...]; an empty list means the map is
        empty, which is what ends the caller's loop. Unlike the redis version
        this is exact rather than approximate -- LIMIT is a limit, where SCAN's
        COUNT was a hint that could overshoot."""
        if count <= 0:
            return []
        with self.conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {self.table} WHERE uri IN ("
                f"  SELECT uri FROM {self.table} LIMIT %s FOR UPDATE SKIP LOCKED"
                f") RETURNING uri, dist, ctype", (count,))
            return [(uri, self._fields(dist, ctype)) for (uri, dist, ctype) in cur.fetchall()]

    def popitem(self):
        got = self.popitems(1)
        return got[0] if got else None

    # -------------------------------------------------------------- the rest

    def get_multi(self, keys, chunk=1000):
        """{key: {field: value}} for the keys that exist."""
        keys = list(keys)
        out = {}
        for i in range(0, len(keys), chunk):
            batch = keys[i:i + chunk]
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT uri, dist, ctype FROM {self.table} WHERE uri = ANY(%s)",
                            (batch,))
                for (uri, dist, ctype) in cur.fetchall():
                    out[uri] = self._fields(dist, ctype)
        return out

    def get(self, key):
        """A plain dict, where the redis backend returns a lazy reference into
        redis whose every field read was another round trip."""
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT dist, ctype FROM {self.table} WHERE uri = %s", (key,))
            row = cur.fetchone()
        return self._fields(row[0], row[1]) if row else None

    def set(self, key, value):
        """Write the fields given, leaving the others alone.

        The redis version issues an HSET per field, so `refs[uri] = {"dist":
        2}` updates the distance and leaves any existing type intact -- which
        is exactly what write_done_refs() relies on. A whole-row upsert here
        would blank the type instead, so the coalesce keeps whatever is
        already stored for a field this call did not mention. Not a merge:
        a provided dist replaces rather than being minimised, which is
        merge_refs()' job."""
        # accepts a dict or anything with .items(), as the redis version did
        fields = value if isinstance(value, dict) else dict(value.items())
        unknown = set(fields) - {"dist", "type"}
        if unknown:
            raise ValueError(f"{self.table} has columns for dist and type, not {sorted(unknown)}")
        dist = fields.get("dist")
        with self.conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {self.table} (uri, dist, ctype) VALUES (%s, %s, %s) "
                f"ON CONFLICT (uri) DO UPDATE SET "
                f"dist  = coalesce(EXCLUDED.dist, {self.table}.dist), "
                f"ctype = coalesce(EXCLUDED.ctype, {self.table}.ctype)",
                (key, None if dist is None else int(dist), fields.get("type")))

    def update(self, values):
        for (k, v) in values.items():
            self.set(k, v)

    def delete_multi(self, keys, chunk=1000):
        # sorted for the same reason merge_refs sorts: consistent lock order
        keys = sorted(set(keys))
        for i in range(0, len(keys), chunk):
            with self.conn.cursor() as cur:
                cur.execute(f"DELETE FROM {self.table} WHERE uri = ANY(%s)", (keys[i:i + chunk],))

    def delete(self, key):
        with self.conn.cursor() as cur:
            cur.execute(f"DELETE FROM {self.table} WHERE uri = %s", (key,))

    def has_item(self, key):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM {self.table} WHERE uri = %s", (key,))
            return cur.fetchone() is not None

    def iter_items(self, chunk=1000):
        """Stream (key, {field: value}) without holding the table in memory."""
        stamp = f"{time.time()}".replace(".", "_")
        conn = psycopg2.connect(**self._conn_kw)
        try:
            cur = conn.cursor(name=f"refs_iter_{self.table}_{stamp}")
            cur.itersize = chunk
            cur.execute(f"SELECT uri, dist, ctype FROM {self.table}")
            for (uri, dist, ctype) in cur:
                yield (uri, self._fields(dist, ctype))
            cur.close()
            conn.commit()
        finally:
            conn.close()

    def iter_keys(self, **kw):
        for (uri, _fields) in self.iter_items():
            yield uri

    def keys(self, **kw):
        return list(self.iter_keys())

    def _getitem(self, db_key, key):
        got = self.get(db_key)
        return None if got is None else got.get(key)

    def _setitem(self, db_key, key, value):
        got = self.get(db_key) or {}
        got[key] = value
        self.set(db_key, got)

    def _items(self, db_key):
        return list((self.get(db_key) or {}).items())

    def _export_state(self):
        return {k: v for (k, v) in self.iter_items()}

    def clear(self):
        with self.conn.cursor() as cur:
            cur.execute(f"TRUNCATE {self.table}")

    def commit(self):
        # autocommit: every write has already landed
        pass

    def queue_length(self, ceiling):
        """How many references are waiting, counted no further than `ceiling`.

        The claim loop only needs the exact number when it is small: it takes
        min(ref_batch, remaining // ref_workers), so once remaining reaches
        ref_batch * ref_workers the answer stops changing. Counting to a
        ceiling is an index-only scan of at most that many rows, where an
        exact count of a queue holding millions would be a full scan on every
        claim -- and reltuples is no use here, because this table is created
        and filled inside a single run, so autovacuum never analyses it and
        the estimate reads 0 for the whole phase."""
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM (SELECT 1 FROM {self.table} LIMIT %s) t",
                        (ceiling,))
            return cur.fetchone()[0]

    def __len__(self):
        """Exact, matching redis DBSIZE. Reporting (manage-data --counts) wants
        the real number; the claim loop uses queue_length() instead."""
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {self.table}")
            return cur.fetchone()[0]

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __delitem__(self, key):
        return self.delete(key)

    def __contains__(self, key):
        return self.has_item(key)
