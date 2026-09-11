"""A subsidiary identity map: new equivalences, kept out of the master.

The use case is a reconciliation you are not yet willing to commit to. You
want the production identities as a starting point -- the same YUIDs, so a
record that moves can be compared against the one that shipped -- but every
decision made on top of them has to land somewhere the production map cannot
see, and you have to be able to say afterwards exactly what changed.

So: the subsidiary owns its own postgres tables, in the same shape as
storage.idmap.postgres.IdMap (it *is* that class, subclassed), and fills them
lazily from a master named in its config. A key nobody has asked about yet
does not exist here. The first lookup that touches it copies what the master
knows into the local tables; from then on the local tables are the only thing
answered from, and the master is never consulted about that key again.

The master is read through its own public interface -- get/get_multi/
get_cluster/has_item -- which is the whole reason this works against a redis
master, a postgres one, or the in-memory one, without caring which. Two
places where that difference is real and handled here: get_cluster only
exists on the postgres backend (`_master_cluster` falls back to the two
lookups it replaces), and redis keeps update tokens as pseudo-members of the
YUID set while postgres keeps them in a column (`_is_token` filters them out
on the way in, as migrate-idmap.py does).

Isolation is enforced three ways, because "must not affect the master" is the
requirement the whole thing exists to satisfy:

  * the master store is wrapped in ReadOnlyMaster, which allows an explicit
    list of read calls and raises on everything else. A write to the master
    is not a code-review question, it is an exception at runtime.
  * `tableName` is mandatory, and instantiation refuses to start if it
    resolves to the same tables the master is using. Inheriting the parent's
    default of "idmap" against a postgres master in the same database would
    otherwise put the "subsidiary" straight into the master's tables.
  * nothing copied from the master is ever re-copied over a local decision:
    every hydrating insert is ON CONFLICT DO NOTHING.

Hydration pulls the whole equivalence class, not the one row asked for, and
that is load-bearing rather than an optimisation. Membership in this schema
is derived -- a YUID's members are the rows carrying it -- so a half-copied
class would answer `idmap[yuid]` with the members that happen to have been
touched. Worse, merging such a class locally would move those members and
leave the rest resolving to the old YUID out of the master. Copying the class
whole means every cluster this map has an opinion about is complete and
local, and the reverse direction cannot disagree with the forward one.

Tracking what changed is two things, deliberately:

  * `{table}_origin` is the snapshot: for every key ever resolved against the
    master, what the master said at the time (NULL = the master had nothing).
    Diffing it against the live rows gives the divergence, and that diff is
    correct by construction -- see iter_divergence(). It doubles as the
    "already asked" marker, which is what stops a local decision from being
    undone by a later fallback to the master.
  * `{table}_changes` is the audit trail: an append-only row per write, with
    what the key pointed at before and after. It carries the history the
    snapshot cannot (order, timestamps, intermediate states), and is written
    outside the transaction that made the change, so treat the snapshot as
    authoritative and the log as the narrative.

What is *not* here, on purpose, is any way to push a divergence back into the
master. Read iter_divergence() and apply it deliberately, with whatever
review that deserves.
"""

import time
import uuid

import psycopg2
import psycopg2.extras

from pipeline.storage.idmap import postgres


class ReadOnlyMasterError(RuntimeError):
    """A write was attempted against the master identity map."""


class MasterUnavailable(RuntimeError):
    """The master could not be reached, or answered with an error.

    Distinct from the master answering "no mapping", and the distinction
    matters: a miss is recorded in the origin table and never asked again, so
    a failure recorded as a miss would be a permanently wrong answer. A
    failure records nothing and is retried on the next lookup.
    """


# What the subsidiary is allowed to call on the master. Deny by default:
# a method added to a backend later is denied until it is listed here, which
# is the right way round for this.
_MASTER_READS = frozenset({
    "get",
    "get_multi",
    "get_cluster",
    "has_item",
    "count",
    "has_update_token",
})


class ReadOnlyMaster:
    """Read-only view of another identity map.

    Cheaper than trusting the code below to only make read calls, and it
    survives someone adding a write to a shared helper later.
    """

    __slots__ = ("_store", "_name")

    def __init__(self, store, name):
        self._store = store
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def store(self):
        """The wrapped store. For isinstance checks and diagnostics."""
        return self._store

    def __getattr__(self, name):
        if name in _MASTER_READS:
            return getattr(self._store, name)
        if hasattr(self._store, name):
            raise ReadOnlyMasterError(
                f"{name}() is not permitted on the master idmap "
                f"'{self._name}': a subsidiary map must not change it")
        raise AttributeError(name)

    def __len__(self):
        return len(self._store)

    def __contains__(self, key):
        return self._store.has_item(key)

    def __getitem__(self, key):
        return self._store.get(key)

    def __setitem__(self, key, value):
        raise ReadOnlyMasterError(
            f"cannot write to the master idmap '{self._name}'")

    def __delitem__(self, key):
        raise ReadOnlyMasterError(
            f"cannot delete from the master idmap '{self._name}'")


def _is_token(value):
    """Update tokens look like __20250101__.

    The redis backend keeps them as members of the YUID set, so they arrive
    mixed in with real members and have to be dropped; this schema keeps them
    in a column. Same test as migrate-idmap.py's.
    """
    return value.startswith("__") and value.endswith("__")


# `key` holds either a member URI (kind 'u') or a YUID (kind 'y'), in the same
# CURIE form as the map itself. Its presence means "the master has been asked
# about this, and the answer is already reflected in the local tables" -- a
# row with yuid NULL is a recorded miss, not an absent one, and is what keeps
# a locally deleted mapping from being resurrected out of the master.
#
# `n` is the master's member count for a YUID at copy time: informational,
# but it is the one number that says whether a class has been pulled apart
# here since.
SUB_SCHEMA = """
CREATE TABLE IF NOT EXISTS {origin} (
    key   TEXT PRIMARY KEY,
    kind  CHAR(1) NOT NULL,
    yuid  TEXT,
    n     INTEGER,
    seen  TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS {origin}_yuid_idx ON {origin} (yuid);
CREATE TABLE IF NOT EXISTS {changes} (
    id        BIGSERIAL PRIMARY KEY,
    ts        TIMESTAMP NOT NULL DEFAULT now(),
    op        TEXT NOT NULL,
    uri       TEXT,
    old_yuid  TEXT,
    new_yuid  TEXT
);
CREATE INDEX IF NOT EXISTS {changes}_uri_idx ON {changes} (uri);
CREATE INDEX IF NOT EXISTS {changes}_ts_idx ON {changes} (ts);
"""


class IdMap(postgres.IdMap):
    """Identity map overlaying a read-only master.

    Config, on top of everything storage.idmap.postgres.IdMap takes:

        masterMap        name of the map store to copy from. Required.
        tableName        this map's own tables. Required, and must not be the
                         master's.
        copyUpdateToken  copy the master's update token onto a YUID as it is
                         copied (default true). Costs one extra call per
                         class against a postgres master; free against redis,
                         which returns the token as a member.
        logChanges       write the {table}_changes audit trail (default
                         true). The origin snapshot is kept either way.
    """

    def __init__(self, config):
        self.master_name = config.get("masterMap") or config.get("master")
        if not self.master_name:
            raise ValueError(
                "a subsidiary idmap needs 'masterMap': the name of the map "
                "store to copy identities from")
        if self.master_name == config.get("name"):
            raise ValueError(
                f"subsidiary idmap '{self.master_name}' cannot be its own master")

        # No default, unlike the parent. The parent falls back to "idmap",
        # and a subsidiary that inherited that against a postgres master in
        # the same database would write into the master's own tables -- the
        # one failure mode this class exists to make impossible.
        table = config.get("tableName")
        if not table:
            raise ValueError(
                "a subsidiary idmap needs an explicit 'tableName', so it "
                "cannot land in the master's tables")
        self.origin_table = f"{table}_origin"
        self.change_table = f"{table}_changes"

        self.copy_update_token = config.get("copyUpdateToken", True)
        self.log_changes = config.get("logChanges", True)

        # Sets self.configs, opens the connection, creates the schema (ours
        # included, via _ensure_schema) and prepares the statements.
        super().__init__(config)

        master = self.configs.instantiate_map(self.master_name)["store"]
        if isinstance(master, ReadOnlyMaster):
            master = master.store
        self._guard_distinct(master)
        self.master = ReadOnlyMaster(master, self.master_name)

        # Capability probe rather than an isinstance check: the master is
        # whatever its own config says, and only the postgres backend has
        # get_cluster.
        if not hasattr(self.master, "get"):
            raise ValueError(
                f"map store '{self.master_name}' has no get() and cannot be "
                f"a master identity map")
        self._master_has_cluster = hasattr(self.master, "get_cluster")
        self._master_has_multi = hasattr(self.master, "get_multi")

    def _guard_distinct(self, master):
        """Refuse to share storage with the master.

        Only checkable when the master is also a postgres map -- and that is
        exactly the case where an accident is possible, because both sides
        reach the same server through the same shared connection.
        """
        if not isinstance(master, postgres.IdMap):
            return
        if getattr(master, "_conn_kw", None) != self._conn_kw:
            return
        clash = {master.table, master.yuid_table} & {
            self.table, self.yuid_table, self.origin_table, self.change_table}
        if clash:
            raise ValueError(
                f"subsidiary idmap tables collide with master "
                f"'{self.master_name}' on the same database: "
                f"{', '.join(sorted(clash))}. Give the subsidiary its own "
                f"tableName.")

    # ------------------------------------------------------------------ setup

    def _ensure_schema(self):
        super()._ensure_schema()
        with self.conn.cursor() as cur:
            cur.execute(SUB_SCHEMA.format(origin=self.origin_table,
                                          changes=self.change_table))

    # One statement, but it runs on the miss path of every lookup in the map,
    # so it gets the same treatment as the parent's: prepared once per
    # connection rather than parsed and planned each time.
    SUB_PREPARED = {
        "origin_has": "SELECT 1 FROM {o} WHERE key = $1",
    }

    def _prepare(self):
        super()._prepare()
        for name, sql in self.SUB_PREPARED.items():
            stmt = f"subidmap_{name}_{id(self) & 0xffff:x}"
            try:
                self._hot.execute(f"PREPARE {stmt} AS "
                                  + sql.format(o=self.origin_table,
                                               c=self.change_table))
                self._stmt[name] = stmt
            except psycopg2.Error as e:
                # Same treatment as the parent: not fatal, the callers below
                # fall back to plain SQL.
                print(f"subsidiary idmap: could not prepare {name}: "
                      f"{str(e).strip()[:80]}")
                self.conn.rollback()

    def shutdown(self):
        # The master is borrowed from the config, and whatever else in this
        # process asked for it still holds it. Not ours to close.
        super().shutdown()

    # ------------------------------------------------------- reading the master

    def _master_get(self, key):
        try:
            return self.master.get(key)
        except ReadOnlyMasterError:
            raise
        except Exception as e:
            raise MasterUnavailable(
                f"master idmap '{self.master_name}' get({key}) failed: {e}") from e

    def _master_get_multi(self, keys, chunk=1000):
        if not keys:
            return {}
        if not self._master_has_multi:
            return {k: self._master_get(k) for k in keys}
        try:
            return self.master.get_multi(keys, chunk=chunk)
        except ReadOnlyMasterError:
            raise
        except Exception as e:
            raise MasterUnavailable(
                f"master idmap '{self.master_name}' get_multi({len(keys)} "
                f"keys) failed: {e}") from e

    def _master_cluster(self, key):
        """(yuid, members) for a member URI, or (None, None) if unknown.

        Members always include `key` itself: a master whose reverse set had
        drifted would otherwise hand back a class the key is not in, and the
        key would never get an origin row -- so every lookup would re-ask.
        """
        if self._master_has_cluster:
            try:
                (yuid, members) = self.master.get_cluster(key)
            except ReadOnlyMasterError:
                raise
            except Exception as e:
                raise MasterUnavailable(
                    f"master idmap '{self.master_name}' get_cluster({key}) "
                    f"failed: {e}") from e
            if not yuid:
                return (None, None)
            return (yuid, set(members or ()) | {key})

        yuid = self._master_get(key)
        if not yuid:
            return (None, None)
        members = self._master_get(yuid)
        return (yuid, set(members or ()) | {key})

    # -------------------------------------------------------------- hydration

    def _hydrate(self, key, ikey):
        """Copy what the master knows about `key` into the local tables.

        `key` is the full external form, `ikey` the internal one. Returns
        True when the master was consulted, meaning the caller should read
        the local tables again; False when this key had already been resolved
        against the master once and the local answer is already final.
        """
        if self._is_hydrated(ikey):
            return False
        try:
            if ikey.startswith("yuid:"):
                members = self._master_get(key)
                if members is None:
                    self._mark_origin([(ikey, "y", None, None)])
                else:
                    self._absorb(key, members)
            else:
                (yuid, members) = self._master_cluster(key)
                if yuid is None:
                    self._mark_origin([(ikey, "u", None, None)])
                else:
                    self._absorb(yuid, members)
        except MasterUnavailable as e:
            # Nothing recorded, so the next lookup tries again. Degrades to
            # "not in this map", which is what the parent does with a failed
            # lookup too.
            print(f"subsidiary idmap: {e}")
        return True

    def _absorb(self, yuid, members):
        """Copy one equivalence class in, without disturbing local decisions.

        Rows first, origin marker last. Neither step is transactional -- the
        connection is in autocommit and shared with the other map stores, and
        the parent's note on _set_once is the reason not to flip it -- so the
        order is the safety: a failure between the two leaves the class
        copied but unmarked, and the next lookup copies it again over ON
        CONFLICT DO NOTHING. The reverse order would leave it marked and
        empty, which reads as "the master has nothing" forever.
        """
        iyuid = self._manage_value_in(yuid)
        imembers = sorted({self._manage_key_in(m)
                           for m in members if not _is_token(m)})

        # Redis hands the token back as a member of the set, so it is already
        # here; postgres keeps it in a column and has to be asked.
        token = next((m for m in members if _is_token(m)), None)
        if token is None and self.copy_update_token:
            try:
                if self.master.has_update_token(yuid):
                    token = self.update_token
            except (AttributeError, ReadOnlyMasterError):
                pass
            except Exception as e:
                print(f"subsidiary idmap: master update token for {yuid}: {e}")

        with self._cursor() as cur:
            cur.execute(f"INSERT INTO {self.yuid_table} (yuid, token) "
                        f"VALUES (%s, %s) ON CONFLICT (yuid) DO NOTHING",
                        (iyuid, token))
            if imembers:
                # DO NOTHING, emphatically not DO UPDATE: if a row is already
                # here it carries a decision made in this map, and the master
                # does not get to overwrite it.
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO {self.table} (uri, yuid) VALUES %s "
                    f"ON CONFLICT (uri) DO NOTHING",
                    [(m, iyuid) for m in imembers], page_size=1000)

        self._mark_origin([(iyuid, "y", None, len(imembers))]
                          + [(m, "u", iyuid, None) for m in imembers])

    def _mark_origin(self, rows):
        """Record the master's answer. First answer wins, always.

        DO NOTHING because the origin table is a baseline, not a cache: once
        a key's master value is recorded, re-reading the master must not move
        it, or the divergence report would quietly lose entries.
        """
        if not rows:
            return
        with self._cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO {self.origin_table} (key, kind, yuid, n) "
                f"VALUES %s ON CONFLICT (key) DO NOTHING",
                rows, page_size=1000)

    def _is_hydrated(self, ikey):
        try:
            return self._run("origin_has", (ikey,)).fetchone() is not None
        except KeyError:
            with self._cursor() as cur:
                cur.execute(f"SELECT 1 FROM {self.origin_table} WHERE key = %s",
                            (ikey,))
                return cur.fetchone() is not None

    def _hydrated_multi(self, ikeys, chunk=1000):
        """Which of these keys have already been resolved against the master."""
        out = set()
        for i in range(0, len(ikeys), chunk):
            batch = ikeys[i:i + chunk]
            with self._cursor() as cur:
                cur.execute(f"SELECT key FROM {self.origin_table} "
                            f"WHERE key = ANY(%s)", (batch,))
                out.update(r[0] for r in cur.fetchall())
        return out

    def _hydrate_multi(self, keys, chunk=1000):
        """Hydrate many keys in as few master round trips as possible.

        Two passes at most: forward pointers for the member URIs, then member
        sets for every YUID involved. Returns the keys actually consulted, so
        the caller can re-read just those.
        """
        pairs, seen = [], set()
        for k in keys:
            ik = self._manage_key_in(k)
            if ik not in seen:
                seen.add(ik)
                pairs.append((k, ik))
        known = self._hydrated_multi(sorted(seen), chunk)
        todo = [(k, ik) for (k, ik) in pairs if ik not in known]
        if not todo:
            return []

        uris = [(k, ik) for (k, ik) in todo if not ik.startswith("yuid:")]
        yuids = [k for (k, ik) in todo if ik.startswith("yuid:")]
        try:
            fwd = self._master_get_multi([k for (k, _) in uris], chunk)
            want = sorted({v for v in fwd.values() if v} | set(yuids))
            mem = self._master_get_multi(want, chunk) if want else {}
        except MasterUnavailable as e:
            print(f"subsidiary idmap: {e}")
            return [k for (k, _) in todo]

        for y in want:
            members = mem.get(y)
            if members is None:
                self._mark_origin([(self._manage_value_in(y), "y", None, None)])
                continue
            # Union in the members that pointed here, for the same reason
            # _master_cluster does.
            self._absorb(y, set(members) | {k for (k, _) in uris if fwd.get(k) == y})

        self._mark_origin([(ik, "u", None, None)
                           for (k, ik) in uris if not fwd.get(k)])
        return [k for (k, _) in todo]

    def _local_yuids(self, ikeys, chunk=1000):
        """Current local yuid per internal key. The before-state for logging."""
        out = {}
        for i in range(0, len(ikeys), chunk):
            batch = ikeys[i:i + chunk]
            with self._cursor() as cur:
                cur.execute(f"SELECT uri, yuid FROM {self.table} "
                            f"WHERE uri = ANY(%s)", (batch,))
                out.update(cur.fetchall())
        return out

    # ------------------------------------------------------------------ reads

    def get(self, key, typ=""):
        key = self._check_qua(key, typ)
        ikey = self._manage_key_in(key)

        if self.memory_cache_enabled:
            maybe = self.memory_cache[ikey]
            if maybe is not self.memory_cache.missing:
                return maybe

        out = self._db_get(ikey)
        if out is None and self._hydrate(key, ikey):
            out = self._db_get(ikey)
        if out is None:
            return None
        if self.memory_cache_enabled:
            self.memory_cache[ikey] = out
        return out

    def get_cluster(self, key, typ=""):
        key = self._check_qua(key, typ)
        (yuid, members) = super().get_cluster(key)
        if yuid is None and self._hydrate(key, self._manage_key_in(key)):
            (yuid, members) = super().get_cluster(key)
        return (yuid, members)

    def get_multi(self, keys, chunk=1000):
        keys = list(keys)
        out = super().get_multi(keys, chunk=chunk)
        missing = [k for k in keys if out.get(k) is None]
        if missing:
            asked = self._hydrate_multi(missing, chunk)
            if asked:
                out.update(super().get_multi(asked, chunk=chunk))
        return out

    def has_item(self, key):
        if super().has_item(key):
            return True
        ikey = self._manage_key_in(key)
        if self._hydrate(self._manage_key_out(ikey), ikey):
            return super().has_item(key)
        return False

    def count(self, key):
        ikey = self._manage_key_in(key)
        self._hydrate(self._manage_key_out(ikey), ikey)
        return super().count(key)

    # ----------------------------------------------------------------- writes
    #
    # Every write hydrates first. That is not for the write's benefit -- the
    # SQL would work either way -- but so that the origin table holds the
    # master's value for the key *before* it was changed. A write that skipped
    # it would be invisible to the divergence report, which is the point of
    # the whole exercise.

    def _qua_write(self, key, typ):
        """The parent's write-path key validation, run early.

        set(), mint() and delete() each run it again themselves; hydration
        has to happen first, and it needs the same qua'd key the write will
        land on.
        """
        if typ in self.configs.ok_record_types:
            return self.configs.make_qua(key, typ)
        if typ:
            raise ValueError(typ)
        if not self.configs.is_qua(key):
            raise ValueError(f"Need a type: {key}")
        return key

    def mint(self, key, slug, typ=""):
        """Mint locally, or adopt what the master already had.

        Inherited semantics, and worth being explicit about: the parent's
        INSERT ... ON CONFLICT DO UPDATE RETURNING yuid adopts an existing
        row rather than replacing it, and after hydration the master's
        identity *is* an existing row. So minting for a URI the master
        already knows returns the master's YUID and changes nothing. Use
        remint() to deliberately give it a new one.
        """
        ckey = self._qua_write(key, typ)
        ikey = self._manage_key_in(ckey)
        self._hydrate(ckey, ikey)
        old = self._db_get(ikey)
        value = super().mint(ckey, slug, "")
        if value != old:
            self._log("mint", ikey, self._manage_value_in(old) if old else None,
                      self._manage_value_in(value))
        return value

    def remint(self, key, slug="", typ=""):
        """Give a key a brand-new YUID of its own.

        The subsidiary's way of saying "this URI is not the thing the master
        thought it was". Only this key moves -- the rest of its old class
        stays where it is, which is the difference from set().
        """
        ckey = self._qua_write(key, typ)
        ikey = self._manage_key_in(ckey)
        self._hydrate(ckey, ikey)
        old = self._db_get(ikey)

        base = self.prefix_map_out["yuid"]
        uu = str(uuid.uuid4())
        value = f"{base}{slug}/{uu}" if slug else f"{base}{uu}"
        ivalue = self._manage_value_in(value)

        with self._cursor() as cur:
            cur.execute(f"INSERT INTO {self.yuid_table} (yuid, token) "
                        f"VALUES (%s, %s) ON CONFLICT (yuid) DO NOTHING",
                        (ivalue, self.update_token))
            cur.execute(f"INSERT INTO {self.table} (uri, yuid) VALUES (%s, %s) "
                        f"ON CONFLICT (uri) DO UPDATE SET yuid = EXCLUDED.yuid",
                        (ikey, ivalue))
        iold = self._manage_value_in(old) if old else None
        self._log("remint", ikey, iold, ivalue)
        if self.memory_cache_enabled:
            # Internal forms: that is what the parent caches member sets and
            # forward pointers under (see _set_once and get_cluster).
            del self.memory_cache[ikey]
            if iold:
                del self.memory_cache[iold]
            del self.memory_cache[ivalue]
            self.memory_cache[ikey] = value
        # The old class may have lost its last member; delete_empty_yuids()
        # is the sweep for that, same as everywhere else.
        return value

    def set(self, key, value, typ=""):
        """Merge, with both classes copied in first.

        Both sides have to be hydrated, not just the key: the parent's merge
        re-points every member of the key's old class at the target, and a
        class that was only half here would leave the rest of it resolving to
        the old YUID out of the master.
        """
        ckey = self._qua_write(key, typ)
        ikey = self._manage_key_in(ckey)
        self._hydrate(ckey, ikey)
        self._hydrate(value, self._manage_value_in(value))

        old = self._db_get(ikey)
        ivalue = self._manage_value_in(value)
        moved = []
        if old is not None and old != value:
            iold = self._manage_value_in(old)
            with self._cursor() as cur:
                cur.execute(f"SELECT uri FROM {self.table} "
                            f"WHERE yuid = %s AND uri <> %s", (iold, ikey))
                moved = [r[0] for r in cur.fetchall()]

        out = super().set(ckey, value, "")
        if old != value:
            rows = [("merge", ikey, self._manage_value_in(old) if old else None,
                     ivalue)]
            rows += [("moved", m, self._manage_value_in(old), ivalue)
                     for m in moved]
            self._log_many(rows)
        return out

    def assign_bulk(self, items):
        """Clusters in bulk, with the master's baseline recorded first.

        The hydration here is the expensive part of running an identity phase
        against a subsidiary: every member and every YUID is looked up in the
        master once, in batches. That is the cost of being able to say what
        changed, and it is paid once per key for the life of the map.
        """
        items = list(items)
        members = [m for (_, ms, _) in items for m in ms]
        yuids = [y for (y, _, _) in items]
        self._hydrate_multi(members + yuids)

        imembers = sorted({self._manage_key_in(m) for m in members})
        before = self._local_yuids(imembers)

        stats = super().assign_bulk(items)

        rows = []
        for yuid, ms, prior in items:
            iyuid = self._manage_value_in(yuid)
            for m in ms:
                im = self._manage_key_in(m)
                was = before.get(im)
                if was != iyuid:
                    rows.append(("assign", im, was, iyuid))
        self._log_many(rows)
        return stats

    def delete(self, key, typ=""):
        ckey = self._qua_write(key, typ)
        ikey = self._manage_key_in(ckey)
        self._hydrate(ckey, ikey)
        old = self._db_get(ikey)
        out = super().delete(ckey, "")
        if old is not None:
            self._log("delete", ikey, self._manage_value_in(old), None)
        return out

    def delete_yuid(self, yuid):
        out = super().delete_yuid(yuid)
        if out:
            self._log("delete_yuid", None, self._manage_value_in(yuid), None)
        return out

    def _add(self, key, *values):
        ikey = self._manage_key_in(key)
        if ikey.startswith("yuid:"):
            self._hydrate(self._manage_key_out(ikey), ikey)
            ivalues = [self._manage_key_in(v) for v in values]
            before = self._local_yuids(ivalues)
            out = super()._add(key, *values)
            self._log_many([("add", v, before.get(v), ikey)
                            for v in ivalues if before.get(v) != ikey])
            return out
        return super()._add(key, *values)

    def _remove(self, key, value):
        ikey = self._manage_key_in(key)
        ivalue = self._manage_value_in(value)
        if not _is_token(ivalue):
            self._hydrate(self._manage_key_out(ikey), ikey)
        out = super()._remove(key, value)
        if not _is_token(ivalue):
            self._log("detach", ivalue, ikey, None)
        return out

    def add_update_token(self, key):
        """Stamp a YUID as seen in this build.

        Hydrates first, like every other write, so that a class whose only
        interaction with this map was a token stamp still has the master's
        baseline recorded rather than appearing as a class minted here. In
        practice a build has already read the identity by this point, so this
        is one prepared lookup that hits.
        """
        ikey = self._manage_key_in(key)
        self._hydrate(self._manage_key_out(ikey), ikey)
        return super().add_update_token(key)

    # ------------------------------------------------------------------ log

    def _log(self, op, iuri, old, new):
        self._log_many([(op, iuri, old, new)])

    def _log_many(self, rows):
        """Append to the audit trail.

        Outside the transaction that made the change, so a crash between the
        two loses a log row but never leaves a change unrecorded: the origin
        snapshot is the authoritative diff and is derived from the rows
        themselves. Best-effort by design -- a failure to log must not fail a
        write that has already landed.
        """
        if not self.log_changes or not rows:
            return
        try:
            with self._cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO {self.change_table} "
                    f"(op, uri, old_yuid, new_yuid) VALUES %s",
                    rows, page_size=1000)
        except Exception as e:
            print(f"subsidiary idmap: could not log {len(rows)} change(s): {e}")

    # -------------------------------------------------------------- tracking

    def iter_changes(self, since=None, ops=None):
        """The audit trail, oldest first, as dicts with external URIs.

        `since` is a timestamp (anything psycopg2 will adapt to TIMESTAMP),
        `ops` an iterable of operation names to restrict to.
        """
        where, params = [], []
        if since is not None:
            where.append("ts >= %s")
            params.append(since)
        if ops:
            where.append("op = ANY(%s)")
            params.append(list(ops))
        sql = (f"SELECT id, ts, op, uri, old_yuid, new_yuid "
               f"FROM {self.change_table}"
               + (" WHERE " + " AND ".join(where) if where else "")
               + " ORDER BY id")
        stamp = f"{time.time()}".replace(".", "_")
        with self._streaming(f"subidmap_changes_{stamp}") as cur:
            cur.execute(sql, params or None)
            for (cid, ts, op, uri, old, new) in cur:
                yield {
                    "id": cid,
                    "ts": ts,
                    "op": op,
                    "uri": self._manage_key_out(uri) if uri else None,
                    "old_yuid": self._manage_value_out(old) if old else None,
                    "new_yuid": self._manage_value_out(new) if new else None,
                }

    def changes(self, since=None, ops=None):
        return list(self.iter_changes(since=since, ops=ops))

    # The diff, derived rather than recorded: the live rows outer-joined to
    # what the master said about the same keys. Correct whatever happened to
    # the audit trail, and it collapses a key that moved five times into the
    # one fact that matters -- where it started and where it is now.
    #
    # kind is which side is missing:
    #   swapped  both have a YUID and they differ
    #   added    the master had nothing; this map minted or assigned one
    #   removed  the master had one; the mapping is gone here
    #   unseen   no origin row at all, so the master was never asked. Only
    #            reachable via _import_state() or hand-written SQL; reported
    #            rather than hidden, because it is the one case where the
    #            baseline is unknown.
    DIVERGENCE = """
    SELECT coalesce(m.uri, o.key) AS uri, o.yuid AS master_yuid,
           m.yuid AS local_yuid, (o.key IS NULL) AS unseen
    FROM {t} m FULL OUTER JOIN {o} o ON o.key = m.uri
    WHERE (o.key IS NULL OR o.kind = 'u')
      AND m.yuid IS DISTINCT FROM o.yuid
    """

    def iter_divergence(self):
        """Every URI whose identity here differs from the master's."""
        stamp = f"{time.time()}".replace(".", "_")
        with self._streaming(f"subidmap_diverge_{stamp}") as cur:
            cur.execute(self.DIVERGENCE.format(t=self.table, o=self.origin_table))
            for (uri, master_yuid, local_yuid, unseen) in cur:
                if unseen:
                    kind = "unseen"
                elif master_yuid is None:
                    kind = "added"
                elif local_yuid is None:
                    kind = "removed"
                else:
                    kind = "swapped"
                yield {
                    "uri": self._manage_key_out(uri),
                    "master_yuid": (self._manage_value_out(master_yuid)
                                    if master_yuid else None),
                    "local_yuid": (self._manage_value_out(local_yuid)
                                   if local_yuid else None),
                    "kind": kind,
                }

    def divergence(self):
        return list(self.iter_divergence())

    def divergence_summary(self):
        """Counts, without streaming the whole diff."""
        out = {}
        with self._cursor() as cur:
            cur.execute(
                "SELECT count(*) FILTER (WHERE unseen), "
                "       count(*) FILTER (WHERE NOT unseen AND master_yuid IS NULL), "
                "       count(*) FILTER (WHERE NOT unseen AND local_yuid IS NULL), "
                "       count(*) FILTER (WHERE NOT unseen AND master_yuid IS NOT NULL "
                "                        AND local_yuid IS NOT NULL) "
                "FROM (" + self.DIVERGENCE.format(t=self.table,
                                                  o=self.origin_table) + ") d")
            (unseen, added, removed, swapped) = cur.fetchone()
            out.update(swapped=swapped, added=added, removed=removed,
                       unseen=unseen)

            cur.execute(f"SELECT count(*) FILTER (WHERE kind = 'u'), "
                        f"       count(*) FILTER (WHERE kind = 'u' AND yuid IS NULL), "
                        f"       count(*) FILTER (WHERE kind = 'y') "
                        f"FROM {self.origin_table}")
            (uris, misses, classes) = cur.fetchone()
            out.update(uris_asked=uris, master_misses=misses,
                       classes_copied=classes)

            cur.execute(f"SELECT count(*) FROM {self.yuid_table} y "
                        f"WHERE NOT EXISTS (SELECT 1 FROM {self.origin_table} o "
                        f"                  WHERE o.key = y.yuid AND o.kind = 'y')")
            out["yuids_minted_here"] = cur.fetchone()[0]

            cur.execute(f"SELECT count(*) FROM {self.change_table}")
            out["logged"] = cur.fetchone()[0]
        return out

    def report(self):
        """Print what this map has taken from the master and done since."""
        s = self.divergence_summary()
        print(f"subsidiary idmap {self.table} "
              f"(master: {self.master_name})")
        print(f"  copied in : {s['uris_asked']:,} URIs asked "
              f"({s['master_misses']:,} unknown to the master), "
              f"{s['classes_copied']:,} classes")
        print(f"  diverged  : {s['swapped']:,} swapped, {s['added']:,} added, "
              f"{s['removed']:,} removed"
              + (f", {s['unseen']:,} with no baseline" if s["unseen"] else ""))
        print(f"  new YUIDs : {s['yuids_minted_here']:,}")
        print(f"  logged    : {s['logged']:,} changes")
        return s

    # -------------------------------------------------------------- lifecycle

    def clear(self):
        with self._cursor() as cur:
            cur.execute(f"TRUNCATE {self.table}, {self.yuid_table}, "
                        f"{self.origin_table}, {self.change_table} "
                        f"RESTART IDENTITY")
        self.memory_cache.clear()

    def optimize(self, analyze=True, report=True):
        before = super().optimize(analyze=analyze, report=report)
        opts = "(ANALYZE)" if analyze else ""
        for name in (self.origin_table, self.change_table):
            try:
                with self._cursor() as cur:
                    cur.execute(f"VACUUM {opts} {name}".replace("  ", " "))
            except Exception as e:
                print(f"  could not vacuum {name}: {e}")
        return before
