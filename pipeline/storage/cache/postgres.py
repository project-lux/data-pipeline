import datetime
import random
import threading
import time

import psycopg2
import ujson

# Parallel workers upserting the same row can be picked as a deadlock victim.
# It is transient, so retry the statement rather than killing the build.
DEADLOCK_RETRIES = 5
DEADLOCK_BACKOFF = 0.05
from psycopg2.extras import Json, RealDictCursor


def _dumps(obj):
    """Serialiser for the jsonb columns. ~1.7x faster than the stdlib json
    psycopg2 uses by default, which showed up as `iterencode` in the reconcile
    profile. escape_forward_slashes stays off: postgres parses either form to
    the same jsonb, but escaping every / would inflate these URI-heavy
    documents on the wire for nothing."""
    return ujson.dumps(obj, escape_forward_slashes=False)


# Reading is the same swap in the other direction. Global because psycopg2
# resolves the jsonb typecaster per connection at connect time and the caches
# open theirs in PoolManager; the gain here is smaller than on the write side
# (~1.1x at our document sizes) so this is the half to drop if it ever gets in
# the way.
psycopg2.extras.register_default_jsonb(globally=True, loads=ujson.loads)

#
# How to index into JSONB arrays:
# SELECT identifier FROM ycba_record_cache,
#    jsonb_array_elements(data -> 'produced_by'->'carried_out_by') ids
#    WHERE ids->>'id' = 'https://ycba-lux.s3.amazonaws.com/v3/person/00/00628d01-deea-4811-b262-5ea81b732fba.json'
#

# How to dump the databases using pg_dump:
# pg_dump -U USER -F c --clean --no-owner -t aat_data_cache record_cache > aat_data_cache.pgdump
# pg_restore -a -U pipeline -W --host HOST -d DATABASE wof_data_cache.pgdump

# We actually only need two connections per process -- one to stay open for iteration, and one to read/write.
# This means we can't iterate two different tables at the same time, but that's fine.


class PoolManager(object):
    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        self.conn = None
        self.iterating_conn = None
        self.pool = None
        # When True, PooledCache.set() leaves the transaction open and
        # commits are driven by checkpoint()/flush() instead. Every cache in
        # the process shares self.conn, so the deferral state and the write
        # count are necessarily process-wide -- which is the point: one
        # commit then covers a merged record and all of its recordcache2
        # rows together, so another worker never sees a half-written record.
        self.deferring = False
        self.commit_every = 0
        self.pending_writes = 0
        # Statements executed since the last commit while deferring. A
        # deadlock aborts the whole transaction, not just the statement that
        # lost, so without these the batch is simply gone; with them it can
        # be replayed. Bounded by commit_every, which is therefore also the
        # bound on how many records are held in memory at once.
        self.deferred_stmts = []

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                # Double-check locking pattern
                if cls._instance is None:
                    cls._instance = PoolManager()
        return cls._instance

    def make_pool(self, name, host=None, port=None, user=None, password=None, dbname=None):
        if self.conn is None:
            if host:
                # TCP/IP
                self.conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
                self.iterating_conn = psycopg2.connect(
                    host=host, port=port, user=user, password=password, dbname=dbname
                )
            else:
                # local socket
                self.conn = psycopg2.connect(user=user, dbname=dbname)
                self.iterating_conn = psycopg2.connect(user=user, dbname=dbname)
            self.pool = name

    def get_conn(self, name, itr=False):
        if itr == False:
            return self.conn
        else:
            return self.iterating_conn

    def put_conn(self, name, close=False, itr=False):
        if close:
            if itr and self.iterating_conn is not None:
                self.iterating_conn.close()
                self.iteration_conn = None
            elif not itr and self.conn is not None:
                self.conn.close()
                self.conn = None

    def commit_all(self):
        # Commit everything outstanding on the shared write connection
        if self.conn is not None:
            self.conn.commit()
        self.pending_writes = 0
        # committed, so there is nothing left to replay
        self.deferred_stmts.clear()

    def record_deferred(self, qry, params):
        self.deferred_stmts.append((qry, params))
        self.pending_writes += 1

    def replay_deferred(self):
        """Re-execute the deferred batch after a rollback threw it away.

        Only upserts are ever deferred (see PooledCache.set), so replaying a
        statement that had already landed before the rollback is a no-op --
        which is what makes recovering from a deadlock possible at all
        rather than just reporting the loss."""
        if self.conn is None or not self.deferred_stmts:
            return
        with self.conn.cursor() as cursor:
            for (qry, params) in self.deferred_stmts:
                cursor.execute(qry, params)
        self.pending_writes = len(self.deferred_stmts)

    def put_all(self, name):
        # Closing a connection with an open transaction rolls it back, so
        # anything deferred has to land first
        self.commit_all()
        self.put_conn(self.pool, close=True)
        self.put_conn(self.pool, close=True, itr=True)


class PooledCache(object):
    def __init__(self, config):
        self.config = config
        self.name = config["name"] + "_" + config["tabletype"]
        self.conn = None
        self.iterating_conn = None
        self._cols = None
        self.pools = PoolManager.get_instance()

        if config["host"]:
            # TCP/IP
            pname = f"{config['host']}:{config['port']}/{config['dbname']}"
            self.pool_name = pname
            self.pools.make_pool(
                pname,
                host=self.config["host"],
                port=self.config["port"],
                user=self.config["user"],
                password=self.config["password"],
                dbname=self.config["dbname"],
            )
        else:
            # local socket
            pname = "localsocket"
            self.pool_name = pname
            self.pools.make_pool(pname, user=self.config["user"], dbname=self.config["dbname"])

        # Test that our table exists
        qry = "SELECT 1 FROM pg_tables WHERE tablename = %s"
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, (self.name,))
            res = cursor.fetchone()
            if res is None:
                # No such table, build it.
                print(f"Making cache table {self.name}")
                self._make_table()

    def shutdown(self):
        # Close our connections
        self.pools.put_all(self.pool_name)
        self.conn = None
        self.iterating_conn = None

    def defer_commits(self, every=100):
        """Stop committing inside every set(). Committing per write cost an
        fsync per write -- for merge, ~5 per record per worker.

        `every` is how many writes may accumulate before the cache commits
        itself; the counting is done here so callers don't each reimplement
        it. The commit happens in checkpoint(), not in set(), so it always
        lands on a boundary the caller chose: a unit of work spanning
        several writes (merge writes a merged record plus its recordcache2
        rows) is never left half-committed. Call checkpoint() at the end of
        each unit -- for most callers that is once per loop iteration.

        Deferral is process-wide, not per-cache, because every cache shares
        one write connection -- see PoolManager.deferring. Nothing commits
        on its own without checkpoint(), so pair this with resume_commits()
        or flush() when the loop ends.

        Expects parallel workers to write disjoint keys. Merge and export
        qualify: each worker owns a slice of YUIDs, and every write a merged
        record makes is keyed by that record's YUID (run-merge's
        claim_member() is what keeps it true). Reconcile does not --
        collect() stores shared external authority records, so every worker
        upserts the same rows, and holding those locks across a batch made
        two workers wait on each other until postgres killed one with
        `deadlock detected`. If workers can write the same key, commit per
        write so each lock lives microseconds.

        A deadlock in a deferred batch is recovered from rather than lost
        (the batch is replayed -- see PoolManager.replay_deferred), but it
        still means two workers are writing the same rows, so treat one as a
        bug in the partitioning rather than as normal contention.

        `every` also bounds memory: the records in the current batch are held
        until it commits, so they can be replayed."""
        self.pools.deferring = True
        self.pools.commit_every = int(every)

    def resume_commits(self):
        """Land anything outstanding and go back to committing per set()."""
        self.flush()
        self.pools.deferring = False
        self.pools.commit_every = 0

    def checkpoint(self):
        """Mark the end of a unit of work: commit if enough writes have
        accumulated since the last one. No-op when not deferring."""
        if not self.pools.deferring:
            return
        if self.pools.commit_every and self.pools.pending_writes >= self.pools.commit_every:
            self.flush()

    def flush(self):
        """Commit outstanding writes on the shared write connection. Safe to
        call when nothing is deferred (an empty commit is a no-op)."""
        if self.conn is None:
            self.conn = self.pools.get_conn(self.pool_name)
        self.pools.commit_all()

    def _cursor(self, internal=True, iter=False, size=0):
        # Ensure cursor is managed server-side otherwise select * from table
        # will return EVERYTHING into python (without a manual LIMIT/OFFSET)
        # Get a connection from the pool

        if iter and not self.iterating_conn:
            self.iterating_conn = self.pools.get_conn(self.pool_name, itr=True)
        elif iter is False and not self.conn:
            self.conn = self.pools.get_conn(self.pool_name, itr=False)
        if iter:
            conn = self.iterating_conn
        else:
            conn = self.conn
        if internal:
            # ensure uniqueness across multiple instances of the code
            name = f"server_cursor_{self.name}_{time.time()}".replace(".", "_")
            cursor = conn.cursor(name=name, cursor_factory=RealDictCursor)
            if not size:
                size = self.config["cursor_size"]
            cursor.itersize = size
        else:
            # Need this for creating the tables/indexes
            cursor = conn.cursor(cursor_factory=RealDictCursor)
        return cursor

    # --- pgcache ---

    def _make_table(self):
        qry = f"""CREATE TABLE public.{self.name} (
            {self.pk_defn}
            insert_time timestamp without time zone,
            record_time timestamp without time zone,
            refresh_time timestamp without time zone,
            valid boolean,
            change VARCHAR,
            data jsonb NOT NULL);"""

        # Enable iteration based on insert_time
        idxQry = f"""CREATE INDEX {self.name}_time_idx ON {self.name} ( insert_time  DESC NULLS LAST )"""

        with self._cursor(internal=False) as cursor:
            try:
                cursor.execute(qry)
                cursor.execute(idxQry)
                self.conn.commit()
            except Exception as e:
                print(f"Make table failed: {e}")
                self.conn.rollback()

    def len(self):
        qry = f"SELECT COUNT(*) FROM {self.name}"
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry)
            res = cursor.fetchone()
        return res["count"]

    def metadata(self, key, field="insert_time", _key_type=None):
        if _key_type is None:
            _key_type = self.key
        if _key_type == "yuid" and len(key) != 36:
            print(f"{self.name} has UUIDs as keys")
            return None
        if not field in ["insert_time", "record_time", "refresh_time", "valid", "change"]:
            raise ValueError(f"Unknown metadata field in cache: {field}")
        qry = f"SELECT {field} FROM {self.name} WHERE {_key_type} = %s"
        params = (key,)
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, params)
            rows = cursor.fetchone()
        return rows

    def set_metadata(self, key, field, value, _key_type=None):
        if _key_type is None:
            _key_type = self.key
        if _key_type == "yuid" and len(key) != 36:
            print(f"{self.name} has UUIDs as keys")
            return None
        if not field in ["record_time", "refresh_time", "valid", "change"]:
            raise ValueError(f"Attempt to set unsettable metadata field in cache: {field}")
        qry = f"UPDATE {self.name} SET {field} = %s WHERE {_key_type} = %s"
        params = (value, key)
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, params)
            self.pools.commit_all()

    def latest(self):
        qry = f"SELECT insert_time FROM {self.name} ORDER BY insert_time DESC LIMIT 1"
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry)
            res = cursor.fetchone()
        if res:
            return res["insert_time"].isoformat()
        else:
            return "0000-01-01T00:00:00"

    def len_estimate(self):
        qry = f"SELECT (reltuples/relpages) * (pg_relation_size('{self.name}') \
        / (current_setting('block_size')::integer)) AS count FROM pg_class WHERE relname='{self.name}';"
        with self._cursor(internal=False) as cursor:
            try:
                cursor.execute(qry)
                res = cursor.fetchone()
            except:
                # print(f"Called len_estimate, didn't get any hits, rolling back")
                self.conn.rollback()
                res = {"count": 0}
        return int(res["count"])

    def _select_list(self, raw=False):
        """Column list for a full-row SELECT.

        With raw=True the data column comes back as the JSON text postgres
        already stores, instead of psycopg2 parsing the jsonb into python
        objects. A caller that only wants to write the document out (export)
        shouldn't pay for a parse and then a re-serialise of the same bytes;
        one that needs to work on the record json.loads() it itself.

        SELECT * can't express this -- there's no way to exclude a column --
        so raw needs the real column list, read once per table."""
        if not raw:
            return "*"
        if self._cols is None:
            qry = """SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position"""
            with self._cursor(internal=False) as cursor:
                cursor.execute(qry, (self.name,))
                self._cols = [r["column_name"] for r in cursor.fetchall()]
        if not self._cols:
            # __init__ creates the table if it's absent, so this shouldn't
            # happen; fail loudly rather than silently handing back parsed
            # data to a caller that is about to json.loads() it
            raise ValueError(f"Could not read columns of {self.name} for a raw select")
        return ", ".join("data::text AS data" if c == "data" else c for c in self._cols)

    def get(self, key, _key_type=None, raw=False):
        # Get a record either by YUID or internal identifier,
        if _key_type is None:
            _key_type = self.key
        if _key_type == "yuid" and len(key) != 36:
            print(f"{self.name} has UUIDs as keys")
            return None

        qry = f"SELECT {self._select_list(raw)} FROM {self.name} WHERE {_key_type} = %s"
        params = (key,)
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, params)
            rows = cursor.fetchone()
        if rows:
            rows["source"] = self.config["name"]
        # sys.stdout.write('G');sys.stdout.flush()
        return rows

    def get_multi(self, keys, _key_type=None, raw=False):
        """Fetch several records in one query. {key: row} with missing keys
        left out. Saves a round trip wherever a caller probes more than one
        form of an identifier."""
        if _key_type is None:
            _key_type = self.key
        keys = [k for k in keys if not (_key_type == "yuid" and len(k) != 36)]
        if not keys:
            return {}
        qry = (f"SELECT {self._select_list(raw)} FROM {self.name} "
               f"WHERE {_key_type} = ANY(%s)")
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, (keys,))
            rows = cursor.fetchall()
        out = {}
        for row in rows:
            row["source"] = self.config["name"]
            out[row[_key_type]] = row
        return out

    def get_fresh(self, key, since=None, raw=False, _key_type=None):
        """Fetch a record only if it is at least as new as `since`.

        Collapses the has_item() + metadata() + get() sequence callers were
        writing by hand -- presence, freshness and the payload -- into one
        round trip. Returns None when the row is absent or older than
        `since`; since=None means any cached row will do.

        Pair with raw=True to write a cached document straight out without
        parsing it."""
        if _key_type is None:
            _key_type = self.key
        if _key_type == "yuid" and len(key) != 36:
            print(f"{self.name} has UUIDs as keys")
            return None

        cols = self._select_list(raw)
        if since is None:
            qry = f"SELECT {cols} FROM {self.name} WHERE {_key_type} = %s"
            params = (key,)
        else:
            qry = f"SELECT {cols} FROM {self.name} WHERE {_key_type} = %s AND insert_time >= %s"
            params = (key, since)
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, params)
            rows = cursor.fetchone()
        if rows:
            rows["source"] = self.config["name"]
        return rows

    def get_like(self, key, _key_type=None):
        # Get a record either by YUID or internal identifier,
        if _key_type is None:
            _key_type = self.key
        if _key_type == "yuid" and len(key) != 36:
            print(f"{self.name} has UUIDs as keys")
            return None

        # ORDER BY in case we have multiple copies from different times:
        # we want the most recent (the comment promised this but the query
        # had no ORDER BY, so postgres returned an arbitrary matching row)
        qry = f"SELECT * FROM {self.name} WHERE {_key_type} LIKE %s ORDER BY insert_time DESC"
        params = (key + "%",)
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, params)
            rows = cursor.fetchone()
        if rows:
            rows["source"] = self.config["name"]
        return rows

    def list(self, timestamp=None):

        raise NotImplementedError("list is not implemented for PostgresCache")

        # List records changed since timestamp
        # cast timestamp into datetime it not already
        # FIXME: This should really be an iterator that pages through
        if timestamp is None:
            qry = f"SELECT {self.key} FROM {self.name}"
            params = []
        else:
            qry = f"SELECT {self.key} FROM {self.name} WHERE insert_time >= %s"
            params = (timestamp,)
        with self._cursor() as cursor:
            cursor.execute(qry, params)
            rows = cursor.fetchall()
        return [x[self.key] for x in rows]

    def _slice_predicate(self, mySlice, maxSlice):
        """WHERE clause partitioning the table into maxSlice disjoint parts.

        row_number() OVER (ORDER BY key) made every one of the N workers sort
        the entire table just to find its 1/N of it. Hashing the key gives
        the same partition in every worker with no window and no sort, and
        leaves a predicate postgres can parallel-scan.

        hashtext() is internal to postgres rather than a documented API, but
        it only has to agree between workers running against one server at
        one time, which it does.

        The mask matters: postgres % takes the sign of its left operand, so
        without it every row with a negative hash -- about half the table --
        would match no slice at all and never be processed."""
        mySlice = int(mySlice)
        maxSlice = int(maxSlice)
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")
        return f"(hashtext({self.key}::text) & 2147483647) % {maxSlice} = {mySlice}"

    # NOTE: none of the iter_records* methods set the "source" key that
    # get() adds, so rows from an iterator and rows from a per-key fetch are
    # not quite the same shape. Callers that need it (run-merge) set it
    # themselves; changing it here would add a key to every existing
    # iterator caller's rows.
    def iter_records_slice(self, mySlice=0, maxSlice=10, raw=False):
        qry = (f"SELECT {self._select_list(raw)} FROM {self.name} "
               f"WHERE {self._slice_predicate(mySlice, maxSlice)}")
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry)
            for res in cursor:
                yield res

    def iter_keys_slice(self, mySlice=0, maxSlice=10):
        qry = f"SELECT {self.key} FROM {self.name} WHERE {self._slice_predicate(mySlice, maxSlice)}"
        with self._cursor(iter=True, size=50000) as cursor:
            cursor.execute(qry)
            for res in cursor:
                yield res[self.key]

    def iter_keys_slice_mem(self, mySlice=0, maxSlice=10):
        # DON'T use row_number() as it's freaking slow
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")

        qry = f"""SELECT {self.key} FROM {self.name} ORDER BY {self.key} ASC"""
        ct = 0
        with self._cursor(iter=True, size=50000) as cursor:
            cursor.execute(qry)
            for res in cursor:
                if (ct % maxSlice) - mySlice == 0:
                    yield res[self.key]
                ct += 1

    def iter_keys_since(self, timestamp=None):
        if timestamp is None:
            qry = f"SELECT {self.key} FROM {self.name} ORDER BY insert_time DESC"
            params = []
        else:
            qry = f"SELECT {self.key} FROM {self.name} WHERE record_time >= %s ORDER BY insert_time DESC"
            params = (timestamp,)
        with self._cursor(iter=True, size=50000) as cursor:
            cursor.execute(qry, params)
            for res in cursor:
                yield res[self.key]

    def iter_records_since(self, timestamp=None):
        if timestamp is None:
            qry = f"SELECT * FROM {self.name} ORDER BY insert_time DESC"
            params = []
        else:
            qry = f"SELECT * FROM {self.name} WHERE record_time >= %s ORDER BY insert_time DESC"
            params = (timestamp,)
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry, params)
            for res in cursor:
                yield res

    def iter_records(self):
        qry = f"SELECT * FROM {self.name}"
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry)
            for res in cursor:
                yield res

    def iter_keys(self):
        qry = f"SELECT {self.key} FROM {self.name}"
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry)
            for res in cursor:
                yield res[self.key]

    def iter_records_type(self, t):
        # allow query for records by top level type value
        # CREATE INDEX merged_type_idx ON merged_merged_record_cache USING BTREE ((data->'type'));
        # is important!

        if t == "Concept":
            qry = f"SELECT * FROM {self.name} WHERE data->>'type' IN ('Type', 'Currency', 'Language', 'Material', 'MeasurementUnit')"
        else:
            qry = f"SELECT * FROM {self.name} WHERE data->'type' = '\"{t}\"'"
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry)
            for res in cursor:
                yield res

    def iter_keys_type_slice(self, t, mySlice=0, maxSlice=10):
        # DON'T use row_number() as it's freaking slow
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")

        # ORDER BY is required: slicing by row position over an unordered
        # result set gives each parallel worker a different view of "row N"
        # (synchronized seq scans start at arbitrary heap offsets), so
        # records get double-processed by some workers and skipped by others
        if t == "Concept":
            qry = f"SELECT * FROM {self.name} WHERE data->>'type' IN ('Type', 'Currency', 'Language', 'Material', 'MeasurementUnit') ORDER BY {self.key} ASC"
        else:
            qry = f"SELECT * FROM {self.name} WHERE data->'type' = '\"{t}\"' ORDER BY {self.key} ASC"
        ct = 0
        with self._cursor(iter=True, size=50000) as cursor:
            cursor.execute(qry)
            for res in cursor:
                if (ct % maxSlice) - mySlice == 0:
                    yield res[self.key]
                ct += 1

    def iter_keys_type(self, t):
        # allow query for records by top level type value
        # CREATE INDEX merged_type_idx ON merged_merged_record_cache USING BTREE ((data->'type'));
        # is important!

        if t == "Concept":
            qry = f"SELECT {self.key} FROM {self.name} WHERE data->>'type' IN ('Type', 'Currency', 'Language', 'Material', 'MeasurementUnit')"
        else:
            qry = f"SELECT {self.key} FROM {self.name} WHERE data->'type' = '\"{t}\"'"
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry)
            for res in cursor:
                yield res[self.key]

    def _upsert(self, qry, params, identifier=None, yuid=None):
        """Run one upsert, surviving the transient deadlocks that parallel
        workers touching the same row produce.

        Postgres aborts the whole transaction when it picks one as a deadlock
        victim, not just the statement that lost. Committing per write made
        that a non-event -- there was nothing else in the transaction -- but
        while deferring it takes every write since the last commit with it.
        So the batch is replayed before the losing statement is retried;
        deferred statements are all upserts, so replaying one that had
        already landed changes nothing."""
        replay = False
        for attempt in range(DEADLOCK_RETRIES):
            try:
                if replay:
                    self.pools.replay_deferred()
                    replay = False
                with self._cursor(internal=False) as cursor:
                    cursor.execute(qry, params)
                if self.pools.deferring:
                    self.pools.record_deferred(qry, params)
                else:
                    self.conn.commit()
                return
            except psycopg2.extensions.TransactionRollbackError as e:
                # NB: the shared base of DeadlockDetected (40P01) and
                # SerializationFailure (40001). errors.TransactionRollback
                # is a *sibling* of those, not a parent, so catching it
                # instead silently catches nothing.
                # Deadlock or serialization failure: postgres picked this
                # transaction as the victim, which happens when parallel
                # workers upsert the same rows. Transient by definition --
                # the statement just needs running again.
                self.conn.rollback()
                if self.pools.deferring:
                    # the rollback threw the batch away; put it back before
                    # retrying, otherwise the retry succeeds into a hole
                    replay = True
                if attempt == DEADLOCK_RETRIES - 1:
                    if self.pools.deferring:
                        print(f"Deadlock in {self.name} while deferring, and "
                              f"{len(self.pools.deferred_stmts)} replayed writes "
                              f"could not land either. Deferral expects workers "
                              f"to write disjoint keys -- see defer_commits().")
                    print(f"Failed to upsert {identifier}/{yuid} in "
                          f"{self.name} after {DEADLOCK_RETRIES} "
                          f"attempts: {e}")
                    self.pools.deferred_stmts.clear()
                    self.pools.pending_writes = 0
                    raise
                # jittered so two victims don't collide again in step
                time.sleep(DEADLOCK_BACKOFF * (attempt + 1) * (0.5 + random.random()))
            except Exception as e:
                # A swallowed failure here silently loses the write; log
                # and re-raise so the caller/build sees it
                print(f"Failed to upsert {identifier}/{yuid} in {self.name}: {e}")
                self.conn.rollback()
                if self.pools.deferring:
                    print(f"  ... discarding {len(self.pools.deferred_stmts)} "
                          f"writes since the last flush()")
                    self.pools.deferred_stmts.clear()
                    self.pools.pending_writes = 0
                raise

    def set(
        self,
        data,
        identifier=None,
        yuid=None,
        format=None,
        valid=None,
        record_time=None,
        refresh_time=None,
        change=None,
    ):
        if not identifier and not yuid:
            raise ValueError("Must give YUID or Identifier or both")
        if not type(data) == dict:
            raise ValueError("Data must be a dict()")
        else:
            jdata = Json(data, dumps=_dumps)
        if yuid is not None and not type(yuid) == str:
            yuid = str(yuid)

        insert_time = datetime.datetime.now()
        if record_time is None:
            record_time = insert_time
        if refresh_time is None:
            refresh_time = "9999-12-31T00:00:00Z"
        if format is None:
            format = "JSON-LD"

        qnames = ["data", "identifier", "yuid", "insert_time", "record_time", "refresh_time", "valid", "change"]
        qvals = (jdata, identifier, yuid, insert_time, record_time, refresh_time, valid, change)
        qd = dict(zip(qnames, qvals))
        qps = [qn for qn in qnames if qd[qn] is not None]
        qvs = tuple([qv for qv in qvals if qv is not None])
        pholders = ",".join(["%s"] * len(qps))
        qpstr = ",".join(qps)

        if self.config["overwrite"]:
            qry = f"""INSERT INTO {self.name} ({qpstr}) VALUES ({pholders})
            ON CONFLICT ({self.key}) DO UPDATE SET ({qpstr}) = ({pholders})"""
            self._upsert(qry, qvs * 2, identifier, yuid)
            return

        # Not an overwrite cache: plain insert, duplicates expected.
        # The UniqueViolation below is survivable, but its rollback would take
        # the whole open transaction with it. Land anything deferred first so
        # one duplicate can't silently discard every record written since the
        # last flush(). Merge writes only to overwrite caches, so this costs
        # nothing there. This branch therefore never defers, and the insert
        # stays committed per statement.
        if self.pools.deferring:
            self.pools.commit_all()
        with self._cursor(internal=False) as cursor:
            try:
                qry = f"INSERT INTO {self.name} ({qpstr}) VALUES ({pholders})"
                cursor.execute(qry, qvs)
                self.conn.commit()
            except psycopg2.errors.UniqueViolation:
                # expected when re-inserting without overwrite; the row
                # is already present, keep it
                print(f"Duplicate key for {identifier}/{yuid} in {self.name}; keeping existing")
                self.conn.rollback()
            except Exception as e:
                # anything else (serialization failure, value too long,
                # deadlock) used to be silently dropped
                print(f"Failed to insert {identifier}/{yuid} in {self.name}: {e}")
                self.conn.rollback()
                raise
        # sys.stdout.write('S');sys.stdout.flush()

    def delete(self, key, _key_type=None):
        if _key_type is None:
            _key_type = self.key
        qry = f"DELETE FROM {self.name} WHERE {_key_type} = %s"
        params = (key,)
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, params)
            self.pools.commit_all()

    def clear(self):
        # WARNING WARNING ... trash all the data in the cache
        qry = f"TRUNCATE TABLE {self.name} RESTART IDENTITY"
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry)
            self.pools.commit_all()

    def has_item(self, key, _key_type=None, timestamp=None):
        if _key_type is None:
            _key_type = self.key
        if timestamp is None:
            qry = f"SELECT 1 FROM {self.name} WHERE {_key_type} = %s LIMIT 1"
            params = (key,)
        else:
            qry = f"SELECT 1 FROM {self.name} WHERE {_key_type} = %s AND record_time >= %s LIMIT 1"
            params = (key, timestamp)

        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, params)
            rows = cursor.fetchone()
        # sys.stdout.write('?');sys.stdout.flush()
        return bool(rows)

    def commit(self):
        # Normally a no-op: we commit after every write unless deferring
        self.flush()

    def start_bulk(self):
        if self.iterating_conn is None:
            self.iterating_conn = self.pools.get_conn(self.pool_name, itr=True)
        self.bulk_cursor = self.iterating_conn.cursor(cursor_factory=RealDictCursor)

    def set_bulk(
        self,
        data,
        identifier=None,
        yuid=None,
        format=None,
        valid=None,
        record_time=None,
        refresh_time=None,
        change=None,
    ):
        data = Json(data, dumps=_dumps)
        insert_time = datetime.datetime.now()
        if record_time is None:
            record_time = insert_time
        if refresh_time is None:
            refresh_time = "9999-12-31T00:00:00Z"
        if format is None:
            format = "JSON-LD"

        qnames = ["data", "identifier", "yuid", "insert_time", "record_time", "refresh_time", "valid", "change"]
        qvals = (data, identifier, yuid, insert_time, record_time, refresh_time, valid, change)
        qd = dict(zip(qnames, qvals))
        qps = [qn for qn in qnames if qd[qn] is not None]
        qvs = tuple([qv for qv in qvals if qv is not None])
        pholders = ",".join(["%s"] * len(qps))
        qpstr = ",".join(qps)

        try:
            qry = f"INSERT INTO {self.name} ({qpstr}) VALUES ({pholders})"
            self.bulk_cursor.execute(qry, qvs)
        except:
            # Could be a psycopg2.errors.UniqueViolation if we're trying to insert without delete
            # BUT this will rollback the entire transaction so bail
            raise

    def end_bulk(self):
        self.iterating_conn.commit()
        self.bulk_cursor.close()
        self.bulk_cursor = None

    def optimize(self):
        # Call VACUUM on the table
        qry = f"VACUUM (ANALYZE) {self.name}"

        if not self.conn:
            self.conn = self.pools.get_conn(self.pool_name)
        old_iso = self.conn.isolation_level
        self.conn.set_isolation_level(0)
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry)
            # commit_all, not conn.commit: VACUUM needs its own transaction,
            # so anything deferred lands here whether we like it or not, and
            # the replay buffer has to be told
            self.pools.commit_all()
        self.conn.set_isolation_level(old_iso)

    ### Behave like a dict
    def __getitem__(self, what):
        return self.get(what)

    def __setitem__(self, what, value):
        if "data" in value:
            # given a full record; DO NOT MUTATE IT
            d = value["data"]
            params = {}
            if "identifier" in value and what != value["identifier"]:
                raise ValueError("Record's identifier value and key given to set are different")
            for v in ["identifier", "yuid", "format", "valid", "change", "record_time"]:
                if v in value:
                    params[v] = value[v]
            return self.set(d, **params)
        else:
            # given only the json
            if self.key == "identifier":
                return self.set(value, identifier=what)
            elif self.key == "yuid":
                return self.set(value, yuid=what)

    def __delitem__(self, what):
        return self.delete(what)

    def __contains__(self, what):
        return self.has_item(what)

    def __len__(self):
        return self.len()

    def __bool__(self):
        # Don't let it go through to len
        # and warn that the code shouldn't do this
        print(f"*** code somewhere is asking for a cache as a boolean value and shouldn't ***")
        return True


class DataCache(PooledCache):
    # YUID is informative, Identifier is PK, data is bespoke

    def __init__(self, config):
        self.pk_defn = """            yuid uuid,
            identifier VARCHAR (256) PRIMARY KEY,"""
        self.key = "identifier"
        if not "tabletype" in config:
            config["tabletype"] = "data_cache"
        super().__init__(config)


class InternalRecordCache(PooledCache):
    # No YUID, Identifier is PK, data is LOD from units

    def __init__(self, config):
        self.pk_defn = """identifier VARCHAR (120) PRIMARY KEY,"""
        self.key = "identifier"
        if not "tabletype" in config:
            config["tabletype"] = "record_cache"
        super().__init__(config)


class ExternalRecordCache(PooledCache):
    # YUID is informative, Identifier is PK, data is LOD mapped from External

    def __init__(self, config):
        self.pk_defn = """            yuid uuid,
        identifier VARCHAR (120) PRIMARY KEY,"""
        self.key = "identifier"
        if not "tabletype" in config:
            config["tabletype"] = "ext_record_cache"
        super().__init__(config)


class ExternalReconciledRecordCache(PooledCache):
    # YUID is informative, Identifier is PK, data is LOD mapped from External

    def __init__(self, config):
        self.pk_defn = """            yuid uuid,
        identifier VARCHAR (120) PRIMARY KEY,"""
        self.key = "identifier"
        if not "tabletype" in config:
            config["tabletype"] = "ext_reconciled_record_cache"
        super().__init__(config)


class RecordCache(PooledCache):
    # YUID is PK, Identifier is informative, data is re-identified LOD

    def __init__(self, config):
        self.pk_defn = """            yuid uuid PRIMARY KEY,
        identifier VARCHAR (120),"""
        self.key = "yuid"
        if not "tabletype" in config:
            config["tabletype"] = "rewritten_record_cache"
        super().__init__(config)


class MergedRecordCache(PooledCache):
    # YUID is PK, no Identifier, data is merged, re-identified LOD
    # Source is here just a naming convention to allow multiple merged caches

    def __init__(self, config):
        self.pk_defn = """            yuid uuid PRIMARY KEY,"""
        self.key = "yuid"
        if not "tabletype" in config:
            config["tabletype"] = "merged_record_cache"
        super().__init__(config)
