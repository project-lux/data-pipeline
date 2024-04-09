import psycopg2
from psycopg2.extras import RealDictCursor, Json
from psycopg2.pool import SimpleConnectionPool
import time
import datetime
import sys

#
# How to index into JSONB arrays:
# SELECT identifier FROM ycba_record_cache, 
#    jsonb_array_elements(data -> 'produced_by'->'carried_out_by') ids
#    WHERE ids->>'id' = 'https://ycba-lux.s3.amazonaws.com/v3/person/00/00628d01-deea-4811-b262-5ea81b732fba.json'
#

# How to dump the databases using pg_dump:
# pg_dump -U USER -F c --clean --no-owner -t aat_data_cache record_cache > aat_data_cache.pgdump
# pg_restore -a -U pipeline -W --host HOST -d DATABASE wof_data_cache.pgdump 


class PoolManager(object):

    def __init__(self):
        self.mincxn = 2
        self.maxcxn = 4
        self.pools = {}

    def make_pool(self, name, *args, **kw):
        if not name in self.pools:
            self.pools[name] = SimpleConnectionPool(self.mincxn, self.maxcxn, *args, **kw)

    def get_conn(self, name, key=None):
        if name in self.pools:
            return self.pools[name].getconn(key)
        else:
            return None

    def put_conn(self, name, conn, key=None, close=False):
        if name in self.pools:
            self.pools[name].putconn(conn, key, close)
        else:
            return None

    def put_all(self, name):
        if name in self.pools:
            conns = list(self.pools[name]._used.values())
            for conn in conns:
                try:
                    conn.commit()
                except:
                    pass
                try:
                    conn.close()
                except:
                    pass
                try:
                    self.pools[name].putconn(conn, None, True)
                except:
                    pass
        else:
            return None


poolman = PoolManager()

class PGCache(object):
    def __init__(self, config):

        self.config = config
        self.name = config['name'] + '_' + config['tabletype']
        self.conn = None
        self.iterating_conn = None

        if config['connect']:
            self.conn = None
            self._connect()

    def _connect(self):
        if self.conn:
            try:
                self.conn.commit()
                self.conn.close()
            except Exception:
                pass
        if self.config['host']:
            # TCP/IP
            self.conn = psycopg2.connect(host=self.config['host'], port=self.config['port'], 
                user=self.config['user'], password=self.config['password'], dbname=self.config['dbname'])
        else:
            # local socket
            self.conn = psycopg2.connect(user=self.config['user'], dbname=self.config['dbname'])

        # Test that our table exists
        qry = f'SELECT 1 FROM {self.name} LIMIT 1'
        with self._cursor(internal=False) as cursor:    
            try:
                cursor.execute(qry)
            except psycopg2.errors.UndefinedTable:
                # No such table, build it.
                print(f"Building cache table {self.name}...")
                self.conn.rollback()
                self._make_table()

    def _make_table(self):
        qry = f"""CREATE TABLE public.{self.name} (
            {self.pk_defn}   
            insert_time timestamp without time zone,
            record_time timestamp without time zone,
            refresh_time timestamp without time zone,
            valid boolean,
            change VARCHAR,
            data jsonb NOT NULL);"""        

        # Create an index on PK and insert time
        # idxQry = f"""CREATE INDEX {self.name}_id_idx ON {self.name} ( {self.key} ASC NULLS LAST )"""
        # This is made by default as _PKEY ; no need to make it manually

        idxQry2 = f"""CREATE INDEX {self.name}_time_idx ON {self.name} ( insert_time  DESC NULLS LAST )"""

        with self._cursor(internal=False) as cursor:
            try:
                cursor.execute(qry)
                cursor.execute(idxQry2)
                self.conn.commit()
            except Exception as e:
                print(f"Make table failed: {e}")
                self.conn.rollback()

    def _cursor(self, internal=True, is_iter=False):
        # Ensure cursor is managed server-side otherwise select * from table
        # will return EVERYTHING into python (without a manual LIMIT/OFFSET)

        if internal:
            # ensure uniqueness across multiple instances of the code
            name = f"server_cursor_{self.name}_{time.time()}".replace('.', '_')
            cursor = self.conn.cursor(name=name, cursor_factory=RealDictCursor)
            cursor.itersize = self.config['cursor_size']
        else:
            # Need this for creating the tables/indexes
            cursor = self.conn.cursor(cursor_factory=RealDictCursor)

        if is_iter:
            self.iterating_conn = self.conn
            self.conn = None
            self._connect()

        return cursor

    def _close(self, conn=None, is_iter=False):
        if is_iter:
            self.iterating_conn.close()
            self.iterating_conn = None

    def len(self):
        qry = f"SELECT COUNT(*) FROM {self.name}"
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry)
            res = cursor.fetchone()
        self._close()
        return res['count']


    def latest(self):
        qry = f"SELECT insert_time FROM {self.name} ORDER BY insert_time DESC LIMIT 1"
        with self._cursor() as cursor:
            cursor.execute(qry)
            res = cursor.fetchone()
        self._close()
        if res:
            return res['insert_time'].isoformat()
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
                res = {'count':0}
        self._close()
        return int(res['count'])

    def get(self, key, _key_type=None):
        # Get a record either by YUID or internal identifier,
        if _key_type is None:
            _key_type = self.key
        if _key_type == 'yuid' and len(key) != 36:
            print(f"{self.name} has UUIDs as keys")
            return None

        qry = f"SELECT * FROM {self.name} WHERE {_key_type} = %s"
        params = (key,)
        cursor = self._cursor(internal=False)
        cursor.execute(qry, params)
        rows = cursor.fetchone()
        cursor.close()        
        self.conn.commit()
        # self._close()
        if rows:
            rows['source'] = self.config['name']

        # sys.stdout.write('G');sys.stdout.flush()
        return rows

    def get_like(self, key, _key_type=None):
        # Get a record either by YUID or internal identifier,
        if _key_type is None:
            _key_type = self.key
        if _key_type == 'yuid' and len(key) != 36:
            print(f"{self.name} has UUIDs as keys")
            return None

        # ORDER BY here is in case we have multiple copies from different times
        # We want the most recent
        qry = f"SELECT * FROM {self.name} WHERE {_key_type} LIKE %s"
        params = (key + "%",)
        cursor = self._cursor(internal=False)
        cursor.execute(qry, params)
        rows = cursor.fetchone()
        cursor.close()        
        self.conn.commit()
        # self._close()
        if rows:
            rows['source'] = self.config['name']
        return rows


    def list(self, timestamp=None):
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
        # self._close()
        return [x[self.key] for x in rows]

    # SELECT t.* FROM (SELECT *, row_number() OVER 
    #   (ORDER BY identifier ASC) AS row FROM ycba_data_cache) 
    #   t WHERE t.row % 10 = 1 LIMIT 10

    def iter_records_slice(self, mySlice=0, maxSlice=10):
        # use row_number() to partition the results into slices for parallel processing
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")

        qry = f"""SELECT t.* FROM (SELECT *, row_number() OVER (ORDER BY {self.key} ASC) 
            AS row FROM {self.name}) t WHERE t.row % {maxSlice} = {mySlice}"""
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry)            
            for res in cursor:
                yield res  
        self._close(is_iter=True)

    def iter_keys_slice(self, mySlice=0, maxSlice=10):
        # use row_number() to partition the results into slices for parallel processing
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")

        qry = f"""SELECT {self.key} FROM (SELECT {self.key}, row_number() OVER (ORDER BY {self.key} ASC) 
            AS row FROM {self.name}) t WHERE t.row % {maxSlice} = {mySlice}"""
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry)            
            for res in cursor:
                yield res[self.key]
        self._close(is_iter=True)

    def iter_keys_since(self, timestamp=None):
        if timestamp is None:
            qry = f"SELECT {self.key} FROM {self.name} ORDER BY insert_time DESC"
            params = []
        else:
            qry = f"SELECT {self.key} FROM {self.name} WHERE record_time >= %s ORDER BY insert_time DESC"        
            params = (timestamp,)
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry, params)            
            for res in cursor:
                yield res[self.key]
        self._close(is_iter=True)

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
        self._close(is_iter=True)

    def iter_records(self):
        qry = f"SELECT * FROM {self.name}"        
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry)            
            for res in cursor:
                yield res 
        self._close(is_iter=True)

    def iter_keys(self):
        qry = f"SELECT {self.key} FROM {self.name}"        
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry)            
            for res in cursor:
                yield res[self.key]     
        self._close(is_iter=True)

    def iter_records_type(self, t):
        # allow query for records by top level type value
        # CREATE INDEX merged_type_idx ON merged_merged_record_cache USING BTREE ((data->'type'));
        # is important!
        qry = f"SELECT * FROM {self.name} WHERE data->'type' = '\"{t}\"'"
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry)            
            for res in cursor:
                yield res     
        self._close(is_iter=True)

    def set(self, data, identifier=None, yuid=None, format=None, valid=None,
            record_time=None, refresh_time=None, change=None):
        if not identifier and not yuid:
            raise ValueError("Must give YUID or Identifier or both")
        if not type(data) == dict:
            raise ValueError("Data must be a dict()")
        else:
            jdata = Json(data)
        if yuid is not None and not type(yuid) == str:
            yuid = str(yuid)

        insert_time = datetime.datetime.now()
        if record_time is None:
            record_time = insert_time
        if refresh_time is None:
            refresh_time = "9999-12-31T00:00:00Z"
        if format is None:
            format = "JSON-LD"

        qnames  = ['data', 'identifier', 'yuid', 'insert_time', 'record_time', 'refresh_time', 'valid', 'change']
        qvals = (jdata, identifier, yuid, insert_time, record_time, refresh_time, valid, change)            
        qd = dict(zip(qnames, qvals))
        qps = [qn for qn in qnames if qd[qn] is not None]
        qvs = tuple([qv for qv in qvals if qv is not None])
        pholders = ",".join(["%s"] * len(qps))
        qpstr = ",".join(qps)

        cursor = self._cursor(internal=False)
        if self.config['overwrite']:
            try:
                qry = f"""INSERT INTO {self.name} ({qpstr}) VALUES ({pholders})
                ON CONFLICT ({self.key}) DO UPDATE SET ({qpstr}) = ({pholders})"""
                cursor.execute(qry, qvs*2)
                self.conn.commit()
            except Exception as e:
                # Could be a psycopg2.errors.UniqueViolation if we're trying to insert without delete
                print(f"DATA: {data}")
                print(f"Failed to upsert!: {e}?\n{qpstr} = {qvs}")
                self.conn.rollback()                
        else:
            try:
                qry = f"INSERT INTO {self.name} ({qpstr}) VALUES ({pholders})"
                cursor.execute(qry, qvs)
                self.conn.commit()
            except Exception as e:
                # Could be a psycopg2.errors.UniqueViolation if we're trying to insert without delete
                print(f"Duplicate key for {identifier}/{yuid} in {self.name}: {e}?")
                self.conn.rollback()                
        cursor.close()
        # sys.stdout.write('S');sys.stdout.flush()
        # self._close()

    def delete(self, key, _key_type=None):
        if _key_type is None:
            _key_type = self.key
        qry = f"DELETE FROM {self.name} WHERE {_key_type} = %s"
        params = (key,)
        with self._cursor(internal=False) as cursor:            
            cursor.execute(qry, params)
            self.conn.commit()
        # self._close()

    def clear(self):
        # WARNING WARNING ... trash all the data in the cache
        qry = f"TRUNCATE TABLE {self.name} RESTART IDENTITY"
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry)
            self.conn.commit()
        self._close()

    def has_item(self, key, _key_type=None, timestamp=None):
        if _key_type is None:
            _key_type = self.key
        if timestamp is None:
            qry = f"SELECT 1 FROM {self.name} WHERE {_key_type} = %s LIMIT 1"
            params = (key,)
        else:
            qry = f"SELECT 1 FROM {self.name} WHERE {_key_type} = %s AND record_time >= %s LIMIT 1"
            params = (key,timestamp)

        cursor = self._cursor(internal=False)
        cursor.execute(qry, params)
        rows = cursor.fetchone()
        cursor.close()
        self.conn.commit()
        # self._close()
        # sys.stdout.write('?');sys.stdout.flush()
        return bool(rows)

    def commit(self):
        # We commit after every transaction, so no need
        pass

    def __getitem__(self, what):
        return self.get(what)

    def __setitem__(self, what, value):
        if 'data' in value:
            # given a full record; DO NOT MUTATE IT
            d = value['data']
            params = {}
            if 'identifier' in value and what != value['identifier']:
                raise ValueError("Record's identifier value and key given to set are different")
            for v in ['identifier', 'yuid', 'format', 'valid', 'change', 'record_time']:
                if v in value:
                    params[v] = value[v]
            return self.set(d, **params)
        else:
            # given only the json
            if self.key == 'identifier':
                return self.set(value, identifier=what)
            elif self.key == 'yuid':
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


class PooledCache(PGCache):

    def __init__(self, config):
        self.config = config
        self.name = config['name'] + '_' + config['tabletype']
        self.conn = None
        self.iterating_conn = None
        if config['host']:
            # TCP/IP
            pname = f"{config['host']}:{config['port']}/{config['dbname']}"
            self.pool_name = pname
            poolman.make_pool(pname, host=self.config['host'], port=self.config['port'], 
                user=self.config['user'], password=self.config['password'], dbname=self.config['dbname'])
        else:
            # local socket
            pname = "localsocket"
            self.pool_name = pname
            poolman.make_pool(pname, user=self.config['user'], dbname=self.config['dbname'])

        # Test that our table exists
        qry = f'SELECT 1 FROM {self.name} LIMIT 1'
        with self._cursor(internal=False) as cursor:    
            try:
                cursor.execute(qry)
            except psycopg2.errors.UndefinedTable:
                # No such table, build it.
                print(f"Making cache table {self.name}")
                self.conn.rollback()
                self._make_table()
        # self._close()


    def _connect(self):
        self.conn = poolman.get_conn(self.pool_name)


    def shutdown(self):
        # Close our connections
        if self.conn is not None:
            self.conn.close()
            poolman.put_conn(self.pool_name, self.conn)
        if self.iterating_conn is not None:
            self.iterating_conn.close()
            poolman.put_conn(self.pool_name, self.iterating_conn)            


    def _close(self, conn=None, is_iter=False):
        if conn is None and is_iter == False:
            print(f"returned main connection")
            poolman.put_conn(self.pool_name, self.conn)
            self.conn = None
        elif is_iter:
            print(f"returned iterator")
            poolman.put_conn(self.pool_name, self.iterating_conn)
            self.iterating_conn = None
        elif conn is not None:
            print(f"returned specific connection")
            poolman.put_conn(self.pool_name, conn)


    def _cursor(self, internal=True, iter=False):
        # Ensure cursor is managed server-side otherwise select * from table
        # will return EVERYTHING into python (without a manual LIMIT/OFFSET)

        # Get a connection from the pool
        if not self.conn:
            self.conn = poolman.get_conn(self.pool_name)
        return PGCache._cursor(self, internal, iter)


    def optimize(self):
        # Call VACUUM on the table
        qry = f"VACUUM (ANALYZE) {self.name}"

        if not self.conn:
            self.conn = poolman.get_conn(self.pool_name)
        old_iso = self.conn.isolation_level
        self.conn.set_isolation_level(0)
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry)
            self.conn.commit()
        self.conn.set_isolation_level(old_iso)
        self._close()

    def start_bulk(self):
        self.bulk_conn = poolman.get_conn(self.pool_name)        
        self.bulk_cursor = self.bulk_conn.cursor(cursor_factory=RealDictCursor)

    def set_bulk(self, data, identifier=None, yuid=None, format=None, valid=None,
            record_time=None, refresh_time=None, change=None):

        data = Json(data)

        insert_time = datetime.datetime.now()
        if record_time is None:
            record_time = insert_time
        if refresh_time is None:
            refresh_time = "9999-12-31T00:00:00Z"
        if format is None:
            format = "JSON-LD"

        qnames  = ['data', 'identifier', 'yuid', 'insert_time', 'record_time', 'refresh_time', 'valid', 'change']
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
        self.bulk_conn.commit()
        self.bulk_cursor.close()
        self.bulk_cursor = None
        poolman.put_conn(self.pool_name, self.bulk_conn)
        self.bulk_conn = None


class DataCache(PooledCache):
    # YUID is informative, Identifier is PK, data is bespoke

    def __init__(self, config):
        self.pk_defn = """            yuid uuid,
            identifier VARCHAR (256) PRIMARY KEY,"""
        self.key = "identifier"
        if not 'tabletype' in config:
            config['tabletype'] = "data_cache"
        super().__init__(config)

class InternalRecordCache(PooledCache):
    # No YUID, Identifier is PK, data is LOD from units

    def __init__(self, config):
        self.pk_defn = """identifier VARCHAR (120) PRIMARY KEY,"""
        self.key = "identifier"
        if not 'tabletype' in config:
            config['tabletype'] = "record_cache"
        super().__init__(config)

class ExternalRecordCache(PooledCache):
    # YUID is informative, Identifier is PK, data is LOD mapped from External

    def __init__(self, config):
        self.pk_defn = """            yuid uuid,
        identifier VARCHAR (120) PRIMARY KEY,"""
        self.key = "identifier"
        if not 'tabletype' in config:
            config['tabletype'] = "ext_record_cache"
        super().__init__(config)

class ExternalReconciledRecordCache(PooledCache):
    # YUID is informative, Identifier is PK, data is LOD mapped from External

    def __init__(self, config):
        self.pk_defn = """            yuid uuid,
        identifier VARCHAR (120) PRIMARY KEY,"""
        self.key = "identifier"
        if not 'tabletype' in config:
            config['tabletype'] = "ext_reconciled_record_cache"
        super().__init__(config)

class RecordCache(PooledCache):
    # YUID is PK, Identifier is informative, data is re-identified LOD

    def __init__(self, config):
        self.pk_defn = """            yuid uuid PRIMARY KEY,
        identifier VARCHAR (120),"""
        self.key = "yuid"
        if not 'tabletype' in config:
            config['tabletype'] = "rewritten_record_cache"
        super().__init__(config)

class MergedRecordCache(PooledCache):
    # YUID is PK, no Identifier, data is merged, re-identified LOD
    # Source is here just a naming convention to allow multiple merged caches

    def __init__(self, config):
        self.pk_defn = """            yuid uuid PRIMARY KEY,"""
        self.key = "yuid"
        if not 'tabletype' in config:
            config['tabletype'] = "merged_record_cache"
        super().__init__(config)
