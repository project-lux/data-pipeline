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

# We actually only need two connections per process -- one to stay open for iteration, and one to read/write.
# This means we can't iterate two different tables at the same time, but that's fine.

class PoolManager(object):
    def __init__(self):
        self.conn = None
        self.iterating_conn = None
        self.pool = None

    def make_pool(self, name, host=None, port=None, user=None, password=None, dbname=None):   
        print(" PG making connection")
        if self.conn is None:
            if host:
                # TCP/IP
                self.conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
                self.iterating_conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
            else:
                # local socket
                self.conn = psycopg2.connect(user=user, dbname=dbname)
                self.iterating_conn = psycopg2.connect(user=user, dbname=dbname)
            self.pool = name
        print(" ... made")

    def get_conn(self, name, itr=False):
        print("  pool was asked for conn")
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

    def put_all(self, name):
        self.put_conn(self.pool, close=True)
        self.put_conn(self.pool, close=True, itr=True)

poolman = PoolManager()

class PooledCache(object):


    def __init__(self, config):
        self.config = config
        self.name = config['name'] + '_' + config['tabletype']
        self.conn = None
        self.iterating_conn = None

        print(f"INIT: {self.name}")
        print("  about to make pool")
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


        print("  about to test exists")
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
        print("  tested")


    def shutdown(self):
        # Close our connections
        poolman.put_all(self.pool_name)
        self.conn = None
        self.iterating_conn = None
           
    def _cursor(self, internal=True, iter=False, size=0):
        # Ensure cursor is managed server-side otherwise select * from table
        # will return EVERYTHING into python (without a manual LIMIT/OFFSET)
        # Get a connection from the pool

        if iter and not self.iterating_conn:
            self.iterating_conn = poolman.get_conn(self.pool_name, itr=True)
        elif iter is False and not self.conn:
            self.conn = poolman.get_conn(self.pool_name, itr=False)

        if iter:
            conn = self.iterating_conn
        else:
            conn = self.conn

        print(" about to make cursor")

        if internal:
            # ensure uniqueness across multiple instances of the code
            name = f"server_cursor_{self.name}_{time.time()}".replace('.', '_')
            cursor = conn.cursor(name=name, cursor_factory=RealDictCursor)
            if not size:
                size = self.config['cursor_size']
            cursor.itersize = size
        else:
            # Need this for creating the tables/indexes
            cursor = conn.cursor(cursor_factory=RealDictCursor)

        print(" Made cursor") 
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
        return res['count']


    def metadata(self, key, field="insert_time", _key_type=None):
        if _key_type is None:
            _key_type = self.key
        if _key_type == 'yuid' and len(key) != 36:
            print(f"{self.name} has UUIDs as keys")
            return None
        if not field in ["insert_time", "record_time", "refresh_time", "valid", "change"]:
            raise ValueError(f"Unknown metadata field in cache: {field}")
        qry = f"SELECT {field} FROM {self.name} WHERE {_key_type} = %s"
        params = (key,)
        with self._cursor() as cursor:
            cursor.execute(qry, params)
            rows = cursor.fetchone()
        return rows

    def set_metadata(self, key, field, value, _key_type=None):
        if _key_type is None:
            _key_type = self.key
        if _key_type == 'yuid' and len(key) != 36:
            print(f"{self.name} has UUIDs as keys")
            return None
        if not field in ["record_time", "refresh_time", "valid", "change"]:
            raise ValueError(f"Attempt to set unsettable metadata field in cache: {field}")        
        qry = f"UPDATE {self.name} SET {field} = %s WHERE {_key_type} = %s"
        params = (value, key)
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, params)
            self.conn.commit()

    def latest(self):
        qry = f"SELECT insert_time FROM {self.name} ORDER BY insert_time DESC LIMIT 1"
        with self._cursor() as cursor:
            cursor.execute(qry)
            res = cursor.fetchone()
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
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, params)
            rows = cursor.fetchone()     
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
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, params)
            rows = cursor.fetchone()     
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

    def iter_keys_slice(self, mySlice=0, maxSlice=10):
        # use row_number() to partition the results into slices for parallel processing
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")

        qry = f"""SELECT {self.key} FROM (SELECT {self.key}, row_number() OVER (ORDER BY {self.key} ASC) 
            AS row FROM {self.name}) t WHERE t.row % {maxSlice} = {mySlice}"""
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
        qry = f"SELECT * FROM {self.name} WHERE data->'type' = '\"{t}\"'"
        with self._cursor(iter=True) as cursor:
            cursor.execute(qry)            
            for res in cursor:
                yield res     

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

        with self._cursor(internal=False) as cursor:
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
        # sys.stdout.write('S');sys.stdout.flush()


    def delete(self, key, _key_type=None):
        if _key_type is None:
            _key_type = self.key
        qry = f"DELETE FROM {self.name} WHERE {_key_type} = %s"
        params = (key,)
        with self._cursor(internal=False) as cursor:            
            cursor.execute(qry, params)
            self.conn.commit()

    def clear(self):
        # WARNING WARNING ... trash all the data in the cache
        qry = f"TRUNCATE TABLE {self.name} RESTART IDENTITY"
        with self._cursor(internal=False) as cursor:
            cursor.execute(qry)
            self.conn.commit()

    def has_item(self, key, _key_type=None, timestamp=None):
        if _key_type is None:
            _key_type = self.key
        if timestamp is None:
            qry = f"SELECT 1 FROM {self.name} WHERE {_key_type} = %s LIMIT 1"
            params = (key,)
        else:
            qry = f"SELECT 1 FROM {self.name} WHERE {_key_type} = %s AND record_time >= %s LIMIT 1"
            params = (key,timestamp)

        with self._cursor(internal=False) as cursor:
            cursor.execute(qry, params)
            rows = cursor.fetchone()
        # sys.stdout.write('?');sys.stdout.flush()
        return bool(rows)

    def commit(self):
        # We commit after every transaction, so no need
        pass

    def start_bulk(self):    
        if self.iterating_conn is None:
            self.iterating_conn = poolman.get_conn(self.pool_name, itr=True)
        self.bulk_cursor = self.iterating_conn.cursor(cursor_factory=RealDictCursor)

    def set_bulk(self, data, identifier=None, yuid=None, format=None, valid=None, record_time=None, refresh_time=None, change=None):
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
        self.iterating_conn.commit()
        self.bulk_cursor.close()
        self.bulk_cursor = None

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

    ### Behave like a dict
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
