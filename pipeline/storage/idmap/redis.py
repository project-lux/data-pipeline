
import os
import redis
import uuid
import sys
import random

class RedisCache(object):

    def __init__(self, config):
        self.configs = config['all_configs']
        self.host = config.get('host', 'localhost')
        self.port = int(config.get('port', 6379))
        self.db = config.get('db', 0)
        self.conn = redis.Redis(host=self.host, port=self.port, db=self.db, decode_responses=True)
        self._restoring_data_state = False
        self.prefix_map_in = {}
        self.prefix_map_out = {}

    # Manage unicode <--> bytes
    # Manage prefix <--> full URI
    def _manage_key_in(self, key):
        # from str to bytes
        if key.startswith('http'):
            # replace full with prefix
            for (k,v) in self.prefix_map_in.items():
                if key.startswith(k):
                    key = key.replace(k, f"{v}:")
                    break
        #key = key.encode('utf-8')
        return key

    def _manage_key_out(self, key):
        # from bytes to str
        #key = key.decode('utf-8')
        if not key.startswith('http'):
            # replace prefix with full
            for (k,v) in self.prefix_map_out.items():
                if key.startswith(k):
                    key = key.replace(f"{k}:", v)
                    break            
        return key

    _manage_value_in = _manage_key_in
    _manage_value_out = _manage_key_out

    # Mostly useful for testing, maybe in prod
    def _export_state(self):
        # return a json dict of the external:yuid values
        state = {}
        for k in self.iter_keys(type='string'):
            state[k] = self[k]
        return state

    def _import_state(self, state):
        # reset internal data to state
        self.clear()
        self._restoring_data_state = True
        x = 0
        for (k, v) in state.items():
            self.set(k, v)
            x += 1
            if not x % 100000:
                print(f'{x}'); sys.stdout.flush()
        self._restoring_data_state = False

    def clear(self):
        return self.conn.flushdb()

    def has_item(self, key):
        return self.conn.exists(self._manage_key_in(key))

    def commit(self):
        return self.conn.save()

    # Make an iterator for keys, that match type and/or pattern
    def iter_keys(self, **kw):
        # match: glob style expression for keys to match
        # scan is defined in redis, but at this scale would be terrible
        # FIXME: match is on the internal form, but can't manage_key_in as it's a glob?
        for key in self.conn.scan_iter(**kw):
            yield self._manage_key_out(key)

    def popitem(self):
        k = self.conn.scan(count=1)
        k = k[1][0]
        val = self.conn.get(k)        
        self.conn.delete(k)
        return (self._manage_key_out(k), self._manage_key_out(val))

    # WARNING: this doesn't scale for long, but is useful for debugging and testing
    def keys(self, **kw):
        return [self._manage_key_out(x) for x in self.conn.keys()]

    # Count number of items mapped to YUID
    def count(self, key):
        key = self._manage_key_in(key)
        return self.conn.scard(key)

    # Present as a dict
    def __len__(self):
        return self.conn.dbsize()

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __delitem__(self, key):
        return self.delete(key)

    def __contains__(self, key):
        return self.has_item(key)


class IdMap(RedisCache):
    # All values are bytes, not strings
    # Wrap to require UTF-8
    # db 0 is default real database
    # db 1 is test database
    # db 2 is network operation cache for redirects, errors

    def __init__(self, config):
        RedisCache.__init__(self, config)
        self.prefix_map_out = {
            "yuid": self.configs.internal_uri
        }
        for cf in self.configs.external.values():
            self.prefix_map_out[cf['name']] = cf['namespace']
        self.prefix_map_in = {}
        for (k,v) in self.prefix_map_out.items():
            self.prefix_map_in[v] = k
        self.memory_cache = {}
        self.clean_on_remove = False

        fh = open(os.path.join(self.configs.data_dir, 'idmap_update_token.txt'))
        token = fh.read()
        fh.close()
        token = token.strip()
        if not token.startswith('__') or not token.endswith('__'):
            print("Idmap Update Token is badly formed, should be 8 character date with leading/trailing __")
            raise ValueError("update token")
        else:
            self.update_token = token

    def _manage_value_in(self, value):
        if value.startswith('http'):
            # replace prefixes
            for (k,v) in self.prefix_map_in.items():
                if value.startswith(k):
                    value = value.replace(k, f"{v}:")
                    break
        #value = value.encode('utf-8')
        return value

    def _manage_value_out(self, value):
        #value = value.decode('utf-8')
        if not value.startswith('http'):
            # replace prefix with full
            for (k,v) in self.prefix_map_out.items():
                if value.startswith(k):
                    value = value.replace(f"{k}:", v)
                    break
        return value

    # External callers should only interact via external ids
    # so we take care of the yuid set management internally
    def _add(self, key, *values):     
        key = self._manage_key_in(key)
        values = [self._manage_value_in(value) for value in values]
        return self.conn.sadd(key, *values)

    def _remove(self, key, value):
        key = self._manage_key_in(key)
        value = self._manage_value_in(value)
        self.conn.srem(key, value) 
        if self.clean_on_remove:
            # If the only thing left is an update token, then delete
            # Can't enable this yet as rebuilding cleans and reproduces
            val = self.conn.smembers(key)
            out = [self._manage_value_out(x) for x in val]
            if len(out) == 1 and out[0].startswith("__"):
                self._remove(self._manage_value_out(key), out[0])

    def has_update_token(self, key):
        key = self._manage_key_in(key)
        val = self.conn.smembers(key)
        out = [self._manage_value_out(x) for x in val]
        return self.update_token in out

    def add_update_token(self, key):
        ikey = self._manage_key_in(key)
        val = self.conn.smembers(ikey)
        out = {self._manage_value_out(x) for x in val}
        for o in out:
            if o.startswith('__') and o.endswith('__'):
                # delete previous tokens
                self._remove(key, o)
        self._add(key, self.update_token)

    def mint(self, key, slug, typ=""):
        if typ in self.configs.ok_record_types:
            key = self.configs.make_qua(key, typ)
        elif typ:
            raise ValueError(f"Unknown type: {typ}")
        elif not self.configs.is_qua(key):
            raise ValueError(f"Need a type: {key}")

        # Make a new id of the form: yuid:{uuid}
        uu = str(uuid.uuid4())
        if slug:
            value = f"{self.prefix_map_out['yuid']}{slug}/{uu}"
        else:
            value = f"{self.prefix_map_out['yuid']}{uu}"
        self._add(value, key)
        self.set(key, value)
        return value


    def get(self, key, typ=""):
        if typ in self.configs.ok_record_types:
            key = self.configs.make_qua(key, typ)
        elif typ:
            raise ValueError(typ)
        elif not self.configs.is_qua(key) and not self.prefix_map_out['yuid'] in key:
            raise ValueError(f"Need a type: {key}")            

        key = self._manage_key_in(key)

        # memory cache for frequent lookups (aat terms) to avoid the network
        if "/aat/" in key and key in self.memory_cache:
            return self.memory_cache[key]

        t = self.conn.type(key)
        if t == 'string':
            val = self.conn.get(key)
            if not val:
                print(f"idmap was asked for {key} but got {val}")
                return None
            out = self._manage_value_out(val)
        elif t == 'set':
            val = self.conn.smembers(key)
            if not val:
                print(f"idmap was asked for {key} but got {val}")
                return None
            out = {self._manage_value_out(x) for x in val}
        elif t == 'none':
            # Asked for a non-existent key
            return None
        else:
            raise ValueError(f"Unknown key type {t}")
        if "/aat/" in key: 
            self.memory_cache[key] = out
        return out

    def set(self, key, value, typ=""):

        if typ in self.configs.ok_record_types:
            key = self.configs.make_qua(key, typ)
        elif typ:
            raise ValueError(typ)
        elif not self.configs.is_qua(key):
            raise ValueError(f"Need a type: {key}")   

        # key is external identifier
        # value is an existing yuid
        if not value in self and not self._restoring_data_state:
            raise ValueError(f"Unknown YUID {value}")
        ikey = self._manage_key_in(key)
        ivalue = self._manage_value_in(value)
        if self.conn.exists(ikey):
            # Could be just setting to the same value
            old = self.conn.get(ikey)
            if old is None or old == ivalue:
                return
            print(f"key: {key} old: {old} new value: {value}")
            # Nope, we're changing to a different yuid!
            # keep all keys assigned to old_yuid assigned to new_yuid
            all_vals = self.conn.smembers(old)
            print(f"all vals: {all_vals}")
            for av in all_vals:
                # print(f"Resetting {av} from {old} -> {ivalue} due to {key}")
                self.conn.set(av, ivalue)
            if all_vals:
                # Sometimes a ghost yuid survives somewhere else
                self.conn.sadd(ivalue, *all_vals)
                # delete old yuid
                self.conn.delete(old)
            else:
                print(f"Requested members for {old} and got [] ???")

        self._add(value, key)
        return self.conn.set(ikey, ivalue)

    def _force_delete(self, key, typ=""):
        if typ in self.configs.ok_record_types:
            key = self.configs.make_qua(key, typ)
        ikey = self._manage_key_in(key)
        value = self.get(key)
        self.conn.delete(ikey)
        if value is not None and value in self:
            self._remove(value, key)

    def delete(self, key, typ=""):
        if typ in self.configs.ok_record_types:
            key = self.configs.make_qua(key, typ)
        elif typ:
            raise ValueError(typ)
        elif not self.configs.is_qua(key):
            raise ValueError(f"Need a type: {key}")         

        ikey = self._manage_key_in(key)
        t = self.conn.type(ikey)
        if t == 'string':
            value = self.get(key)
            self.conn.delete(ikey)
            self._remove(value, key)
        else:
            raise ValueError(f"{key} is a YUID ({t}) and cannot be manually deleted")
        return None


class NetworkOperationMap(RedisCache):

    def __init__(self, config):
        if not 'db' in config:
            config['db'] = 2
        RedisCache.__init__(self, config)

    def get(self, key):
        # key: "https://viaf.org/viaf/lccn/nr+123123123/viaf.jsonld"
        ikey = self._manage_key_in(key)
        ival = self.conn.get(ikey)
        if ival:
            val = self._manage_key_out(ival)
            return val
        else:
            return None

    def set(self, key, value):
        # Error conditions
        if value in [0, None]:
            value = "000"
        elif type(value) == int:
            value = str(value)

        ikey = self._manage_key_in(key)
        ival = self._manage_key_in(value)
        self.conn.set(ikey, ival)

    def delete(self, key):
        ikey = self._manage_key_in(key)        
        self.conn.delete(ikey)


class MultiMap(RedisCache):

    # a --> [b,c] ; no implications
    # all values are sets

    def __init__(self, config):
        if not 'db' in config:
            config['db'] = 8
        RedisCache.__init__(self, config)
        if 'maxItems' in config:
            self.max_items = config['maxItems']
        else:
            self.max_items = 0 # meaning infinite

    def get(self, key):
        # values are always sets
        # This returns an empty set for missing keys
        key = self._manage_key_in(key)
        val = self.conn.smembers(key)
        out = {self._manage_value_out(x) for x in val}
        return out

    def set(self, key, value):
        # map[uri-1] = uri-2  ==> add uri-2 to the set for uri-1
        if self.max_items > 0:
            l = self.count(key)
            if l >= self.max_items:
                return -1

        ikey = self._manage_key_in(key)
        ivalue = self._manage_value_in(value)
        # sadd is set operation. adding x twice is noop
        self.conn.sadd(ikey, ivalue)
        if self.max_items and l == self.max_items-1:
            self.conn.sadd(ikey, b'__HIGHWATER__')

    def add(self, key, value):
        return self.set(key, value)

    def remove(self, key, value):
        ikey = self._manage_key_in(key)
        ivalue = self._manage_value_in(value) 
        # if ikey doesn't exist, returns empty set
        all_vals = self.conn.smembers(ikey)            
        if ivalue in all_vals:
            self.conn.srem(ikey, ivalue)

    def delete(self, key):
        ikey = self._manage_key_in(key)
        self.conn.delete(ikey)


class TransitiveMultiMap(MultiMap):

    # a --> [b,c] ; (implies b --> [a] and c --> [a])
    # `map[a] = b` --> add b to the set with key a, and a to set with key b

    def __init__(self, config):
        if not 'db' in config:
            config['db'] = 5
        RedisCache.__init__(self, config)

    def set(self, key, value):
        ikey = self._manage_key_in(key)
        ivalue = self._manage_value_in(value)
        # sadd is set operation. adding x twice is noop
        self.conn.sadd(ikey, ivalue)
        self.conn.sadd(ivalue, ikey)

    def remove(self, key, value):
        ikey = self._manage_key_in(key)
        ivalue = self._manage_value_in(value) 
        # if ikey doesn't exist, returns empty set
        all_vals = self.conn.smembers(ikey)            
        if not ivalue in all_vals:
            return
        else:
            self.conn.srem(ikey, ivalue)
            self.conn.srem(ivalue, ikey)

    def delete(self, key, typ=""):
        # Shouldn't be allowed to ensure transitive consistency 
        # Instead rely on redis auto-clean when value is empty
        raise ValueError("You need to .remove(key, value)")


class RedisDictValue(object):

    def __init__(self, key, refmap):
        self.persistence = refmap
        self.pkey = key

    def __getitem__(self, key):
        return self.persistence._getitem(self.pkey, key)

    def __setitem__(self, key, value):
        return self.persistence._setitem(self.pkey, key, value)

    def items(self):
        return self.persistence._items(self.pkey)


class ReferenceMap(NetworkOperationMap):

    def __init__(self, config):
        if not 'db' in config:
            config['db'] = 3
        RedisCache.__init__(self, config)

    def _export_state(self):
        # return a json dict of the external:yuid values
        state = {}
        for k in self.iter_keys():
            state[k] = dict(self._items(k))
        return state

    def _manage_value_in(self, value):
        if type(value) == int:
            value = str(value)
        return self._manage_key_in(value)

    def _manage_value_out(self, value):
        # Allow ints
        val = self._manage_key_out(value)
        if val.isnumeric():
            return int(val)
        else:
            return val

    def _getitem(self, db_key, key):
        val = self.conn.hget(db_key, self._manage_key_in(key))
        if val:
            return self._manage_value_out(val)

    def _setitem(self, db_key, key, value):
        return self.conn.hset(db_key, self._manage_key_in(key), self._manage_value_in(value))

    def _items(self, db_key):
        d = self.conn.hgetall(db_key)
        n = []
        for (k,v) in d.items():
            n.append((self._manage_key_out(k), self._manage_value_out(v)))
        return n

    def get(self, key):
        if self.conn.exists(key):
            return RedisDictValue(key, self)

    def set(self, key, value):
        # convert from dict or RDV to hset
        for (k,v) in value.items():
            self._setitem(key, k, v)

    def popitem(self):
        if len(self) ==  0:
            return None
        rk = self.conn.randomkey()
        try:
            d = self.conn.hgetall(rk)
            self.conn.delete(rk)
        except:
            return None
        n = {}
        for (k,v) in d.items():
            n[self._manage_key_out(k)] = self._manage_value_out(v)
        return (self._manage_key_out(rk), n)

    def update(self, values):
        # Iter through all of the pairs in the dict and set
        for (k,v) in values.items():
            self.set(k, v)
