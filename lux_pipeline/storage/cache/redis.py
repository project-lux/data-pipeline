
import redis
import ujson as json

class RedisDataCache(object):

    def __init__(self, config):

        self.config = config
        self.host = self.config.get('host', 'localhost')
        self.port = self.config.get('port', 6379)

        # dbs are integers, need to map name to global int
        # 0 = idmap, 1 = test, 2 = network map, 9 = internal config

        # start caches at 10, with config at 9
        name = config['name'] + '_' + config['tabletype']

        if config['tabletype'] in ['data_cache', 'record_cache', 'ext_record_cache']:
            self.key = 'identifier'
        elif config['tabletype'] in ["rewritten_record_cache", "merged_record_cache"]:
            self.key = 'yuid'
        else:
            raise ValueError(f"Config has unknown tabletype: {config['tabletype']}")

        temp = redis.Redis(host=self.host, port=self.port, db=9)
        # look up our name to get correct int
        iname = self._manage_key_in(name)
        try:

            db = int(temp[iname])
        except:
            try:
                n = int(temp[b'next'])
            except:
                n = 10
            db = n
            temp[iname] = n
            temp[b'next'] = n+1
        temp.close()
        self.db = db
        self.conn = redis.Redis(host=self.host, port=self.port, db=self.db)
        self._restoring_data_state = False

    # Manage unicode <--> bytes
    def _manage_key_in(self, key):
        # from str to bytes
        key = key.encode('utf-8')
        return key

    def _manage_key_out(self, key):
        # from bytes to str
        key = key.decode('utf-8')         
        return key

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
        for (k, v) in state.items():
            self.set(k, v)
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
        # _type: string, list, set, zset, hash, stream
        # scan is defined in redis, but at this scale would be terrible
        for key in self.conn.scan_iter(**kw):
            yield self._manage_key_out(key)

    # WARNING: this doesn't scale for long, but is useful for debugging and testing
    def keys(self, **kw):
        return [self._manage_key_out(x) for x in self.conn.keys()]

    # Present as a dict
    def __len__(self):
        return self.conn.dbsize()

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        if self.key == 'identifier':
            return self.set(value, identifier=key)
        else:
            return self.set(value, yuid=key)

    def __delitem__(self, key):
        return self.delete(key)

    def __contains__(self, key):
        return self.has_item(key)

    def get(self, key):          
        key = self._manage_key_in(key)
        t = self.conn.type(key)
        if t == b'string':
            val = self.conn.get(key)
            out = self._manage_key_out(val)
            # This is the "record" in terms of postgres
            return json.loads(out)
        elif t == b'none':
            # Asked for a non-existent key
            return None
        else:
            raise ValueError(f"Unknown key type {t}")

    def set(self, data, identifier=None, yuid=None, format=None, valid=None,
            record_time=None, refresh_time=None, change=None):
        # data is the JSON
        # construct the value and serialize to string
        out = {'data':data, 'identifier':identifier, 'yuid':yuid, 'format':format, 'valid':valid,
        'record_time':'', 'refresh_time':'', 'change':''}
        outs = json.dumps(out)
        if yuid:
            key = yuid
        else:
            key = identifier
        ikey = self._manage_key_in(key)
        ivalue = self._manage_key_in(outs)
        return self.conn.set(ikey, ivalue)

    def delete(self, key):
        ikey = self._manage_key_in(key)
        t = self.conn.type(ikey)
        if t == b'string':
            self.conn.delete(ikey)
        return None
