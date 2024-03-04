

# Mostly useful for testing callers to an IdMap without having a real service
# e.g. for testing reidentify and collector
# Could be deduplicated with code in redis.py, but value doesn't seem high


### XXX FIXME This needs to be updated
### DO NOT USE 

import uuid

class IdMap(object):

    def __init__(self):
        self.data = {}
        self._restoring_data_state = False
        self.prefix_map_out = {
            "yuid": self.configs.internal_uri
            "ycba": "https://lux.britishart.yale.edu/data/",
            "test": "http://test.example.com/"
        }
        self.prefix_map_in = {}
        for (k,v) in self.prefix_map_out.items():
            self.prefix_map_in[v] = k
        # FIXME: extend _in to cover common badly constructed URIs (/page/ /wiki/ etc)

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
        key = key.encode('utf-8')
        return key

    def _manage_key_out(self, key):
        # from bytes to str
        key = key.decode('utf-8')
        if not key.startswith('http'):
            # replace prefix with full
            for (k,v) in self.prefix_map_out.items():
                if key.startswith(k):
                    key = key.replace(f"{k}:", v)
                    break            
        return key

    def _manage_value_in(self, value):
        if value.startswith('http'):
            # replace prefixes
            for (k,v) in self.prefix_map_in.items():
                if value.startswith(k):
                    value = value.replace(k, f"{v}:")
                    break
        value = value.encode('utf-8')
        return value

    def _manage_value_out(self, value):
        value = value.decode('utf-8')
        if not value.startswith('http'):
            # replace prefix with full
            for (k,v) in self.prefix_map_out.items():
                if value.startswith(k):
                    value = value.replace(f"{k}:", v)
                    break
        return value

    # Mostly useful for testing, maybe in prod
    def _export_state(self):
        # return a json dict of the external:yuid values
        return self.data

    def _import_state(self, state):
        # reset internal data to state
        self.data = {}
        self._restoring_data_state = True
        for (k,v) in state.items():
            self.set(k, v)
        self._restoring_data_state = False

    # External callers should only interact via external ids
    # so we take care of the yuid set management internally
    def _add(self, key, *values):     
        key = self._manage_key_in(key)
        values = [self._manage_value_in(value) for value in values]
        if not key in self.data:
            self.data[key] = set([])
        self.data[key].update(values)
        return self.data[key]

    def _remove(self, key, value):
        key = self._manage_key_in(key)
        value = self._manage_value_in(value)
        try:
            self.data[key].remove(value)
        except:
            pass

    def _dropdb(self):
        self.data = {}

    def mint(self, key, slug=""):
        # Make a new id of the form: yuid:{uuid}
        uu = str(uuid.uuid4())
        if slug:
            value = f"{self.prefix_map_out['yuid']}{slug}/{uu}"
        else:
            value = f"{self.prefix_map_out['yuid']}{uu}"
        self._add(value, key)
        self.set(key, value)
        return value

    def get(self, key):
        key = self._manage_key_in(key)
        val = self.data.get(key, None)
        if val is None:
            return val
        elif type(val) == bytes:
            return self._manage_value_out(val)
        elif type(val) == set:
            return {self._manage_value_out(x) for x in val}
        else:
            raise ValueError(f"Unknown key type {type(val)}")

    def set(self, key, value):
        # key is external identifier
        # value is an existing yuid
        if not value in self and not self._restoring_data_state:
            raise ValueError(f"Unknown YUID {value}")
        self._add(value, key)
        ikey = self._manage_key_in(key)
        ivalue = self._manage_value_in(value)
        self.data[ikey] = ivalue

    def delete(self, key):
        ikey = self._manage_key_in(key)
        val = self.data.get(key, None)
        if val == None:
            raise KeyError()
        elif type(val) == str:
            self.conn.delete(ikey)
            self._remove(val, key)
        else:
            raise ValueError(f"{key} is a YUID and cannot be manually deleted")
        return None

    def has_item(self, key):
        return self._manage_key_in(key) in self.data

    def commit(self):
        pass

    # Count number of items mapped to YUID
    def count(self, key):
        key = self._manage_key_in(key)
        return len(self.data[key])

    # Make an iterator for keys, that match type and/or pattern
    def iter_keys(self, **kw):
        # match: glob style expression for keys to match

        for key in self.data.keys():
            yield self._manage_key_out(key)

    # WARNING: this doesn't scale for long, but is useful for debugging and testing
    def keys(self, **kw):
        return [self._manage_key_out(x) for x in self.data.keys()]

    # Present as a dict
    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __delitem__(self, key):
        return self.delete(key)

    def __contains__(self, key):
        return self.has_item(key)

