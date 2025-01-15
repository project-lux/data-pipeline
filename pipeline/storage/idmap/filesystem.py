

# A memory managed, filesystem backed map
# For use with tests or debugging, not in a production environment

import os
import sys
import uuid
import ujson as json

class FsMap(object):

    def __init__(self, config):
        self.prefix_map_in = {}
        self.prefix_map_out = {}
        self.directory = config['base_dir']
        self.name = config['name']
        self.data = {}
        self.filename = os.path.join(self.directory, f"{self.name}.json")
        self.should_mint_new_ids = True
        self.configs = config['all_configs']
        self._load()

    def _load(self):
        # read from disk
        if os.path.exists(self.filename):
            with open(self.filename) as fh:
                data = fh.read()
            self.data = json.loads(data)
        else:
            self._persist()

    def _persist(self):
        # write to disk
        with open(self.filename,"w") as fh:
            fh.write(json.dumps(self.data))

    def clear(self):
        self.data = {}
        self._persist()

    def commit(self):
        self._persist()

    def get(self, key):
        return self.data.get(key, None)

    def set(self, key, value):
        self.data[key] = value
        self._persist()

    def delete(self, key):
        try:
            del self.data[key]
            self._persist()
        except:
            print(f"idmap tried to delete non-existent key: {key}")

    def has_item(self, key):
        return key in self.data

    def iter_keys(self):
        for key in self.data.keys():
            yield key

    def popitem(self):
        val = self.data.popitem()
        self._persist()
        return val

    def update(self, values):
        # Iter through all of the pairs in the dict and set
        for (k,v) in values.items():
            self.set(k, v)

    def keys(self):
        return list(self.data.keys())

    # Present our dict as a dict
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


class IdMap(FsMap):


    def __init__(self, config):
        FsMap.__init__(self, config)
        with open(os.path.join(cfgs.data_dir, 'idmap_update_token.txt')) as fh:
            token = fh.read()
        token = token.strip()
        if not token.startswith('__') or not token.endswith('__'):
            print("Idmap Update Token is badly formed, should be 8 character date with leading/trailing __")
            raise ValueError("update token")
            sys.exit(0)
        else:
            self.update_token = token        

        self.prefix_map_out = {
            "yuid": self.configs.internal_uri
        }
        for cf in self.configs.external.values():
            self.prefix_map_out[cf['name']] = cf['namespace']
        self.prefix_map_in = {}
        for (k,v) in self.prefix_map_out.items():
            self.prefix_map_in[v] = k

    def has_update_token(self, key):
        return self.update_token in self.data[key]

    def add_update_token(self, key):
        # Delete previous
        to_remove = []
        for k in self.data[key]:
            if k.startswith('__') and k.endswith('__'):
                to_remove.append(k)
        for k in to_remove:
            self.data[key].remove(k)
        self.data[key].append(self.update_token)
        self._persist()

    def mint(self, key, slug, typ=""):

        if typ in self.configs.ok_record_types:
            key = self.configs.make_qua(key, typ)
        elif typ:
            raise ValueError(f"Unknown type: {typ}")
        elif not self.configs.is_qua(key):
            raise ValueError(f"Need to provide a type for: {key}")

        if self.should_mint_new_ids:
            # Make a new id of the form: yuid:{uuid}
            uu = str(uuid.uuid4())
            print(f"Minting new YUID {slug}/{uu} for {key}")
            if slug:
                value = f"{self.prefix_map_out['yuid']}{slug}/{uu}"
            else:
                value = f"{self.prefix_map_out['yuid']}{uu}"
            self.data[key] = value
            self.data[value] = [key]
            self._persist()
            return value
        else:
            raise ValueError(f"Refusing to make a new YUID in test mode")
