import os
from lmdbm import Lmdb
import ujson as json


### Storage Layer


class StringLmdb(Lmdb):
    # key is string, value is string
    def _pre_key(self, value):
        val = value.encode("utf-8")
        if len(val) > 500:
            raise ValueError("LMDB cannot have keys longer than 500 characters")
        return val

    def _post_key(self, value):
        return value.decode("utf-8")

    def _pre_value(self, value):
        return value.encode("utf-8")

    def _post_value(self, value):
        return value.decode("utf-8")

    def __contains__(self, key):
        try:
            return Lmdb.__contains__(self, key)
        except Exception as e:
            print(f"\n*** __contains__ got {e} for {key} ***")
            return False

    def commit(self):
        self.sync()


class TabLmdb(StringLmdb):
    # key is string, value is tab separated __strings__
    # eg URI\tURI\tURI or URI\tType

    def _pre_value(self, value):
        if type(value) is str:
            return value.encode("utf-8")
        elif type(value) in [list, set]:
            try:
                return "\t".join(value).encode("utf-8")
            except Exception:
                raise ValueError(f"TabLmdb cannot accept {value}")
        elif type(value) is bytes:
            return value
        else:
            # Don't accept ints/floats/dicts
            raise ValueError(f"TabLmdb cannot accept {type(value)}: {value}")

    def _post_value(self, value):
        value = value.decode("utf-8")
        if "\t" in value:
            value = value.split("\t")
        return value


class JsonLmdb(StringLmdb):
    # key is string, value is arbitrary JSON
    # but adds the cost of dumps/loads if really just strings

    def _pre_value(self, value):
        return json.dumps(value).encode("utf-8")

    def _post_value(self, value):
        return json.loads(value.decode("utf-8"))


### Pipeline Layer
#
# open flags for LMDB:
# r - read only, w - read/write, c - create if not exists, n - recreate even if exists
#
# This is stupid, but the only way to get the configuration to pass through sensibly
# as open() for the underlying lmdb is a constructor. and thus not reusable
# Otherwise we need one class per underlying structure (string, tab, json, etc)
# and per storage mechanism (lmdb, sqlite, etc) If a better way exists, then this API
# to the index object shouldn't need to change.
#
class LmdbIndex:
    def __init__(self, config):
        self.config = config
        self.path = config["path"]
        self.name = config["name"]
        self.type = config.get("type", "Tab")
        self.type_hash = {"String": StringLmdb, "Tab": TabLmdb, "Json": JsonLmdb}
        self.passthrough = ["clear", "get", "keys", "values", "items", "commit", "update", "pop", "popitem"]
        self.index = None
        if self.type not in self.type_hash:
            raise ValueError(f"Invalid type: {self.type}")
        if not os.path.exists(self.path):
            # create it
            self.initialize(config)

    def initialize(self, config):
        mapExp = config.get("mapExponent", 30)
        readahead = config.get("readahead", False)
        writemap = config.get("writemap", True)
        cls = self.type_hash[self.type]
        index = cls.open(self.path, "c", map_size=2**mapExp, readahead=readahead, writemap=writemap)
        index.commit()
        index.close()

    # Bla. Can't change magic methods dynamically, so here they all are
    def __enter__(self):
        """Context manager entry point: ensure open for read"""
        if not self.index:
            self.open("r")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __contains__(self, key):
        if self.index is not None:
            return self.index.__contains__(key)

    def __getitem__(self, key):
        if self.index is not None:
            return self.index.__getitem__(key)

    def __delitem__(self, key):
        if self.index is not None:
            return self.index.__delitem__(key)

    def __setitem__(self, key, value):
        if self.index is not None:
            return self.index.__setitem__(key, value)

    def __len__(self):
        if self.index is not None:
            return self.index.__len__()

    def __iter__(self):
        if self.index is not None:
            return self.index.__iter__()

    def open(self, rw="r"):
        if rw not in ["r", "a", "w"]:
            raise ValueError(f"Unknown read/write flag: {rw} ('w' is really 'a')")
        elif rw == "a":
            rw = "w"
        cls = self.type_hash[self.type]
        readahead = self.config.get("readahead", False)
        writemap = self.config.get("writemap", True)
        self.index = cls.open(self.path, rw, readahead=readahead, writemap=writemap)
        # (puke)
        for fn in self.passthrough:
            setattr(self, fn, getattr(self.index, fn))
        return self

    def close(self):
        self.index.close()
        for fn in self.passthrough:
            setattr(self, fn, None)
        self.index = None
