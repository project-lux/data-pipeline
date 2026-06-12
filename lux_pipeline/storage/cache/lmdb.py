# Use an export of data as zlib compressed json

import logging
import os
import time
import uuid
import zlib

import lmdb
import ujson as json

from .abstract import AbstractCache

logger = logging.getLogger("lux_pipeline")

# A single LMDB cache that stores compressed JSON records by UUID key
# Multiple sources can be present, with each source having its name as a prefix for the key
# Should not be used for writing, as not thread / multiprocess safe
# Thus they live in dumps_dir


class LmdbCache(AbstractCache):
    def __init__(self, config):
        # AbstractCache sets self.config, self.name, self.key, self.metadata_fields
        super().__init__(config)
        self.db_path = os.path.join(config["base_dir"], config["db_path"])
        self.env = lmdb.open(self.db_path, max_dbs=3, readonly=True, lock=False)
        self.db = env.open_db(b"data", dupsort=False)

    def len(self):
        # return the number of records in the database
        with self.env.begin() as txn:
            return txn.stat(db=self.db)["entries"]

    def iter_records(self):
        """Yield every full record row."""
        raise NotImplementedError()

    def iter_keys(self):
        """Yield every primary key."""
        raise NotImplementedError()

    def iter_records_slice(self, mySlice=0, maxSlice=10):
        """Yield the mySlice-th 1/maxSlice partition of all records."""
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be >= {maxSlice}")
        raise NotImplementedError()

    def iter_records_type(self, type=""):
        """Yield every record of the given type."""
        raise NotImplementedError()

    def iter_keys_slice(self, mySlice=0, maxSlice=10):
        """Yield the mySlice-th 1/maxSlice partition of all keys."""
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be >= {maxSlice}")
        raise NotImplementedError()

    def has_item(self, key, _key_type=None, timestamp=None):
        """Return True if a record with this key exists (and is newer than timestamp, if given)."""
        raise NotImplementedError()

    def get(self, key, _key_type=None):
        """Fetch and return one record dict, or None if not found."""
        if _key_type is None:
            _key_type = self.key
        self._validate_key_type(key, _key_type)


class DataCache(LmdbCache):
    pass
