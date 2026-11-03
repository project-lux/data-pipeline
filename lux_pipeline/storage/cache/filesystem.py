import datetime
import logging
import os
import ujson as json

from lux_pipeline.storage.cache.abstract import (AbstractCache, METADATA_FIELDS,
                                                 WRITABLE_METADATA_FIELDS)

logger = logging.getLogger("lux_pipeline")

# A record per file in a directory: <key>.json holds the record's data, with
# '/' in the key written as '___'. The file's modification time is both its
# insert_time and its record_time (set(record_time=...) and
# set_metadata("record_time") move it). refresh_time, valid and change, when
# a record has any, are kept beside it in <key>.json.meta.

# metadata kept in the .meta file rather than in the file's time
SIDECAR_FIELDS = ["refresh_time", "valid", "change"]


def _as_datetime(value):
    """A naive local datetime, as file times are, from a datetime or an ISO
    string (a trailing Z allowed)."""
    if not isinstance(value, datetime.datetime):
        value = str(value)
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        value = datetime.datetime.fromisoformat(value)
    if value.tzinfo is not None:
        value = value.astimezone().replace(tzinfo=None)
    return value


class FsCache(AbstractCache):
    def __init__(self, config):

        self.config = config
        # e.g. test1_datacache
        self.name = config['name'] + '_' + config['tabletype']
        self.directory = os.path.join(config['base_dir'], self.name)
        self.suffix = ".json"
        self.source = config['name']
        self.slash_replacement = "___"
        self.key = "identifier"

        if not os.path.exists(self.directory):
            os.mkdir(self.directory)

    def _list(self):
        l = [x for x in os.listdir(self.directory) if x.endswith(self.suffix)]
        l.sort()
        return l

    def _filename(self, key):
        """The record file's name (not path) for `key`."""
        key = key.replace('/', self.slash_replacement)
        if not key.endswith(self.suffix):
            key = key + self.suffix
        return key

    def _path(self, key):
        return os.path.join(self.directory, self._filename(key))

    def _file_to_key(self, file):
        # same transform iter_keys() yields; passing the raw filename to
        # get() worked but returned rows whose identifier still carried the
        # suffix and the escaped slashes
        return file.replace(self.suffix, '').replace(self.slash_replacement, '/')

    def _mtime(self, path):
        return datetime.datetime.fromtimestamp(os.path.getmtime(path))

    def _read_meta(self, path):
        try:
            with open(path + ".meta") as fh:
                return json.loads(fh.read())
        except FileNotFoundError:
            return {}

    def _write_meta(self, path, meta):
        meta = {k: v for k, v in meta.items() if v is not None}
        if meta:
            tmp = path + ".meta.tmp"
            with open(tmp, "w") as fh:
                fh.write(json.dumps(meta))
            os.replace(tmp, path + ".meta")
        elif os.path.exists(path + ".meta"):
            os.remove(path + ".meta")

    # Write batching: there is no transaction here -- set() writes the file
    # and that's that -- so AbstractCache's no-ops stand.

    def len(self):
        return len(self._list())

    def latest(self):
        times = [os.path.getmtime(os.path.join(self.directory, f)) for f in self._list()]
        if not times:
            return "0000-01-01T00:00:00"
        return datetime.datetime.fromtimestamp(max(times)).isoformat()

    def metadata(self, key, field="insert_time", _key_type=None):
        if field not in METADATA_FIELDS:
            raise ValueError(f"Unknown metadata field in cache: {field}")
        path = self._path(key)
        if not os.path.exists(path):
            return None
        if field in SIDECAR_FIELDS:
            return {field: self._read_meta(path).get(field)}
        return {field: self._mtime(path)}

    def set_metadata(self, key, field, value, _key_type=None):
        if field not in WRITABLE_METADATA_FIELDS:
            raise ValueError(f"Attempt to set unsettable metadata field in cache: {field}")
        path = self._path(key)
        if not os.path.exists(path):
            return None
        if field == "record_time":
            t = _as_datetime(value).timestamp()
            os.utime(path, (t, t))
        else:
            meta = self._read_meta(path)
            meta[field] = value
            self._write_meta(path, meta)

    def clear(self):
        """Delete every record file (a restore empties the store first)."""
        for fn in os.listdir(self.directory):
            if fn.endswith(self.suffix) or fn.endswith(self.suffix + ".meta"):
                os.remove(os.path.join(self.directory, fn))

    def has_item(self, key, _key_type=None, timestamp=None):
        # apply the same slash replacement as set()/get(); without it any
        # identifier containing '/' was reported absent
        path = self._path(key)
        if not os.path.exists(path):
            return False
        return timestamp is None or self._mtime(path) >= _as_datetime(timestamp)

    def _read_file(self, fn):
        fn = os.path.join(self.directory, fn)
        if os.path.exists(fn):
            with open(fn) as fh:
                data = fh.read()
            try:
                js = json.loads(data)
            except:
                js = {"data": data}
            return js
        else:
            return None

    def get(self, key, _key_type=None, raw=False):
        key2 = self._filename(key)
        js = self._read_file(key2)
        if js is not None:
            # raw means "data is JSON text, not parsed objects"; postgres
            # gets that for free from the column, here it costs a dumps()
            # -- but the contract is what lets callers not care which
            # backend they have
            path = os.path.join(self.directory, key2)
            t = self._mtime(path)
            row = {self.key: key, 'data': json.dumps(js) if raw else js,
                   'source': self.source, 'insert_time': t, 'record_time': t}
            row.update(self._read_meta(path))
            return row
        else:
            # as postgres: a miss is an answer, not a problem
            logger.debug(f"File does not exist: {self.directory}/{key2}")
            return None

    def set(self, value, identifier=None, yuid=None, format=None, valid=None,
            record_time=None, refresh_time=None, change=None):
        # Use key as filename
        if identifier:
            key = identifier
        elif yuid:
            key = yuid
        else:
            raise ValueError("No key provided")

        # subst / in identifier to avoid hierarchy
        fn = self._path(key)

        if 'data' in value and 'identifier' in value:
            value = value['data']

        # atomic write: a crash mid-write must not leave a truncated record
        tmp = fn + ".tmp"
        with open(tmp, "w") as fh:
            fh.write(json.dumps(value))
        os.replace(tmp, fn)
        if record_time is not None:
            t = _as_datetime(record_time).timestamp()
            os.utime(fn, (t, t))
        # format is always JSON-LD here; the rest replace what was there
        self._write_meta(fn, {"refresh_time": refresh_time, "valid": valid, "change": change})

    def delete(self, key, _key_type=None):
        # was missing entirely: __delitem__ raised AttributeError
        fn = self._path(key)
        for path in (fn, fn + ".meta"):
            if os.path.exists(path):
                os.remove(path)

    def iter_keys(self):
        for file in self._list():
            yield self._file_to_key(file)

    def iter_records(self, raw=False):
        for file in self._list():
            yield self.get(self._file_to_key(file), raw=raw)

    def iter_keys_slice(self, mySlice=0, maxSlice=10):
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")
        for file in self._list()[mySlice::maxSlice]:
            yield self._file_to_key(file)

    def iter_records_slice(self, mySlice=0, maxSlice=10, raw=False):
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")
        for file in self._list()[mySlice::maxSlice]:
            yield self.get(self._file_to_key(file), raw=raw)

    def _since(self, timestamp):
        """Files with a record_time of at least `timestamp`, newest first,
        as postgres orders them."""
        files = [(os.path.getmtime(os.path.join(self.directory, f)), f) for f in self._list()]
        if timestamp is not None:
            ts = _as_datetime(timestamp).timestamp()
            files = [x for x in files if x[0] >= ts]
        files.sort(key=lambda x: x[0], reverse=True)
        return [f for _, f in files]

    def iter_keys_since(self, timestamp=None):
        for file in self._since(timestamp):
            yield self._file_to_key(file)

    def iter_records_since(self, timestamp=None):
        for file in self._since(timestamp):
            yield self.get(self._file_to_key(file))


class DataCache(FsCache):
    def __init__(self, config):
        if not 'tabletype' in config:
            config['tabletype'] = "data_cache"
        super().__init__(config)

class InternalRecordCache(FsCache):
    def __init__(self, config):
        if not 'tabletype' in config:
            config['tabletype'] = "record_cache"
        super().__init__(config)

class ExternalRecordCache(FsCache):
    def __init__(self, config):
        if not 'tabletype' in config:
            # was "record_cache", colliding with InternalRecordCache
            config['tabletype'] = "ext_record_cache"
        super().__init__(config)

class ReconciledRecordCache(FsCache):
    def __init__(self, config):
        if not 'tabletype' in config:
            config['tabletype'] = "ext_reconciled_record_cache"
        super().__init__(config)

class RecordCache(FsCache):
    def __init__(self, config):
        if not 'tabletype' in config:
            # was "record_cache", colliding with InternalRecordCache
            config['tabletype'] = "rewritten_record_cache"
        super().__init__(config)
        self.key = "yuid"

class MergedRecordCache(FsCache):
    def __init__(self, config):
        if not 'tabletype' in config:
            config['tabletype'] = "record2_cache"
        super().__init__(config)
        self.key = "yuid"