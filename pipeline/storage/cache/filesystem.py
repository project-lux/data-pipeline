import os
import ujson as json

# Very simple cache API against files in a directory
# Could be more complete for actual FS based implementation

class FsCache(object):
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

    # Write batching API, matching postgres.PooledCache. There is no
    # transaction here -- set() writes the file and that's that -- so these
    # are no-ops, present so callers can batch against any cache backend
    # without checking which one they have.
    def defer_commits(self, every=100):
        pass

    def resume_commits(self):
        pass

    def checkpoint(self):
        pass

    def flush(self):
        pass

    def has_item(self, key):
        # apply the same slash replacement as set()/get(); without it any
        # identifier containing '/' was reported absent
        key = key.replace('/', self.slash_replacement)
        if not key.endswith(self.suffix):
            key = key + self.suffix
        fn = os.path.join(self.directory, key)
        return os.path.exists(fn)

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

    def get(self, key, raw=False):
        key2 = key.replace('/', self.slash_replacement)
        if not key2.endswith(self.suffix):
            key2 = key2 + self.suffix

        js = self._read_file(key2)
        if js is not None:
            # raw means "data is JSON text, not parsed objects"; postgres
            # gets that for free from the column, here it costs a dumps()
            # -- but the contract is what lets callers not care which
            # backend they have
            return {self.key: key, 'data': json.dumps(js) if raw else js,
                    'source': self.source, 'insert_time':'', 'record_time':''}
        else:
            print(f"File does not exist: {self.directory}/{key2}")
            return None

    def set(self, value, identifier=None, yuid=None, record_time=None, change=None):
        # Use key as filename
        if identifier:
            key = identifier
        elif yuid:
            key = yuid
        else:
            raise ValueError("No key provided")

        # subst / in identifier to avoid hierarchy
        key = key.replace('/', self.slash_replacement)

        if not key.endswith(self.suffix):
            key = key + self.suffix
        fn = os.path.join(self.directory, key)        

        if 'data' in value and 'identifier' in value:
            value = value['data']

        # atomic write: a crash mid-write must not leave a truncated record
        tmp = fn + ".tmp"
        with open(tmp, "w") as fh:
            fh.write(json.dumps(value))
        os.replace(tmp, fn)

    def delete(self, key):
        # was missing entirely: __delitem__ raised AttributeError
        key = key.replace('/', self.slash_replacement)
        if not key.endswith(self.suffix):
            key = key + self.suffix
        fn = os.path.join(self.directory, key)
        if os.path.exists(fn):
            os.remove(fn)

    def iter_keys(self):
        for file in self._list():
            yield file.replace(self.suffix, '').replace(self.slash_replacement, '/')

    def iter_keys_since(self, timestamp=None):
        if timestamp is None:
            timestamp = "0001-01-01T00:00:00"
        # FIXME: Should respect timestamp
        for file in self._list():
            yield file.replace(self.suffix, '').replace(self.slash_replacement, '/')

    def _file_to_key(self, file):
        # same transform iter_keys() yields; passing the raw filename to
        # get() worked but returned rows whose identifier still carried the
        # suffix and the escaped slashes
        return file.replace(self.suffix, '').replace(self.slash_replacement, '/')

    def iter_records(self, raw=False):
        for file in self._list():
            yield self.get(self._file_to_key(file), raw=raw)

    def iter_records_slice(self, mySlice, maxSlice, raw=False):
        for file in self._list()[mySlice::maxSlice]:
            yield self.get(self._file_to_key(file), raw=raw)

    def len(self):
        return len(self._list())

    def __getitem__(self, what):
        return self.get(what)

    def __setitem__(self, what, value):
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