import datetime
import difflib
import json as stdjson
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

# How records are written ("json_format" in the cache's configuration): a
# name here, or the ujson.dumps parameters themselves. Without the key,
# ujson's defaults (one line, "/" escaped), as data caches always have been.
# Both named formats leave "/" and non-ASCII alone, so URIs read as written.
JSON_FORMATS = {
    "compact": {"escape_forward_slashes": False, "ensure_ascii": False},
    "normalized": {"indent": 2, "escape_forward_slashes": False, "ensure_ascii": False},
}


class RecordError(ValueError):
    """A record that cannot be read or written; `problems` lists why."""

    def __init__(self, message, problems=()):
        self.problems = list(problems)
        if self.problems:
            message = message + ": " + "; ".join(self.problems)
        super().__init__(message)


# Problems are sentences; one starting "warning:" does not stop a write.
def errors_only(problems):
    return [p for p in problems if not p.startswith("warning:")]


def warnings_only(problems):
    return [p for p in problems if p.startswith("warning:")]


def text_problems(text):
    """Problems a record's text has that its data cannot show: not JSON,
    or a key written twice in one object (JSON keeps the last value, so the
    first is silently ignored; a warning)."""
    twice = []

    def pairs(items):
        seen = set()
        for k, _ in items:
            if k in seen:
                twice.append(k)
            seen.add(k)
        return dict(items)

    try:
        stdjson.loads(text, object_pairs_hook=pairs)
    except ValueError as e:
        return [f"not JSON: {e}"]
    return [f"warning: {k} appears twice in the file; only the last value counts"
            for k in dict.fromkeys(twice)]


def dumps_params(json_format):
    """The ujson.dumps keyword arguments for a ``json_format`` value."""
    if json_format is None:
        return {}
    if isinstance(json_format, str):
        if json_format not in JSON_FORMATS:
            raise ValueError(f"json_format {json_format!r} is not one of "
                             f"{', '.join(JSON_FORMATS)} or an object of ujson.dumps parameters")
        return dict(JSON_FORMATS[json_format])
    if isinstance(json_format, dict):
        return dict(json_format)
    raise ValueError(f"json_format should be a name or an object, not {json_format!r}")


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
    """One JSON record per file in a directory.

    Besides the cache interface, the record-level operations a directory
    of hand-maintained JSON files (configuration, say) needs, all built on
    the cache's ``json_format`` and ``json_schema``: `text` and `read`
    (strictly: a file that is not JSON is an error, not a string),
    `validate` and `check`, `diff`, and `write` / `write_text` / `create`
    / `delete`, which refuse a record with errors (`RecordError`, or the
    subclass's `error_class`). `set` stays the plain cache write: it logs
    schema problems and writes anyway. A subclass adds its own checks by
    extending `validate` and `check`, and its own wording of schema errors
    with `describe_error`."""

    error_class = RecordError

    def __init__(self, config):

        self.config = config
        # e.g. test1_datacache
        self.name = config['name'] + '_' + config['tabletype']
        self.directory = os.path.join(config['base_dir'], self.name)
        self.suffix = ".json"
        self.source = config['name']
        self.slash_replacement = "___"
        self.key = "identifier"
        self.dumps_params = dumps_params(config.get("json_format"))

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

    def path(self, key):
        """The (absolute) file record `key` is, or would be, kept in."""
        return os.path.abspath(self._path(key))

    @property
    def where(self):
        """The cache, for messages."""
        return self.directory

    def read_text(self, key):
        """Record `key`'s file as text; None when there is none."""
        try:
            with open(self._path(key), encoding="utf-8") as fh:
                return fh.read()
        except FileNotFoundError:
            return None

    def dumps(self, value):
        """`value` as this cache writes it (`json_format`); an indented
        file ends with a newline."""
        text = json.dumps(value, **self.dumps_params)
        return text + "\n" if self.dumps_params.get("indent") else text

    # --- records as files: read, check and write them --------------------------------------

    def text(self, key):
        """Record `key`'s file as text; `error_class` when there is none."""
        text = self.read_text(key)
        if text is None:
            raise self.error_class(f"record {key!r} is not in {self.where}")
        return text

    def read(self, key):
        """Record `key` as its file holds it, parsed strictly."""
        try:
            return stdjson.loads(self.text(key))
        except ValueError as e:
            raise self.error_class(f"record {key!r} is not JSON: {e}") from None

    def schema_validators(self):
        """The validators `validate` applies: the cache's ``json_schema``,
        when it has one."""
        validator = self.schema_validator()
        return [validator] if validator is not None else []

    def describe_error(self, error, data):
        """A jsonschema error as sentences."""
        return [f"{'.'.join(str(p) for p in error.path) or 'record'}: {error.message}"]

    def validate(self, key, data, *, new=False):
        """Problems with writing `data` as record `key` (a new one when
        `new`), as sentences; ``[]`` when there are none. Here: the
        ``json_schema`` (`schema_validators`), and for a new record a key
        not already taken."""
        problems = []
        try:
            validators = self.schema_validators()
        except ValueError as e:
            problems.append(f"the schema: {e}")
            validators = []
        for validator in validators:
            for error in sorted(validator.iter_errors(data), key=lambda e: [str(p) for p in e.path]):
                problems.extend(p for p in self.describe_error(error, data) if p not in problems)
        if new and key in self:
            problems.append(f"record {key!r} already exists in {self.where}")
        return problems

    def check(self, key):
        """Every problem with record `key` as it is on disk: its text
        (`text_problems`) and its data (`validate`)."""
        try:
            text = self.text(key)
        except OSError as e:
            return [f"cannot read record {key!r}: {e}"]
        problems = text_problems(text)
        if errors_only(problems):
            return problems
        return problems + self.validate(key, stdjson.loads(text))

    def refuse_errors(self, key, data, new=False):
        """`validate`, raising `error_class` on errors; returns the
        warnings."""
        problems = self.validate(key, data, new=new)
        errors = errors_only(problems)
        if errors:
            raise self.error_class(f"not writing {key}", errors)
        return warnings_only(problems)

    def diff(self, key, data):
        """What `write` would change, as a unified diff (empty when nothing
        would)."""
        path = self.path(key)
        return "".join(difflib.unified_diff(self.text(key).splitlines(True),
                                            self.dumps(data).splitlines(True),
                                            fromfile=path, tofile=path))

    def write(self, key, data):
        """Write `data` as record `key` (``json_format``), once it
        validates; the file is left alone when its text would not change.
        Returns ``(path, warnings)``."""
        warnings = self.refuse_errors(key, data)
        if self.read_text(key) != self.dumps(data):
            self.set(data, identifier=key)
        return self.path(key), warnings

    def write_text(self, key, text):
        """Write an editor's `text` as record `key` once it parses and
        validates, stored as `write` stores it. Returns ``(path,
        warnings)``, a key the text has twice among the warnings."""
        problems = text_problems(text)
        if errors_only(problems):
            raise self.error_class(f"not writing {key}: {problems[0]}")
        path, warnings = self.write(key, stdjson.loads(text))
        return path, problems + warnings

    def create(self, key, data):
        """Write `data` as a new record `key`. Returns ``(path, warnings)``."""
        warnings = self.refuse_errors(key, data, new=True)
        if os.path.exists(self._path(key)):
            raise self.error_class(f"not writing {key}: {self.path(key)} exists")
        self.set(data, identifier=key)
        return self.path(key), warnings

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
            with open(fn, encoding="utf-8") as fh:
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

        self.warn_problems(key, value)

        # atomic write: a crash mid-write must not leave a truncated record
        tmp = fn + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(self.dumps(value))
        os.replace(tmp, fn)
        if record_time is not None:
            t = _as_datetime(record_time).timestamp()
            os.utime(fn, (t, t))
        # format is always JSON-LD here; the rest replace what was there
        self._write_meta(fn, {"refresh_time": refresh_time, "valid": valid, "change": change})

    def delete(self, key, _key_type=None):
        """Remove record `key` (and its .meta). Returns its path."""
        # was missing entirely: __delitem__ raised AttributeError
        fn = self._path(key)
        for path in (fn, fn + ".meta"):
            if os.path.exists(path):
                os.remove(path)
        return os.path.abspath(fn)

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