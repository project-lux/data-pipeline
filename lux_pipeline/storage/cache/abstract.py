"""The interface every record cache implements.

A cache holds one row per record: ``{"data": {...}, "identifier": ...,
"yuid": ..., "insert_time": ..., "record_time": ..., "refresh_time": ...,
"valid": ..., "change": ...}``, keyed by `self.key` (``"identifier"`` or
``"yuid"``). `get()` adds ``"source"``, the source's name. With
``raw=True`` a read returns ``"data"`` as JSON text instead of parsed
objects, for callers that only write it out again.

The methods that raise `NotImplementedError` here are what a backend has to
provide. The rest have working defaults built on those, which a backend
overrides when it can do better (Postgres answers `get_multi` in one
query). Callers may rely on every method below, on any backend.

Not part of the interface, and so not defined here: capabilities only some
backends have, which callers find with ``getattr``. Defining a stub for one
would make every backend appear to have it:

* ``backup(dest_dir, stem, progress)``, ``restore(src_dir, entry,
  progress)``, ``backup_size()``: native backup (`storage/backup.py` falls
  back to a portable JSONL copy without them);
* Postgres maintenance: ``optimize``, ``bloat``, ``bloat_line``,
  ``rewrite``, ``drop_time_index``;
* ``get_like(prefix)``.
"""

import datetime
import logging

logger = logging.getLogger("lux_pipeline")

# the metadata fields metadata() reads, and the ones set_metadata() may write
METADATA_FIELDS = ["insert_time", "record_time", "refresh_time", "valid", "change"]
WRITABLE_METADATA_FIELDS = ["record_time", "refresh_time", "valid", "change"]

# iter_*_type("Concept") means any of these
CONCEPT_TYPES = ["Type", "Currency", "Language", "Material", "MeasurementUnit"]


class AbstractCache(object):
    key = "identifier"

    def __init__(self, config):
        self.config = config
        self.name = config["name"] + "_" + config["tabletype"]

    # --- connections and write batching ---
    #
    # A backend without transactions writes in set() and these do nothing.

    def shutdown(self):
        """Close any connections."""

    def commit(self):
        """Land anything written so far."""

    def defer_commits(self, every=100, batch=0):
        """Stop committing in every set(); commit at a checkpoint() once
        `every` writes have accumulated. See PooledCache.defer_commits."""

    def resume_commits(self):
        """Land anything outstanding and go back to committing per set()."""

    def checkpoint(self):
        """The end of a unit of work: commit if enough writes are pending."""

    def flush(self):
        """Commit outstanding writes now."""

    def end_read(self):
        """End a transaction a read left open."""

    # --- size ---

    def len(self):
        """The exact number of records."""
        raise NotImplementedError()

    def len_estimate(self):
        """A fast approximate count; exact where that is just as cheap."""
        return self.len()

    def latest(self):
        """The newest insert_time as an ISO string, or
        ``"0000-01-01T00:00:00"`` for an empty cache."""
        raise NotImplementedError()

    # --- metadata ---

    def metadata(self, key, field="insert_time", _key_type=None):
        """``{field: value}`` for the record, or None if it is not there.
        `field` is one of METADATA_FIELDS (ValueError otherwise)."""
        raise NotImplementedError()

    def set_metadata(self, key, field, value, _key_type=None):
        """Set one of WRITABLE_METADATA_FIELDS on an existing record
        (ValueError for any other field)."""
        raise NotImplementedError()

    # --- single records ---

    def get(self, key, _key_type=None, raw=False):
        """The record's row, or None."""
        raise NotImplementedError()

    def get_multi(self, keys, _key_type=None, raw=False):
        """``{key: row}`` for those of `keys` the cache holds."""
        out = {}
        for k in keys:
            row = self.get(k, _key_type=_key_type, raw=raw)
            if row is not None:
                out[k] = row
        return out

    def get_fresh(self, key, since=None, raw=False, _key_type=None):
        """The row only if its insert_time is at least `since` (any row when
        `since` is None), else None."""
        row = self.get(key, _key_type=_key_type, raw=raw)
        if row is None or since is None:
            return row
        md = self.metadata(key, "insert_time", _key_type=_key_type)
        if md is None or md["insert_time"] < since:
            return None
        return row

    def has_item(self, key, _key_type=None, timestamp=None):
        """True if the record is there (with a record_time of at least
        `timestamp`, when given)."""
        raise NotImplementedError()

    def has_multi(self, keys, _key_type=None):
        """The set of those of `keys` the cache holds."""
        return {k for k in keys if self.has_item(k, _key_type=_key_type)}

    def set(self, data, identifier=None, yuid=None, format=None, valid=None,
            record_time=None, refresh_time=None, change=None):
        """Write one record: `data` is the record's JSON, keyed by
        `identifier` or `yuid` (ValueError if neither)."""
        raise NotImplementedError()

    def delete(self, key, _key_type=None):
        """Remove the record, if it is there."""
        raise NotImplementedError()

    def clear(self):
        """Remove every record."""
        raise NotImplementedError()

    # --- bulk writes ---

    def start_bulk(self):
        """Begin a bulk load (plain inserts, no upsert)."""

    def set_bulk(self, data, identifier=None, yuid=None, format=None, valid=None,
                 record_time=None, refresh_time=None, change=None):
        """Write one record inside start_bulk() / end_bulk()."""
        return self.set(data, identifier=identifier, yuid=yuid, format=format, valid=valid,
                        record_time=record_time, refresh_time=refresh_time, change=change)

    def end_bulk(self):
        """Land the bulk load."""

    # --- iteration ---
    #
    # A slice is one of maxSlice disjoint parts; the same slice must come
    # out in every worker, so every record is seen by exactly one.

    def iter_keys(self):
        raise NotImplementedError()

    def iter_records(self, raw=False):
        raise NotImplementedError()

    def iter_keys_slice(self, mySlice=0, maxSlice=10):
        raise NotImplementedError()

    def iter_records_slice(self, mySlice=0, maxSlice=10, raw=False):
        raise NotImplementedError()

    def iter_keys_slice_mem(self, mySlice=0, maxSlice=10):
        """Every maxSlice-th key in key order, starting at mySlice."""
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")
        for k in sorted(self.iter_keys())[mySlice::maxSlice]:
            yield k

    def iter_keys_since(self, timestamp=None):
        """Keys whose record_time is at least `timestamp` (all when None)."""
        raise NotImplementedError()

    def iter_records_since(self, timestamp=None):
        """Rows whose record_time is at least `timestamp` (all when None)."""
        raise NotImplementedError()

    def iter_items(self):
        """``(key, row)`` for every record."""
        for row in self.iter_records():
            yield str(row[self.key]), row

    @staticmethod
    def _type_matches(row, t):
        typ = row["data"].get("type") if isinstance(row.get("data"), dict) else None
        return typ in CONCEPT_TYPES if t == "Concept" else typ == t

    def iter_records_type(self, t):
        """Rows whose data's top-level type is `t` ("Concept": any of
        CONCEPT_TYPES)."""
        for row in self.iter_records():
            if self._type_matches(row, t):
                yield row

    def iter_keys_type(self, t):
        for row in self.iter_records_type(t):
            yield row[self.key]

    def iter_keys_type_slice(self, t, mySlice=0, maxSlice=10):
        """Every maxSlice-th key of type `t` in key order, from mySlice."""
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be > {maxSlice}")
        for k in sorted(self.iter_keys_type(t))[mySlice::maxSlice]:
            yield k

    # --- write defaults, shared by set() implementations ---

    def _prepare_write_params(self, data, identifier, yuid, format, record_time, refresh_time):
        """Check set()'s arguments and fill in its defaults. Returns
        ``(yuid, insert_time, record_time, refresh_time, format)``."""
        if not identifier and not yuid:
            raise ValueError("Must give YUID or Identifier or both")
        if not type(data) == dict:
            raise ValueError("Data must be a dict()")
        if yuid is not None and not type(yuid) == str:
            yuid = str(yuid)
        insert_time = datetime.datetime.now()
        if record_time is None:
            record_time = insert_time
        if refresh_time is None:
            refresh_time = "9999-12-31T00:00:00Z"
        if format is None:
            format = "JSON-LD"
        return yuid, insert_time, record_time, refresh_time, format

    # --- behave like a dict ---

    def __getitem__(self, what):
        return self.get(what)

    def __setitem__(self, what, value):
        if "data" in value:
            # given a full record; DO NOT MUTATE IT
            d = value["data"]
            params = {}
            if "identifier" in value and what != value["identifier"]:
                raise ValueError("Record's identifier value and key given to set are different")
            for v in ["identifier", "yuid", "format", "valid", "change", "record_time"]:
                if v in value:
                    params[v] = value[v]
            return self.set(d, **params)
        else:
            # given only the json
            if self.key == "identifier":
                return self.set(value, identifier=what)
            elif self.key == "yuid":
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
        logger.error(f"*** code somewhere is asking for a cache as a boolean value and shouldn't ***")
        return True
