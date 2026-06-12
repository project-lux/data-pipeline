import datetime
import logging

logger = logging.getLogger("lux_pipeline")

# Fields that can be read back via metadata()
_READABLE_METADATA_FIELDS = [
    "insert_time",
    "record_time",
    "refresh_time",
    "valid",
    "change",
]
# Subset that callers are allowed to mutate via set_metadata()
_WRITABLE_METADATA_FIELDS = ["record_time", "refresh_time", "valid", "change"]


class AbstractCache(object):
    def __init__(self, config):
        self.config = config
        self.name = config["name"] + "_" + config["tabletype"]
        self.key = "identifier"
        self.metadata_fields = _READABLE_METADATA_FIELDS
        self.writable_metadata_fields = _WRITABLE_METADATA_FIELDS

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def commit(self):
        """Flush any pending writes. No-op for backends that auto-commit."""
        pass

    def shutdown(self):
        """Sync and close all connections."""
        raise NotImplementedError()

    def make_threadsafe(self):
        """Drop any shared connections so each thread/fork gets its own."""
        pass

    # ------------------------------------------------------------------
    # Sizing
    # ------------------------------------------------------------------

    def len(self):
        """Exact record count."""
        raise NotImplementedError()

    def latest(self):
        """Return the ISO datetime string of the most recently inserted record."""
        raise NotImplementedError()

    def len_estimate(self):
        """Fast approximate count. Falls back to exact len() by default."""
        return self.len()

    # ------------------------------------------------------------------
    # Metadata access — validation lives here so subclasses don't repeat it
    # ------------------------------------------------------------------

    def _validate_key_type(self, key, _key_type):
        """Raise ValueError if a yuid key doesn't look like a UUID."""
        if _key_type == "yuid" and len(key) != 36:
            raise ValueError(
                f"{self.name} has UUIDs as keys; '{key}' is not a valid UUID"
            )

    def metadata(self, key, field="insert_time", _key_type=None):
        """Return a metadata field value for the record identified by key."""
        if _key_type is None:
            _key_type = self.key
        self._validate_key_type(key, _key_type)
        if field not in self.metadata_fields:
            raise ValueError(f"Unknown metadata field in cache: {field}")
        raise NotImplementedError()

    def set_metadata(self, key, field, value, _key_type=None):
        """Overwrite a single metadata field on an existing record."""
        if _key_type is None:
            _key_type = self.key
        self._validate_key_type(key, _key_type)
        if field not in self.writable_metadata_fields:
            raise ValueError(
                f"Attempt to set unsettable metadata field in cache: {field}"
            )
        raise NotImplementedError()

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def iter_records(self):
        """Yield every full record row."""
        raise NotImplementedError()

    def iter_records_slice(self, mySlice=0, maxSlice=10):
        """Yield the mySlice-th 1/maxSlice partition of all records."""
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be >= {maxSlice}")
        raise NotImplementedError()

    def iter_records_since(self, timestamp=None):
        """Yield full record rows whose record_time >= timestamp."""
        raise NotImplementedError()

    def iter_keys(self):
        """Yield every primary key."""
        raise NotImplementedError()

    def iter_keys_slice(self, mySlice=0, maxSlice=10):
        """Yield the mySlice-th 1/maxSlice partition of all keys."""
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be >= {maxSlice}")
        raise NotImplementedError()

    def iter_keys_slice_mem(self, mySlice=0, maxSlice=10):
        """Yield every maxSlice-th key starting at mySlice, without server-side
        row_number() — faster for backends where that is expensive."""
        if mySlice >= maxSlice:
            raise ValueError(f"{mySlice} cannot be >= {maxSlice}")
        raise NotImplementedError()

    def iter_keys_since(self, timestamp=None):
        """Yield keys whose record_time >= timestamp."""
        raise NotImplementedError()

    # ------------------------------------------------------------------
    # Single-record CRUD
    # ------------------------------------------------------------------

    def has_item(self, key, _key_type=None, timestamp=None):
        """Return True if a record with this key exists (and is newer than timestamp, if given)."""
        raise NotImplementedError()

    def get(self, key, _key_type=None):
        """Fetch and return one record dict, or None if not found."""
        if _key_type is None:
            _key_type = self.key
        self._validate_key_type(key, _key_type)
        raise NotImplementedError()

    def _prepare_write_params(
        self, data, identifier, yuid, format, record_time, refresh_time
    ):
        """Validate and apply defaults for write operations.

        Returns (yuid, insert_time, record_time, refresh_time, format) with all
        None-defaults filled in. Raises ValueError for bad arguments.
        Subclasses call this at the top of set() and set_bulk() so the logic
        lives in exactly one place.
        """
        if not identifier and not yuid:
            raise ValueError("Must give YUID or Identifier or both")
        if not isinstance(data, dict):
            raise ValueError("Data must be a dict()")
        if yuid is not None and not isinstance(yuid, str):
            yuid = str(yuid)
        insert_time = datetime.datetime.now()
        if record_time is None:
            record_time = insert_time
        if refresh_time is None:
            refresh_time = "9999-12-31T00:00:00Z"
        if format is None:
            format = "JSON-LD"
        return yuid, insert_time, record_time, refresh_time, format

    def set(
        self,
        data,
        identifier=None,
        yuid=None,
        format=None,
        valid=None,
        record_time=None,
        refresh_time=None,
        change=None,
    ):
        """Write one record. Subclasses must call _prepare_write_params() first."""
        raise NotImplementedError()

    def delete(self, key, _key_type=None):
        """Remove the record identified by key."""
        raise NotImplementedError()

    def clear(self):
        """Delete all records from the cache."""
        raise NotImplementedError()

    # ------------------------------------------------------------------
    # Bulk write interface
    # ------------------------------------------------------------------

    def start_bulk(self):
        """Begin a bulk-write session."""
        raise NotImplementedError()

    def set_bulk(
        self,
        data,
        identifier=None,
        yuid=None,
        format=None,
        valid=None,
        record_time=None,
        refresh_time=None,
        change=None,
    ):
        """Write one record inside an open bulk session."""
        raise NotImplementedError()

    def end_bulk(self):
        """Commit and close the bulk-write session."""
        raise NotImplementedError()

    # ------------------------------------------------------------------
    # Dict-like interface — implemented once here for all backends
    # ------------------------------------------------------------------

    def __getitem__(self, what):
        return self.get(what)

    def __setitem__(self, what, value):
        if "data" in value:
            # given a full record dict; DO NOT MUTATE IT
            d = value["data"]
            params = {}
            if "identifier" in value and what != value["identifier"]:
                raise ValueError(
                    "Record's identifier value and key given to set are different"
                )
            for v in ["identifier", "yuid", "format", "valid", "change", "record_time"]:
                if v in value:
                    params[v] = value[v]
            return self.set(d, **params)
        else:
            # given only the data dict
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
        # Don't let truth-testing fall through to __len__; warn the caller.
        logger.critical(
            "*** code somewhere is asking for a cache as a boolean value and shouldn't ***"
        )
        return True
