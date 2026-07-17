import ujson as json
from lmdbm import Lmdb

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

    def commit(self):
        pass

    def __contains__(self, key):
        if not str(key).strip():
            return False
        try:
            return Lmdb.__contains__(self, key)
        except ValueError as e:
            # over-long key: cannot exist in this store. Report it, since a
            # caller treating this as "absent" may mint a duplicate identity.
            print(f"\n*** __contains__ got {e} for '{str(key)[:100]}' ***")
            return False
        # real LMDB/environment errors now propagate instead of silently
        # reporting the key as absent


class TabLmdb(StringLmdb):
    # key is string, value is tab separated __strings__
    # eg URI\tURI\tURI or URI\tType
    # NB: a single-member list/set round-trips back as a bare str (callers
    # must handle both), and tab is the separator -- so members containing
    # a literal tab would silently split into phantom members on read.
    # Reject them at write time instead.

    def _pre_value(self, value):
        if type(value) == str:
            if "\t" in value:
                raise ValueError(f"TabLmdb member contains a tab: {value!r}")
            return value.encode("utf-8")
        elif type(value) in [list, set]:
            try:
                for v in value:
                    if "\t" in v:
                        raise ValueError(f"TabLmdb member contains a tab: {v!r}")
                return "\t".join(value).encode("utf-8")
            except ValueError:
                raise
            except Exception:
                raise ValueError(f"TabLmdb cannot accept {value}")
        elif type(value) == bytes:
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
