import lmdb
from lmdbm import Lmdb
import ujson as json

### Storage Layer

class StringLmdb(Lmdb):
    # key is string, value is string
    def _pre_key(self, value):
        return value.encode('utf-8')
    def _post_key(self, value):
        return value.decode('utf-8')
    def _pre_value(self, value):
        return value.encode('utf-8')
    def _post_value(self, value):
        return value.decode('utf-8')
    def commit(self):
        pass

    @classmethod
    def open(
        cls, file: str, flag: str = "r", mode: int = 0o755, map_size: int = 2**30, 
        autogrow: bool = True, **kw) -> "Lmdb":
        """
        Opens the database `file`.
        `flag`: r (read only, existing), w (read and write, existing),
                c (read, write, create if not exists), n (read, write, overwrite existing)
        """

        if flag == "r":  # Open existing database for reading only (default)
            env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=True, create=False, mode=mode, **kw)
        elif flag == "w":  # Open existing database for reading and writing
            env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=False, create=False, mode=mode, **kw)
        elif flag == "c":  # Open database for reading and writing, creating it if it doesn't exist
            env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=False, create=True, mode=mode, **kw)
        elif flag == "n":  # Always create a new, empty database, open for reading and writing
            # remove_lmdbm(file)
            raise ValueError("can't")
            env = lmdb.open(file, map_size=map_size, max_dbs=1, readonly=False, create=True, mode=mode, **kw)
        else:
            raise ValueError("Invalid flag")

        return cls(env, autogrow)


class TabLmdb(StringLmdb):
    # key is string, value is tab separated __strings__
    # eg URI\tURI\tURI or URI\tType

    def _pre_value(self, value):
        if type(value) == str:
            return value.encode('utf-8')
        elif type(value) in [list, set]:
            try:
                return '\t'.join(value).encode('utf-8')
            except:
                raise ValueError(f"TabLmdb cannot accept {value}")
        elif type(value) == bytes:
            return value
        else:
            # Don't accept ints/floats/dicts
            raise ValueError(f"TabLmdb cannot accept {type(value)}: {value}")

    def _post_value(self, value):
        value = value.decode('utf-8')
        if '\t' in value:
            value = value.split('\t')
        return value

class JsonLmdb(StringLmdb):
    # key is string, value is arbitrary JSON
    # but adds the cost of dumps/loads if really just strings

    def _pre_value(self, value):
        return json.dumps(value).encode('utf-8')
    def _post_value(self, value):
        return json.loads(value.decode('utf-8'))

