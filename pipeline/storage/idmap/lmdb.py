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

    def __contains__(self, key):
        try:
            return Lmdb.__contains__(self, key)
        except Exception as e:
            print(f"\n*** __contains__ got {e} for {key} ***")
            return False

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

