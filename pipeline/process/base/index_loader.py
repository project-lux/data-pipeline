
import sys
import time

from sqlitedict import SqliteDict
from pipeline.storage.idmap.lmdb import TabLmdb

class IndexLoader(object):
    def __init__(self, config):
        self.in_cache = config['datacache']
        self.out_path = config['reconcileDbPath']
        self.inverse_path = config['inverseEquivDbPath']
        self.namespace = config['namespace']
        self.reconciler = config.get('reconciler', None)
        self.mapper = config.get('mapper', None)
        self.config = config

    def extract_names(self, rec):
        return self.reconciler.extract_names(rec)

    def extract_uris(self, rec):
        return self.reconciler.extract_uris(rec)

    def acquire_record(self, rec):
        res = self.mapper.transform(rec, None)
        self.mapper.post_mapping(res)
        return res

    def load(self):
        (index, eqindex) = self.get_storage()

        if self.reconciler is None:
            self.reconciler = self.config['reconciler']
        if self.mapper is None:
            self.mapper = self.config['mapper']

        # Clear all current entries
        if index is not None:
            index.clear()
        if eqindex is not None:
            eqindex.clear()

        ttl = self.in_cache.len_estimate()
        n = 0
        start = time.time()
        for rec in self.in_cache.iter_records():

            res = self.acquire_record(rec)
            try:
                typ = res['data']['type']
            except:
                typ = self.mapper.guess_type(rec)
            recid = rec['identifier']

            if index is not None:
                names = self.extract_names(res['data'])
                for nm in names:
                    index[nm.lower()] = [recid, typ]
            if eqindex is not None:
                eqs = self.extract_uris(res['data'])
                for eq in eqs:
                    eqindex[eq] = [recid, typ]

            n += 1
            if not n % 10000:
                if index is not None:
                    index.commit()
                if eqindex is not None:
                    eqindex.commit()
                durn = time.time()-start
                print(f"{n} of {ttl} in {int(durn)} = {n/durn}/sec -> {ttl/(n/durn)} secs")
                sys.stdout.flush()
        if index is not None:
            index.commit()
        if eqindex is not None:
            eqindex.commit()
        

class LmdbIndexLoader(object):

    def get_storage(self):
        if self.out_path:
            index = TabLmdb.open(self.out_path, 'c')
        else:
            index = None
        if self.inverse_path:
            eqindex = TabLmdb.open(self.inverse_path, 'c')
        else:
            eqindex = None
        return (index, eqindex)


class SqliteIndexLoader(object):

    def get_storage(self):
        if out_path:
            index = SqliteDict(out_path, autocommit=False)
        else:
            index = None
        if inverse_path:
            eqindex = SqliteDict(inverse_path, autocommit=False)
        else:
            eqindex = None
        return (index, eqindex)
