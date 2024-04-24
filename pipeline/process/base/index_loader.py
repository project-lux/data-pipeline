
import sys
import time
from sqlitedict import SqliteDict
from pipeline.storage.idmap.lmdb import TabLmdb

class IndexLoader(object):

    def __init__(self, config):
        self.config = config
        self.configs = config['all_configs']
        self.in_cache = config['datacache']
        self.namespace = config['namespace']
        self.in_path = config.get("reconcileDumpPath", None)
        self.out_path = config.get('reconcileDbPath', None)
        self.inverse_path = config.get('inverseEquivDbPath', None)
        self.reconciler = config.get('reconciler', None)
        self.mapper = config.get('mapper', None)

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
        all_names = {}
        all_uris = {}
        for rec in self.in_cache.iter_records():

            res = self.acquire_record(rec)
            if res is None:
                # Mapper might kill it
                continue
            recid = rec['identifier']
            try:
                typ = res['data']['type']
            except:
                typ = self.mapper.guess_type(res['data'])

            if index is not None:
                names = self.extract_names(res['data'])
                for nm in names:
                    all_names[nm.lower()] = [recid, typ]
            if eqindex is not None:
                eqs = self.extract_uris(res['data'])
                for eq in eqs:
                    all_uris[eq] = [recid, typ]

            n += 1
            if not n % 100000:
                durn = time.time()-start
                print(f"{n} of {ttl} in {int(durn)} = {n/durn}/sec -> {ttl/(n/durn)} secs")
                sys.stdout.flush()

        if index is not None and all_names:
            start = time.time()
            index.update(all_names)
            durn = time.time() - start
            print(f"names insert time: {int(durn)} = {len(all_names)/durn}/sec")
        if eqindex is not None and all_uris:
            start = time.time()
            eqindex.update(all_uris)
            durn = time.time() - start
            print(f"uris insert time: {int(durn)} = {len(all_names)/durn}/sec")

class LmdbIndexLoader(IndexLoader):

    def get_storage(self):
        mapExp = self.config.get('mapSizeExponent', 30)
        # n = remove and recreate
        if self.out_path:
            index = TabLmdb.open(self.out_path, 'n', map_size=2**mapExp, readahead=False, writemap=True)
        else:
            index = None
        if self.inverse_path:
            eqindex = TabLmdb.open(self.inverse_path, 'n', map_size=2**mapExp, readahead=False, writemap=True)
        else:
            eqindex = None
        return (index, eqindex)


class SqliteIndexLoader(IndexLoader):

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
