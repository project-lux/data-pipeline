
import sys
import time
from sqlitedict import SqliteDict

class IndexLoader(object):
    def __init__(self, config):
        self.in_cache = config['datacache']
        self.out_path = config['reconcileDbPath']
        self.config = config

    def load(self):
        in_cache = self.config['datacache']
        rec_cache = self.config['recordcache']
        out_path = self.config.get('reconcileDbPath', None)
        inverse_path = self.config.get('inverseEquivDbPath', None)
        if out_path:
            index = SqliteDict(out_path, autocommit=False)
        else:
            index = None
        if inverse_path:
            eqindex = SqliteDict(inverse_path, autocommit=False)
        else:
            eqindex = None
        mapper = self.config['mapper']
        ttl = self.config.get('totalRecords', -1)
        n = 0

        # Clear all current entries
        if index is not None:
            index.clear()
        if eqindex is not None:
            eqindex.clear()

        start = time.time()
        for rec in in_cache.iter_records():
            # Might as well transform into linked art on the way through
            recid = rec['identifier']
            res = mapper.transform(rec, None)
            mapper.fix_links(res)
            if not res:
                # This could be for (e.g.) LCNAF NameTitles or just broken
                continue

            typ = res['data']['type']
            # rec_cache[recid] = res
            if index is not None:
                names = res['data'].get('identified_by', [])
                for nm in names:
                    # index primary names
                    cxns = nm.get('classified_as', [])
                    if 'http://vocab.getty.edu/aat/300404670' in [cx['id'] for cx in cxns]:
                        val = nm['content']
                        index[val.lower()] = [recid, typ]
            if eqindex is not None:
                for eq in [x.get('id', None) for x in res['data'].get('equivalent', [])]:
                    if eq:
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
        