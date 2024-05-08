
import os
import sys
import csv
from pipeline.process.base.index_loader import LmdbIndexLoader, TabLmdb

class GlobalIndexLoader(LmdbIndexLoader):

    def get_storage(self):
        mapExp = self.config.get('mapSizeExponent', 30)
        # n = remove and recreate

        diff_path = self.config.get('differentDbPath', None)

        if diff_path:
            index = TabLmdb.open(diff_path, 'c', map_size=2**mapExp, readahead=False, writemap=True)
        else:
            index = None
        if self.inverse_path:
            eqindex = TabLmdb.open(self.inverse_path, 'c', map_size=2**mapExp, readahead=False, writemap=True)
        else:
            eqindex = None
        return (index, eqindex)

    def clear(self, which):
        (diffindex, eqindex) = self.get_storage()
        if which == "equivs":
            eqindex.clear()
        elif which == "diffs":
            diffindex.clear()
        else:
            print(f"Unknown index to clear {which}; should be equivs or diffs")

    def load(self, filename, which="equivs"):
        (diffindex, eqindex) = self.get_storage()

        if diffindex is None and eqindex is None:
            print(f"{self.name} has no indexes configured")
            return None
        elif not os.path.exists(filename):
            print(f"{filename} does not exist to load")
            return None
        elif not which in ['equivs', 'diffs']:
            print(f"{which} is not equivs or diffs")
            return None

        # Don't clear, as we'll call this multiple times
        n = 0
        updates = {}
        with open(filename) as csvfh:
            rdr = csv.reader(csvfh, delimiter=delim)
            x = 0
            for row in rdr:
                if not row[0].startswith('http') or not row[1].startswith('http'):
                    continue
                (a,b) = row[:2]

                if a.startswith('https://lux.collections.yale.edu/data/'):
                    # Don't do this! 
                    raise ValueError(f"File {csvfn} has LUX URI: {a}")
                else:                
                    a = self.configs.canonicalize(a)

                if b.startswith('https://lux.collections.yale.edu/data/'):
                    # Don't do this! 
                    raise ValueError(f"File {csvfn} has LUX URI: {b}")
                else:                
                    b = self.configs.canonicalize(b)

                # NOTE: This doesn't have class
                updates[a] = b
                updates[b] = a

        if updates:
            if which == "equivs":
                eqindex.update(updates)
            else:
                diffindex.update(updates)
