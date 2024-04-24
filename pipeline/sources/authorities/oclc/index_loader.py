from pipeline.process.base.index_loader import LmdbIndexLoader
import gzip
import time

class ViafIndexLoader(LmdbIndexLoader):

    def load(self):
        viaf_prefixes = {
            "ISNI": "isni",
            "WKP": "wikidata",
            "NDL": "japan",
            "DNB": "dnb",
            "BNF": "bnf",
            "LC": "lcnaf",
            "LCSH": "lcsh",
            "JPG": "ulan",
            "FAST": "fast"
        }

        total = 115000000 # approx as of 2024-04
        (index, eqindex) = self.get_storage()
        fh = gzip.open(self.in_path)
        l = fh.readline()
        n = 1
        start = time.time()
        updates = {}
        while l:
            l = l.strip()
            l = l.decode('utf-8')
            viaf, ident = l.split('\t')
            viaf = viaf.replace('http://viaf.org/viaf/', '')

            if '|' in ident:
                (pfx, ident) = ident.split('|')
                ident = ident.replace(' ', '')
                if pfx in self.viaf_prefixes:
                    if pfx == "LC":
                        # test for sh vs n
                        if ident[0] == "s": 
                            pfx = "LCSH"
                    p = self.viaf_prefixes[pfx]
                    updates[f"{p}:{ident}"] = f"{viaf}"
            l = fh.readline()
            n += 1
            if not n % 100000:
                self.index.commit()
                durn = time.time() - start
                nps = n / durn
                ttld = self.total / nps
                print(f"Read: {n} / {total} in {durn} = {nps} ==> {ttld} secs")

        fh.close()
        start = time.time()
        eqindex.update(updates)
        end = time.time()
        durn = end-start
        print(f"Load: {durn} = {n/durn}/sec")