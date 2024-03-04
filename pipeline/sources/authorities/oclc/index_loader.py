from pipeline.process.base.index_loader import IndexLoader
import gzip
from sqlitedict import SqliteDict
import time

class ViafIndexLoader(IndexLoader):

    # Load from viaf-links.txt file
    # Can't build records from this as we don't know whether the entity is a 
    # Person, Place, Group, Concept or other

    def __init__(self, config):
        self.total = 127525010

        self.in_path = config['reconcileDumpPath']
        self.out_path = config['inverseEquivDbPath']
        self.viaf_prefixes = {
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
        if self.out_path:
            try:
                self.index = SqliteDict(self.out_path, autocommit=False)
            except:
                print(f"Couldn't built index file at {self.out_path}")
        else:
            print("No inverseEquiv path for VIAF")

    def load(self):
        fh = gzip.open(self.in_path)
        l = fh.readline()
        n = 1
        start = time.time()
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
                    self.index[f"{p}:{ident}"] = f"{viaf}"
            l = fh.readline()
            n += 1
            if not n % 100000:
                self.index.commit()
                durn = time.time() - start
                nps = n / durn
                ttld = self.total / nps
                print(f"{n} / {self.total} in {durn} = {nps} ==> {ttld} secs")

        fh.close()
        self.index.commit()

