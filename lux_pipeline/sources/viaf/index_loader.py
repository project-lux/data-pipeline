from lux_pipeline.process.base.index_loader import LmdbIndexLoader
import gzip
import time
import os


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
            "FAST": "fast",
        }

        total = 115000000  # approx as of 2024-04
        (index, eqindex) = self.get_storage()

        # Now use data extracted from records
        # Merge and sort
        outfn = os.path.join(self.configs.temp_dir, f"viaf_sort_equivs.csv")
        if not os.path.exists(outfn):
            fns = os.path.join(self.configs.temp_dir, f"viaf_equivs_*.csv")
            os.system(f"sort {fns} > {outfn}")

        with open(outfn) as fh:
            l = fh.readline()
            n = 1
            start = time.time()
            updates = {}
            while l:
                l = l.strip()
                # l = l.decode("utf-8")
                ident, viaf = l.split("\t")

                if ":" in ident:
                    (pfx, ident) = ident.split(":")
                    ident = ident.replace(" ", "")
                    if pfx in viaf_prefixes:
                        if pfx == "LC":
                            # test for sh vs n
                            if ident[0] == "s":
                                pfx = "LCSH"
                        p = viaf_prefixes[pfx]
                        updates[f"{p}:{ident}"] = f"{viaf}"
                l = fh.readline()
                n += 1
                if not n % 100000:
                    durn = time.time() - start
                    nps = n / durn
                    ttld = total / nps
                    print(f"Read: {n} / {total} in {durn} = {nps} ==> {ttld} secs")

        start = time.time()
        eqindex.update(updates)
        end = time.time()
        durn = end - start
        print(f"Load: {durn} = {n/durn}/sec")
