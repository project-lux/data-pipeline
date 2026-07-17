from lux_pipeline.process.base.index_loader import IndexLoader
import glob
import gzip
import time
import os


class ViafIndexLoader(IndexLoader):
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
        # Merge and sort. Regenerate whenever any shard is newer than the
        # sorted file -- a stale viaf_sort_equivs.csv from a previous build
        # used to silently feed this build's index. LC_ALL=C pins the sort
        # order so it doesn't vary with the environment locale.
        outfn = os.path.join(self.configs.temp_dir, f"viaf_sort_equivs.csv")
        fns = os.path.join(self.configs.temp_dir, f"viaf_equivs_*.csv")
        regenerate = not os.path.exists(outfn)
        if not regenerate:
            out_mtime = os.path.getmtime(outfn)
            shards = glob.glob(fns)
            regenerate = any(os.path.getmtime(s) > out_mtime for s in shards)
        if regenerate:
            os.system(f"LC_ALL=C sort {fns} > {outfn}")

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
                        # an ident can appear in multiple VIAF clusters;
                        # last-writer-wins made the winner depend on shard
                        # layout. Keep the smallest VIAF id deterministically.
                        k = f"{p}:{ident}"
                        prev = updates.get(k)
                        if prev is None:
                            updates[k] = f"{viaf}"
                        else:
                            try:
                                smaller = int(viaf) < int(prev)
                            except ValueError:
                                smaller = str(viaf) < str(prev)
                            if smaller:
                                updates[k] = f"{viaf}"
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
