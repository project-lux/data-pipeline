from pipeline.process.base.loader import Loader
import ujson as json
import os
import time


class RorLoader(Loader):
    def load(self):
        # Just load it all into memory
        with open(self.in_path) as fh:
            recs = json.load(fh)
        x = 0
        start = time.time()
        for r in recs:
            x += 1
            ident = r["id"]
            ident = ident.split("/")[-1]
            self.out_cache[ident] = r
            if not x % 10000:
                t = time.time() - start
                xps = x / t
                ttls = self.total / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        self.out_cache.commit()
