
from pipeline.process.base.loader import Loader
import json_stream   # pip install json-stream
import os
import time

class RorLoader(Loader):
    
    def load(self):
        # Load external links from separate file, temporarily!

        fh = open(self.in_path)
        recs = json_stream.load(fh)
        x = 0
        start = time.time()
        for r in recs:
            x += 1
            rec = json_stream.to_standard_types(r)
            ident = rec['id']
            ident = ident.split('/')[-1]
            self.out_cache[ident] = rec
            if not x % 10000:
                t = time.time() - start
                xps = x/t
                ttls = self.total / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        fh.close()
        self.out_cache.commit()
