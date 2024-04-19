
from pipeline.process.base.loader import Loader
from .fetcher import WdFetcher

import json
import gzip
import time

class WdLoader(WdFetcher, Loader):

    def __init__(self, config):
        WdFetcher.__init__(self, config)
        Loader.__init__(self, config)
        self.skip_lines = 0

    def get_identifier_raw(self, line):
        ididx = line.find('"id":"Q')
        endidx =line[ididx+7:ididx+100].find('"')
        what = line[ididx+6:ididx+7+endidx]
        return what

    def get_identifier_json(self, js):
        # Should never actually get called, but for completeness...
        return js['id']

    def filter_line(self, line):
        # Filter out properties
        return line[:100].find(b'"type":"property",') > 0

    def post_process_json(self, js, identifier):
        # Call on Fetcher parent class
        return self.post_process(js, identifier)

    def load(self):
        # ensure we have the dump file
        self.fetch_dump()

        fh = gzip.open(self.in_path)
        fh.readline() # chomp initial [
        x = 0 
        done_x = 0
        l = 1

        self.out_cache.start_bulk()

        xx = 0
        xstart = time.time()
        while xx < self.skip_lines:
            xx += 1
            l = fh.readline()
            if not xx % 1000000:
                print(f"skipped lines: {xx} in {time.time() - xstart}")

        start = time.time()
        while l:
            l = fh.readline()
            if not l:
                break
            if self.filter_line(l):
                continue

            l = l.decode('utf-8').strip()

            # Find id and check if already exists before processing JSON
            what = self.get_identifier_raw(l)

            if what in self.out_cache:
                done_x += 1
                if not done_x % 10000:
                    print(f"Skipping past {done_x} {time.time() - start}")
                continue

            if l.endswith(','):
                js = json.loads(l[:-1])
            else:
                js = json.loads(l)

            x += 1
            try:
                new = self.post_process_json(js, what)
            except:
                print(f"Failed to process {l}")
                continue
            self.out_cache.set_bulk(new, identifier=what)
            if not x % 10000:
                t = time.time() - start
                xps = x/t
                ttls = self.total / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
                self.out_cache.end_bulk()
                self.out_cache.start_bulk()
        fh.close()
        self.out_cache.end_bulk()
        self.out_cache.commit()