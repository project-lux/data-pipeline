
from pipeline.process.base.loader import Loader
from .fetcher import WdFetcher
from .base import WdConfigManager
import ujson as json
import gzip
import time
import os

class WdLoader(WdFetcher, WdConfigManager, Loader):

    def __init__(self, config):
        WdConfigManager.__init__(self, config)
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
        try:
            return line[:100].find('"type":"property",') > 0
        except:
            return line[:100].find(b'"type":"property",') > 0

    def post_process_json(self, js, identifier):
        # Call on Fetcher parent class
        return self.post_process(js, identifier)

    def load(self, slicen=None, maxSlice=None):
        # ensure we have the dump file
        # self.fetch_dump()

        with gzip.open(self.in_path, "rt") as fh, \
            open(os.path.join(self.configs.temp_dir, f'wd_equivs_{slicen}.csv'), 'w') as efh, \
            open(os.path.join(self.configs.temp_dir, f'wd_diffs_{slicen}.csv'), 'w') as dfh:


            fh.readline() # chomp initial [
            x = 0 
            l = 1
            done_x = 0

            self.out_cache.start_bulk()
            start = time.time()
            while l:
                l = fh.readline()
                if not l:
                    break
                if maxSlice is not None and x % maxSlice - slicen != 0:
                    x += 1
                    continue

                x += 1
                if self.filter_line(l):
                    continue
                done_x += 1
                l = l.decode('utf-8').strip()
                what = self.get_identifier_raw(l)
                if l.endswith(','):
                    js = json.loads(l[:-1])
                else:
                    js = json.loads(l)

                try:
                    new = self.post_process_json(js, what)
                except:
                    print(f"Failed to process {l}")
                    raise
                    continue
                self.out_cache.set_bulk(new, identifier=what)

                # Create intermediate files for indexing
                sames, diffs = self.process_equivs({'data':new})
                for sx,sy in sames:
                    efh.write(f'{sx},{sy}\n')
                for dx,dy in diffs:
                    dfh.write(f'{dx},{dy}\n')

                if not done_x % 10000:
                    t = time.time() - start
                    xps = x/t
                    ttls = self.total / xps
                    print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
                    self.out_cache.end_bulk()
                    self.out_cache.start_bulk()

        self.out_cache.end_bulk()
        self.out_cache.commit()
