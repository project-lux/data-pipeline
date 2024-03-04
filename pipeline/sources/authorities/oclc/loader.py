from pipeline.process.base.loader import Loader
import gzip
from lxml import etree
import time
import re

from cromulent import model, vocab


class ViafLoader(Loader):

    def __init__(self, config):
        self.in_url = config.get('remoteDumpFile', '')
        self.in_path = config['dumpFilePath']
        self.out_cache = config['datacache']
        self.total = config.get('totalRecords', -1)
        self.skip_lines = 0

    def get_identifier_raw(self, line):
        # Find identifier from raw line
        return None


    def load(self):

        fh = gzip.open(self.in_path)

        xstart = time.time()
        x = 0
        xx = 0 
        done_x = 0
        l = 1

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
            # Find id and check if already exists before processing JSON
            l = l.decode('utf-8')
            what, xml = l.split('\t')

            if what and what in self.out_cache:
                done_x += 1
                if not done_x % 10000:
                    print(f"Skipping past {done_x} {time.time() - start}")
                continue
            x += 1
            # Cache assumes JSON as input, so need to wrap it
            # line is (identifer)\t(blob)\n
            # write {"xml": "<record>"} to data cache
            # and let the mapper sort it out downstream

            new = {"xml": xml}
            self.out_cache[what] = new

            if not x % 10000:
                t = time.time() - start
                xps = x/t
                ttls = self.total / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        fh.close()
        self.out_cache.commit()

