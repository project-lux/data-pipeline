from pipeline.process.base.loader import Loader
import gzip
import zipfile
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


    def load(self, slicen=None, maxSlice=None):

        fh = gzip.open(self.in_path)

        xstart = time.time()
        x = 0
        done_x = 0
        l = 1

        start = time.time()
        while l:
            l = fh.readline()
            if not l:
                break
            if maxSlice is not None and x % maxSlice - slicen != 0:
                x+= 1
                continue

            l = l.decode('utf-8')
            what, xml = l.split('\t')
            x += 1
            done_x += 1
            new = {"xml": xml}
            self.out_cache[what] = new

            if not done_x % 10000:
                t = time.time() - start
                xps = x/t
                ttls = self.total / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        fh.close()
        self.out_cache.commit()

class FastLoader(ViafLoader):

    def __init__(self, config):
        super().__init__(config)

    def load(self, slicen=None, maxSlice=None):

        start = time.time()
        fh = zipfile.ZipFile(self.in_path)
        members = fh.namelist()

        x = 0
        done_x = 0

        #self.total = len(members)

        for f in members:
            if not f.endswith(".marcxml"):
                pass
            facet = fh.open(f)
            #ident = f

            l = facet.read()
            try:
                facet.close()
            except:
                pass

            l = l.decode('utf-8')
            records = l.split("</mx:record>")
            for i, record in enumerate(records):
                if '<mx:record' in record:
                    record = record.split("<mx:record")[1]
                    print(record)
                    break

            x += 1
            done_x += 1
            new = {"xml": xml}
            self.out_cache[what] = new

            if not done_x % 10000:
                t = time.time() - start
                xps = x/t
                ttls = self.total / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        fh.close()
        self.out_cache.commit()


