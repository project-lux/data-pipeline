from pipeline.process.base.loader import Loader
from pipeline.sources.external.viaf.loader import ViafLoader
import gzip
import zipfile
from lxml import etree
import time
import re
import os

class FastLoader(ViafLoader):
    def __init__(self, config):
        super().__init__(config)

    def load(self):
        start = time.time()
        fh = zipfile.ZipFile(self.in_path)
        members = fh.namelist()

        x = 0
        done_x = 0

        for f in members:
            if not f.endswith(".marcxml"):
                pass
            facet = fh.open(f)

            tree = etree.parse(facet)
            try:
                facet.close()
            except:
                pass

            nss = {"mx": "http://www.loc.gov/MARC21/slim"}
            records = tree.xpath("//mx:record", namespaces=nss)
            for record in records:
                try:
                    identfield = record.xpath('//mx:controlfield[@tag="001"]', namespaces=nss)
                except:
                    # no id??
                    continue
                if identfield:
                    ident = identfield[0].text
                    ident = ident.split("fst")[-1]
                    if ident.startswith("0"):
                        ident = ident.lstrip("0")

            x += 1
            done_x += 1
            new = {"xml": record}
            self.out_cache[ident] = new

            if not done_x % 10000:
                t = time.time() - start
                xps = x / t
                ttls = self.total / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        fh.close()
        self.out_cache.commit()
