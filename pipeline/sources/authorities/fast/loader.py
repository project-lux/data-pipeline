from pipeline.process.base.loader import Loader
import gzip
import zipfile
from lxml import etree
import time
import re
import os

class FastLoader(Loader):
    def __init__(self, config):
        self.in_url = config.get("remoteDumpFile", "")
        self.in_path = config["dumpFilePath"]
        self.out_cache = config["datacache"]
        self.total = config.get("totalRecords", 14000000)
        self.skip_lines = 0
        self.config = config
        self.configs = config["all_configs"]

    def load(self):
        start = time.time()
        with zipfile.ZipFile(self.in_path) as fh:
            members = fh.namelist()

            x = 0
            done_x = 0
            
            for f in members:
                if not f.endswith(".marcxml"):
                    continue
                with fh.open(f) as facet:
                    tree = etree.parse(facet)
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
        self.out_cache.commit()
