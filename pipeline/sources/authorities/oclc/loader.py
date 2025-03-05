from pipeline.process.base.loader import Loader
import gzip
import zipfile
from lxml import etree
import time
import re
import os


class ViafLoader(Loader):
    def __init__(self, config):
        self.in_url = config.get("remoteDumpFile", "")
        self.in_path = config["dumpFilePath"]
        self.out_cache = config["datacache"]
        self.total = config.get("totalRecords", 14000000)
        self.skip_lines = 0
        self.config = config
        self.configs = config["all_configs"]

    def get_identifier_raw(self, line):
        # Find identifier from raw line
        return None

    def load(self, slicen=None, maxSlice=None):
        with gzip.open(self.in_path, "rt") as fh, \
            open(os.path.join(self.configs.temp_dir, f"viaf_equivs_{slicen}.csv"), "w") as efh:

            mapper = self.config["mapper"]
            wdm = mapper.wikidata_config["mapper"]
            nss = mapper.nss

            xstart = time.time()
            start = time.time()

            x = 0
            done_x = 0
            l = 1
            while l:
                l = fh.readline()
                if not l:
                    break
                if maxSlice is not None and x % maxSlice - slicen != 0:
                    x += 1
                    continue

                l = l.decode("utf-8")
                what, xml = l.split("\t")
                x += 1
                done_x += 1
                new = {"xml": xml}
                self.out_cache[what] = new

                # Need to extract links to index
                top = mapper.parse_xml(xml)
                equivs = top.xpath("./viaf:sources/viaf:source/text()", namespaces=nss)
                for eq in equivs:
                    (which, val) = eq.split("|")
                    if which == "LC" and val[0] == "s":
                        which = "LCSH"
                    elif which in ["DNB", "BNF"]:
                        # processed via @nsid above for now
                        continue
                    elif which == "FAST":
                        val = val.replace("fst", "")
                    if which in mapper.viaf_prefixes:
                        val = val.replace(" ", "")  # eg sometimes LC is "n  123456" and should be n123456
                        # Only include Wikidata references from VIAF if they guess to the right class
                        if which == "WKP":
                            wdeq = wdm.get_reference(val)
                            nameType = top.xpath("./viaf:nameType/text()", namespaces=nss)[0]
                            topCls = mapper.nameTypeMap.get(nameType, None)

                            if wdeq is not None and wdeq.type == topCls.__name__:
                                efh.write(f"{which}:{val}\t{what}\n")
                        else:
                            efh.write(f"{which}:{val}\t{what}\n")

                if not done_x % 10000:
                    efh.flush()
                    t = time.time() - start
                    xps = x / t
                    ttls = self.total / xps
                    print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")

        self.out_cache.commit()
