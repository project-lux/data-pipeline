import os
import shutil
import time
import ujson as json
import pathlib
import tarfile

from lux_pipeline.process.base.loader import Loader


class PmcLoader(Loader):
    def __init__(self, config):
        self.in_path = config["dumpFilePath"]
        self.out_cache = config["datacache"]
        self.total = config.get("totalRecords", -1)

    def get_identifier_raw(self, line):
        # Find identifier from raw line
        return None

    def get_identifier_json(self, js):
        return js["id"].replace("https://data.paul-mellon-centre.ac.uk/", "")

    def post_process_json(self, js):
        return js

    def filter_line(self, line):
        return False

    def load(self):
        # in is a directory of tgz files

        start = time.time()
        tf = tarfile.open(self.in_path, "r:gz")
        members = tf.getmembers()
        x = 0
        done_x = 0
        for ti in members:
            if ti.name.endswith("json") and "/" in ti.name:
                bio = tf.extractfile(ti)
                ident = ti.name
                what = ident.replace(".json", "")
            else:
                continue
            l = bio.read()
            try:
                bio.close()
            except:
                pass
            if len(l) < 30:
                # Empty record means was previously deleted
                continue

            if what and what in self.out_cache:
                done_x += 1
                if not done_x % 10000:
                    print(f"Skipping past {done_x} {time.time() - start}")
                continue
            # Cache assumes JSON as input, so need to parse it
            try:
                js = json.loads(l)
            except Exception as e:
                print(f"REALLY Broken record {ident} in {f}: {e}")
                continue
            x += 1
            self.out_cache[what] = js
            if not x % 10000:
                t = time.time() - start
                xps = x / t
                ttls = 4000000 / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        tf.close()
        self.out_cache.commit()
