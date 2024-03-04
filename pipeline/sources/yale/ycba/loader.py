import os
import shutil
import time
import json
import pathlib
import tarfile

from pipeline.process.base.loader import Loader

class YcbaLoader(Loader):

    def __init__(self, config):
        self.in_path = config['dumpFilePath']
        self.out_cache = config['datacache']
        self.total = config.get('totalRecords', -1)

    def get_identifier_raw(self, line):
        # Find identifier from raw line
        return None

    def get_identifier_json(self, js):
        return js['id'].replace('https://ycba-lux.s3.amazonaws.com/v3/', '')

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
            if ti.name.endswith('json') and "/" in ti.name:
                bio = tf.extractfile(ti)
                ident = ti.name
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

            what = ident.replace('linked_art/', '')
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
                xps = x/t
                ttls = 4000000 / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        tf.close()
        self.out_cache.commit()

    def load_from_disk(self):
        # in is a directory of top level directories
        # each has sub directories, with files
        # use pathlib to glob in everything

        try:
            files = pathlib.Path(self.in_path).rglob("*.json")
        except:
            files = [pathlib.Path(self.in_path)]

        x = 0 
        done_x = 0
        start = time.time()
        for f in files:
            l = f.read_text()
            if not l:
                break

            # Cache assumes JSON as input, so need to parse it
            try:
                js = json.loads(l)
            except:
                raise    
            x += 1
            what = self.get_identifier_json(js)
            self.out_cache[what] = js
            if not x % 10000:
                t = time.time() - start
                xps = x/t
                ttls = (self.total / (maxSlice+1)) / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        self.out_cache.commit()