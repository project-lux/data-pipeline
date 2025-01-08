
import os
import requests
import shutil
import time
import gzip
import zipfile
import ujson as json
import sys

from pipeline.process.base.loader import Loader

class YulLoader(Loader):

    def __init__(self, config):
        self.in_path = config['dumpFilePath']
        self.out_cache = config['datacache']
        self.total = config.get('totalRecords', -1)

    def get_identifier_raw(self, line):
        # Find identifier from raw line
        return None

    def get_identifier_json(self, js):
        return js['id'].replace('https://linked-art.library.yale.edu/node/', '')

    def post_process_json(self, js):
        return js

    def filter_line(self, line):
        return False

    def load(self, slicen=None, maxSlice=None):
        # in is a directory of jsonl files
        try:
            files = os.listdir(self.in_path)
            files.sort()
        except:
            files = [self.in_path]
        # slice down to only the files for this task
        # maxSlice is EXCLUSIVE e.g. slices should be 0-19 for maxSlice=20
        if slicen is not None:
            files = files[slicen::maxSlice]
        else:
            maxSlice = 1
        x = 0 
        done_x = 0
        start = time.time()
        for f in files:
            if not 'jsonl' in f:
                continue
            if f.endswith(('jsonl','gz')):
                open_func = gzip.open if f.endswith('gz') else open
            else:
                continue

            with open_func(os.path.join(self.in_path, f), "rt") as fh:
                l = 1
                while l:
                    l = fh.readline()
                    if not l:
                        break
                    # Find id and check if already exists before processing JSON
                    what = self.get_identifier_raw(l)
                    if what and what in self.out_cache:
                        done_x += 1
                        if not done_x % 10000:
                            print(f"Skipping past {done_x} {time.time() - start}")
                        continue
                    # Cache assumes JSON as input, so need to parse it
                    x += 1
                    try:
                        js = json.loads(l)
                    except:
                        print(f"Failed to parse JSON in {what}")                        
                        continue
                    try:
                        new = self.post_process_json(js)
                    except:
                        print(f"Failed to process {l}")
                        continue
                    if not what:
                        what = self.get_identifier_json(new)
                        if not what:
                            print(l)
                            raise NotImplementedError(f"is get_identifier_raw or _json implemented for {self.__class__.__name__}?")
                    self.out_cache[what] = new
                    if not x % 10000:
                        t = time.time() - start
                        xps = x/t
                        ttls = (self.total / (maxSlice+1)) / xps
                        print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
                        sys.stdout.flush()
        self.out_cache.commit()