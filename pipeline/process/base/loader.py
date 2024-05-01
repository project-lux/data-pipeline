
import os
import requests
import shutil
import time
import gzip
import zipfile
import ujson as json

class Loader(object):

    def __init__(self, config):
        self.config = config
        self.in_url = config.get('remoteDumpFile', '')
        self.in_path = config['dumpFilePath']
        self.out_cache = config['datacache']
        self.total = config.get('totalRecords', -1)

    def get_identifier_raw(self, line):
        # Find identifier from raw line
        return None

    def get_identifier_json(self, js):
        return None

    def post_process_json(self, js):
        return js

    def filter_line(self, line):
        return False

    def load(self):
        # default is to assume gzipped JSONL
        # without headers/footers or other wrapping
        # Dump in raw without parsing

        if self.in_path.endswith('.gz'):
            fh = gzip.open(self.in_path)
        elif self.in_path.endswith('.zip'):
            zh = zipfile.ZipFile(self.in_path)
            # Assume a single zipped file
            names = zh.namelist()
            if len(names) != 1:
                raise ValueError("Too many zipped files")
            else:
                fh = zh.open(names[0])

        start = time.time()
        x = 0 
        done_x = 0
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
            try:
                js = json.loads(l)
            except:
                raise    
            x += 1
            try:
                new = self.post_process_json(js)
            except:
                print(f"Failed to process {l}")
                raise
            if new is not None:
                if not what:
                    what = self.get_identifier_json(new)
                    if not what:
                        print(l)
                        raise NotImplementedError(f"is get_identifier_raw or _json implemented for {self.__class__.__name__}?")
                self.out_cache[what] = new
            if not x % 10000:
                t = time.time() - start
                xps = x/t
                ttls = self.total / xps
                print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
        fh.close()
        self.out_cache.commit()