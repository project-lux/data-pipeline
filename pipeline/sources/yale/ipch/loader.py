import os
import shutil
import time
import ujson as json

from pipeline.process.base.loader import Loader

class IpchLoader(Loader):

    def __init__(self, config):
        self.in_path = config['dumpFilePath']
        self.out_cache = config['datacache']
        self.total = config.get('totalRecords', -1)

    def get_identifier_raw(self, line):
        # Find identifier from raw line
        return None

    def get_identifier_json(self, js):
        return js['id'].replace(self.namespace, '')

    def post_process_json(self, js):
        return js

    def filter_line(self, line):
        return False

    def load(self):
        # in is a single JSONL file

        start = time.time()
        fh = open(self.in_path)
        x = 0
        for l in fh.readlines():
            try:
                js = json.loads(l) 
            except Exception as e:
                print(f"Broken record: {e}\n{l}")
                continue   
            x += 1
            what = self.get_identifier_json(js)
            self.out_cache[what] = js
        fh.close()
        self.out_cache.commit()
