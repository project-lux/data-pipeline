
from pipeline.process.base.loader import Loader
import ujson as json
import gzip
import time
import os

class EthanLoader(Loader):

    def __init__(self, config):
        Loader.__init__(self, config)

    def get_identifier_json(self, js):
        # Should never actually get called, but for completeness...
        return js['id']

    def load(self, slicen=None, maxSlice=None):

        with open(self.in_path) as fh:
            js = json.load(fh)
            for item in js:
                what = self.get_identifier_json(item)
                self.out_cache.set(item, identifier=what)
