
import os
import time
import gzip
import tarfile
import ujson as json
import zipfile

from lux_pipeline.process.base.loader import Loader, Pointer

class YpmLoader(Loader):

    def make_identifier(self, value):

        if isinstance(value, Pointer):
            value = value.get_name()
        value = value.replace(self.config["namespace"], "")
        return value

    def extract_identifier(self, data):
        return data['id'].replace(self.config["namespace"], '')
