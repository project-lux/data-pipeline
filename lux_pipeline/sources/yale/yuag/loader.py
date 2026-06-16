import os
import shutil
import time
import ujson as json
import pathlib
import zipfile

from lux_pipeline.process.base.loader import Loader, Pointer

class YuagLoader(Loader):

    def make_identifier(self, value):

        if isinstance(value, Pointer):
            value = value.get_name()
        value = value.replace("home/ec2-user/reconciliation/data/ycba/linked_art/", "")
        value = value.replace(self.config["namespace"], "")
        return value

    def extract_identifier(self, data):
        return data['id'].replace('https://media.art.yale.edu/content/lux/', '')
