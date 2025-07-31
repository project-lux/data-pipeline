import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config

from rich import pretty, print, inspect

pretty.install()

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()


wd = cfgs.external["wikidata"]["datacache"]

outh = open("wikidata_wikipedia_en.jsonl", "w")
index = {}

for rec in wd.iter_records():
    data = rec["data"]
    if "sitelinks" in data and "enwiki" in data["sitelinks"]:
        wp = data["sitelinks"]["enwiki"]["title"]
        outh.write(json.dumps(data))
        outh.write("\n")
        index[wp] = data["id"]
outh.close()

idx = open("wd_wp.json", "w")
json.dump(index, idx)
idx.close()
