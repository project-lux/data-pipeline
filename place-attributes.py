import os
import sys
from dotenv import load_dotenv
from pipeline.config import Config
from cromulent.model import Place

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

wd_data = cfgs.external["wikidata"]["datacache"]

attrib_counts = {}


if os.path.exists("testing.tsv"):
    fh = open("testing.tsv")
    for l in fh.readlines():
        info = l.split("\t")
        okay = info[7]
        if not okay in ["NOT PLACE", "WD invalid"]:
            # fetch from datacache, hash the properties
            wd = okay[2].split("/")[-1]
            data = wd_data[wd]
            for k in data["data"].keys():
                if k.startswith("P"):
                    try:
                        attrib_counts[k] += 1
                    except:
                        attrib_counts[k] = 1
            if "P31" in data["data"]:
                for p in data["data"]["P31"]:
                    try:
                        attrib_counts[f"P31_{p}"] += 1
                    except:
                        attrib_counts[f"P31_{p}"] = 1
    fh.close()
