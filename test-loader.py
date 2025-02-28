import os
import sys
import json
import time
import datetime
import gzip
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.base.loader import NewLoader

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

base = "/Users/rs2668/Development/lux/data-pipeline/tests/loader_tests/"


lcfgs = [
    {
        "name": "json",
        "input_files": {
            "records": [
                {"path": base + "json/record.json", "type": "json"}
            ]
        }
    },
    {
        "name": "other",
        "input_files": {
            "records": [
                {"path": base + "other/record.xml", "type": "other"}
            ]
        }
    },
    {
        "name": "zip_json",
        "input_files": {
            "records": [
                {"path": base + "zip_json/records.zip", "type": "zip/json"}
            ]
        }
    },
    {
        "name": "tar",
        "input_files": {
            "records": [
                {"path": base + "tar/records.tar", "type": "tar/json"}
            ]
        }
    },
    {
        "name": "tgz",
        "input_files": {
            "records": [
                {"path": base + "tar/records.tgz", "type": "tar.gz/json"}
            ]
        }
    },
    {
        "name": "tbz",
        "input_files": {
            "records": [
                {"path": base + "tar/records.tar.bz2", "type": "tar.bz2/json"}
            ]
        }
    },
    {
        "name": "jsonl",
        "input_files": {
            "records": [
                {"path": base + "lines/records.jsonl", "type": "lines/json"}
            ]
        }
    },
    {
        "name": "jsonl.gz",
        "input_files": {
            "records": [
                {"path": base + "lines/records.jsonl.gz", "type": "lines.gz/json"}
            ]
        }
    },
    {
        "name": "zip_jsonl",
        "input_files": {
            "records": [
                {"path": base + "zip_json/records.zip", "type": "zip/lines/json"}
            ]
        }
    },
    {
        "name": "dir_zip_jsonl",
        "input_files": {
            "records": [
                {"path": base + "zip_json", "type": "dir/zip/lines/json"}
            ]
        }
    }
]

for lc in lcfgs:
    ldr = NewLoader(lc)
    ldr.prepare_for_load()
    ldr.load()
    assert(len(ldr.out_cache) == 1)

