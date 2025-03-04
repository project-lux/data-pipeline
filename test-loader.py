import os
import sys
import json
import time
import datetime
import gzip
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.base.loader import Loader

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
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "json/record.json", "type": "json"}
            ]
        }
    },
    {
        "name": "json.gz",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "json/record.json.gz", "type": "json.gz"}
            ]
        }
    },
    {
        "name": "other",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "other/record.xml", "type": "other"}
            ]
        }
    },
    {
        "name": "zip_json",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "zip_json/records.zip", "type": "zip/json"}
            ]
        }
    },
    {
        "name": "tar",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "tar/records.tar", "type": "tar/json"}
            ]
        }
    },
    {
        "name": "tgz",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "tar/records.tgz", "type": "tar.gz/json"}
            ]
        }
    },
    {
        "name": "tgz_l",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "tar/records.tgz", "type": "tar.gz/lines/json"}
            ]
        }
    },
    {
        "name": "tbz",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "tar/records.tar.bz2", "type": "tar.bz2/json"}
            ]
        }
    },
    {
        "name": "jsonl",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "lines/records.jsonl", "type": "lines/json"}
            ]
        }
    },
    {
        "name": "jsonl.gz",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "lines/records.jsonl.gz", "type": "lines.gz/json"}
            ]
        }
    },
    {
        "name": "zip_jsonl",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "zip_json/records.zip", "type": "zip/lines/json"}
            ]
        }
    },
    {
        "name": "dir_zip_jsonl",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "zip_json", "type": "dir/zip/lines/json"}
            ]
        }
    },
    {
        "name": "dir_jsonl",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "dir", "type": "dir/lines/json"}
            ]
        }
    },
    {
        "name": "dir_json",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "dir", "type": "dir/json"}
            ]
        }
    },
    {
        "name": "array_json",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "array/records.json", "type": "array/raw"}
            ]
        }
    },
    {
        "name": "dict_json",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "dict/records.json", "type": "dict/raw"}
            ]
        }
    },
    {
        "name": "tgz_lgz",
        "totalRecords": 1,
        "input_files": {
            "records": [
                {"path": base + "tar/records-gz.tar.gz", "type": "tar.gz/lines.gz/json"}
            ]
        }
    },
]

for lc in lcfgs:
    lc['all_configs'] = cfgs
    ldr = Loader(lc)
    ldr.prepare_load()
    ldr.load()
    assert(len(ldr.out_cache) == 1)


mcfg = {
    "name": "jsonl",
    'all_configs': cfgs,
    "totalRecords": 20,
    "input_files": {
        "records": [
            {"path": base + "multi/records.jsonl", "type": "lines/json"}
        ]
    }
}

ldr = Loader(mcfg)
ldr.prepare_load(0, 5)
ldr.load()
assert(len(ldr.out_cache) == 4)
assert('5' in ldr.out_cache)
