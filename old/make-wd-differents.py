import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
cfg = cfgs.instantiate('wikidata', 'external')
wd = cfg['datacache']

with open('wikidata_differents.csv', 'w') as dfh:
    print("Starting...")
    x = 0
    ttl = 100000000
    start = time.time()
    for rec in wd.iter_records():
        x += 1
        if 'P1889' in rec['data']:
            me = rec['identifier']
            for diff in rec['data']['P1889']:
                dfh.write(f"http://www.wikidata.org/entity/{me},http://www.wikidata.org/entity/{diff}\n")
        if not x % 1000000:
            secs = time.time() - start
            print(f"{x} in {secs} = {x/secs}/sec")
