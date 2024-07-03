import os
import sys
import time
import datetime
from pipeline.config import Config
from dotenv import load_dotenv

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
cfgs.cache_globals()
cfgs.instantiate_all()

### STOP. You probably want to use MLCP instead

ml = cfgs.results['marklogic']['recordcache']
store = cfgs.marklogic_stores['ml_sandbox']['store']


total = ml.len_estimate()
BATCH_SIZE = 200

if len(sys.argv) > 2:
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])
else:
    my_slice = 0
    max_slice = 1

to_do = int(total / max_slice)
done = 0
curr_amt = 0.0
prev_pct = 0

batch = []
start = time.time()
for rec in ml.iter_records_slice(my_slice, max_slice):
    rec = rec['data']
    batch.append(rec)

    if len(batch) >= BATCH_SIZE:
        store.update_multiple(batch)
        done += len(batch)
        # spit out every 1% like mlcp does
        if done / to_do > curr_amt:
            ct = time.time()
            durn = ct - start
            now = datetime.datetime.utcnow().isoformat()
            if prev_pct:
                diff = int(ct - prev_pct)
            else:
                diff = 0
            persec = done / durn
            print(f"[{now}] {done}/{to_do} = {curr_amt * 100}% last:{diff} per sec: {persec}")
            curr_amt += 0.005
        batch = []

if batch:
    store.update_multiple(batch)
