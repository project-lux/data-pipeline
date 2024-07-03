import os
import sys
import time
from pipeline.config.config import Config
from dotenv import load_dotenv

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
cfgs.cache_globals()
cfgs.instantiate_all()

### STOP. You probably want to use MLCP instead

ml = cfgs.results['marklogic']['recordcache']
store = cfgs.marklogic_stores['sandbox']['store']

BATCH_SIZE = 200

if len(sys.argv) > 2:
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])
else:
    my_slice = 0
    max_slice = 1

batch = []
start = time.time()
for rec in ml.iter_records_slice(my_slice, max_slice):
    rec = rec['data']
    batch.append(rec)

    if len(batch) >= BATCH_SIZE:
        print(f"Batch built: {time.time()-start}")
        nstart = time.time()
        store.update_multiple(batch)
        print(f"Batch sent: {time.time()-nstart}")
        batch = []
        start = time.time()

if batch:
    store.update_multiple(batch)
