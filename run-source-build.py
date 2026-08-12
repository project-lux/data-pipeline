import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

# --- process command line arguments ---

my_slice = -1
max_slice = -1

if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])

if my_slice > -1:
    # Running in parallel, will cause cross-process errors
    idmap.disable_memory_cache()
else:
    # Running single, memory cache will remain accurate
    idmap.enable_memory_cache()

print("Starting...")
print(f"Update token is: {idmap.update_token}")
sys.stdout.flush()

sources = ['aat', 'ulan', 'lcsh']

for source in sources:
    src = cfgs.external[source]
    print(f" *** {source} ***")
    sys.stdout.flush()
    in_db = src["datacache"]
    #acq = src['acquirer']
    out_db = src['recordcache']
    mapper = src["mapper"]

    out_db.defer_commits(every=1000)
    for rec in in_db.iter_records_slice(my_slice, max_slice):
        rec2 = mapper.transform(rec, None)
        if rec2 is not None:
            idq = cfgs.make_qua(rec2['identifier'], rec2['data']['type'])
            rec2['identifier'] = idq
            out_db[idq] = rec2
            out_db.checkpoint()
    out_db.flush()
