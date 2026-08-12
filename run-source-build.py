import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.reconciler import Reconciler
from pipeline.process.reference_manager import ReferenceManager
from pipeline.process.identity_resolver import IdentityResolver
from pipeline.storage.cache.postgres import PoolManager

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

# --- process command line arguments ---

my_slice = -1
max_slice = -1


if my_slice > -1:
    # Running in parallel, will cause cross-process errors
    idmap.disable_memory_cache()
else:
    # Running single, memory cache will remain accurate
    idmap.enable_memory_cache()

print("Starting...")
print(f"Update token is: {idmap.update_token}")
sys.stdout.flush()

sources = ['aat', 'ulan', 'tgn', 'lcsh']

for source in sources:
    src = cfgs.external[source]

for name, cfg, recids in to_do:
    print(f" *** {name} ***")
    sys.stdout.flush()
    in_db = cfg["datacache"]
    out_db = cfg['recordcache']
    mapper = cfg["mapper"]

    out_db.defer_commits(every=1000)

    for rec in in_db.iter_records_slice(my_slice, max_slice):
        rec2 = mapper.transform(rec)
        out_db.store_record(rec2)
        out_db.checkpoint()
    out_db.flush()
