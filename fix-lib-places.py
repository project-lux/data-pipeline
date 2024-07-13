import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.reconciler import Reconciler
from pipeline.process.reference_manager import ReferenceManager
from pipeline.storage.cache.postgres import poolman

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
networkmap = cfgs.instantiate_map("networkmap")["store"]
cfgs.cache_globals()
cfgs.instantiate_all()

# --- process command line arguments ---

DO_REFERENCES = False
to_do = [("ils", cfgs.internal["ils"])]
recids = []

# read recids in from json file
fh = open("../data/files/lib_enhance_places.json")
data = fh.read()
fh.close()
place_js = json.loads(data)
recids = list(place_js.keys())
del data
del place_js

# --- set up environment ---
reconciler = Reconciler(cfgs, idmap, networkmap)
ref_mgr = ReferenceManager(cfgs, idmap)
debug = cfgs.debug_reconciliation

idmap.enable_memory_cache()

print("Starting...")
print(f"Update token is: {idmap.update_token}")

sys.stdout.flush()

for name, cfg in to_do:
    print(f" *** {name} ***")
    sys.stdout.flush()
    in_db = cfg["datacache"]
    mapper = cfg["mapper"]
    acquirer = cfg["acquirer"]

    for recid in recids:
        sys.stdout.write(".")
        sys.stdout.flush()
        # Acquire the record from cache or network
        if acquirer.returns_multiple():
            recs = acquirer.acquire_all(recid)
        else:
            rec = acquirer.acquire(recid)
            if rec is not None:
                recs = [rec]
            else:
                recs = []
        if not recs:
            print(f" *** Failed to acquire any record for {name}/{recid} ***")
        for rec in recs:
            # Reconcile it
            rec2 = reconciler.reconcile(rec)
            # Do any post-reconciliation clean up
            mapper.post_reconcile(rec2)
            # XXX Shouldn't this be stored somewhere after reconciliation?

            # Find references from the record
            ref_mgr.walk_top_for_refs(rec2["data"], 0)
            # Manage identifiers for rec now we've reconciled and collected
            ref_mgr.manage_identifiers(rec2)
    recids = []

# final tidy up
ref_mgr.write_metatypes(my_slice)
# force all postgres connections to close
poolman.put_all("localsocket")
