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
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
networkmap = cfgs.instantiate_map('networkmap')['store']
cfgs.cache_globals()
cfgs.instantiate_all()

# --- process command line arguments ---

my_slice = -1
max_slice = -1
 
# NOTE: This implies the existence and use of AAT
if '--baseline' in sys.argv:
    # Only do global terms. Only needed with a new idmap
    to_do = [("aat", cfgs.external['aat'])]
    recids = list(cfgs.globals_cfg.values())
    lngs = cfgs.external['aat']['mapper'].process_langs.values()
    recids.extend([l.id.replace('http://vocab.getty.edu/aat/', '') for l in lngs])

else:
    recids = []
    if '--all' in sys.argv:
        to_do = list(cfgs.internal.items())      
    else:
        to_do = []
        for src, cfg in cfgs.internal.items():
            if f"--{src}" in sys.argv:
                to_do.append((src, cfg))
        for src, cfg in cfgs.external.items():
            if f"--{src}" in sys.argv:
                to_do.append((src, cfg))

    while '--recid' in sys.argv:
        idx = sys.argv.index('--recid')
        recid = sys.argv[idx+1]
        recids.append(recid)
        sys.argv.pop(idx)
        sys.argv.pop(idx)

    if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
        my_slice = int(sys.argv[1])
        max_slice = int(sys.argv[2])

# --- set up environment ---

reconciler = Reconciler(cfgs, idmap, networkmap)
ref_mgr = ReferenceManager(cfgs, idmap)
debug = cfgs.debug_reconciliation

print("Starting...")

for name, cfg in to_do:
    print(f" *** {name} ***")
    in_db = cfg['datacache']
    mapper = cfg['mapper']
    acquirer = cfg['acquirer']

    if not recids:
        if my_slice > -1:
            recids = in_db.iter_keys_slice(my_slice, max_slice)
        else:
            recids = in_db.iter_keys()

    for recid in recids:
        sys.stdout.write('.');sys.stdout.flush()
        # Acquire the record from cache or network
        rec = acquirer.acquire(recid)
        if rec is not None:
            # Reconcile it
            rec2 = reconciler.reconcile(rec)
            # Do any post-reconciliation clean up
            mapper.post_reconcile(rec2)
            # XXX Shouldn't this be stored somewhere after reconciliation?

            # Find references from the record
            ref_mgr.walk_top_for_refs(rec['data'], 0)

            # Manage identifiers for rec now we've reconciled and collected
            ref_mgr.manage_identifiers(rec, rebuild=True)
        else:
            print(f"*** Failed to acquire an internal record: {name}/{recid} ***")
    recids = []

# now do references

print("\nProcessing References...")

item = 1
while item:
    # Item is uri, {dist, type} or None
    item = ref_mgr.pop_ref()
    try:
        (uri, dct) = item
        distance = dct['dist']
    except:
        continue
    try:
        maptype = dct['type']
    except:
        continue
    if distance > cfgs.max_distance:
        continue

    ref_mgr.did_ref(uri, distance)

    if cfgs.is_qua(uri):
        uri, rectype = cfgs.split_qua(uri)
    else:
        raise ValueError(f"No qua in referenced {uri} and needed")
    try:
        (source, recid) = cfgs.split_uri(uri)
    except:
        if debug: print(f"Not processing: {uri}")
        continue
    if not source['type'] == 'external':
        # Don't process internal or results
        continue

    # put back the qua to the id after splitting/canonicalizing in split_uri
    mapper = source['mapper']
    acquirer = source['acquirer']

    # Acquire the record from cache or network
    rec = acquirer.acquire(recid, rectype=rectype)
    if rec is not None:
        sys.stdout.write('.');sys.stdout.flush()
        # Reconcile it
        rec2 = reconciler.reconcile(rec)
        # Do any post-reconciliation clean up
        mapper.post_reconcile(rec2)
        # XXX Shouldn't this be stored somewhere after reconciliation?

        # Find references from this record
        ref_mgr.walk_top_for_refs(rec['data'], distance)
        # Manage identifiers for rec now we've reconciled and collected
        ref_mgr.manage_identifiers(rec, rebuild=True)
    else:
        print(f"Failed to acquire reference: {source['name']}/{recid}")    

# final tidy up
ref_mgr.write_metatypes(my_slice)
# force all postgres connections to close
poolman.put_all('localsocket')

# Report Status for orchestration
if my_slice > -1:
    fn = os.path.join(cfgs.log_dir, "flags", f"reconcile_is_done-{my_slice}.txt")
    fh = open(fn, 'w')
    fh.write("1")
    fh.close()
