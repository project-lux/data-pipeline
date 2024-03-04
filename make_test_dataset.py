import os
import sys
import json
import time
import random
import shutil
import datetime
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.storage.cache.filesystem import FsCache

MAX_REFS = 10

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
record_refs = cfgs.instantiate_map('record_refs')['store']
cfgs.cache_globals()
cfgs.instantiate_all()
merged = cfgs.results['merged']['recordcache']

# e.g. python ./make_test_dataset.py test1 yuid:person/c225d5e4-8767-4a25-802b-af054d5e8f52

INCLUDE_REFS = False
ONLY_LISTED = False
NEW_IDMAP_TOK = False
SET_DEBUG = False

if '--references' in sys.argv:
    # include records that reference this one, from references table
    INCLUDE_REFS = True
    sys.argv.remove('--references')

if '--only-listed' in sys.argv:
    # include records that reference this one, from references table
    ONLY_LISTED = True
    sys.argv.remove('--only-listed')

if '--new-idmap-token' in sys.argv:
    NEW_IDMAP_TOK = True
    sys.argv.remove('--new-idmap-token')

if '--set-debug' in sys.argv:
    SET_DEBUG = True
    sys.argv.remove('--set-debug')

# other arguments are a list of recids to include, of the form `source:recid`
# expectation is to run every component, so only need input, not 
# generated data.
if len(sys.argv) < 3:
    print("Usage: make_test_dataset [--references] dataset-name recid-list")
    sys.exit()


def walk_for_refs(node, refs, top=False):
    # Test if we need to record the node

    if not top and 'id' in node and not node['id'].startswith('_'):
        refs[node['id']] = 1

    for (k,v) in node.items():
        if k in ['equivalent', 'access_point', 'conforms_to']:
            continue
        if type(v) == list:
            for vi in v:
                if type(vi) == dict:
                    walk_for_refs(vi, refs)
        elif type(v) == dict:
            walk_for_refs(v, refs)


test_name = sys.argv[1]
tests_base = cfgs.tests_dir
test_dir = os.path.join(tests_base, test_name)
if os.path.exists(test_dir):
    print(f"Test directory already exists ({test_dir}), aborting")
    sys.exit()

recids = sys.argv[2:]
recids.extend(cfgs.globals.values())

idmap2 = {}
record_list = {}
cfgs_needed = {}

print("Collecting Records")
for recid in recids:
    if recid.startswith('https://lux.collections.yale.edu/data/'):
        recid = recid.replace('https://lux.collections.yale.edu/data/', 'yuid:')
    try:
        (src, recid) = cfgs.split_curie(recid)
    except TypeError:
        try:
            (src, recid) = cfgs.split_uri(recid)
        except:
            # couldn't split. Abort
            print(f"Couldn't split {recid} from input, aborting")
            sys.exit()

    if not src['name'] in cfgs.results:
        yuid = idmap[f"{src['namespace']}{recid}"]
    else:
        yuid = f"{src['namespace']}{recid}"
    inputs = idmap[yuid]
    idmap2[yuid] = list(inputs)

    for i in inputs:
        if i.startswith("__") and i.endswith("__"):
            # internal token
            continue
        i = i.split("##qua")[0]
        (src2, id2) = cfgs.split_uri(i)
        record_list[f"{src2['name']}:{id2}"] = (src2, id2)

    # Now walk referred to records on the result

    rec = merged[yuid.split('/')[-1]]
    refs = {}
    if not ONLY_LISTED:
        walk_for_refs(rec['data'], refs, top=True)
        for ref in refs.keys():
            # YUIDs
            try:
                inputs = idmap[ref]
            except:
                continue
            idmap2[ref] = list(inputs)
            for i in inputs:
                if i.startswith("__") and i.endswith("__"):
                    # internal token
                    continue
                i = i.split("##qua")[0]
                (src2, id2) = cfgs.split_uri(i)
                record_list[f"{src2['name']}:{id2}"] = (src2, id2)

        if INCLUDE_REFS:
            # And add a selection of referring records
            refs = record_refs[yuid.split('/')[-1]]
            if len(refs) > MAX_REFS:
                # just UUIDs, not full URIs :(
                refs = random.sample(refs, MAX_REFS)
                for r in refs:
                    rrec = merged[r]
                    ry = rrec['data']['id']
                    inputs = idmap[ry]
                    idmap2[ry] = list(inputs)
                    for i in inputs:
                        if i.startswith("__") and i.endswith("__"):
                            # internal token
                            continue
                        i = i.split("##qua")[0]
                        (src2, id2) = cfgs.split_uri(i)
                        record_list[f"{src2['name']}:{id2}"] = (src2, id2)

if not record_list:
    print("No records to process, aborting")
    sys.exit()

print("Setting up directory structure")
# Make test directory structure to receive files
os.mkdir(test_dir)
cache_dir = os.path.join(test_dir, 'caches')
os.mkdir(cache_dir)
indexes_dir = os.path.join(test_dir, 'indexes')
os.mkdir(indexes_dir)
maps_dir = os.path.join(test_dir, 'maps')
os.mkdir(maps_dir)

sames = cfgs.instantiate_map('equivalents')['store']
diffs = cfgs.instantiate_map('distinct')['store']

id_rlrs = {}
name_rlrs = {}
same_vals = {}
diff_vals = {}

rclrs = []
for src in cfgs.external.values():
    rlr = src.get('reconciler', None)
    if rlr:
        rclrs.append(rlr)
        if rlr.id_index:
            id_rlrs[src['name']] = {}
        if rlr.name_index:
            name_rlrs[src['name']] = {}
for src in cfgs.results.values():
    rlr = src.get('reconciler', None)
    if rlr:
        rclrs.append(rlr)
        if hasattr(rlr, 'id_index') and rlr.id_index:
            id_rlrs[src['name']] = {}
        if hasattr(rlr, 'name_index') and rlr.name_index:
            name_rlrs[src['name']] = {}

print("Writing data")
# Make json files on disk
for (source, identifier) in record_list.values():
    fscfg = {"name": source['name'], "base_dir": cache_dir, "tabletype": "data_cache", "source": source['name']}
    cache = FsCache(fscfg)
    rec = source['datacache'][identifier]
    if not rec and source['name'] == 'aspace':
        # :( try ils
        source = cfgs.internal['ils']
        rec = source['datacache'][identifier]
    elif not rec and source['name'] == 'ils':
        source = cfgs.internal['aspace']
        rec = source['datacache'][identifier]
    if not rec:
        print(f"Couldn't find record for {source['name']}:{identifier}; skipping")
        continue
    cache[identifier] = rec['data']

    # extract sames, diffs, labels
    # ... from mapped record
    if rclrs:
        rec2 = source['mapper'].transform(rec, None)
        if not rec2:
            continue
        try:
            equivs = rclrs[0].extract_uris(rec2['data'])
            names = rclrs[0].extract_names(rec2['data'])
        except:
            continue

        for rlr in rclrs:
            if hasattr(rlr, 'id_index') and rlr.id_index:
                for eq in equivs:
                    if eq in rlr.id_index:
                        id_rlrs[rlr.config['name']][eq] = rlr.id_index[eq]
            if hasattr(rlr, 'name_index') and rlr.name_index:
                for nm in names:
                    if nm in rlr.name_index:
                        name_rlrs[rlr.config['name']][nm] = rlr.name_index[nm]

        for eq in equivs:
            # sames
            if eq in sames:
                same_vals[eq] = list(sames[eq])
            # diffs    
            if eq in diffs:
                diff_vals[eq] = list(diffs[eq])

print("Writing maps, indexes")
# Write out maps and indexes, as constructed

# First expand idmap values
tmp_map = {}
for (k,v) in idmap2.items():
    for v2 in v:
        tmp_map[v2] = k
idmap2.update(tmp_map)

for (map_name, vals) in [('idmap', idmap2), ('sameAs', same_vals), ('differentFrom', diff_vals)]:
    if vals:
        outstr = json.dumps(vals)
        fn = os.path.join(maps_dir, f"{map_name}.json")
        fh = open(fn, 'w')
        fh.write(outstr)
        fh.close()

for (nm, vals) in id_rlrs.items():
    if vals:
        outstr = json.dumps(vals)
        fn = os.path.join(indexes_dir, f"{nm}_id.json")
        fh = open(fn, 'w')
        fh.write(outstr)
        fh.close()

for (nm, vals) in name_rlrs.items():
    if vals:
        outstr = json.dumps(vals)
        fn = os.path.join(indexes_dir, f"{nm}_name.json")
        fh = open(fn, 'w')
        fh.write(outstr)
        fh.close()

print("Writing configs")
# And now set up environment for test
cfg_dir = os.path.join(test_dir, 'config')
os.mkdir(cfg_dir)
cfgc_dir = os.path.join(cfg_dir, 'config_cache')
os.mkdir(cfgc_dir)
os.mkdir(os.path.join(test_dir, "logs"))
data_dir = os.path.join(test_dir, "data")
os.mkdir(data_dir)
os.mkdir(os.path.join(test_dir, "exports"))

fh = open(os.path.join(test_dir,'.env'), 'w')
fh.write('LUX_BASEPATH="config"\n')
fh.close()

bootstrap = {
    "name": "config",
    "datacacheClass": "storage.cache.filesystem.FsCache",
    "base_dir": cfg_dir,
    "tabletype": "cache"
}
new_cfgs = FsCache(bootstrap)

for rec in cfgs.configcache.iter_records():
    cfg = rec['data']
    if cfg['type'] == 'base':
        cfg['base_dir'] = test_dir
        cfg['indexes_dir'] = "indexes"
        cfg['exports_dir'] = "exports"
        cfg['data_dir'] = "data"
        cfg['log_dir'] = "logs"
        cfg['max_distance'] = 0 if ONLY_LISTED else 1
        cfg['allow_network'] = False
        cfg["debug_reconciliation"] = SET_DEBUG

    elif cfg['type'] in ['internal', 'external', 'results']:
        cfg['base_dir'] = cache_dir
        cfg['datacacheClass'] = "storage.cache.filesystem.DataCache"
        if cfg['type'] == 'results':
            cfg['recordcacheClass'] = "storage.cache.filesystem.MergedRecordCache"
        else:
            cfg['recordcacheClass'] = "storage.cache.filesystem.RecordCache"
        cfg['recordcacheReconciledClass'] = "storage.cache.filesystem.ReconciledRecordCache"
        cfg['recordcache2Class'] = "storage.cache.filesystem.MergedRecordCache"
    elif cfg['type'] == 'map':
        cfg['storeClass'] = "storage.idmap.filesystem.IdMap"
        cfg['base_dir'] = maps_dir
    elif cfg['type'] == "marklogic":
        continue
    elif cfg['type'] == 'globals':
        # no need to change, just copy over
        pass
    elif cfg['type'] == 'caches':
        # rewrite to ensure no connection to real caches
        cfg = {"name": "_caches", "type": "caches", "base_dir": test_dir}
    else:
        print(f"Unhandled config type: {cfg['type']}")
        # Skip in case it would break things
        continue
    new_cfgs[rec['identifier']] = cfg

shutil.copytree(os.path.join(cfgs.data_dir, 'schema'), os.path.join(data_dir, 'schema'))
for fn in os.listdir(cfgs.data_dir):
    if fn.endswith('.json') or fn.endswith('.csv'):
        shutil.copyfile(os.path.join(cfgs.data_dir, fn), os.path.join(data_dir, fn))

if not NEW_IDMAP_TOK:
    shutil.copyfile(os.path.join(cfgs.data_dir, 'idmap_update_token.txt'), os.path.join(data_dir, 'idmap_update_token.txt'))
else:
    t = datetime.date.today()
    tok = t.isoformat().replace('-','')
    fh = open(os.path.join(data_dir, 'idmap_update_token.txt'), 'w')
    fh.write(f'__{tok}__')
    fh.close()

files = ['run-integrated.py', 'run-merge2.py', 'run-export.py', 'run-all.sh']
for fn in files:
    shutil.copyfile(fn, os.path.join(test_dir, fn))
