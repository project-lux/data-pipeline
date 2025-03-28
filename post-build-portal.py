import os
import sys
from dotenv import load_dotenv
from pipeline.config import Config
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from pipeline.storage.cache.postgres import PoolManager

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

# Strategy:  Iterate the YPM recordcache2 records
# and then walk the merged equivalents to find referenced records
# record them all as YPM portal required

TQDM_DISABLE = "--no-tqdm" in sys.argv

def walk_for_refs(node, distance, distances, added, top=False):
    if not top and "id" in node and not node["id"].startswith("_"):
        yuid = node['id'].rsplit('/', 1)[-1]
        dist = distances.get(yuid, 100)
        if distance < dist:
            distances[yuid] = distance
        if dist == 100:
            added[yuid] = distance

    for k, v in node.items():
        if k in ["equivalent", "access_point", "conforms_to"]:
            continue
        if type(v) == list:
            for vi in v:
                if type(vi) == dict:
                    walk_for_refs(vi, distance, distances, added)
        elif type(v) == dict:
            walk_for_refs(v, distance, distances, added)

def process_recids(recids, thr):
    pbar = tqdm(total=len(recids),
                desc=f'Process {thr}',
                position=thr,  # Position the progress bar based on process number
                leave=True,
                disable=TQDM_DISABLE)
    merged = cfgs.results['merged']['recordcache']
    local_dists = {}
    local_added = {}
    for recid in recids:
        try:
            rec = merged[recid]
            if not rec:
                continue
            walk_for_refs(rec['data'], 1, local_dists, local_added, top=True)
        except KeyError:
            missing[k] = 1
            print(f"missing: {recid}")
        pbar.update(1)
    pbar.close()
    return [local_dists, local_added]

def set_portal(portal_ids, portal, thr):
    pbar = tqdm(total=len(portal_ids),
                desc=f'Process {thr}',
                position=thr,  # Position the progress bar based on process number
                leave=True,
                disable=TQDM_DISABLE)
    merged = cfgs.results['merged']['recordcache']
    done = 0
    for recid in portal_ids:
        try:
            merged.set_metadata(recid, 'change', portal)
            pbar.update(1)
            done += 1
        except:
            print(f"Failed to set metadata on {recid}")
    pbar.close()
    return done

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    procs = multiprocessing.cpu_count() * 2 // 3


    src = cfgs.internal['ypm']
    rc = src['recordcache2']

    all_distances = {}
    print("Keys...")
    # populate all at distance 0
    for k in tqdm(rc.iter_keys(), total=len(rc), disable=TQDM_DISABLE):
        # k is the uuid
        all_distances[k] = 0

    added_refs = {}
    missing = {}
    recids = list(all_distances.keys())

    print(f"dist=0, {procs} processes, {len(recids)} keys")
    future_dists = {}
    with ProcessPoolExecutor(max_workers=procs) as executor:  # Uses processes instead of threads
        chunk_size = len(recids) // procs
        futures = []
        for x in range(procs):
            start_idx = x * chunk_size
            end_idx = start_idx + chunk_size if x < procs - 1 else len(recids)
            future = executor.submit(process_recids, recids[start_idx:end_idx], x)
            future_dists[future] = x
        for future in as_completed(future_dists):
            (local_dists, local_added) = future.result()
            all_distances.update(local_dists)
            added_refs.update(local_added)

    print(f"Added refs: {len(added_refs)} base")
    pbar = tqdm(total=len(added_refs), desc="Referenced", disable=TQDM_DISABLE)
    merged = cfgs.results['merged']['recordcache']
    done = 0
    while added_refs:
        (k,d) = added_refs.popitem()
        done += 1
        if d > 4:
            continue
        rec = merged[k]
        if not rec:
            continue
        walk_for_refs(rec['data'], d+1, all_distances, added_refs, top=True)
        pbar.update(1)
        pbar.total = done + len(added_refs)

    print(f"Setting metadata: {procs} processes, {len(all_distances)}")
    portal_ids = list(all_distances.keys())
    future_sets = {}
    with ProcessPoolExecutor(max_workers=procs) as executor:  # Uses processes instead of threads
        chunk_size = len(portal_ids) // procs
        futures = []
        for x in range(procs):
            start_idx = x * chunk_size
            end_idx = start_idx + chunk_size if x < procs - 1 else len(portal_ids)
            future = executor.submit(set_portal, portal_ids[start_idx:end_idx], 'ypm', x)
            future_sets[future] = x
        for future in as_completed(future_sets):
            done = future.result()

