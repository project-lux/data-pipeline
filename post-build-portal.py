import os
import sys
from dotenv import load_dotenv
from pipeline.config import Config
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing
import time
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

src = cfgs.internal['ypm']
rc = src['recordcache2']


all_distances = {}
print("Keys...")
# populate all at distance 0
for k in tqdm(rc.iter_keys(), total=len(rc)):
    # k is the uuid
    all_distances[k] = 0


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


added_refs = {}
missing = {}

def process_recids(recids, thr):
    pbar = tqdm(total=len(recids),
                desc=f'Process {thr}',
                position=thr,  # Position the progress bar based on process number
                leave=True)
    merged = cfgs.results['merged']['recordcache']
    merged.make_threadsafe()
    local_dists = {}
    local_added = {}
    start = time.time()
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


recids = list(all_distances.keys())
procs = 24
print(f"dist=0, {procs} processes, {len(recids)} keys")
future_dists = {}
with ProcessPoolExecutor(max_workers=procs) as executor:  # Uses processes instead of threads
    gstart = time.time()
    chunk_size = len(recids) // procs
    futures = []

    for x in range(procs):
        start_idx = x * chunk_size
        end_idx = start_idx + chunk_size if x < procs - 1 else len(recids)
        future = executor.submit(process_recids, recids[start_idx:end_idx], x)
        future_dists[future] = x

    for future in concurrent.futures.as_completed(future_dists):
        (local_dists, local_added) = future.result()
        all_distances.update(local_dists)
        added_refs.update(local_added)
    gdurn = int(time.time() - gstart)

time.sleep(1)

print("Added refs")
pbar = tqdm(total=len(added_refs), desc="Referenced")
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

portal_ids = list(all_distances.keys())
for uu in tqdm(portal_ids):
    merged[uu].set_metadata('change', 'ypm')
