import os
import sys
from dotenv import load_dotenv
from pipeline.config import Config
import concurrent.futures
import time

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
merged = cfgs.results['merged']['recordcache']


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

all_distances = {}
added_refs = {}
missing = {}

x = 0
ttl = len(rc)

print("Keys...")
# populate all at distance 0
for k in rc.iter_keys():
    # k is the uuid
    all_distances[k] = 0
    x += 1
    if not x % 250000:
        print(f"{x}/{ttl}")
        break


def process_recids(recids, thr):
    local_dists = {}
    local_added = {}
    x = 0
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
        x += 1
        if not x % 10000:
            durn = int(time.time() - start)
            print(f"Thread {thr} at {x} after {durn}")
    return [local_dists, local_added]


recids = list(all_distances.keys())

print(f"dist=0, 20 threads, {len(recids)} keys")
future_dists = {}
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    gstart = time.time()
    for x in range(20):
        future = executor.submit(process_recids, recids[x::20], x)
        future_dists[future] = x
    for future in concurrent.futures.as_completed(future_dists):
        (local_dists, local_added) = future.result()
        all_distances.update(local_dists)
        added_refs.update(local_added)
    gdurn = int(time.time() - gstart)

# print("dist=0")
# x = 0
# # populate all distance 1 references into all_distances
# for k in list(all_distances.keys()):
#     try:
#         rec = merged[k]
#         if not rec:
#             continue
#         walk_for_refs(rec['data'], 1, all_distances, added_refs, top=True)
#     except KeyError:
#         missing[k] = 1
#         print(f"missing: {k}")
#     x += 1
#     if not x % 50000:
#         print(f"{x}/{ttl} - {len(added_refs)} new")

print("Added refs")
x = 0
while added_refs:
    (k,d) = added_refs.popitem()
    if d > 4:
        continue
    rec = merged[k]
    if not rec:
        continue
    walk_for_refs(rec['data'], d+1, top=True)
    x += 1
    if not x % 50000:
        print(f"{x}/{ttl} - {len(added_refs)} remaining")
