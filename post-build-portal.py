import os
import sys
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()


src = cfgs.internal['ypm']
rc = src['recordcache2']
merged = cfgs.results['merged']['recordcache']

def walk_for_refs(node, distance, top=False):

    if not top and "id" in node and not node["id"].startswith("_"):
        yuid = node['id'].rsplit('/', 1)[-1]
        dist = all_distances.get(yuid, 100)
        if distance < dist:
            all_distances[yuid] = distance
        if dist == 100:
            added_refs[yuid] = distance

    for k, v in node.items():
        if k in ["equivalent", "access_point", "conforms_to"]:
            continue
        if type(v) == list:
            for vi in v:
                if type(vi) == dict:
                    walk_for_refs(vi, distance)
        elif type(v) == dict:
            walk_for_refs(v, distance)

all_distances = {}
added_refs = {}
missing = {}

x = 0
ttl = len(rc)

print("Keys...")
# populate all at 0
for k in rc.iter_keys():
    # k is the uuid
    all_distances[k] = 0
    x += 1
    if not x % 250000:
        print(f"{x}/{ttl}")

print("dist=0")
x = 0
for k in list(all_distances.keys()):
    try:
        rec = merged[k]
        walk_for_refs(rec['data'], 1, top=True)
    except KeyError:
        missing[k] = 1
        print(f"missing: {k}")
    x += 1
    if not x % 100000:
        print(f"{x}/{ttl}")

while added_refs:
    (k,d) = added_refs.popitem()
    rec = merged[k]
    walk_for_refs(rec['data'], d, top=True)
