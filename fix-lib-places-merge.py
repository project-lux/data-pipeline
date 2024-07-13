import os
import sys
import json
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.reidentifier import Reidentifier
from pipeline.process.merger import MergeHandler
from pipeline.process.reference_manager import ReferenceManager
from pipeline.storage.cache.postgres import poolman

import datetime
import io

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

fh = open(os.path.join(cfgs.data_dir, "idmap_update_token.txt"))
token = fh.read()
fh.close()
token = token.strip()
if not token.startswith("__") or not token.endswith("__"):
    print("Idmap Update Token is badly formed, should be 8 character date with leading/trailing __")
    raise ValueError("update token")
    sys.exit(0)
else:
    idmap.update_token = token

profiling = False
DO_REFERENCES = False
NAME = None

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

MAX_DISTANCE = cfgs.max_distance
order = sorted([(x["namespace"], x.get("merge_order", -1)) for x in cfgs.external.values()], key=lambda x: x[1])
PREF_ORDER = [x[0] for x in order if x[1] >= 0]

reider = Reidentifier(cfgs, idmap)
ref_mgr = ReferenceManager(cfgs, idmap)
merger = MergeHandler(cfgs, idmap, ref_mgr)

merged_cache = cfgs.results["merged"]["recordcache"]
merged_cache.config["overwrite"] = True
final = cfgs.results["merged"]["mapper"]

merged_is_empty = merged_cache.len_estimate() < 10
start_time = datetime.datetime.now()
# merge only reads, so enable AAT memory cache
idmap.enable_memory_cache()

for src_name, src in to_do:
    rcache = src["recordcache"]

    for recid in recids:
        distance = 0
        rec = rcache[recid]
        if not rec:
            print(f"Couldn't find {src['name']} / {recid}")
            continue
        recuri = f"{src['namespace']}{recid}"
        qrecid = cfgs.make_qua(recuri, rec["data"]["type"])
        yuid = idmap[qrecid]
        if not yuid:
            print(f" !!! Couldn't find YUID for internal record: {qrecid}")
            continue
        yuid = yuid.rsplit("/", 1)[1]
        ins_time = merged_cache.metadata(yuid, "insert_time")
        if ins_time is not None and ins_time["insert_time"] > start_time:
            # Already processed this record this build
            continue
        elif not yuid in src["recordcache2"]:
            rec2 = reider.reidentify(rec)
            src["recordcache2"][rec2["yuid"]] = rec2["data"]
        else:
            rec2 = src["recordcache2"][yuid]

        if NAME is not None and ins_time is not None:
            # We're in merged previously
            curr_name = merged_cache.metadata(yuid, "change")["change"]
            if curr_name in ["create", "update"]:
                curr_name = ""
        else:
            curr_name = ""

        equivs = idmap[rec2["data"]["id"]]
        if equivs:
            if qrecid in equivs:
                equivs.remove(qrecid)
            if recuri in equivs:
                equivs.remove(recuri)
            if idmap.update_token in equivs:
                equivs.remove(idmap.update_token)
        else:
            equivs = []
        sys.stdout.write(".")
        sys.stdout.flush()

        rec3 = merger.merge(rec2, equivs)
        # Final tidy up after merges
        try:
            rec3 = final.transform(rec3, rec3["data"]["type"])
        except:
            print(f"*** Final transform raised exception for {rec2['identifier']}")
            raise
        # Store it
        if rec3 is not None:
            try:
                del rec3["identifier"]
            except:
                pass
            merged_cache[rec3["yuid"]] = rec3
            if NAME:
                if curr_name:
                    if NAME in curr_name:
                        new_name = None
                    else:
                        new_name = f"{curr_name}|{NAME}"
                else:
                    new_name = NAME
                if new_name:
                    merged_cache.set_metadata(yuid, "change", new_name)
        else:
            print(f"*** Final transform returned None")
    recids = []
