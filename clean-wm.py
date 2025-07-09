import os
import json
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
cfgs.cache_globals()
cfgs.instantiate_all()

wm = cfgs.external["wikimedia"]["datacache"]

bad = []
x = 0
deleted = 0
ttl = 81000000
for k in wm.iter_keys():
    x += 1
    try:
        skip = False
        rec = wm[k]
        if not rec:
            raise ValueError("Record is empty")
    except:
        bad.append(k)
        skip = True

    if not skip:
        try:
            p = rec["data"]["query"]["pages"]
            if not p:
                del wm[k]
                deleted += 1
        except Exception:
            del wm[k]
            deleted += 1
    if not x % 100000:
        print(f"{x}/{ttl} = {x / ttl}")
        print(f"bad: {len(bad)} / deleted: {deleted}")

fh = open("bad-wm-keys.json", "w")
fh.write(json.dumps(bad))
fh.close()
