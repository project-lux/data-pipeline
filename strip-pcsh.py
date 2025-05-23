import os
import sys
from dotenv import load_dotenv
from pipeline.config import Config
import json

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

# Run through Types in merged and strip out any that are PCSH without other equivalents

merged = cfgs.results["merged"]["recordcache"]
idx = cfgs.internal["ils"]["indexLoader"].load_index()
vocabs = ["wikidata.org", "getty.edu", "art.yale", "ycba-lux", "images.peabody"]


killed = []
kept = []

for recid in idx:
    sys.stdout.write(".")
    sys.stdout.flush()
    if not len(killed) % 50000:
        print(len(killed))
    try:
        yuid = idmap[f"{recid}##quaType"]
        equivs = idmap[yuid]
    except Exception as e:
        print(f"Error processing {recid}: {e}")
        continue
    okay = 0
    # min is self and token
    if len(equivs) > 2:
        for e in equivs:
            for s in vocabs:
                if s in e:
                    okay += 1
    if not okay:
        killed.append(yuid)
        del merged[yuid.rsplit("/", 1)[1]]
    else:
        print(f"{recid} / {yuid} has {okay} equivalents: {equivs}")
        kept.append(yuid)

jstr = json.dumps(killed)
with open("killed.json", "w") as f:
    f.write(jstr)

kstr = json.dumps(kept)
with open("kept.json", "w") as f:
    f.write(kstr)
