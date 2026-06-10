import json
import os

from dotenv import load_dotenv

from pipeline.config import Config

_ = load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

fh = open("deletions.jsonl")
lines = fh.readlines()
fh.close()

part_of_pred = "http://www.cidoc-crm.org/cidoc-crm/P89_falls_within"

deletes = {}
for line in lines:
    l = json.loads(line)
    parent = l["parent_uuid"]
    child = l["child_uuid"]

    if child not in deletes:
        deletes[child] = []
    deletes[child].append(parent)

ml = cfgs.results["marklogic"]["recordcache"]

all_recs = []
equivs = {}

for child, parents in deletes.items():
    for parent in parents:
        if parent not in equivs:
            equivs[parent] = idmap[
                f"https://lux.collections.yale.edu/data/place/{parent}"
            ]
    if child not in equivs:
        equivs[child] = idmap[f"https://lux.collections.yale.edu/data/place/{child}"]

    rec = ml[child]
    parts = rec["data"]["json"]["part_of"]
    new_parts = []
    for p in parts:
        if "id" in p and p["id"] not in parents:
            new_parts.append(p)
    rec["data"]["json"]["part_of"] = new_parts

    new_triples = []
    for t in rec["data"]["triples"]:
        if (
            t["triple"]["predicate"] == part_of_pred
            and t["triple"]["object"].rsplit("/")[-1] in parents
        ):
            continue
        new_triples.append(t)
    rec["data"]["triples"] = new_triples
    all_recs.append(rec)

with open("fixed_places.jsonl", "w") as fh:
    for rec in all_recs:
        fh.write(json.dumps(rec) + "\n")

with open("equivs.json", "w") as fh:
    json.dump(equivs, fh)
