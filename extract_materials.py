import os
import sys
import json
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.reference_manager import ReferenceManager
from pipeline.process.update_manager import UpdateManager

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

srcs = [cfgs.internal["ycba"], cfgs.internal["yuag"], cfgs.external["getty_museum"]]

MATERIAL = "http://vocab.getty.edu/aat/300435429"
DESCRIPTION = "http://vocab.getty.edu/aat/300435416"


records = []
x = 0
for src in srcs:
    print(f"Processing {src['name']}...")
    dc = src["datacache"]
    for rec in dc.iter_records_type("HumanMadeObject"):
        data = rec["data"]
        id = data["id"]
        classified_as = data.get("classified_as", [])
        made_of = data.get("made_of", [])
        for desc in data.get("referred_to_by", []):
            cxns = [x.get("id", None) for x in desc.get("classified_as", [])]
            if MATERIAL in cxns:
                material_desc = desc.get("content", "")
            elif DESCRIPTION in cxns:
                description = desc.get("content", "")
        records.append(
            {
                "id": id,
                "classifications": classified_as,
                "materials": made_of,
                "description": description,
                "material_description": material_desc,
            }
        )
        x += 1
        if not x % 25000:
            print(f"Processed {x} records")

json.dump(records, open("materials.json", "w"), indent=2)
