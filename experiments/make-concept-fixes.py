import os
import csv
import json

from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()

# {'scc_id': '0', 
# 'child_uuid': 'cf22e902-b88d-4af9-beb4-1665f9c18403', 
# 'child_label': 'Stationery', 
# 'child_internal_ids': 'https://linked-art.library.yale.edu/node/c1abb9d3-2419-4efa-bf5d-ca0ced6e28cc https://linked-art.library.yale.edu/node/846ae651-191d-4b0b-bc01-ec78ad087458 https://linked-art.library.yale.edu/node/ecb88378-b132-4db1-87fa-a4b6d0202263 https://linked-art.library.yale.edu/node/078a9b42-bd5b-4b05-877a-0d4d9d6fc9a3 https://linked-art.library.yale.edu/node/658931b7-32eb-43b9-812e-bb63ce38dd48 https://linked-art.library.yale.edu/node/673bb716-72b7-4ff5-a545-e4d7311c3c31 https://linked-art.library.yale.edu/node/0a004720-55d5-4fb8-9770-b65ea165f3e9 https://linked-art.library.yale.edu/node/4985f75c-bbb9-4553-a7c7-f5c0a542dc0f https://linked-art.library.yale.edu/node/5ba67b25-e236-4625-b697-b63ba06b0f4d https://linked-art.library.yale.edu/node/f6e6df80-9085-4c46-82ed-321015ec5f2c https://ycba-lux.s3.amazonaws.com/v3/concept/1e/1ec15c50-1f04-48f6-b17b-02f346f7547c.json https://linked-art.library.yale.edu/node/70c7f4d0-3adb-4498-bfcd-f798d48d6f03', 
# 'parent_uuid': 'c3909234-e0b2-448d-a871-546da6ab732a', 
# 'parent_label': 'Reserves (Accounting)', 
# 'parent_internal_ids': 'https://linked-art.library.yale.edu/node/4ccd646d-f98a-495a-bb17-24b20f6d9d6a', 
# 'reason': 'Stationery is not a type of accounting reserve.', 
# 'source': 'fragment_16'}

qua = "Type"

missed = {}
changed = {}
with open('deletions.csv') as fh:
    reader = csv.DictReader(fh)

    for row in reader:
        kid_uu = row['child_uuid']
        parent_uu = row['parent_uuid']
        parents = row['parent_internal_ids'].strip().split(" ")
        kids = row['child_internal_ids'].strip().split(" ")
        for p in parents:
            if p:
                pt = f"{p}##qua{qua}"
                if pt in missed:
                    continue
                p_uu = idmap[pt]
                if p_uu is None:
                    missed[pt] = 1
                    print(f"Missing parent UUID for {pt}")
                else:
                    p_uu = p_uu.rsplit('/', 1)[1]
                    if p_uu != parent_uu:
                        changed[pt] = 1
                        print(f"Parent UUID mismatch for {pt}: {p_uu} vs {parent_uu}")
        for k in kids:
            if k:
                kt = f"{k}##qua{qua}"
                if kt in missed:
                    continue
                k_uu = idmap[kt]
                if k_uu is None:
                    missed[kt] = 1
                    print(f"Missing child UUID for {kt}")
                else:
                    k_uu = k_uu.rsplit('/', 1)[1]
                    if k_uu != kid_uu:
                        changed[kt] = 1
                        print(f"Child UUID mismatch for {kt}: {k_uu} vs {kid_uu}")

