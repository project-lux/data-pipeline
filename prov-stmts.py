import os
import sys
import json
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

all_stmts = []
x = 0

rc = cfgs.internal['yuag']['recordcache']
for rec in rc.iter_records():
    data = rec['data']
    if data['type'] == 'HumanMadeObject':
        stmts = data.get('referred_to_by', [])
        for stmt in stmts:
            cxns = stmt.get('classified_as', [])
            for cxn in cxns:
                if 'id' in cxn and cxn['id'] == "http://vocab.getty.edu/aat/300435438":
                    uri = data['id']
                    qua = cfgs.make_qua(uri, 'HumanMadeObject')
                    yuid = idmap[qua]
                    provenance = stmt.get('content', '')
                    label = data.get('_label', '')
                    all_stmts.append({"uri": yuid, "label": label, "provenance": provenance})
                    x += 1
                    if x % 1000 == 0:
                        print(f"Processed {x} statements")
                    break

with open('prov-stmts.json', 'w') as fh:
    json.dump(all_stmts, fh)
