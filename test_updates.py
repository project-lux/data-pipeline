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


q = """SELECT ?s WHERE {
  ?s <https://lux.collections.yale.edu/ns/allRefCtr> <%%yuid%%>
}"""

h = cfgs.internal['ypm']['harvester']
dc = cfgs.internal['ypm']['datacache']
sbx = cfgs.marklogic_stores['ml_sandbox']['store']

g = h.crawl()
(chg, ident, new_rec, dt) = next(g)

old_rec = dc[ident]
if not old_rec:
    if chg == "update":
        chg = "create"
    elif chg == "delete":
        print(f"Already deleted {ident}, can't continue")
        raise ValueError()
uri = old_rec['data']['id']
typ = old_rec['data']['type']
quri = cfgs.make_qua(uri, typ)
yuid = idmap[quri]
uris = idmap[yuid]

q2 = q.replace('%%yuid%%', yuid)
refs = cfgs.marklogic_stores['ml_sandbox']['store'].search_sparql_ids(q2)

