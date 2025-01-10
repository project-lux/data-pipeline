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


STORE_OKAY = False

q = """SELECT ?s WHERE {
  ?s <https://lux.collections.yale.edu/ns/allRefCtr> <%%yuid%%>
}"""

h = cfgs.internal['ypm']['harvester']
dc = cfgs.internal['ypm']['datacache']
ypmns = cfgs.internal['ypm']['namespace']

# In prod, this could be acquirer to run the mappers, store in cache and so on
acquirer = cfgs.internal['ypm']['acquirer']
latest = dc.latest()
lday = latest[:10] + "T00:00:00"
sbx = cfgs.marklogic_stores['ml_sandbox']['store']

g = h.crawl(last_harvest=lday, refsonly=True)
chgs = list(g)

creates = []
updates = []
deletes = []

chgd = {}
for (chg, ident, empty, dt) in chgs:
    if chg == "create":
        creates.append(ident)
    elif chg == "update":
        updates.append(ident)
    elif chg == "delete":
        deletes.append(ident)
    else:
        print(f"Saw chg: {chg} for {ident} ?")
    if not ident in chgd:
        chgd[ident] = chg
    else:
        print(f"saw duplicate {ident}: {chg}, more recently {chgd[ident]}")

for i in updates[:]:
    if not i in dc:
        creates.append(i)
        updates.remove(i)
        chgd[i] = "create"


# First process creates as easy
for ident in creates:
    # new_rec = acquirer.acquire(ident, store=STORE_OKAY)

    # reconcile, and track referenced as normal
    # merge all (incl refs)
    # save to batches
    # upload batches

    pass

for ident in updates:

    old_rec = acquirer.acquire(ident, store=STORE_OKAY)
    new_rec = acquirer.acquire(ident, store=STORE_OKAY, refetch=True)




    uri = old_rec['data']['id']
    typ = old_rec['data']['type']
    quri = cfgs.make_qua(uri, typ)
    yuid = idmap[quri]
    uris = idmap[yuid]
    q2 = q.replace('%%yuid%%', yuid)
    refs = cfgs.marklogic_stores['ml_sandbox']['store'].search_sparql_ids(q2)

    arefs = []
    for r in refs:
        ar = idmap[r]
        for a in ar:
            if a.startswith('__'):
                continue
            # get source + ident
            (ref_uri, ref_typ) = cfgs.split_qua(a)
            (ref_src, ref_ident) = cfgs.split_uri(ref_uri)
            if ref_src['name'] == 'ypm':
                # this is internal reffing record
                changed = ref_ident in chgd



    break
