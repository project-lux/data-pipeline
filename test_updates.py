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

def _walk_refs(node, refs):
    if 'id' in node:
        refs[node['id']] = node['type']

    for k, v in node.items():
        if not type(v) in [list, dict]:
            continue
        if type(v) == list:
            for vi in v:
                if type(vi) == dict:
                    _walk_refs(vi, refs)
                else:
                    print(f"found non dict in a list :( {node}")
        elif type(v) == dict:
            _walk_refs(v, refs)

h = cfgs.internal['ypm']['harvester']
dc = cfgs.internal['ypm']['datacache']
ypmns = cfgs.internal['ypm']['namespace']

# In prod, this could be acquirer to run the mappers, store in cache and so on
acquirer = cfgs.internal['ypm']['acquirer']
latest = dc.latest()
lday = latest[:10] + "T00:00:00"
sbx = cfgs.marklogic_stores['ml_sandbox']['store']
creates = []
updates = []
deletes = []

for (chg, ident, empty, dt) in h.crawl(last_harvest=lday, refsonly=True):
    if chg == "create":
        creates.append(ident)
    elif chg == "update":
        updates.append(ident)
    elif chg == "delete":
        deletes.append(ident)
    else:
        print(f"Saw chg: {chg} for {ident} ?")

for i in updates[:]:
    if not i in dc:
        creates.append(i)
        updates.remove(i)

print("Fetching new recs")

temp_recs = []

# First process creates as easy
for ident in creates:
    new_rec = acquirer.acquire(ident, store=STORE_OKAY)
    if not STORE_OKAY:
        temp_recs.append(new_rec)


print("Fetching updated recs")

maybe_delete = {}
for ident in updates:
    old_rec = acquirer.acquire(ident, store=STORE_OKAY)
    new_rec = acquirer.acquire(ident, store=STORE_OKAY, refetch=True)

    if new_rec['data'] == old_rec['data']:
        # Already seen this one, carry on
        continue

    if not STORE_OKAY:
        temp_recs.append(new_rec)

    # First -- is this a boring update with no reference changes
    old_refs = {}
    new_refs = {}
    _walk_refs(old_rec['data'], old_refs)
    _walk_refs(new_rec['data'], new_refs)
    sold = set(old_refs.keys())
    snew = set(new_refs.keys())
    if sold == snew:
        # no changes to referenced records
        # Treat as create -- we can modify in place without concern
        continue
    else:
        # referenced records changed
        removed = sold - snew
        if not removed:
            # This is also able to be updated in place, as won't cause a delete
            # Can add refs no problem, as going to submit all changes before querying
            continue
        else:
            # This might cause a delete on any of these
            for r in removed:
                ruri = cfgs.make_qua(r, removed[r])
                maybe_delete[ruri] = 1

# Now send all updates to ML before doing any queries

for ident in deletes:
    # Can I delete ident?

    # Are there any records that reference ident, that aren't also slated for deletion?
    old_rec = acquirer.acquire(ident, store=STORE_OKAY)
    uri = old_rec['data']['id']
    typ = old_rec['data']['type']
    quri = cfgs.make_qua(uri, typ)
    yuid = idmap[quri]
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
                changed = ref_ident in deletes

    # if no references, then get record's references to consider deletion and delete

    # if there are references

    # is there a reference that will not be deleted? If so, can't delete
    # e.g. from another internal source, or from a global

    # if only YPM or extenal references, then track

    # At the end of the pass, for each record, if the set of preventing records is the a subset of the deletions, then delete
