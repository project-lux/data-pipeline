import os
import sys
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.reconciler import Reconciler
from pipeline.process.reference_manager import ReferenceManager
from pipeline.process.reidentifier import Reidentifier
from pipeline.process.merger import MergeHandler

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

STORE_OKAY = False

q = """SELECT DISTINCT ?ref WHERE {
  { ?ref lux:refCtr <https://lux.collections.yale.edu/data/person/c225d5e4-8767-4a25-802b-af054d5e8f52> . }
  UNION
  { ?ref lux:any <https://lux.collections.yale.edu/data/person/c225d5e4-8767-4a25-802b-af054d5e8f52> . }
} LIMIT 10"""

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
#latest = dc.latest()
latest = "2025-01-21T00:00:00"
lday = latest[:10] + "T00:00:00"

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

print("")
print(f"Fetching {len(creates)} new recs")
temp_recs = []
# First process creates as easy
for ident in creates:
    new_rec = acquirer.acquire(ident, store=STORE_OKAY)
    temp_recs.append(new_rec)


print(f"Fetching {len(updates)} updated recs")
maybe_delete = {}
for ident in updates:
    old_rec = acquirer.acquire(ident, store=STORE_OKAY)
    new_rec = acquirer.acquire(ident, store=STORE_OKAY, refetch=True)

    if new_rec['data'] == old_rec['data']:
        # Already seen this one, carry on!
        continue

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
                ruri = cfgs.make_qua(r, old_refs[r])
                maybe_delete[ruri] = 1


print(f"Have maybe_deletes: {list(maybe_delete.keys())}")

# At this point we should have all of the data stored in datacache
# And know all of the idents to process

# Reconciling

ref_mgr = ReferenceManager(cfgs, idmap)
mapper = cfgs.internal['ypm']['mapper']
networkmap = cfgs.instantiate_map("networkmap")["store"]
reconciler = Reconciler(cfgs, idmap, networkmap)
merged = cfgs.results["merged"]["recordcache"]

print("Reconciling")

# These are only from YPM
recs2 = []
for rec in temp_recs:
    sys.stdout.write('.')
    sys.stdout.flush()
    rec2 = reconciler.reconcile(rec)
    mapper.post_reconcile(rec2)
    ref_mgr.walk_top_for_refs(rec2["data"], 0)
    ref_mgr.manage_identifiers(rec2)
    recs2.append(rec2)

print("References")

item = 1
while item:
    sys.stdout.write('.')
    sys.stdout.flush()
    # Item is uri, {dist, type} or None
    item = ref_mgr.pop_ref()
    try:
        (uri, dct) = item
        distance = dct["dist"]
    except:
        continue
    try:
        maptype = dct["type"]
    except:
        continue
    if distance > cfgs.max_distance:
        continue

    # We only care if the reference is a *new* record
    # Otherwise it'll just be the same as previously
    if cfgs.is_qua(uri):
        ref_yuid = idmap[uri]
    else:
        # Internal ref somehow, which we'll process at some point
        # if the changeset is consistent
        continue
    if ref_yuid is not None:
        ref_uu = ref_yuid[-36:]
        if ref_uu in merged:
            continue
    # At this point there's either no uuid or one that's not in the dataset
    # so we need to build it

    ref_mgr.did_ref(uri, distance)
    uri, rectype = cfgs.split_qua(uri)
    try:
        (source, recid) = cfgs.split_uri(uri)
    except:
        continue
    if source["type"] != "external":
        # Double check we're external
        continue

    # put back the qua to the id after splitting/canonicalizing in split_uri
    mapper = source["mapper"]
    acquirer = source["acquirer"]

    # Acquire the record from cache or network
    rec = acquirer.acquire(recid, rectype=rectype)
    if rec is not None:
        rec2 = reconciler.reconcile(rec)
        mapper.post_reconcile(rec2)
        ref_mgr.walk_top_for_refs(rec2["data"], distance)
        ref_mgr.manage_identifiers(rec2)
        recs2.append(rec2)
    else:
        print(f"Failed to acquire {rectype} reference: {source['name']}:{recid}")


print("Merging")
# Now we should have all our records saved and in recs2
reider = Reidentifier(cfgs, idmap)
merger = MergeHandler(cfgs, idmap, ref_mgr)
final = cfgs.results["merged"]["mapper"]
mlmapper = cfgs.results["marklogic"]["mapper"]
ml = cfgs.results["marklogic"]["recordcache"]
ml_store = cfgs.marklogic_stores['ml_dev']['store']

for rec in recs2:
    sys.stdout.write('.')
    sys.stdout.flush()
    recuri = rec['data']['id']
    try:
        src = cfgs.internal[rec['source']]
    except:
        src = cfgs.external[rec['source']]
    qrecid = cfgs.make_qua(recuri, rec["data"]["type"])
    yuid = idmap[qrecid]
    if not yuid:
        print(f" !!! Couldn't find YUID for record: {qrecid}")
        continue
    yuid = yuid.rsplit("/", 1)[1]
    rec2 = reider.reidentify(rec)
    if STORE_OKAY:
        src["recordcache2"][rec2["yuid"]] = rec2["data"]

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
        if STORE_OKAY:
            merged[rec3["yuid"]] = rec3
    else:
        print(f"*** Final transform returned None")

    rec4 = mlmapper.transform(rec3, rec3["data"]["type"])
    if STORE_OKAY:
        ml[yuid] = rec4

    # And send to ML environment!
    if STORE_OKAY:
        ml_store[yuid] = rec4

# Now we're done with reconciling, merging and processing to ML
# Can reconcile end up deleting an existing record other than a record in the current set? Yes
#    Change is to add an equivalent. That triggers merge with existing. Then update becomes delete, or triggers a delete

print("Deleting!")

for ident in deletes:
    # Can I delete ident? These are all YPM at this stage
    # Are there any records that reference ident, that aren't also slated for deletion?
    old_rec = acquirer.acquire(ident, store=STORE_OKAY)
    uri = old_rec['data']['id']
    typ = old_rec['data']['type']
    quri = cfgs.make_qua(uri, typ)
    yuid = idmap[quri]
    q2 = q.replace('%%yuid%%', yuid)
    refs = ml_store.search_sparql_ids(q2)

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




