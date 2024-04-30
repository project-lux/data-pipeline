import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.reconciler import Reconciler
from pipeline.process.reference_manager import ReferenceManager
from pipeline.storage.cache.postgres import poolman

import networkx as nx

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
networkmap = cfgs.instantiate_map('networkmap')['store']
cfgs.cache_globals()
cfgs.instantiate_all()

# --- process command line arguments ---
### Give a YUID and calculate how the records got there
yuids = []
while '--recid' in sys.argv:
    idx = sys.argv.index('--recid')
    recid = sys.argv[idx+1]
    sys.argv.pop(idx)
    sys.argv.pop(idx)

    if recid.startswith(cfgs.internal_uri):
        # we're a YUID
        yuids.append(recid)
    else:
        try:
            (src, ident) = cfgs.split_uri(recid)
        except:
            print(f"Unknown URI: {recid}")
            continue
        try:
            ref = src['mapper'].get_reference(ident)
            base = cfgs.canonicalize(recid)
            qua = cfgs.make_qua(base, ref.type)
        except:
            print(f"Could not make typed URI for {recid}")
            continue
        yuid = idmap[qua]
        yuids.append(yuid)

if not yuids:
    print("No YUIDs/URIs able to be found")
    sys.exit()

# --- set up environment ---
reconciler = Reconciler(cfgs, idmap, networkmap)
debug = cfgs.debug_reconciliation

curr = "A"
idents = {}

from_p = "https://media.art.yale.edu/content/lux/agt/31202.json"
to_p = "http://vocab.getty.edu/ulan/500490811"

for yuid in yuids:
    uris = idmap[yuid]

    # uri: [uris,that,are,connected]
    graph = {}

    for u in uris:
        if u.startswith('__'):
            continue
        (base, qua) = cfgs.split_qua(u)
        (src, ident) = cfgs.split_uri(base)
        idents[base] = f"{src['name']}:{curr}"
        curr = chr(ord(curr)+1)
        rec = src['acquirer'].acquire(ident)
        if 'equivalent' in rec['data']:
            for eq in rec['data']['equivalent']:
                if 'id' in eq:
                    eqid = eq['id']
                    if not eqid in idents:
                        try:
                            (eqsrc, eqident) = cfgs.split_uri(eqid)
                            idents[eqid] = f"{src['name']}:{curr}"
                            curr = chr(ord(curr)+1)
                        except:
                            idents[eqid] = eqid
                    try:
                        graph[base].append(eq['id'])
                    except:
                        graph[base] = [eq['id']]

    G = nx.Graph()
    G.add_nodes_from(list(idents.values()))

    new_graph = {}
    for (k,v) in graph.items():
        subj = idents[k]
        l = []
        for u in v:
            if u in idents:
                obj = idents[u]
                l.append(obj)
                G.add_edge(subj, obj)
            else:
                pass
        l.sort()
        new_graph[subj] = l


    key = []
    for (k,v) in idents.items():
        key.append((v, k))

    key.sort()
    print("  -- Key --")
    for k in key:
        print(f"  {k[0]:<16}{k[1]}")
    print("")
    print("Connected Nodes:")
    print(list(nx.connected_components(G)))
    print(nx.shortest_path(G, idents[from_p], idents[to_p]))

 