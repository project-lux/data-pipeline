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
### Give from and to

from_p = sys.argv[1]
to_p = sys.argv[2]

try:
    (src, ident) = cfgs.split_uri(from_p)
except:
    print(f"Unknown URI: {from_p}")
    sys.exit()
try:
    ref = src['mapper'].get_reference(ident)
    base = cfgs.canonicalize(from_p)
    qua = cfgs.make_qua(base, ref.type)
except:
    print(f"Could not make typed URI for {from_p}")
    raise
    sys.exit()
yuid = idmap[qua]

# --- set up environment ---
reconciler = Reconciler(cfgs, idmap, networkmap)

curr = "0"
idents = {}
# uri: [uris,that,are,connected]
graph = {}

uris = idmap[yuid]

names = {}

for u in uris:
    if u.startswith('__'):
        continue
    (base, qua) = cfgs.split_qua(u)
    (src, ident) = cfgs.split_uri(base)
    idents[base] = f"{src['name']}:{curr}"
    curr = chr(ord(curr)+1)
    rec = src['acquirer'].acquire(ident)
    if not rec:
        print(f"Couldn't acquire {src['name']}:{ident}")
        continue
    if '_label' in rec['data']:
        names[base] = rec['data']['_label']
    if 'equivalent' in rec['data']:
        for eq in rec['data']['equivalent']:
            if 'id' in eq:
                eqid = eq['id']
                if not eqid in idents:
                    try:
                        (eqsrc, eqident) = cfgs.split_uri(eqid)
                        idents[eqid] = f"{src['name']}:{curr}"
                        if not eqid in names:
                            ref = src['mapper'].get_reference(eqident)
                            if hasattr(ref, '_label'):
                                names[eqid] = ref._label
                            else:
                                names[eqid] = "-no label-"
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
inv_ident = {}
for (k,v) in idents.items():
    key.append((v, k))
    inv_ident[v] = k

key.sort()
print("  -- Key --")
for k in key:
    print(f"  {k[0]:<16}{k[1]} ({names.get(k[1], '?')})")

print("\nConnected Nodes:")
for sets in list(nx.connected_components(G)):
    print(sets)

print(f"\nShortest path from {from_p} to {to_p}")
print(" --> ".join([inv_ident[x] for x in nx.shortest_path(G, idents[from_p], idents[to_p])]))

print("\nLongest Path:")
longest_path = []
for node in G.nodes:
    for path in nx.all_simple_paths(G, source=node, target=idents[to_p]):
        if len(path) > len(longest_path):
            longest_path = path
print(" --> ".join([inv_ident[x] for x in longest_path]))

try:
    cycle = nx.find_cycle(G, orientation='original')
    print("Cycle found:", cycle)
except nx.NetworkXNoCycle:
    print("No cycle found.")
 