import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.reconciler import Reconciler
from pipeline.process.reference_manager import ReferenceManager
from pipeline.storage.cache.postgres import poolman

import matplotlib.pyplot as plt
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

if '##qua' in from_p:
    qua = from_p
else:
    try:
        ref = src['mapper'].get_reference(ident)
        base = cfgs.canonicalize(from_p)
        qua = cfgs.make_qua(base, ref.type)
    except:
        print(f"Could not make typed URI for {from_p}")
        raise
        sys.exit()

if '##qua' in to_p:
    qua2 = to_p
else:
    try:
        ref = src['mapper'].get_reference(ident)
        base = cfgs.canonicalize(to_p)
        qua2 = cfgs.make_qua(base, ref.type)
    except:
        print(f"Could not make typed URI for {to_p}")
        raise
        sys.exit()

yuid = idmap[qua]
yuid2 = idmap[qua2]

if yuid != yuid2:
    print("{from_p} is {yuid} but {to_p} is {yuid2}")
    print("No way to connect these two URIs")
    sys.exit()

# --- set up environment ---
reconciler = Reconciler(cfgs, idmap, networkmap)
cfgs.external['gbif']['fetcher'].enabled = True
reconciler.debug = True
cfgs.debug_reconciliation = True


curr = "0"
uris = idmap[yuid]

idents = {}
graph = {}
names = {}

### Idea: We know the final result, try to reconstruct it by following what the reconciler
# does, but tracking which URIs prompted the inclusion of which others
# Should do it in the approximately same order as 

inputs = []
for u in uris:
    if u.startswith('__'):
        continue
    (base, qua) = cfgs.split_qua(u)
    (src, ident) = cfgs.split_uri(base)
    inputs.append((src, ident, u, src['acquirer'].acquire(ident)))

inputs.sort(key=lambda x: x[0]['datacache'].len_estimate())
inputs.sort(key=lambda x: 1 if x[0]['type'] == 'internal' else 2)

# start with inputs[0] and reconcile
# then filter out any in the new equivalents, and iterate until we find them all

while inputs:
    inp = inputs.pop(0)
    reconciler.reconcile(inp[3])
    eqs = inp[3]['data'].get('equivalent', [])
    for e in eqs:
        for i in inputs[:]:
            if e == i[2]:
                print(f"removing {e}")
                inputs.remove(i) 
                break


raise ValueError()







curr = chr(ord(curr)+1)
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


#this code worked until I introduced reconcile, now it takes WAY too long to compile. 
#we can't use the DAG function because we have cycles in our graph
# print("\nLongest Path:")
# longest_path = []
# for node in G.nodes:
#     for path in nx.all_simple_paths(G, source=node, target=idents[to_p]):
#         if len(path) > len(longest_path):
#             longest_path = path
# print(" --> ".join([inv_ident[x] for x in longest_path]))


plt.figure(figsize=(12, 12))
node_color_values = ['skyblue' for _ in G.nodes()]  
node_labels = {node: node for node in G.nodes()} 
edge_color_values = ['black' for _ in G.edges()] 
pos = nx.spring_layout(G, k=1)

nodes = nx.draw_networkx_nodes(G, pos, node_color=node_color_values, node_size=150)
edges = nx.draw_networkx_edges(G, pos, edge_color=edge_color_values)
nx.draw_networkx_labels(G, pos, labels=node_labels, font_size=14)

#plt.legend([nodes, edges], ['Nodes', 'Edges'])

plt.savefig("graph.png")
plt.show(block=True) 