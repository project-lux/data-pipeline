import os
import sys
import json
import time
import datetime
import gzip
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
cfgs.cache_globals()
cfgs.instantiate_all()

def _walk_rec(node, links):
    if 'id' in node:
        if 'vocab.getty.edu' in node['id']:
            links[node['id']] = 1    
        elif not node['id'] in links:
            if node['id'] in cache:
                cached = cache[node['id']]
            else:
                qid = cfgs.make_qua(node['id'], node['type'])
                if qid:
                    cached = idmap[idmap[qid]]
                else:
                    cached = []
                cache[node['id']] = cached
            for c in cached:
                if 'vocab.getty.edu' in c:
                    # found one
                    links[c] = 2
                    break

    for (k,v) in node.items():
        if not type(v) in [list, dict]:
            continue
        elif k in ['access_point', 'equivalent']:
            continue
        elif type(v) == list:
            for vi in v:
                if type(vi) == dict:
                    _walk_rec(vi, links)
                else:
                    print(f"found non dict in a list :( {node}")
        elif type(v) == dict:
            _walk_rec(v, links)


def get_getty_links(rec):
    links = {}
    _walk_rec(rec['data'], links)
    direct = [x for x in links.keys() if links[x] == 1]
    indirect = [x for x in links.keys() if links[x] == 2]
    return (direct, indirect)

cache = {}
results = {}

x = 0
ttl = 151528 + 404099
for src in ['ycba', 'yuag']:
    store = cfgs.internal[src]['recordcache']
    for rec in store.iter_records():
        x += 1
        (direct, indirect) = get_getty_links(rec)
        for r in direct:
            if r in results:
                results[r][src] = rec['identifier']
            else:
                results[r] = {src: rec['identifier']}
        for r in indirect:
            if r in results:
                # if both, only report direct
                if not src in results[r] and not f"{src}_i" in results[r]:
                    results[r][f"{src}_i"] = rec['identifier']
            else:
                results[r] = {f"{src}_i": rec['identifier']}

        if not x % 10000:
            print(f"{x} / {ttl}")
