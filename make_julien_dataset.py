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
                try:
                    cached = idmap[idmap[node['id']]]
                except:
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
        if type(v) == list:
            for vi in v:
                if type(vi) == dict:
                    self._walk_rec(vi, links)
                else:
                    print(f"found non dict in a list :( {node}")
        elif type(v) == dict:
            self._walk_rec(v, links)


def get_getty_links(rec):
    links = {}
    _walk_rec(rec['data'], links)
    return list(links.keys())

cache = {}
results = {}

for src in ['ycba', 'yuag']:
    store = cfgs.internal[src]['recordcache']
    for rec in store.iter_records():
        refs = get_getty_links(rec)
        for r in refs:
            if r in results:
                results[r][src] = rec['identifier']
            else:
                results[r] = {src: rec['identifier']}
        sys.stdout.write('.');sys.stdout.flush()

