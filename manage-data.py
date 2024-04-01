import os
import sys
import json
import time
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
all_refs = cfgs.instantiate_map('all_refs')['store']
done_refs = cfgs.instantiate_map('done_refs')['store']
cfgs.cache_globals()
cfgs.instantiate_all()


### HARVEST EXTERNAL NON-DUMP DATASETS

if '--harvest' in sys.argv:
    if '--aat' in sys.argv:
        which = 'aat'
    elif '--ulan' in sys.argv:
        which = 'ulan'
    else:
        print("Need to know which database to harvest, --aat or --ulan")
        sys.exit(0)
    fh = open(os.path.join(cfgs.data_dir, f'all_{which}s.csv'))
    uris = fh.readlines()
    fh.close()
    uris = uris[1:] # chomp header
    acq = cfgs.external[which]['acquirer']
    acq.debug = True
    acq.fetcher.enabled = True
    for uri in uris:
        ident = uri.split('/')[-1][:-1] # chomp off \n
        acq.acquire(ident, dataonly=True)
        print(ident)

### LOAD DATABASES

if '--load' in sys.argv:
    if '--ycba' in sys.argv or '--all' in sys.argv:
        cfgs.internal['ycba']['datacache'].clear()
        cfgs.internal['ycba']['loader'].load()
    if '--yuag' in sys.argv or '--all' in sys.argv:
        cfgs.internal['yuag']['datacache'].clear()
        cfgs.internal['yuag']['loader'].load()
    if '--ypm' in sys.argv or '--all' in sys.argv:
        cfgs.internal['ypm']['datacache'].clear()
        cfgs.internal['ypm']['loader'].load()
    if '--aspace' in sys.argv or '--all' in sys.argv:
        cfgs.internal['aspace']['datacache'].clear()
        cfgs.internal['aspace']['loader'].load()
    if '--wikidata' in sys.argv or '--all' in sys.argv:
        cfgs.external['wikidata']['datacache'].clear()
        cfgs.external['wikidata']['loader'].load()
    if '--viaf' in sys.argv or '--all' in sys.argv:
        cfgs.external['viaf']['datacache'].clear()
        cfgs.external['viaf']['loader'].load()
    if '--lcnaf' in sys.argv or '--all' in sys.argv:
        cfgs.external['lcnaf']['datacache'].clear()
        cfgs.external['lcnaf']['loader'].load()
    if '--lcsh' in sys.argv or '--all' in sys.argv:
        cfgs.external['lcsh']['datacache'].clear()
        cfgs.external['lcsh']['loader'].load()

### LOAD INDEXES

if '--load-index' in sys.argv:
    if '--wikidata' in sys.argv or '--all' in sys.argv:
        cfgs.external['wikidata']['indexLoader'].load()
    if '--viaf' in sys.argv or '--all' in sys.argv:
        cfgs.external['viaf']['indexLoader'].load()
    if '--lcnaf' in sys.argv or '--all' in sys.argv:
        cfgs.external['lcnaf']['indexLoader'].load()
    if '--lcsh' in sys.argv or '--all' in sys.argv:
        cfgs.external['lcsh']['indexLoader'].load()
    if '--aat' in sys.argv or '--all' in sys.argv:
        cfgs.external['aat']['indexLoader'].load()
    if '--ulan' in sys.argv or '--all' in sys.argv:
        cfgs.external['ulan']['indexLoader'].load()

### VALIDATION

if '--validate' in sys.argv:

    ignore_matches = []

    rc = cfgs.internal['ils']['recordcache']
    v = cfgs.validator
    for rec in rc.iter_records():
        sys.stdout.write('.');sys.stdout.flush()
        errs = v.validate(rec)
        if errs:
            filtered = []
            for error in errs:
                done = False
                for im in ignore_matches:
                    if im in error.message:
                        done = True
                if not done:
                    filtered.append(error)
            if filtered:
                print(f"\n{rec['identifier']}")
                for error in filtered:
                    print(f"  /{'/'.join([str(x) for x in error.absolute_path])} --> {error.message} ")
        else:
            # print(f"{rec['identifier']}: Valid")
            pass


### CLEAN IDMAP

if '--clean-idmap' in sys.argv:
    killed = {None:0}
    token = idmap.update_token
    x = 0
    total = len(idmap)
    start = time.time()
    for key in idmap.iter_keys(match="yuid:*", count=20000):
        x += 1
        val = idmap[key]
        done = False
        for v in val:
            if v.startswith('__'):
                if v.startswith('__2023'):
                    del idmap[key]
                    try:
                        killed[v] += 1
                    except:
                        killed[v] = 1
                done = True
                break
        if not done:
            # no token at all
            del idmap[key]
            killed[None] += 1
        if not x % 100000:
            durn = int(time.time() - start)
            print(f"{x} in {durn} = {x/durn}/sec = {total/(x/durn)} total")
    print("Cleaned idmap:")
    print(killed)


### CLEAR DATABASES

if '--clear' in sys.argv:
    cidx = sys.argv.index('--clear')
    cache = sys.argv[cidx+1]
    # Now find the named cache
    (src, ctype) = cache.split('_', 1)
    # And clear it
    if src in cfgs.internal:
        c = cfgs.internal[src][ctype]
        c.clear()
    elif src in cfgs.external:
        c = cfgs.external[src][ctype]
        c.clear()
    elif src in cfgs.results:
        c = cfgs.results[src][ctype]
        c.clear()

if '--vacuum' in sys.argv:
    for c in cfgs.internal.values():
        for t in ['datacache', 'recordcache', 'recordcache2']:
            if t in c and c[t] is not None:
                print(f"{c['name']}/{t}...")
                c[t].optimize()
    for c in cfgs.external.values():
        for t in ['datacache', 'recordcache', 'recordcache2']:
            if t in c and c[t] is not None:
                print(f"{c['name']}/{t}...")
                c[t].optimize()
    for c in cfgs.results.values():
        for t in ['recordcache', 'recordcache2']:
            if t in c and c[t] is not None:
                print(f"{c['name']}/{t}...")
                c[t].optimize()


if '--counts' in sys.argv:
    for c in cfgs.internal.values():
        for t in [ 'datacache', 'recordcache', 'recordcache2']:
            if t in c and c[t] is not None:
                est = c[t].len_estimate()
                pref = "~"
                if est < 30000:
                    est = len(c[t])
                    pref = "="
                print(f"{c['name']} {t}: {pref}{est}")
    for c in cfgs.external.values():
        for t in [ 'datacache', 'recordcache', 'reconciledRecordcache', 'recordcache2']:
            if t in c and c[t] is not None:
                est = c[t].len_estimate()
                pref = "~"
                if est < 30000:
                    est = len(c[t])
                    pref = "="
                print(f"{c['name']} {t}: {pref}{est}")
    for c in cfgs.results.values():
        for t in ['recordcache', 'recordcache2']:
            if t in c and c[t] is not None:
                est = c[t].len_estimate()
                pref = "~"
                if est < 50000:
                    est = len(c[t])
                    pref = "="
                print(f"{c['name']} {t}: {pref}{est}")
    print(f"idmap: {len(idmap)}")
    print(f"references found: {len(all_refs)}")
    print(f"references done: {len(done_refs)}")

if '--clear-external' in sys.argv or '--clear-all' in sys.argv:
    for c in cfgs.external.values(): 
        for t in ['recordcache', 'reconciledRecordcache', 'recordcache2']:
            if t in c and c[t] is not None:
                print(f"Clearing {c['name']} {t}")
                c[t].clear()
if '--clear-results' in sys.argv or '--clear-all' in sys.argv:
    for c in cfgs.results.values():
        for t in ['recordcache', 'recordcache2']:
            if t in c and c[t] is not None:
                print(f"Clearing {c['name']} {t}")
                c[t].clear()
if '--clear-internal' in sys.argv or '--clear-all' in sys.argv:
    for c in cfgs.internal.values():
        # Don't clear datacache, only computed
        for t in ['recordcache', 'recordcache2']:
            if t in c and c[t] is not None:
                print(f"Clearing {c['name']} {t}")
                c[t].clear()  

if '--clear-refs' in sys.argv or '--clear-all' in sys.argv:
    print("Clearing Built Refs")
    all_refs.clear()
    print("Clearing Done Refs")
    done_refs.clear()

if '--clear-idmap' in sys.argv:
    # WARNING WARNING!      
    idmap.clear()
