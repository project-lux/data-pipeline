import os
import sys
import json
import time
import datetime
import gzip
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.reference_manager import ReferenceManager
from pipeline.process.update_manager import UpdateManager

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
all_refs = cfgs.instantiate_map("all_refs")["store"]
done_refs = cfgs.instantiate_map("done_refs")["store"]
cfgs.cache_globals()
cfgs.instantiate_all()

update_mgr = UpdateManager(cfgs, idmap)
ref_mgr = ReferenceManager(cfgs, idmap)


### LOAD DATABASES
if "--load" in sys.argv:
    if "--ycba" in sys.argv or "--all" in sys.argv:
        cfgs.internal["ycba"]["datacache"].clear()
        cfgs.internal["ycba"]["loader"].load()
    if "--yuag" in sys.argv or "--all" in sys.argv:
        cfgs.internal["yuag"]["datacache"].clear()
        cfgs.internal["yuag"]["loader"].load()
    if "--ypm" in sys.argv or "--all" in sys.argv:
        cfgs.internal["ypm"]["datacache"].clear()
        cfgs.internal["ypm"]["loader"].load()
    if "--lcnaf" in sys.argv or "--all" in sys.argv:
        cfgs.external["lcnaf"]["datacache"].clear()
        cfgs.external["lcnaf"]["loader"].load()
    if "--lcsh" in sys.argv or "--all" in sys.argv:
        cfgs.external["lcsh"]["datacache"].clear()
        cfgs.external["lcsh"]["loader"].load()
    if "--geonames" in sys.argv or "--all" in sys.argv:
        cfgs.external["geonames"]["datacache"].clear()
        cfgs.external["geonames"]["loader"].load()
    if "--ror" in sys.argv or "--all" in sys.argv:
        cfgs.external["ror"]["datacache"].clear()
        cfgs.external["ror"]["loader"].load()

    if "--viaf" in sys.argv or "--all" in sys.argv:
        my_slice = int(sys.argv[1])
        max_slice = int(sys.argv[2])
        cfgs.external["viaf"]["loader"].load(my_slice, max_slice)
    if "--ils" in sys.argv:
        my_slice = int(sys.argv[1])
        max_slice = int(sys.argv[2])
        cfgs.internal["ils"]["loader"].load(my_slice, max_slice)
    if "--wikidata" in sys.argv:
        my_slice = int(sys.argv[1])
        max_slice = int(sys.argv[2])
        cfgs.external["wikidata"]["loader"].load(my_slice, max_slice)

### LOAD INDEXES
if "--load-index" in sys.argv:
    if "--wikidata" in sys.argv or "--all" in sys.argv:
        # cfgs.instantiate('wikidata', 'external')
        cfgs.external["wikidata"]["indexLoader"].load()
    if "--viaf" in sys.argv or "--all" in sys.argv:
        cfgs.external["viaf"]["indexLoader"].load()
    if "--lcnaf" in sys.argv or "--all" in sys.argv:
        cfgs.external["lcnaf"]["indexLoader"].load()
    if "--lcsh" in sys.argv or "--all" in sys.argv:
        cfgs.external["lcsh"]["indexLoader"].load()
    if "--aat" in sys.argv or "--all" in sys.argv:
        cfgs.external["aat"]["indexLoader"].load()
    if "--ulan" in sys.argv or "--all" in sys.argv:
        cfgs.external["ulan"]["indexLoader"].load()

### RELOAD

if "--reload" in sys.argv:
    if "--tgn" in sys.argv:
        acq = cfgs.external["tgn"]["acquirer"]
        cfgs.external["tgn"]["fetcher"].enabled = True
        dc = cfgs.external["tgn"]["datacache"]
    elif "--ulan" in sys.argv:
        acq = cfgs.external["ulan"]["acquirer"]
        cfgs.external["ulan"]["fetcher"].enabled = True
        dc = cfgs.external["ulan"]["datacache"]

    elif "--wikidata" in sys.argv:
        acq = cfgs.external["wikidata"]["acquirer"]
        cfgs.external["wikidata"]["fetcher"].enabled = True
        rc = cfgs.external["wikidata"]["recordcache"]

        # For wikidata, fetch only retrieve ones that we've used

        acq.force_rebuild = True
        if len(sys.argv) > 4 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
            my_slice = int(sys.argv[1])
            max_slice = int(sys.argv[2])
        else:
            my_slice = max_slice = -1

        for recid in rc.iter_keys_slice(my_slice, max_slice):
            recid = cfgs.split_qua(recid)[0]
            acq.do_fetch(recid)
            sys.stdout.write(".")
            sys.stdout.flush()
        sys.exit()

    # For most, just refresh everything
    acq.force_rebuild = True
    if len(sys.argv) > 4 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
        my_slice = int(sys.argv[1])
        max_slice = int(sys.argv[2])
    else:
        my_slice = max_slice = -1

    for recid in dc.iter_keys_slice(my_slice, max_slice):
        acq.do_fetch(recid)
        sys.stdout.write(".")
        sys.stdout.flush()


### VALIDATION
if "--validate" in sys.argv:
    ignore_matches = []
    to_do = []
    for src, cfg in cfgs.internal.items():
        if f"--{src}" in sys.argv:
            to_do.append((src, cfg))

    if len(sys.argv) > 4 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
        my_slice = int(sys.argv[1])
        max_slice = int(sys.argv[2])
    else:
        my_slice = max_slice = -1

    v = cfgs.validator
    for src, cfg in to_do:
        rc = cfg["recordcache"]
        for rec in rc.iter_records_slice(my_slice, max_slice):
            sys.stdout.write(".")
            sys.stdout.flush()
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

### WRITE NEW IDMAP TOKEN

if "--new-token" in sys.argv:
    now = datetime.datetime.now()
    mm = f"0{now.month}" if now.month < 10 else now.month
    dd = f"0{now.day}" if now.day < 10 else now.day
    stok = f"__{now.year}{mm}{dd}"
    if idmap.update_token.startswith(stok):
        if idmap.update_token[-3].isalpha():
            letter = chr(ord(idmap.update_token[-3]) + 1)
        else:
            letter = "a"
        tok = f"{stok}{letter}__"
    else:
        tok = f"{stok}__"
    fn = os.path.join(cfgs.data_dir, "idmap_update_token.txt")
    with open(fn, "w") as fh:
        fh.write(f"{tok}\n")
    print(f"New update token: {tok}")


### EXPORT REFERENCE LIST
if "--write-refs" in sys.argv:
    ref_mgr = ReferenceManager(cfgs, idmap)
    ref_mgr.write_done_refs()
    done_refs.clear()


### Quick places export
if "--places" in sys.argv:
    rc = cfgs.results["merged"]["recordcache"]
    print("Starting")
    with open("places.jsonl", "w") as fh:
        for rec in rc.iter_records_type("Place"):
            fh.write(json.dumps(rec["data"], separators=(",", ":")))
            fh.write("\n")
    print("Done")

if "--concepts" in sys.argv:
    rc = cfgs.results["merged"]["recordcache"]
    print("Starting")
    with open("concepts.jsonl", "w") as fh:
        for rec in rc.iter_records_type("Concept"):
            fh.write(json.dumps(rec["data"], separators=(",", ":")))
            fh.write("\n")
    print("Done")

if "--periods" in sys.argv:
    rc = cfgs.results["merged"]["recordcache"]
    print("Starting")
    with open("periods.jsonl", "w") as fh:
        for rec in rc.iter_records_type("Period"):
            fh.write(json.dumps(rec["data"], separators=(",", ":")))
            fh.write("\n")
    print("Done")


### EXPORT AS NTRIPLES
if "--nt" in sys.argv:
    # parallelize
    if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
        my_slice = int(sys.argv[1])
        max_slice = int(sys.argv[2])
    else:
        my_slice = max_slice = -1

    from pipeline.sources.lux.qlever.mapper2 import QleverMapper

    mpr = QleverMapper(cfgs.results["marklogic"])
    rc = cfgs.results["merged"]["recordcache"]
    # FIXME: this path should go to config
    with gzip.open(f"/data-export/output/lux/nt/lux_{my_slice}.nt.gz", "wt", 1) as fh:
        if my_slice == -1:
            itr = rc.iter_keys()
        else:
            itr = rc.iter_keys_slice(my_slice, max_slice)

        x = 0
        start = time.time()
        for recid in itr:
            rec = rc[recid]
            res = mpr.transform(rec)
            for r in res:
                fh.write(f"{r}\n")
            x += 1
            if not x % 100000:
                print(f"{x} {time.time() - start}")
                sys.stdout.flush()


### CLEAN IDMAP

if "--clean-idmap" in sys.argv:
    killed = {None: 0}
    token = idmap.update_token
    x = 0
    total = 47300000
    start = time.time()
    for key in idmap.iter_keys(match="yuid:*", count=20000):
        x += 1
        val = idmap[key]
        done = False
        kill = False
        for v in val:
            if v.startswith("__"):
                if v.startswith("__2024"):
                    kill = True
                done = True
                break
        if not done:
            # no token at all
            kill = True
            v = None
        if kill:
            # Can't trash YUIDs directly, only kill their constituents
            for v2 in val:
                if not v2.startswith("__"):
                    try:
                        del idmap[v2]
                    except Exception as e:
                        print(f"Failed to delete {v2}: {e}")
            try:
                killed[v] += 1
            except:
                killed[v] = 1
        if not x % 100000:
            durn = int(time.time() - start)
            print(f"{x} in {durn} = {x / durn}/sec = {total / (x / durn)} total")
            print(killed)

    print("Cleaned idmap")
    print(killed)

if "--test-ils-idmap" in sys.argv:
    datacache = cfgs.internal["ils"]["datacache"]
    ttl = datacache.len_estimate()  # give or take
    x = 0
    old = []
    print("Starting...")
    start = time.time()
    for key in idmap.iter_keys(match="https://linked-art.library.yale.edu/*", count=20000):
        (uri, q) = cfgs.split_qua(key)
        ident = uri.rsplit("/", 1)[-1]
        if not ident in datacache:
            old.append(key)
        x += 1
        if not x % 50000:
            durn = int(time.time() - start)
            print(f"{x}/{ttl} = {x / durn}/sec = {ttl / (x / durn)}")
            print(f"    Found old: {len(old)} = {int(len(old) / x * 100)}% = {int(len(old) / x * ttl)} to go")
    with open("old_ils_idmap.txt", "w") as fh:
        for o in old:
            fh.write(f"{o}\n")

if "--clean-ils-idmap" in sys.argv:
    keep = [
        "03e31766-14b5-4e4b-a79a-595a7283c444",
        "7afbe7b3-fd94-464c-b598-ae56904307b0",
        "8197d709-73ce-4074-bf3a-aa5daf4c07c7",
        "adfd0ca5-84ed-4fa7-b564-3728cb89eabb",
    ]
    with open("old_ils_idmap.sort.txt") as fh:
        for l in fh.readlines():
            l = l.strip()
            cont = False
            for k in keep:
                if k in l:
                    cont = True
                    break
            if cont:
                continue
            try:
                yuid = idmap[l]
            except:
                continue
            if yuid is not None:
                res = idmap[yuid]
                del idmap[l]
                if len(res) == 2:
                    res.remove(l)
                    tok = res.pop()
                    if tok.startswith("__"):
                        idmap._remove(yuid, tok)


### CLEAR DATABASES

if "--clear" in sys.argv:
    cidx = sys.argv.index("--clear")
    cache = sys.argv[cidx + 1]
    # Now find the named cache
    (src, ctype) = cache.split("_", 1)
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

if "--vacuum" in sys.argv or "--optimize" in sys.argv:
    for c in cfgs.internal.values():
        for t in ["datacache", "recordcache", "recordcache2"]:
            if t in c and c[t] is not None:
                print(f"{c['name']}/{t}...")
                c[t].optimize()
    for c in cfgs.external.values():
        for t in ["datacache", "recordcache", "recordcache2"]:
            if t in c and c[t] is not None:
                print(f"{c['name']}/{t}...")
                c[t].optimize()
    for c in cfgs.results.values():
        for t in ["recordcache", "recordcache2"]:
            if t in c and c[t] is not None:
                print(f"{c['name']}/{t}...")
                c[t].optimize()


if "--counts" in sys.argv:
    ttl = 0
    for c in cfgs.internal.values():
        for t in ["datacache", "recordcache", "recordcache2"]:
            if t in c and c[t] is not None:
                est = c[t].len_estimate()
                pref = "~"
                if est < 100000:
                    est = len(c[t])
                    pref = "="
                print(f"{c['name']} {t}: {pref}{est}")
                ttl += est
    for c in cfgs.external.values():
        for t in ["datacache", "recordcache", "reconciledRecordcache", "recordcache2"]:
            if t in c and c[t] is not None:
                est = c[t].len_estimate()
                pref = "~"
                if est < 100000:
                    est = len(c[t])
                    pref = "="
                print(f"{c['name']} {t}: {pref}{est}")
                ttl += est
    for c in cfgs.results.values():
        for t in ["recordcache", "recordcache2"]:
            if t in c and c[t] is not None:
                est = c[t].len_estimate()
                pref = "~"
                if est < 100000:
                    est = len(c[t])
                    pref = "="
                print(f"{c['name']} {t}: {pref}{est}")
                ttl += est
    print(f"Total in Postgres: {ttl}")
    print(f"idmap: {len(idmap)}")
    print(f"references found: {len(all_refs)}")
    print(f"references done: {len(done_refs)}")

if "--clear-external" in sys.argv or "--clear-all" in sys.argv:
    for c in cfgs.external.values():
        for t in ["recordcache", "reconciledRecordcache", "recordcache2"]:
            if t in c and c[t] is not None:
                print(f"Clearing {c['name']} {t}")
                c[t].clear()
if "--clear-results" in sys.argv or "--clear-all" in sys.argv:
    for c in cfgs.results.values():
        for t in ["recordcache", "recordcache2"]:
            if t in c and c[t] is not None:
                print(f"Clearing {c['name']} {t}")
                c[t].clear()
if "--clear-internal" in sys.argv or "--clear-all" in sys.argv:
    for c in cfgs.internal.values():
        # Don't clear datacache, only computed
        for t in ["recordcache", "recordcache2"]:
            if t in c and c[t] is not None:
                print(f"Clearing {c['name']} {t}")
                c[t].clear()

if "--clear-refs" in sys.argv or "--clear-all" in sys.argv:
    print("Clearing Built Refs")
    all_refs.clear()
    print("Clearing Done Refs")
    done_refs.clear()

if "--clear-idmap" in sys.argv:
    # WARNING WARNING!
    idmap.clear()
