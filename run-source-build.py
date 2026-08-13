import os
import sys
import ujson as json
import time
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.sources.lux.qlever.mapper2 import QleverMapper

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

# --- process command line arguments ---

my_slice = -1
max_slice = -1

if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])

if my_slice > -1:
    # Running in parallel, will cause cross-process errors
    idmap.disable_memory_cache()
else:
    # Running single, memory cache will remain accurate
    idmap.enable_memory_cache()

print("Starting...")
print(f"Update token is: {idmap.update_token}")
sys.stdout.flush()

sources = ['aat', 'ulan', 'lcsh', 'tgn']

x = 0
with gzip.open(f"/data-export/output/lux/nt/sources_{my_slice}.nt.gz", "wt", 1) as fh,\
     gzip.open(f"/data-export/output/lux/latest/sources_{my_slice}.jsonl.gz", "wt", 1) as fh2:
    for source in sources:
        src = cfgs.external[source]
        print(f" *** {source} ***")
        sys.stdout.flush()
        in_db = src["datacache"]
        out_db = src['recordcache']
        mapper = src["mapper"]
    
        ql_mpr = QleverMapper(src)
    
        out_db.defer_commits(every=1000)
        for rec in in_db.iter_records_slice(my_slice, max_slice):
            rec2 = mapper.transform(rec, None)
            if rec2 is not None:
                idq = cfgs.make_qua(rec2['identifier'], rec2['data']['type'])
                rec2['identifier'] = idq
                out_db[idq] = rec2
                out_db.checkpoint()

                # Export JSON to JSONL files
                jstr = json.dumps(rec2['data'])
                fh2.write(jstr + "\n")

                # Export NTriples
                try:    
                    res = ql_mpr.transform(rec2)
                    if res:
                        fh.write("\n".join(res))
                        fh.write("\n")
                except Exception as e:
                    print(f"*** {rec2.get('yuid', '?')} failed in the qlever mapper: {e}")
                    sys.stdout.flush()
                x += 1
                if not x % 100000:
                    print(f"{x} {time.time() - start}")
                    sys.stdout.flush()
        out_db.flush()
