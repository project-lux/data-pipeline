import os
import sys
import json
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.storage.cache.postgres import poolman

import datetime
import io
import cProfile
import pstats
from pstats import SortKey

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.instantiate_map('idmap')['store']
cfgs.cache_globals()
cfgs.instantiate_all()

merged = cfgs.results['merged']['recordcache']
ml = cfgs.results['marklogic']['recordcache']
mapper = cfgs.results['marklogic']['mapper']

if '--profile' in sys.argv:
    sys.argv.remove('--profile')
    profiling = True
else:
    profiling = False

if len(sys.argv) > 2:
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])
else:
    my_slice = 0
    max_slice = 1

# Only reading from idmap, not writing, so can cache
idmap.enable_memory_cache()

if profiling:
    pr = cProfile.Profile()
    pr.enable()

if not os.path.exists(cfgs.exports_dir):
    os.mkdir(cfgs.exports_dir)

fn = os.path.join(cfgs.exports_dir, f'export_full_{my_slice}.jsonl')
outh = open(fn, 'w')
x = 0
for rec in merged.iter_records_slice(my_slice, max_slice):
    yuid = rec['yuid']
    if not yuid in ml:
        try:
            rec2 = mapper.transform(rec, rec['data']['type'])
        except Exception as e:
            print(f"{yuid} errored in final mapper: {e}")
            continue
        ml[yuid] = rec2
    else:
        rec2 = ml[yuid]['data']
    jstr = json.dumps(rec2, separators=(',',':'))
    outh.write(jstr)
    outh.write('\n')
    sys.stdout.write('.');sys.stdout.flush()
    x += 1
    if profiling and x >= 10000:
        break
outh.close()


if profiling:
    pr.disable()
    s = io.StringIO()
    sortby = SortKey.CUMULATIVE
    # sortby = SortKey.TIME
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats()
    print(s.getvalue())
    raise ValueError()

# Explicitly force all postgres connections to close
poolman.put_all('localsocket')

fh = open(os.path.join(cfgs.log_dir, "flags", f"export_is_done-{my_slice}.txt"), 'w')
fh.write('1\n')
fh.close()
