import os
import sys
import json
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.storage.cache.postgres import poolman

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
cfgs.cache_globals()
cfgs.instantiate_all()

merged = cfgs.results['merged']['recordcache']
ml = cfgs.results['marklogic']['recordcache']
mapper = cfgs.results['marklogic']['mapper']

if len(sys.argv) > 2:
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])
else:
    my_slice = 0
    max_slice = 1

fn = os.path.join(cfgs.exports_dir, f'export_full_{my_slice}.jsonl')
outh = open(fn, 'w')
for rec in merged.iter_records_slice(my_slice, max_slice):
    yuid = rec['yuid']
    try:
        rec2 = mapper.transform(rec, rec['data']['type'])
    except Exception as e:
        print(f"{yuid} errored in final mapper: {e}")
        continue
    ml[yuid] = rec2
    jstr = json.dumps(rec2, separators=(',',':'))
    outh.write(jstr)
    outh.write('\n')
    sys.stdout.write('.');sys.stdout.flush()
outh.close()

# Explicitly force all postgres connections to close
poolman.put_all('localsocket')

fh = open(os.path.join(cfgs.log_dir, "flags", f"export_is_done-{my_slice}.txt"), 'w')
fh.write('1\n')
fh.close()
