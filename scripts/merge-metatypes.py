import glob
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lux_pipeline import runtime
from lux_pipeline.process.reference_manager import metatypes_pattern

# The slice files live under the configuration's temp_dir (lux metatypes
# does this merge too, and deletes them); sorted so per-key metatype list
# order doesn't depend on filesystem order
cfgs = runtime.get_configs(instantiate=False)
files = sorted(glob.glob(metatypes_pattern(cfgs)))

mt = {}

for f in files:
    with open(f) as fh:
        data = fh.read()
    js = json.loads(data)
    if not mt:
        mt = js
    else:
        for k,v in js.items():
            if not k in mt:
                mt[k] = v
            else:
                for i in v:
                    if not i in mt[k]:
                        mt[k].append(i)

with open('metatypes.json', 'w') as fh:
    outstr = json.dumps(mt)
    fh.write(outstr)

for f in files:
    os.remove(f)

