import glob
import os
import sys
import json

files = glob.glob("metatypes-*.json")

mt = {}

for f in files:
    fh = open(f)
    data = fh.read()
    fh.close()
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

fh = open('metatypes.json', 'w')
outstr = json.dumps(mt)
fh.write(outstr)
fh.close()
