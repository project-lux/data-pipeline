import os
import sys
import ujsonjson
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv('LUX_BASEPATH', "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

loader = cfgs.internal['ils']['indexLoader']


# Dedicated LMDB index for heading mappings
heading_index = loader.load_index()
filename = sys.argv[-1]
jsonfn = os.path.join(cfgs.data_dir, filename)


if "--clear" in sys.argv:
    loader.clear()

if "--update" in sys.argv:
    loader.update(jsonfn)

if not os.path.exists(jsonfn):
    print(f"That file ({jsonfn}) does not exist")
    sys.exit(0)

BATCH_SIZE = 10000
batch = []

with open(jsonfn, encoding='utf-8') as fh:
    data = ujson.load(fh)
    for key, value_list in data.items():
        values = [json.dumps(v, ensure_ascii=False) for v in value_list]
        batch.append((key, values))
        if len(batch) >= BATCH_SIZE:
            for key, values in batch:
                loader.set(heading_index, key, values)
            batch = []
    if batch:
        # Process any remaining items in the batch
        for key, values in batch:
            loader.set(heading_index, key, values)


print(f"Loaded heading map from {jsonfn}")

