# One-time repack of the ORCID annual summaries dump into per-slice JSONL
# shards, so that every later load -- in every environment -- reads only the
# records it is actually going to store.
#
# The dump is a 46GB tar.gz: a single gzip stream, so nothing can seek into it.
# Every parallel loader therefore walks the whole archive to find its own
# slice (~30 mins), and 56% of what it decompresses is then thrown away by
# should_load(). This pays that once. Afterwards each loader worker reads its
# own shard: ~3GB, sequential, already filtered.
#
#   python split-orcid.py 24                          # one pass, 24 shards
#   python split-orcid.py 3 24                        # just shard 3 of 24
#   for i in `seq 0 23`; do nohup python split-orcid.py $i 24 & done
#
# The one-pass form reads the archive once but compresses serially, which is
# the expensive half (~90 mins). The sliced form divides the compression, at
# the cost of every process walking the archive -- the same read amplification
# the load has today, but paid once. Use fewer processes if the disk can't
# feed them; the shard count and the loader's worker count are independent.
#
# Shards land beside the dump, named after it, so a new annual dump never
# picks up the previous one's shards. Once they exist the orcid loader prefers
# them automatically -- delete them to go back to reading the .tgz. Re-run
# this if should_load() changes, since the filter is baked into the shards.

import os
import sys

from dotenv import load_dotenv

from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
cfgs.cache_globals()
cfgs.instantiate_all()

loader = cfgs.external["orcid"]["loader"]

# <slice> <count> writes just that shard; <count> on its own writes them all
# in one pass
if len(sys.argv) > 2 and sys.argv[1].isnumeric() and sys.argv[2].isnumeric():
    my_slice = int(sys.argv[1])
    max_slice = int(sys.argv[2])
elif len(sys.argv) > 1 and sys.argv[1].isnumeric():
    my_slice = None
    max_slice = int(sys.argv[1])
else:
    print("Usage: split-orcid.py <shards> | split-orcid.py <slice> <shards>")
    sys.exit(1)

if max_slice < 1 or (my_slice is not None and not 0 <= my_slice < max_slice):
    print(f"Slice {my_slice} is not in 0..{max_slice - 1}")
    sys.exit(1)

loader.split(my_slice, max_slice)
