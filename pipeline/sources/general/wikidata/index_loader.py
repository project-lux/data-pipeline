
import os
import sys
import json
import time
import gzip
from .loader import WdLoader
from .fetcher import WdFetcher
from .base import WdConfigManager
from pipeline.process.base.index_loader import LmdbIndexLoader


class WdFileIndexLoader(LmdbIndexLoader):
	# Load diffs in the normal load-csv-map way
	def load(self):
		n = 0
		ttl = 23000000 # estimate as of 2024-04
		(index, eqindex) = self.get_storage()
		start = time.time()

		files = [x for x in os.listdir(self.configs.temp_dir) if x.startswith('wd_equivs_')]
		print("Starting...")
		for fn in files:
			print(fn)
			efh = open(os.path.join(self.configs.temp_dir, fn))
			l = efh.readline().strip()
			while l:
				(x,y) = l.rsplit(',', 1)
				eqindex[x] = y
				n += 1
				l = efh.readline().strip()
				if not n % 50000:
					eqindex.commit()
					durn = time.time()-start
					print(f"{n} of {ttl} in {int(durn)} = {n/durn}/sec -> {ttl/(n/durn)} secs")
					sys.stdout.flush()
			efh.close()
		eqindex.commit()

