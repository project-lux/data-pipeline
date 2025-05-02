
import os
import sys
import ujson as json
import time
import gzip
from .loader import WdLoader
from .fetcher import WdFetcher
from .base import WdConfigManager
from lux_pipeline.process.base.index_loader import IndexLoader


class WdFileIndexLoader(IndexLoader):
	# Load diffs in the normal load-csv-map way
	def load(self):
		n = 0
		ttl = 23000000 # estimate as of 2024-04
		(index, eqindex) = self.get_storage()
		start = time.time()

		files = [x for x in os.listdir(self.configs.temp_dir) if x.startswith('wd_equivs_')]
		files.sort()
		print("Starting...")
		updates = {}
		for fn in files:
			print(fn)
			with open(os.path.join(self.configs.temp_dir, fn)) as efh:
				l = efh.readline().strip()

				while l:
					(x,y) = l.rsplit(',', 1)
					updates[x] = y
					n += 1
					l = efh.readline().strip()
					if not n % 100000:
						eqindex.update(updates)
						updates = {}
						# eqindex.commit()
						durn = time.time()-start
						print(f"{n} of {ttl} in {int(durn)} = {n/durn}/sec -> {ttl/(n/durn)} secs")
						sys.stdout.flush()

		eqindex.update(updates)
		# eqindex.commit()
