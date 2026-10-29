
import logging
import os
import sys
import ujson as json
import time
import gzip
from .loader import WdLoader
from .fetcher import WdFetcher
from .base import WdConfigManager
from lux_pipeline.process.base.index_loader import LmdbIndexLoader

logger = logging.getLogger("lux_pipeline")


class WdFileIndexLoader(LmdbIndexLoader):
	# Load diffs in the normal load-csv-map way
	def load(self):
		n = 0
		ttl = 23915661 # estimate as of 2024-04
		(index, eqindex) = self.get_storage()
		start = time.time()

		files = [x for x in os.listdir(self.configs.temp_dir) if x.startswith('wd_equivs_')]
		updates = {}
		lines = []
		logger.info("Reading...")
		for fn in files:
			with open(os.path.join(self.configs.temp_dir, fn)) as efh:
				lines.extend(efh.readlines())

		logger.info("Sorting...")
		lines.sort()

		logger.info("Making dict")
		for l in lines:
			(x,y) = l.strip().rsplit(',', 1)
			updates[x] = y

		logger.info("Writing index")
		eqindex.update(updates)
