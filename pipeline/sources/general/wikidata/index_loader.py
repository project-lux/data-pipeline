
import os
import sys
import json
import time
import gzip
from .loader import WdLoader
from .fetcher import WdFetcher
from .base import WdConfigManager
from pipeline.process.base.index_loader import IndexLoader
from sqlitedict import SqliteDict

class WdIndexLoader(IndexLoader, WdConfigManager):

	def __init__(self, config):
		WdConfigManager.__init__(self, config)
		self.in_cache = config['datacache']
		self.out_path = config['inverseEquivDbPath']
		self.index = SqliteDict(self.out_path, autocommit=False)

	def load(self):
		n = 0
		#ttl = len(self.in_cache)
		ttl = 100000000
		start = time.time()
		print("Starting...")
		for rec_int in self.in_cache.iter_records():
			sames, diffs = self.process_equivs(rec_int)
			for s in sames:
				self.index[s[0]] = s[1]

			n += 1
			if not n % 50000:
				self.index.commit()
				durn = time.time()-start
				print(f"{n} of {ttl} in {int(durn)} = {n/durn}/sec -> {ttl/(n/durn)} secs")
				sys.stdout.flush()
		self.index.commit()


class WdFileIndexLoader(IndexLoader, WdConfigManager):

	def __init__(self, config):
		WdConfigManager.__init__(self, config)
		self.in_cache = config['datacache']
		self.out_path = config['inverseEquivDbPath']
		self.index = SqliteDict(self.out_path, autocommit=False)

	def load(self):
		n = 0
		#ttl = len(self.in_cache)
		ttl = 23000000
		start = time.time()

		files = [x for x in os.listdir(self.configs.temp_dir) if x.startswith('wd_equivs_')]
		print("Starting...")
		for fn in files:
			print(fn)
			efh = open(os.path.join(self.configs.temp_dir, fn))
			l = efh.readline().strip()
			while l:
				(x,y) = l.rsplit(',', 1)
				self.index[x] = y
				n += 1
				l = efh.readline().strip()
				if not n % 50000:
					self.index.commit()
					durn = time.time()-start
					print(f"{n} of {ttl} in {int(durn)} = {n/durn}/sec -> {ttl/(n/durn)} secs")
					sys.stdout.flush()
			efh.close()
		self.index.commit()

		# Load diffs in the normal load-csv-map way


class RawWdIndexLoader(WdIndexLoader, WdLoader, WdFetcher):

	def __init__(self, config):
		WdIndexLoader.__init__(self, config)
		self.in_path = config['dumpFilePath']
		self.total = 100000000

	def load(self):

		fh = gzip.open(self.in_path)
		fh.readline() # chomp first line
		start = time.time()
		x = 0 
		done_x = 0
		l = 1

		while l:
			l = fh.readline()
			if not l:
				break
			if l[:100].find(b'"type":"property",') > 0:
				# filter properties
				continue
			l = l.decode('utf-8').strip()
			# Find id and check if already exists before processing JSON
			what = self.get_identifier_raw(l)
			try:
				js = json.loads(l[:-1])
			except:
				print(l)
				continue
			x += 1
			try:
				new = self.post_process_json(js, what)
			except Exception as e:
				print(f"Failed to process {l}: {e}")
				continue
			if not what:
				what = self.get_identifier_json(new)
				if not what:
					print(new)
					raise NotImplementedError(f"is get_identifier_raw or _json implemented for {self.__class__.__name__}?")

			for prop, pref in self.pref_map.items():
				if prop in new:
					val = new[prop]
					if type(val) == str:
						val = [val]
					elif type(val) == list:
						pass
					else:
						print(f"unknown identifier value type: {val}")
						continue
					for i in val:
						if type(i) != str:
							print(f"Unknown identifier item value type: {i}")
						else:
							self.index[f"{pref}:{i}"] = what

			# what: Q123, val: [Q567, Q678]
			#   index[Q123] = [Q567, Q678]
			#   index[Q567] = [Q123]
			#   index[Q678] = [Q123]

			#if self.different_prop in new:
			#	val = new[self.different_prop]
			#	if type(val) == str:
			#		val = [val]
			#	for i in val:
			#		pass

			if not x % 50000:
				t = time.time() - start
				xps = x/t
				ttls = self.total / xps
				print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")
				self.index.commit()

		fh.close()
		self.index.commit()


class CreatorIndexLoader(WdIndexLoader, WdLoader, WdFetcher):

	def __init__(self, config):
		self.in_path = config['dumpFilePath']
		self.out_path = config['inverseEquivDbPath'] + "_creators"
		self.index = SqliteDict(self.out_path, autocommit=False)		
		self.total = 100000000

	def load(self):

		fh = gzip.open(self.in_path)
		fh.readline() # chomp first line
		start = time.time()
		x = 0 
		done_x = 0
		l = 1

		while l:
			l = fh.readline()
			if not l:
				break
			if l[:100].find(b'"type":"property",') > 0:
				# filter properties
				continue
			# Find id and check if already exists before processing JSON
			what = self.get_identifier_raw(l)
			try:
				js = json.loads(l[:-2])
			except:
				print(l)
				continue
			x += 1
			try:
				new = self.post_process_json(js, what)
			except Exception as e:
				print(f"Failed to process {l}: {e}")
				continue
			if not what:
				what = self.get_identifier_json(new)
				if not what:
					print(new)
					raise NotImplementedError(f"is get_identifier_raw or _json implemented for {self.__class__.__name__}?")

			prop = "P170"

			if prop in new:
				val = new[prop]
				if type(val) == str:
					val = [val]
				elif type(val) == list:
					pass
				else:
					print(f"unknown identifier value type: {val}")
					continue
				for i in val:
					curr = self.index.get(i, [])
					curr.append(what)
					self.index[i] = curr

			if not x % 50000:
				t = time.time() - start
				xps = x/t
				ttls = self.total / xps
				print(f"{x} in {t} = {xps}/s --> {ttls} total ({ttls/3600} hrs)")

		fh.close()
		self.index.commit()
