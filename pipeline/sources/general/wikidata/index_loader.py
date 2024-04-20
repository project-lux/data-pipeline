
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


		# Make sure to update the inverse in reconciler
		# if you add something to pref_map
		self.pref_map = {
			'P244': 'lc',
			'P2163': 'fast',
			'P214': 'viaf',
			'P1014': 'aat',
			'P245': 'ulan',
			'P1667': 'tgn',
			'P8902': 'aspace',
			'P1566': 'geonames',
			'P6766': 'wof',
			'P846': 'gbif',
			'P830': 'eol',
			'P6944': 'bionomia',
			'P243': 'oclcnum',
			'P305': 'lang',
			'P4801': 'lcvoc',
			'P1149': 'lcc',
			'P269': 'idref',
			'P227': 'gnd',
			'P213': 'isni',
			'P11858': 'nsf',
			'P3500': 'ringgold',
			'P6782': 'ror', # https://ror.org/00qb4ds53
			'P3430': 'snac', # https://snaccooperative.org/ark:/99166/{ident}
			'P496': 'orcid', # https://orcid.org/<ident>
			'P8516': 'lcpm', # https://id.loc.gov/authorities/performanceMediums/{ident}
			'P3763': 'mimo', # http://www.mimo-db.eu/InstrumentsKeywords/{ident}
			'P402': 'osm', # Open Street Map relation id
			'P349': 'ndl', # Japan
			'P5587': 'snl' # Sweden
		}
		self.different_prop = "P1889"

	def load(self):
		n = 0
		#ttl = len(self.in_cache)
		ttl = 100000000
		start = time.time()
		print("Starting...")
		for rec_int in self.in_cache.iter_records():
			rec = rec_int['data']
			recid = rec['id']
			for prop, pref in self.pref_map.items():
				if prop in rec:
					val = rec[prop]
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
							self.index[f"{pref}:{i}"] = recid
			if self.different_prop in rec:
				val = rec[self.different_prop]
				if type(val) == str:
					val = [val]
				for i in val:
					if type(i) == str:
						pass

			n += 1
			if not n % 50000:
				self.index.commit()
				durn = time.time()-start
				print(f"{n} of {ttl} in {int(durn)} = {n/durn}/sec -> {ttl/(n/durn)} secs")
				sys.stdout.flush()
		self.index.commit()


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
