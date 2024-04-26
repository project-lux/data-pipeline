import requests
import ujson as json
import sys
from lxml import etree

class Harvester(object):

	def __init__(self, config):
		self.overwrite = config.get('harvest_overwrite', True)
		self.last_harvest = config.get('last_harvest', "0001-01-01T00:00:00")
		self.harvest_from = config.get('harvest_from', "9999-01-01T00:00:00")
		self.prefix = config['name']
		self.namespace = config['namespace']
		self.fetcher = config.get('fetcher', None)
		self.seen = {}
		self.deleted = {}
		self.config = config

	def fetch_json(self, uri, typ):
		# generically useful fallback
		try:
			resp = requests.get(uri)
		except Exception as e:
			print(f"Failed to get anything from {typ} at {uri}: {e}")
			return {}
		try:
			what = resp.json()
		except Exception as e:
			print(f"Failed to get JSON from {typ} at {uri}: {e}")
			return {}
		return what

	def crawl(self, last_harvest=None):
		self.fetcher = self.config['fetcher']
		if self.fetcher is not None:
			self.fetcher.enabled = True
		self.seen = {}
		if last_harvest is not None:
			self.last_harvest = last_harvest
		self.deleted = {}


class PmhHarvester(Harvester):

	# "fetch": "https://photoarchive.paul-mellon-centre.ac.uk/apis/oai/pmh/v2?verb=GetRecord&metadataPrefix=lido&identifier={identifier}"

	def __init__(self, config):
		Harvester.__init__(self, config)
		self.endpoint = config['pmhEndpoint']
		self.metadataPrefix = config.get('pmhMetadataPrefix', 'oai_dc')
		self.namespaces = {"oai": "http://www.openarchives.org/OAI/2.0/"}
		# https://photoarchive.paul-mellon-centre.ac.uk/apis/oai/pmh/v2
		# https://snd.gu.se/oai-pmh

	def make_pmh_uri(self, verb, token=None):
		if token is None:
			return f"{self.endpoint}?verb={verb}&metadataPrefix={self.metadataPrefix}"
		else:
			return f"{self.endpoint}?verb={verb}&resumptionToken={token}"

	def fetch_pmh(self, uri):
		resp = requests.get(uri)
		dom = etree.XML(resp.text.encode("utf-8"))
		return dom


	def get_token(self, dom):
		token = dom.xpath('/oai:OAI-PMH/oai:ListIdentifiers/oai:resumptionToken/text()', namespaces=self.namespaces)
		if not token:
			return None
		else:
			return token[0]

	def process_page(self, dom):
		recs = dom.xpath('/oai:OAI-PMH/oai:ListIdentifiers/oai:header', namespaces=self.namespaces)
		for rec in recs:
			date = rec.xpath('./oai:datestamp/text()', namespaces=self.namespaces)[0]
			if date < self.last_harvest:
				return None
			# Otherwise we've not seen it
			ident = rec.xpath('./oai:identifier/text()', namespaces=self.namespaces)[0]
			try:
				itjs = self.fetcher.fetch(ident)
			except:
				raise
				# continue
			# PMH doesn't have types of change, so make everything an update
			yield ("update", ident, itjs, date)
			sys.stdout.write('.');sys.stdout.flush()

	def crawl(self, last_harvest=None):
		Harvester.crawl(self, last_harvest)
		start = self.make_pmh_uri('ListIdentifiers')
		dom = self.fetch_pmh(start)
		while dom is not None:
			sys.stdout.write('P');sys.stdout.flush()
			for p in self.process_page(dom):
				pass
			token = self.get_token(dom)
			if token:
				nxt = self.make_pmh_uri('ListIdentifiers', token)
				dom = self.fetch_pmh(nxt)
			else:
				dom = None
				break


class ASHarvester(Harvester):

	def __init__(self, config):
		Harvester.__init__(self, config)
		self.change_types = ['update', 'create', 'delete', 'move', 'merge', 'split', 'refresh']
		self.collections = config['activitystreams']
		self.collection_index = 0		
		self.page = config.get('start_page', None)
		self.page_cache = None
		self.datacache = None

	def fetch_collection(self, uri):
		coll = self.fetch_json(uri, 'collection')
		try:
			self.page = coll['last']['id']		
		except:
			self.page = None
			print(f"Failed to get last page from collection {uri}")

	def fetch_page(self):
		# fetch page in self.page
		print(f"    {self.page}")
		if self.page_cache is not None and self.page in self.page_cache:
			rec = self.page_cache[self.page]
			page = rec['data']
		else:
			page = self.fetch_json(self.page, 'page')
			if self.page_cache is not None and page is not None:
				self.page_cache[self.page] = page
		try:
			items = page['orderedItems']
			items.reverse()
		except:
			print(f"Failed to get items from page {self.page}")
			items = []
		try:
			prev = page.get('prev', {'id': ''})['id']
			if prev != self.page:
				# infinite loop
				self.page = prev
			else:
				self.page = None
		except:
			# This is normal behavior for first page
			self.page = None
		sys.stdout.write('P');sys.stdout.flush()
		return items

	def process_items(self, items, refsonly=False):
		for it in items:
			try:
				dt = it['endTime']
			except:
				print(f"Missing endTime for item:\n{it}")
				continue
			if dt < self.last_harvest:
				# We're done with the stream, not just this page
				self.page = None
				return

			try:
				chg = it['type'].lower()
				if not chg in self.change_types:
					chg = 'update'
			except:
				# just make it an update
				chg = 'update'

			if chg == 'refresh':
				# FIXME: Process refresh markers for deletes; for now just stop
				# This isn't in the AS from the units yet
				self.page = None
				print("Saw refresh token")
				break

			try:
				uri = it['object']['id']
			except:
				# FIXME: update state to flag bad entry
				print("no item id: {it}")
				continue

			# smush http/https to match namespace
			if uri.startswith('https://') and self.namespace.startswith('http://'):
				uri = uri.replace('https://', 'http://')
			elif uri.startswith('http://') and self.namespace.startswith('https://'):
				uri = uri.replace('http://', 'https://')

			ident = uri.replace(self.namespace, "")
			if ident in self.seen:
				# already processed, continue
				continue
			self.seen[ident] = 1

			if ident in self.deleted:
				continue
			if chg == 'delete':
				self.deleted[ident] = 1

			if self.harvest_from and dt > self.harvest_from:
				continue

			if refsonly:
				yield (chg, ident, {}, dt)
				continue

			if chg == 'delete':
				yield (chg, ident, {}, "")
				sys.stdout.write('X');sys.stdout.flush()
				continue

			# only fetch if insert_time on the datacache for the record is < dt
			if self.datacache is not None:
				try:
					tm = self.datacache.metadata(ident, 'insert_time')['insert_time']
				except TypeError:
					# NoneType is not subscriptable
					tm = None
				if tm is not None and tm.isoformat() > dt:
					# inserted after the change, no need to fetch
					continue

			if self.fetcher is None:
				try:
					itjs = self.fetch_json(uri, 'item')
				except:
					continue
			else:
				try:
					itjs = self.fetcher.fetch(ident)
				except:
					continue
			if not itjs:
				# Could have gotten None
				print(f"Harvester got {itjs} from {ident}")
				continue
			yield (chg, ident, itjs, dt)
			sys.stdout.write('.');sys.stdout.flush()

	# API function for Harvester
	def crawl(self, last_harvest=None, refsonly=False):
		Harvester.crawl(self, last_harvest)
		self.datacache = self.config['datacache']
		
		while self.collection_index < len(self.collections):
			if not self.page:
				collection = self.collections[self.collection_index]
				print(f" {collection}")
				self.fetch_collection(collection)
			while self.page:
				items = self.fetch_page()
				for rec in self.process_items(items, refsonly):
					yield rec
			self.collection_index += 1
			self.page = None
			