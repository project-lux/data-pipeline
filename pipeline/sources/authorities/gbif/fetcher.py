
from pipeline.process.base.fetcher import Fetcher 

# URI is https://www.gbif.org/species/212
# API is https://api.gbif.org/v1/species/212

class GBIFFetcher(Fetcher):
	# I don't think this is necessary
	# def __init__(self, config):
	# 	Fetcher.__init__(self, config)

	def fetch(self, identifier):
		#need to provide more error handling?
		core = super().fetch(identifier)
		drec = super().fetch(f"{identifier}/descriptions")
		srec = super().fetch(f"{identifier}/speciesProfiles")

		if drec:
			if rec['data']['results']:
				desc = {"description":[]}
				for d in drec['data']['results']:
					if d['description'].startswith("Figs"):
						continue
					desc['description'].append(d)

				core['data'].update(desc)

		if srec:
			if srec['data']['results']:
				altids = {"altids":[]}
				for a in srec['data']['results']:
					altids['altids'].append(a)
				
				core['data'].update(altids)

		return core
