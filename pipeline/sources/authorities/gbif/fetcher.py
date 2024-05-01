
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

		coredata = core['data']
		if drec:
			desc = {"description":[]}
			for d in drec['data']['results']:
				if d['description'].startswith("Figs"):
					continue
				desc['description'].append(d)
			coredata.update(desc)

		if srec:
			altids = {"altids":[]}
			for a in srec['data']['results']:
				altids['altids'].append(a)
			coredata.update(altids)

		return core
