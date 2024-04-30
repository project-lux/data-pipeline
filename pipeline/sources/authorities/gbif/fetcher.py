
from pipeline.process.base.fetcher import Fetcher 
import requests

# URI is https://www.gbif.org/species/212
# API is https://api.gbif.org/v1/species/212

class GBIFFetcher(Fetcher):
	# def __init__(self, config):
	# 	Fetcher.__init__(self, config)

	def fetch(self, identifier):

		core = Fetcher.fetch(self, identifier) 
		drec = Fetcher.fetch(self,f"{identifier}/descriptions")
		srec = Fetcher.fetch(self,f"{identifier}/speciesProfiles")

		print(core)
		print(drec)
		print(srec)

#return {'data': data, 'source': self.name, 'identifier': identifier}

        # #toss out useless descriptions
        # if desc.startswith("Figs"):
        #     continue