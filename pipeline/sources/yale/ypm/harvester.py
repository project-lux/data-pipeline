
import ujson as json
from pipeline.process.base.harvester import ASHarvester

class YpmHarvester(ASHarvester):
	def fetch_json(self, uri, typ):
		# generically useful fallback
		uri = uri.replace('https://images.peabody.yale.edu/', 'http://10.5.33.13/')

		try:
			resp = self.session.get(uri)
			print(uri)
		except Exception as e:
			print(f"Failed to get anything from {typ} at {uri}: {e}")
			return {}
		try:
			what = json.loads(resp.text)
		except Exception as e:
			print(f"Failed to get JSON from {typ} at {uri}: {e}")
			return {}
		return what

