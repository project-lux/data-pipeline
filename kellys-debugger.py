import os
import sys
import json
import time
import requests
from dotenv import load_dotenv
from pipeline.config import Config


load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
cfgs.debug_reconciliation = True  # Ensure debug is on
idmap = cfgs.get_idmap()
networkmap = cfgs.instantiate_map("networkmap")["store"]
cfgs.cache_globals()
cfgs.instantiate_all()


#given a uri, get the equivalents, check them in the recordcache, spit out the primary names
#https://lux-front-sbx.collections.yale.edu/data/concept/d7964e61-cca4-408a-b726-35d1717cb7f6

uri = sys.argv[1]
try:
	rec = requests.get(uri).json()
except:
	print(f"Could not fetch uri {uri}")

typ = uri.rsplit("/",2)[-2]
if typ == "concept":
	typ == "type"

typ = typ.title()


if rec:
	equivs = rec.get("equivalent",[])
	if equivs:
		for e in equivs:
			ident = e.get("id","")
			if ident:
				(src, identifier) = cfgs.split_uri(ident)
				try:
					cache = cfgs.external[src]['recordcache']
				except:
					print(f"It doesn't work to fetch the cache this way")
				identqua = identifier + "##qua" + typ
				try:
					cacherec = cache[identqua]
				except:
					cacherec = None
					print(f"Could not fetch {identqua} from cache")



	else:
		print(f"No equivs in {uri}??")


