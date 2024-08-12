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

equivrecs = {}

if rec:
	equivs = rec.get("equivalent",[])
	if equivs:
		for e in equivs:
			ident = e.get("id","")
			if ident:
				try:
					(src, identifier) = cfgs.split_uri(ident)
					print(src)
					print(identifier)
					break
				except:
					print("failed at line 44")
				try:
					cache = src['recordcache']
					identqua = identifier + "##qua" + typ
				except:
					print("failed at line 48")
				try:
					cacherec = cache[identqua]
				except:
					cacherec = None
					print(f"Could not fetch {identqua} from cache")
				if cacherec:
					data = cacherec['data']
					names = data['identified_by']
					for n in names:
						cont = n.get("content")
						if identqua in equivrecs:
							equivrecs[identqua].append(cont)
						else:
							equivrecs[identqua] = [cont]						
	else:
		print(f"No equivs in {uri}??")

for recs, names in equivrecs.items():
	print(f"Record {recs} is \n")
	for n in names:
		print(f"{names}\n")


