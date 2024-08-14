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
	typ = "type"

typ = typ.title()

recnames = {}
recequivs = {}

if rec:
	equivs = rec.get("equivalent",[])
	if equivs:
		for e in equivs:
			ident = e.get("id","")
			if ident:
				(src, identifier) = cfgs.split_uri(ident)
				cache = src['recordcache']
				cachename = src['name']
				identqua = identifier + "##qua" + typ
				cacherec = cache[identqua]
				keyname = cachename + ": " + identqua
				if cacherec:
					data = cacherec['data']
					names = data['identified_by']
					for n in names:
						cont = n.get("content")
						if keyname in recnames:
							recnames[keyname].append(cont)
						else:
							recnames[keyname] = [cont]

					try:
						cacheequivs = data['equivalent']
					except:
						print(f"Record {keyname} has no equivalents")
						cacheequivs = None
					if cacheequivs:
						for c in cacheequivs:
							cid = c.get("id","")
							if cid:
								(src, identifier) = cfgs.split_uri(cid)
								cache = src['recordcache']
								cachename = src['name']
								identqua = identifier + "##qua" + typ
								cacherec = cache[identqua]
								if cacherec:
									data = cacherec['data']
									names = data['identified_by']
									pns = {cid:[]}
									for n in names:
										cont = n.get("content")
										pns[cid].append(n)
										if cont and keyname not in recequivs:
											recequivs[keyname] = [pns]
										elif cont and keyname in recequivs:
											recequivs[keyname].extend([pns])

##this is not doing exactly what I want, needs more work
#recnames: key: each equivalent uri from original record: their PNs
#recequivs: key: each equivalent uri from original record: their equivalents uris + PNs
	else:
		print(f"No equivs in {uri}??")

for rec, names in recnames.items():
	print(f"Record {rec} is \n")
	print(f"{names}\n")
	if rec in recequivs:
		eqv = recequivs[rec]
		for k, v in eqv.items():
			print(f"And says it is {k}:{v}\n")


