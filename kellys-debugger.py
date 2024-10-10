import os
import sys
import json
import time
import requests
from dotenv import load_dotenv
from pipeline.config import Config
from pipeline.process.reconciler import Reconciler


load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
#cfgs.debug_reconciliation = True  # Ensure debug is on
idmap = cfgs.get_idmap()
networkmap = cfgs.instantiate_map("networkmap")["store"]
cfgs.cache_globals()
cfgs.instantiate_all()


#given a uri, get the equivalents, check them in the recordcache, spit out the primary names
reconciler = Reconciler(cfgs, idmap , networkmap)

uri = sys.argv[1]
if "view" in uri:
	uri = uri.replace("view","data")

try: 
	rec = requests.get(uri).json()
except:
	print(f"Could not fetch uri {uri}")
	sys.exit()



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
				srctype = src['type']
				if src['type'] == "external":
					#identqua = identifier + "##qua" + typ
					continue
				else:
				 	identqua = identifier
				if identqua:
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
						equivs = data.get("equivalent",[])
						for e in equivs:
							cid = e.get("id","")
							try:
								(src, identifier) = cfgs.split_uri(cid)
								cache = src['recordcache']
								cachename = src['name']
								identqua = identifier + "##qua" + typ
								cacherec = cache[identqua]
							except:
								cacherec = None
								print(f"could not split uri on {cid}")
							if cacherec:
								data = cacherec['data']
								names = data['identified_by']
								cont = names[0]['content']
								if keyname not in recequivs:
									recequivs[keyname] = [f"{cid}:{cont}"]
								elif keyname in recequivs:
									recequivs[keyname].append(f"{cid}:{cont}")

						#reconcile cacherec
						# try:
						# 	reconrec = reconciler.reconcile(cacherec)
						# except:
						# 	print(f"Could not reconcile {ident}")
						# 	pass
						# if reconrec:
						# 	#copy of rec with all reconcilation done
						# 	reconlist = reconrec['data']['equivalent']
						# 	for c in reconlist:
						# 		cid = c.get("id","")
						# 		if cid:
						# 			try:
						# 				(src, identifier) = cfgs.split_uri(cid)
						# 				cache = src['recordcache']
						# 				cachename = src['name']
						# 				identqua = identifier + "##qua" + typ
						# 				cacherec = cache[identqua]
						# 			except:
						# 				cacherec = None
						# 				print(f"could not split uri on {cid}")
						# 			if cacherec:
						# 				data = cacherec['data']
						# 				names = data['identified_by']
						# 				cont = names[0]['content']
						# 				if keyname not in recequivs:
						# 					recequivs[keyname] = [f"{cid}:{cont}"]
						# 				elif keyname in recequivs:
						# 					recequivs[keyname].append(f"{cid}:{cont}")

#recnames: key: each equivalent uri from original record: their PNs
#recequivs: key: each equivalent uri from original record: their equivalents uris + PN
	else:
		print(f"No equivs in {uri}??")

for rec, names in recnames.items():
	print(f"Record {rec} is \n")
	print(f"{names}\n")
	if rec in recequivs:
		eqv = recequivs[rec]
		print("And says it is...\n")
		for k in eqv:
			print(f"...{k}\n")
	else:
		continue


