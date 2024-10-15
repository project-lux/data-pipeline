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
idmap = cfgs.get_idmap()
networkmap = cfgs.instantiate_map("networkmap")["store"]
cfgs.cache_globals()
cfgs.instantiate_all()
reconciler = Reconciler(cfgs, idmap , networkmap)

#given a uri, get the equivalents, check them in the recordcache, spit out the primary names

def process_uri(uri, option1=False, option2=False):
	results = []

	try: 
		rec = requests.get(uri).json()
	except:
		return {"records": [], "error": f"Failure: could not fetch uri {uri}"}

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
					if src['type'] == "external":
						if option1:
							continue
						else:
							identqua = identifier + "##qua" + typ
					else:
						identqua = identifier
					cacherec = cache[identqua]
					if cacherec:
						data = cacherec['data']
						names = data['identified_by']
						for n in names:
							cont = n.get("content")
							if ident in recnames:
								recnames[ident].append(cont)
							else:
								recnames[ident] = [cont]
						equivlst = data.get("equivalent",[])
						if equivlst:
							for v in equivlst:
								cid = v.get("id","")
								try:
									(src, identifier) = cfgs.split_uri(cid)
									#these are all external
									cache = src['recordcache']
									cachename = src['name']
									identqua = identifier + "##qua" + typ
									cacherec = cache[identqua]
								except:
									cacherec = None
									continue
								if cacherec:
									data = cacherec['data']
									names = data['identified_by']
									cont = names[0]['content']
									if ident not in recequivs:
										recequivs[ident] = [f"{cid}:{cont}"]
									elif ident in recequivs:
										recequivs[ident].append(f"{cid}:{cont}")
						if not equivlst and not option2:
							if ident not in recequivs:
								recequivs[ident] = []

					if option2:
						#do name-based reconciliation
						try:
							reconrec = reconciler.reconcile(cacherec)
						except:
							reconrec = None
						if reconrec:
							#copy of rec with all reconcilation done
							reconlist = reconrec['data']['equivalent']
							for c in reconlist:
								cid = c.get("id","")
								if cid:
									try:
										(src, identifier) = cfgs.split_uri(cid)
										cache = src['recordcache']
										cachename = src['name']
										identqua = identifier + "##qua" + typ
										cacherec = cache[identqua]
									except:
										cacherec = None
										continue
									if cacherec:
										data = cacherec['data']
										names = data['identified_by']
										cont = names[0]['content']
										if ident not in recequivs:
											recequivs[ident] = [f"{cid}:{cont}"]
										elif ident in recequivs:
											recequivs[ident].append(f"{cid}:{cont}")

	#recnames: key: each equivalent uri from original record: their PNs
	#recequivs: key: each equivalent uri from original record: their equivalents uris + PN
		else:
            return {"records": [], "error": f"No equivalents found for the original URI: {uri}"}

	for rec, names in recnames.items():
		record_data = {
			"uri": rec,
			"names": names,
			"equivalents": []
		}
		if rec in recequivs:
			equivalents = recequivs[rec]
			for equivs in equivalents:
				parts = equiv.split(":")
				if len(parts) == 2:
					record_data["equivalents"].append({
						"uri":parts[0],
						"names":parts[1]
						})
		results.append(record_data)

	return {"records":results}



