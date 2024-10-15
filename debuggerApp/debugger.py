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
	output = []

	try: 
		rec = requests.get(uri).json()
	except:
		return (f"Failure: could not fetch uri {uri}")


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
							print(f"got equivlist from {identifier}")
							for v in equivlst:
								cid = v.get("id","")
								try:
									(src1, identifier1) = cfgs.split_uri(cid)
									cache1 = src1['recordcache']
									cachename1 = src1['name']
									identqua1 = identifier + "##qua" + typ
									cacherec1 = cache1[identqua1]
								except:
									cacherec1 = None
									output.append(f"Could not split uri on {cid}")
								if cacherec1:
									data1 = cacherec['data']
									names1 = data['identified_by']
									cont1 = names1[0]['content']
									if identifier not in recequivs:
										recequivs[identifier] = [f"{cid}:{cont1}"]
									elif identifier in recequivs:
										recequivs[identifier].append(f"{cid}:{cont1}")
						else:
							output.append(f"Record {ident} does not have any URI equivalents.")
					else:
						output.append(f"Could not fetch {identifier} from cache.")
					if option2:
						#do name-based reconciliation
						try:
							reconrec = reconciler.reconcile(cacherec)
						except:
							output.append(f"Could not reconcile {ident}")
							pass
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
										output.append(f"could not split uri on {cid}")
									if cacherec:
										data = cacherec['data']
										names = data['identified_by']
										cont = names[0]['content']
										if keyname not in recequivs:
											recequivs[keyname] = [f"{cid}:{cont}"]
										elif keyname in recequivs:
											recequivs[keyname].append(f"{cid}:{cont}")

	#recnames: key: each equivalent uri from original record: their PNs
	#recequivs: key: each equivalent uri from original record: their equivalents uris + PN
		else:
			return (f"No equivs in original uri: {uri}??")

	for rec, names in recnames.items():
		if rec in recequivs:
			output.append(f"Record {rec} is:\n {names}")
			eqv = recequivs[rec]
			output.append("And says it is...\n")
			for k in eqv:
				output.append(f"...{k}\n")

	return "\n".join(output)



