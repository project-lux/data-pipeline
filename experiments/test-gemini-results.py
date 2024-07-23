import os
import sys
import json
import requests
from dotenv import load_dotenv
from pipeline.config import Config
from cromulent.model import Place

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

#  Wikimedia API response for wikipedia --> wikidata
#  {"batchcomplete": "", "query": {"normalized": [{"from": "Narayanganj_District", "to": "Narayanganj District"}],
#   "pages": {"2096733": {"pageid": 2096733, "ns": 0, "title": "Narayanganj District", "pageprops": {"wikibase_item": "Q2208354"}}}}}

wd_data = cfgs.external["wikidata"]["datacache"]
wd_mapper = cfgs.external["wikidata"]["mapper"]
wd_acq = cfgs.external["wikidata"]["acquirer"]
merged = cfgs.results["merged"]["recordcache"]
wmuri = "https://{LANG}.wikipedia.org/w/api.php?format=json&action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&titles={PAGENAME}"
wdns = cfgs.external["wikidata"]["namespace"]
libns = cfgs.internal["ils"]["namespace"]

results = []
place_hash = {}
if os.path.exists("../data/files/results.jsonl.copy"):
    fh = open("../data/files/results.jsonl.copy")
    for l in fh.readlines():
        jss = json.loads(l)
        results.append(jss)
        place_hash[jss["child_id"][0]] = jss
    fh.close()

print(f"Testing {len(results)} rows")

test_res = []

tsv = open("testing.tsv", "w")

for r in results:
    if "wp" in r:
        wp = r["wp"]
        wpname = wp.rsplit("/", 1)[-1]


        if not "wp_wd" in r:
            # Could be overly specific


        qry = r["wp_wd"].get("query", {})
        if "pages" in qry:
            pg = list(qry["pages"].values())[0]
            if "pageprops" in pg and "wikibase_item" in pg["pageprops"]:
                wd = pg["pageprops"]["wikibase_item"]
                # Now look to see if this item is connected back in LUX
                cid = r["child_id"][0]
                yuid = idmap[libns + cid + "##quaPlace"]
                if yuid:
                    equivs = idmap[yuid]
                else:
                    yuid = ""
                    equivs = []
                wduri = wdns + wd

                sys.stdout.write(".")
                sys.stdout.flush()

                tr = [
                    libns + cid,  # Library record URI
                    wp,  # Wikipedia URI
                    wduri,  # Wikidata URI
                    yuid,  # LUX URI from idmap
                    r["child_id"][1],  # Name from Library
                    r.get("name", ""),  # Name from LLM
                    wpname,
                ]

                if wduri + "##quaPlace" in equivs:
                    tr.append("match!!")
                    test_res.append(tr)
                else:
                    wdrec = wd_acq.acquire(wd, rectype="Place")
                    if wdrec:
                        # Does it predict as a Place?
                        data = wd_data[wd]
                        guessed = wd_mapper.guess_type(data)
                        if guessed != Place:
                            # Probably bad
                            tr.append("NOT PLACE")
                        else:
                            tr.append("place okay")
                        try:
                            names = [x["content"] for x in wdrec["data"]["identified_by"]]
                            # print(f" ... WD Name 1: {names[0]}")
                            tr.append("WD Exists")
                            tr.append(names[0])
                        except:
                            # No names? Terrible record...
                            tr.append("WD Exists")
                            tr.append("...But No Names")
                    else:
                        tr.append("WD invalid")
                        tr.append("")

                    wyuid = idmap[wduri + "##quaPlace"]
                    if wyuid:
                        uu = wyuid.rsplit("/", 1)[-1]
                        luxrec = merged[uu]
                        if luxrec:
                            names = [x["content"] for x in luxrec["data"]["identified_by"] if x["type"] == "Name"]
                            # print(f" ... LUX name 1: {names[0]} ")
                            tr.append("WD in LUX")
                            tr.append(wyuid)
                            tr.append(names[0])
                    test_res.append(tr)

                tsv.write("\t".join(tr))
                tsv.write("\n")
                tsv.flush()
tsv.close()
