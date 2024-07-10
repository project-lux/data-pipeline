import os
import json
import requests
from dotenv import load_dotenv
from pipeline.config import Config

load_dotenv()
basepath = os.getenv("LUX_BASEPATH", "")
cfgs = Config(basepath=basepath)
idmap = cfgs.get_idmap()
cfgs.cache_globals()
cfgs.instantiate_all()

#  Wikimedia API response for wikipedia --> wikidata
#  {"batchcomplete": "", "query": {"normalized": [{"from": "Narayanganj_District", "to": "Narayanganj District"}],
#   "pages": {"2096733": {"pageid": 2096733, "ns": 0, "title": "Narayanganj District", "pageprops": {"wikibase_item": "Q2208354"}}}}}

wmuri = "https://{LANG}.wikipedia.org/w/api.php?format=json&action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&titles={PAGENAME}"

results = []
place_hash = {}
if os.path.exists("../data/files/results.jsonl.copy"):
    fh = open("../data/files/results.jsonl.copy")
    for l in fh.readlines():
        jss = json.loads(l)
        results.append(jss)
        place_hash[jss["child_id"][0]] = jss
    fh.close()

wd_acq = cfgs.external["wikidata"]["acquirer"]

for r in results:
    if "wp" in r:
        # fetch it from wikimedia and recache at end
        try:
            wp = r["wp"]
            wpname = wp.rsplit("/", 1)[-1]
            lang = wp.replace("https://", "")
            lang = lang.replace("http://", "")  # just in case
            lang = lang.split(".")[0]
            if lang != "en" or not "wp_wd" in r:
                print(f"Fetching: {wpname} from {lang}")
                resp = requests.get(wmuri.replace("{PAGENAME}", wpname).replace("{LANG}", lang))
                js = resp.json()
                r["wp_wd"] = js
        except Exception as e:
            print("Wikimedia API request raised exception:")
            print(e)
        if "wp_wd" in r:
            qry = r["wp_wd"].get("query", {})
            if "normalized" in qry:
                title = qry["normalized"][0]["to"]
            else:
                title = r["wp"].rsplit("/", 1)[-1]
            if "pages" in qry:
                pg = list(qry["pages"].values())[0]
                if "pageprops" in pg and "wikibase_item" in pg["pageprops"]:
                    wd = pg["pageprops"]["wikibase_item"]
                    # Now look to see if this item is connected back in LUX
                    print(f"{r['child_id'][0]}/{r['child_id'][1]} = {r['wp']} = {wd}")
                    wdrec = wd_acq.acquire(wd, rectype="Place")
                    names = [x["content"] for x in wdrec["data"]["identified_by"]]
                    print(names)
            else:
                print("no pages")
