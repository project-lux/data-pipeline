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

test_res = []

tsv = open("testing.tsv", "w")

for r in results:
    if "wp" in r:
        wp = r["wp"]
        wpname = wp.rsplit("/", 1)[-1]

        # fetch it from wikimedia and recache at end
        if 0:  # run through current known
            try:
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
                title = ""
            if "pages" in qry:
                pg = list(qry["pages"].values())[0]
                if "pageprops" in pg and "wikibase_item" in pg["pageprops"]:
                    wd = pg["pageprops"]["wikibase_item"]
                    # Now look to see if this item is connected back in LUX
                    cid = r["child_id"][0]
                    yuid = idmap[libns + cid + "##quaPlace"]
                    equivs = idmap[yuid]
                    wduri = wdns + wd

                    sys.stdout.write(".")
                    sys.stdout.flush()

                    tr = [cid, r["child_id"][1], r.get("name", ""), wpname, title, wduri, yuid]
                    print(f"{cid}/{r['child_id'][1]} = {r['wp']} = {wd}")
                    if wduri in equivs:
                        tr.append("match")
                        test_res.append(tr)
                    else:
                        wyuid = idmap[wduri + "##quaPlace"]
                        if wyuid:
                            # print(f" ... Assigned YUID: {wyuid}")
                            uu = wyuid.rsplit("/", 1)[-1]
                            luxrec = merged[uu]
                            names = [x["content"] for x in luxrec["data"]["identified_by"] if x["type"] == "Name"]
                            # print(f" ... LUX name 1: {names[0]} ")
                            tr.append("other")
                            tr.append(uu)
                            tr.append(names[0])
                            test_res.append(tr)
                        else:
                            # print(f" ... Unknown WD entry")
                            wdrec = wd_acq.acquire(wd, rectype="Place")
                            names = [x["content"] for x in wdrec["data"]["identified_by"]]
                            # print(f" ... WD Name 1: {names[0]}")
                            tr.append("not seen")
                            tr.append("")
                            tr.append(names[0])
                            test_res.append(tr)

                    tsv.write("\t".join(test_res))
                    tsv.flush()

tsv.close()