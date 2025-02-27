import os
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
reconciler = Reconciler(cfgs, idmap, networkmap)

TYPE_MAP = {
    "object": "HumanMadeObject",
    "digital": "DigitalObject",
    "concept": "Type",
    "text": "LinguisticObject",
    "visual": "VisualWork",
    "person": "Person",
    "group": "Group"
}

def fetch_json(uri):
    try:
        return requests.get(uri).json()
    except Exception as e:
        return {"records": [], "error": f"Failure: could not fetch uri {uri}"}

def fetch_cache_record(src, identifier, typ):
    try:
        cache = src['recordcache']
        identqua = identifier + "##qua" + typ if src['type'] == "external" else identifier
        return cache.get(identqua, None)
    except KeyError:
        return None

def extract_names(cacherec, identifier):
    try:
        names = cacherec['data']['identified_by']
        return [n.get("content") for n in names if "content" in n]
    except KeyError:
        print(f"Could not fetch names from {identifier}")
        return []

def process_equivalents(equivs, typ, option1, option2, recnames, recequivs):
    for e in equivs:
        ident = e.get("id", "")
        if not ident:
            continue
        src, identifier = cfgs.split_uri(ident)
        if src['type'] == "external" and option1:
            continue
        cacherec = fetch_cache_record(src, identifier, typ)
        if not cacherec:
            continue
        names = extract_names(cacherec, ident)
        recnames[ident] = recnames.get(ident, []) + names
        process_nested_equivalents(cacherec, ident, typ, recequivs)
        if option2:
            process_reconciliation(cacherec, ident, typ, recequivs)

def process_nested_equivalents(cacherec, ident, typ, recequivs):
    for v in cacherec['data'].get("equivalent", []):
        cid = v.get("id", "")
        if not cid:
            continue
        src, identifier = cfgs.split_uri(cid)
        cacherec = fetch_cache_record(src, identifier, typ)
        if cacherec:
            names = extract_names(cacherec, cid)
            if names:
                recequivs.setdefault(ident, []).append({
                    "uri": cid,
                    "name": names[0],
                    "added_by_reconciliation": False
                })

def process_reconciliation(cacherec, ident, typ, recequivs):
    try:
        reconrec = reconciler.reconcile(cacherec)
        if not reconrec:
            return
        for c in reconrec['data']['equivalent']:
            cid = c.get("id", "")
            if not cid:
                continue
            src, identifier = cfgs.split_uri(cid)
            cacherec = fetch_cache_record(src, identifier, typ)
            if cacherec:
                names = extract_names(cacherec, cid)
                if names:
                    recequivs.setdefault(ident, []).append({
                        "uri": cid,
                        "name": names[0],
                        "added_by_reconciliation": True
                    })
    except Exception:
        print(f"Error during reconciliation for {ident}")

def process_uri(uri, option1=False, option2=False):
    results = []
    rec = fetch_json(uri)
    if "error" in rec:
        return rec
    
    uri_typ = uri.rsplit("/", 2)[-2]
    typ = TYPE_MAP.get(uri_typ, None)
    recnames, recequivs = {}, {}
    process_equivalents(rec.get("equivalent", []), typ, option1, option2, recnames, recequivs)
    
    if not recnames:
        return {"records": [], "error": f"No equivalents found for the original URI: {uri}"}
    
    for rec, names in recnames.items():
        record_data = {
            "uri": rec,
            "names": names,
            "equivalents": recequivs.get(rec, [])
        }
        results.append(record_data)
    
    return {"records": results}
