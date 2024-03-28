
from .base import WdConfigManager
from pipeline.process.base.reconciler import SqliteReconciler

class WdReconciler(SqliteReconciler, WdConfigManager):

    def __init__(self, config):
        WdConfigManager.__init__(self, config)
        SqliteReconciler.__init__(self, config)
        self.ext_hash = {
            "http://id.worldcat.org/fast/": "fast",
            "http://vocab.getty.edu/aat/": "aat",
            "http://vocab.getty.edu/ulan/": "ulan",
            "http://vocab.getty.edu/tgn/": "tgn",
            "https://viaf.org/viaf/": "viaf",
            "https://sws.geonames.org/": "geonames",      
            "https://d-nb.info/gnd/": "gnd",
            "http://id.loc.gov/vocabulary/": "lcvoc",
            "https://id.loc.gov/vocabulary/": "lcvoc",
            "http://id.loc.gov/authorities/subjects/": "lc",
            "http://id.loc.gov/authorities/names/": "lc",
            "https://www.idref.fr/": "idref",
            "https://archives.yale.edu/agents/": "aspace",
            "http://isni.org/isni/": "isni",          # isni:0000 0005 0342 4893
            "https://www.gbif.org/species/": "gbif",
            "https://eol.org/pages/": "eol",
            "https://www.worldcat.org/oclc/": "oclcnum",
            "https://ror.org/": "ror",
            "https://snaccooperative.org/ark:/99166/": "snac",
            "https://orcid.org/": "orcid",
            "https://id.loc.gov/authorities/performanceMediums/": "lcmp",
            "http://www.mimo-db.eu/InstrumentsKeywords/": "mimo",
            "http://id.ndl.go.jp/auth/ndlna/": "ndl",
            "http://id.ndl.go.jp/auth/ndlsh/": "ndl",
            "http://data.whosonfirst.org/": "wof",
            "https://libris.kb.se/": "snl",
            "https://bionomia.net/": "bionomia",
            "https://nsf.gov/data/awards/": "nsf",
            "https://ringgold.com/": "ringgold"    # ringgold:5755 == Yale/Q49112
        }

        # 'P305': 'lang', # this is the IETF/BCP47 language tag, not an actual URI
        # 'P1149': 'lcc', # https://id.loc.gov/authorities/classification/ Not useful?
        # 'P402': 'osm', # Open Street Map relation -- osm:2964696


    def should_reconcile(self, rec, reconcileType="all"):
        if not SqliteReconciler.should_reconcile(self, rec, reconcileType):
            return False
        if 'data' in rec:
            rec = rec['data']
        if 'equivalent' in rec:
            return True
        else:
            return False

    def reconcile(self, rec, reconcileType="all"):
        if not reconcileType in ['all', 'uri']:
            return None
        if self.should_reconcile(rec):
            # does entity exist in this dataset?
            if 'data' in rec:
                rec = rec['data']
            ids = {}
            truth = None
            eqids = [x['id'] for x in rec['equivalent']]
            for eq in eqids:
                # print(f" wikidata Checking {eq}")
                for (ns, prefix) in self.ext_hash.items():
                    # Should have been normalized by the time we get it here
                    if eq.startswith(ns):
                        key = eq.replace(ns, f"{prefix}:")
                        if key in self.id_index:
                            ids[key] = self.id_index[key]
                            # print(f"Found: {eq} --> {self.id_index[key]}")
                        elif "wikidata.org/" in eq:
                            # record assigned one already
                            truth = eq.rsplit('/',1)[1]
                        break # found the match, so break inner, and go to next eq
            idss = set(ids.values())
            if len(idss) == 1:
                recommend = list(ids.values())[0]
                if truth and truth != recommend:
                    print(f"WD: Record suggests {truth} and reconciler suggests {recommend} for {rec['id']}")
                    print("NOT making recommendation; but FIXME: add to log for manual check")                
                    return None
                return f"{self.namespace}{recommend}"
            elif not ids:
                return None
            else:
                # urgh, matches multiple!
                # Invert:
                ivtd = {}
                for (k,v) in ids.items():
                    try:
                        ivtd[v].append(k)
                    except:
                        ivtd[v] = [k]
                if self.debug: print(f"Record {rec['id']} matches multiple wikidata ids: {ivtd}")                        

                if truth and truth in ivtd:
                    # Just believe it
                    print(f"Wikidata value in record {truth} and in reconciler; not overriding, FIXME: add to log for manual check")
                    return None
                elif truth:
                    print(f"Wikidata value in record {truth} but NOT found from reconciliation! FIXME: add to log for manual check")
                    return None
                elif len(ids) > 2:
                    # Need at least three opinions for any sort of decision
                    if len(ivtd) == 2:
                        poss = [k for k in ivtd.keys() if len(ivtd[k]) > 1]
                        if len(poss) == 1:
                            return f"{self.namespace}{poss[0]}"
                    # strip viaf and geonames as often the cause of bad matches
                    poss = set([ids[k] for k in ids.keys() if not k.startswith('viaf:') and not k.startswith('geonames:')])
                    if len(poss) == 1:
                        return f"{self.namespace}{list(poss)[0]}"
                    # See if there's just a clear 2:1 or better majority?
                    counts = [(k, len(v)) for (k,v) in ivtd.items()]
                    counts.sort(key=lambda x: x[1], reverse=True)
                    if self.debug: print(f"id counts: {counts}")
                    if counts[0][1] >= (2*counts[1][1]):
                        return f"{self.namespace}{counts[0][0]}"
                else:
                    # Strip VIAF and see if that helps?
                    poss = set([ids[k] for k in ids.keys() if not k.startswith('viaf:') and not k.startswith('geonames:')])
                    if len(poss) == 1:
                        return f"{self.namespace}{list(poss)[0]}"                    

                return None
        return None
