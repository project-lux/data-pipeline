from pipeline.process.base.reconciler import LmdbReconciler

class ViafReconciler(LmdbReconciler):

    def __init__(self, config):
        LmdbReconciler.__init__(self, config)

        self.viaf_prefixes = {
            "ISNI": "isni",
            "WKP": "wikidata",
            "NDL": "japan",
            "DNB": "dnb",
            "BNF": "bnf",
            "LC": "lcnaf",
            "LCSH": "lcsh",
            "JPG": "ulan",
            "FAST": "fast"
        }

        self.ext_hash = {
            "http://id.worldcat.org/fast/": "fast",
            "http://vocab.getty.edu/ulan/": "ulan",    
            "https://d-nb.info/gnd/": "dnb",
            "http://id.loc.gov/authorities/subjects/": "lcsh",
            "http://id.loc.gov/authorities/names/": "lcnaf",
            "http://isni.org/isni/": "isni",
            "http://www.wikidata.org/entity/": "wikidata"
        }


    def should_reconcile(self, rec, reconcileType="all"):
        if not LmdbReconciler.should_reconcile(self, rec, reconcileType):
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
                for (ns, prefix) in self.ext_hash.items():
                    # Should have been normalized by the time we get it here
                    if eq.startswith(ns):
                        key = eq.replace(ns, f"{prefix}:")
                        if key in self.id_index:
                            ids[key] = self.id_index[key]
                            # print(f"Found: {eq} --> {self.index[key]}")
                        elif "/viaf/" in eq:
                            # record assigned one already
                            truth = eq.rsplit('/',1)[1]
                        break # found the match, so break inner, and go to next eq
            idss = set(ids.values())
            if len(idss) == 1:
                recommend = list(ids.values())[0]
                if truth and truth != recommend:
                    print(f"VIAF: Record suggests {truth} and reconciler suggests {recommend} for {rec['id']}")
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
                if self.debug: print(f"Record {rec['id']} matches multiple VIAF ids: {ivtd}")                        

                if truth and truth in ivtd:
                    # Just believe it
                    print(f"VIAF value in record {truth} and in reconciler; not overriding")
                    return None
                elif truth:
                    print(f"VIAF value in record {truth} but NOT found from reconciliation!")
                    return None
                elif len(ids) > 2:
                    # Need at least three opinions for any sort of decision
                    if len(ivtd) == 2:
                        poss = [k for k in ivtd.keys() if len(ivtd[k]) > 1]
                        if len(poss) == 1:
                            return f"{self.namespace}{poss[0]}"

                    # See if there's just a clear 2:1 or better majority?
                    counts = [(k, len(v)) for (k,v) in ivtd.items()]
                    counts.sort(key=lambda x: x[1], reverse=True)
                    if self.debug: print(f"viaf id counts: {counts}")
                    if counts[0][1] >= (2*counts[1][1]):
                        return f"{self.namespace}{counts[0][0]}"                  
                return None
        return None
