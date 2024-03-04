
from .collector import Collector

class Reconciler(object):
    def __init__(self, config, idmap, networkmap):
        self.reconcilers = []
        self.debug = config.debug_reconciliation
        self.reconcileTypes = config.reconcile_record_types

        for src in config.external.values():
            rlr = src.get('reconciler', None)
            if rlr:
                self.reconcilers.append(rlr)
        for src in config.results.values():
            rlr = src.get('reconciler', None)
            if rlr:
                self.reconcilers.append(rlr)
        self.distinct = config.instantiate_map('distinct')['store']
        self.collector = Collector(config, idmap, networkmap)


    def reconcile(self, record):

        # We only process these types...
        if not record['data']['type'] in self.reconcileTypes:
            return record

        if self.debug: print(f"\n--- {rec['data']['id']} ---")
        leq = len(record['data'].get('equivalent', []))
        try:
            if self.debug: print("    (uris)")
            self.reconcile_uris(record)
            nleq = len(record['data'].get('equivalent', []))
            if nleq == leq and nleq < 3:
                # Try and reconcile based on names
                if self.debug: print("    (names)")
                self.call_reconcilers(record, reconcileType="name")
                n2leq = len(record['data'].get('equivalent', []))
                if n2leq > nleq:
                    if self.debug: print("    (uris)")                    
                    self.reconcile_uris(record)
        except:
            print(f"\nERROR --- reconciliation errored for {record['identifier']}")            
            raise
        return record

    def reconcile_uris(self, record):
        r_equivs = 1
        cr_equivs = 2
        # Check distinct / sameAs now
        try:
            while r_equivs == 1 or (not cr_equivs.issubset(r_equivs)):
                self.call_reconcilers(record, reconcileType="uri")
                r_equivs = set([x['id'] for x in record['data'].get('equivalent', [])])
                if self.debug: print(f"r_equivs: {r_equivs}")
                if cr_equivs == 2 or (not cr_equivs.issubset(r_equivs)):
                    if self.debug: print("      (collecting)")
                    self.collector.collect(record)
                    cr_equivs = set([x['id'] for x in record['data'].get('equivalent', [])])
                    if self.debug: print(f"cr_equivs: {cr_equivs}")                    
        except Exception as e:
            print(f"\nERROR: Reconciling broke for {record['source']}/{record['identifier']}: {e}")
            raise
        return record

    def call_reconcilers(self, record, reconcileType="all"):

        ids = [x['id'] for x in record['data'].get('equivalent', [])]
        new_equivs = True

        # sameAs is just a reconciler
        for eq in ids:
            diffs = self.distinct[eq]
            if diffs:
                for d in diffs:
                    if d in ids:
                        print(f"UHOH... Found two distinct entities already in equivalents: {d} and {eq} in {record['data']['id']}")
                        # FIXME: Just trash d and hope it's the right one?
                        # Can we do any better?
                        ids.remove(d)

        while new_equivs:
            new_equivs = False
            for r in self.reconcilers:
                try:
                    # Might return a single entry (reconciled to single source)
                    # or a list (if the reconciler knows multiple sources,
                    #   or if there's more than one actual match to add from a single source)
                    newids = r.reconcile(record, reconcileType=reconcileType)
                except:
                    print(r)
                    raise
                if newids:
                    if type(newids) != list:
                        newids = [newids]
                    for nid in newids:
                        if not nid in ids:
                            if self.debug: print(f" --- reconciler {r} / {reconcileType} found {nid} for {record['data']['id']}")
                            # Test distinct to avoid adding bad
                            diffs = self.distinct[nid]
                            okay_to_add = True
                            for d in diffs:
                                if d in ids:
                                    if self.debug: print(f"Found two distinct entities: {d} and {nid} in {record['data']['id']}")
                                    okay_to_add = False
                            if okay_to_add:
                                ids.append(nid)

            if ids:
                t = record['data']['type']
                lbl = record['data'].get('_label', '')
                if not 'equivalent' in record['data']:
                    record['data']['equivalent'] = []
                curr = [x['id'] for x in record['data']['equivalent']]
                for i in ids:
                    if not i in curr:
                        if self.debug: print(f"Adding {i} to record")
                        record['data']['equivalent'].append({"id": i, "type": t, '_label':lbl})
                        new_equivs = True
        return record
        