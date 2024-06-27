
from pipeline.process.base.reconciler import LmdbReconciler

class AatReconciler(LmdbReconciler):

    def extract_names(self, rec):
        aat_primaryName = self.configs.external['aat']['namespace'] + self.configs.globals_cfg['primaryName']
        aat_english = self.configs.external['aat']['namespace'] + self.configs.globals_cfg['lang_en']

        vals = []
        typ = rec['type']
        nms = rec.get('identified_by', [])
        for nm in nms:
            cxns = nm.get('classified_as', [])
            langs = nm.get('language', [])
            if aat_primaryName in [cx['id'] for cx in cxns] and 'content' in nm:
                if (langs and aat_english in [l['id'] for l in langs]) or not langs:
                    val = nm['content'].lower().strip()
                    vals.append(val)
        return vals

    def should_reconcile(self, rec, reconcileType="all"):
        if not LmdbReconciler.should_reconcile(self, rec, reconcileType):
            return False
        if 'data' in rec:
            rec = rec['data']

        if not rec['type'] in ['Type', 'Material', 'Currency', 'Language', 'MeasurementUnit']:
            return False
        elif 'equivalent' in rec:
            # Already reconciled
            eqids = [x['id'] for x in rec.get('equivalent', []) if \
                x['id'].startswith('http://vocab.getty.edu/aat/')]
            return not eqids
        else:
            return True

class UlanReconciler(LmdbReconciler):
        
    def should_reconcile(self, rec, reconcileType="all"):
        if not LmdbReconciler.should_reconcile(self, rec, reconcileType):
            return False
        if 'data' in rec:
            rec = rec['data']

        if rec['type'] in ['Group', 'Person']:
            # Don't second guess existing equivalent
            eqids = [x['id'] for x in rec.get('equivalent', []) if \
                x['id'].startswith('http://vocab.getty.edu/ulan/')]
            return not eqids
        else:
            return False