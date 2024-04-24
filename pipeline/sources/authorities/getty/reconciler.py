
from pipeline.process.base.reconciler import LmdbReconciler

class AatReconciler(LmdbReconciler):

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