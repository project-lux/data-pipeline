
from pipeline.process.base.reconciler import Reconciler

class CsvReconciler(Reconciler):

    def __init__(self, config):
        configs = config['all_configs']
        self.sameAs = configs.instantiate_map('equivalents')['store']

    def reconcile(self, record, reconcileType="all"):
        ids = [x['id'] for x in record['data'].get('equivalent', [])]
        if 'id' in record['data']:
            ids.append(record['data']['id'])
        else:
            print(f"No id in {record}")
        for eq in ids:
            sames = self.sameAs[eq]
            if sames:
                print(f"\nFOUND {eq} SAMES:{sames}\n")
                return list(sames)
        return None
