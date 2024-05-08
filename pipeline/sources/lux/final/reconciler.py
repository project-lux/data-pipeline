
from pipeline.process.base.reconciler import LmdbReconciler

class GlobalReconciler(LmdbReconciler):

    def __init__(self, config):
        super().__init__(config)
        if self.id_index is None:
            print(f"Could not find global equivalents index?")

        self.diff_index = None
        fn2 = config.get("differentDbPath", "")
        if fn2:
            self.diff_index = TabLmdb.open(fn2, 'r', readahead=False, writemap=True)
        else:
            print(f"No differentDbPath in merged for global differents index?")

    def reconcile(self, record, reconcileType="all"):
        ids = [x['id'] for x in record['data'].get('equivalent', [])]
        if 'id' in record['data']:
            ids.append(record['data']['id'])
        else:
            print(f"No id in {record}")
        for eq in ids:
            sames = self.id_index[eq]
            if sames:
                print(f"\nFOUND {eq} SAMES:{sames}\n")
                return list(sames)
        return None
