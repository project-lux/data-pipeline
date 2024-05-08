
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

    def reconcile(self, record, reconcileType="uri"):

        if not reconcileType in ["uri", "diffs"]:
            print(f"Called global reconciler with {reconcileType}; should be uri or diffs")
            raise ValueError(reconcileType)

        ids = [x['id'] for x in record['data'].get('equivalent', [])]
        if 'id' in record['data']:
            ids.append(record['data']['id'])
        else:
            print(f"No id in {record}")
            return []

        # Only have a map of uris, no types
        vals = []
        idx = self.id_index if reconcileType == "uri" else self.diff_index
        for eq in ids:
            try:
                s = idx[eq]
            except:
                continue
            if s:
                vals.append(s)
        return vals