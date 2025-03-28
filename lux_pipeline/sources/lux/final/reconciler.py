
from lux_pipeline.process.base.reconciler import LmdbReconciler, TabLmdb
import logging
logger = logging.getLogger("lux_pipeline")

class GlobalReconciler(LmdbReconciler):

    def __init__(self, config):
        super().__init__(config)
        if self.id_index is None:
            logger.warning(f"Could not find global equivalents index?")

        self.diff_index = None
        fn2 = config.get("differentDbPath", "")
        try:
            if fn2:
                self.diff_index = TabLmdb.open(fn2, 'r', readahead=False, writemap=True)
            else:
                # print(f"No differentDbPath in merged for global differents index?")
                pass
        except:
            #print(f"No differentDbPath in merged for global differents index?")
            pass

    def reconcile(self, record, reconcileType="uri"):

        if not reconcileType in ["uri", "diffs"]:
            # we'll get called with name from call_reconcilers
            # and harder to prevent than just return []
            return None

        if type(record) == str:
            ids = [record]
        else:
            try:
                ids = [x['id'] for x in record['data'].get('equivalent', [])]
                if 'id' in record['data']:
                    ids.append(record['data']['id'])
                else:
                    logger.error(f"No id in {record}")
                    return []
            except:
                ids = None

        # Only have a map of uris, no types
        vals = set([])
        idx = self.id_index if reconcileType == "uri" else self.diff_index
        if ids:
            for eq in ids:
                try:
                    s = idx[eq]
                except:
                    continue
                if s:
                    if type(s) in [set, list]:
                        vals.update(s)
                    elif type(s) == str:
                        vals.add(s)
        return vals
