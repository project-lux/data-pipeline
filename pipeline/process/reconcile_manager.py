import tqdm
import sys
from concurrent.futures import ProcessPoolExecutor
from pipeline.cli.entry import cfgs
from pipeline.process.reconciler import Reconciler
from pipeline.process.reference_manager import ReferenceManager

class ReconcileManager:
    """
    manages reconcilation phase
    """
    def __init__(self,
                 configs,
                 max_workers: int = 0
                 ):
        self.configs = configs
        self.verbose = False
        self.disable_tqdm = False
        if max_workers > 0:
            self.max_workers = max_workers
        else:
            self.max_workers = configs.max_workers
        self.my_slice = -1
        self.sources = []
        self.ref_mgr = None
        self.reconciler = None
        self.total = 0

    def open_progress_bar(self, name):
        self.progress_bar = tqdm.tqdm(
            total=self.total,
            desc=f"{name}/{self.my_slice}",
            position=self.my_slice,
            leave=True)

    def close_progress_bar(self):
        if self.progress_bar is not None:
            self.progress_bar.close()

    def _handle_record(self, recid, cfg, rectype=None, distance=0):
        acquirer = cfg['acquirer']
        mapper = cfg['mapper']
        if acquirer.returns_multiple():
            recs = acquirer.acquire_all(recid, rectype=rectype)
        else:
            rec = acquirer.acquire(recid, rectype=rectype)
            if rec is not None:
                recs = [rec]
            else:
                recs = []
        if not recs:
            print(f" *** Failed to acquire any record for {name}/{recid} ***")
        for rec in recs:
            # Reconcile it
            rec2 = self.reconciler.reconcile(rec)
            mapper.post_reconcile(rec2)
            self.ref_mgr.walk_top_for_refs(rec2["data"], distance)
            self.ref_mgr.manage_identifiers(rec2)
        if self.progress_bar is not None:
            self.progress_bar.update(1)
        #sys.stdout.write('.');sys.stdout.flush()


    def _pool_reconcile_records(self, n):
        # Configure ourselves from global configs and CLI args
        self.configs = cfgs
        idmap = cfgs.get_idmap()
        networkmap = cfgs.instantiate_map("networkmap")["store"]
        self.reconciler = Reconciler(cfgs, idmap, networkmap)
        self.ref_mgr = ReferenceManager(cfgs, idmap)
        self.my_slice = n

        # Now're we're set up again, do reconcilation as slice n out of self.max_workers
        for (which, name, recids) in self.sources:
            cfg = getattr(cfgs, which)[name]
            if not recids:
                in_db = cfg["datacache"]
                if n > -1:
                    recids = in_db.iter_keys_slice(n, self.max_workers)
                    self.total = in_db.len_estimate() // self.max_workers
                else:
                    recids = in_db.iter_keys()
                    self.total = in_db.len_estimate()
            else:
                self.total = len(recids)

            if not self.disable_tqdm:
                self.open_progress_bar(name)
            for recid in recids:
                self._handle_record(recid, cfg)
            if not self.disable_tqdm:
                self.close_progress_bar()


    def _pool_reconcile_refs(self, n):
        self.configs = cfgs
        idmap = cfgs.get_idmap()
        self.ref_mgr = ReferenceManager(cfgs, idmap)
        self.my_slice = n

        self.total = self.ref_mgr.get_len_refs()
        if not self.disable_tqdm:
            self.open_progress_bar("references")
        item = 1
        while item:
            item = self.ref_mgr.pop_ref()
            try:
                (uri, dct) = item
                distance = dct["dist"]
            except:
                continue
            try:
                maptype = dct["type"]
            except:
                continue
            if distance > cfgs.max_distance:
                continue
            self.ref_mgr.did_ref(uri, distance)
            if cfgs.is_qua(uri):
                uri, rectype = cfgs.split_qua(uri)
            else:
                raise ValueError(f"No qua in referenced {uri} and needed")
            try:
                (source, recid) = cfgs.split_uri(uri)
            except:
                continue
            if not source["type"] == "external":
                raise ValueError(f"Got internal reference! {uri}")
            self._handle_record(recid, cfg, rectype, distance)
        if not self.disable_tqdm:
            self.close_progress_bar()


    def maybe_add(self, which, cfg):
        # Test if we should add it?
        self.sources.append((which, cfg['name'], []))

    def prepare_single(self, name) -> bool:
        if name in self.configs.internal:
            self.maybe_add('internal', self.configs.internal[name])
        elif name in self.configs.external:
            self.maybe_add('external', self.configs.external[name])
        else:
            raise ValueError(f"Unknown source: {name}")

    def prepare_all(self) -> bool:
        for cfg in self.configs.external.values():
            self.maybe_add('external', cfg)
        for cfg in self.configs.internal.values():
            self.maybe_add('internal', cfg)

    def reconcile(self, disable_tqdm=False, verbose=None, no_refs=False) -> bool:
        self.configs = None
        self.disable_tqdm = disable_tqdm
        self.verbose = verbose
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._pool_reconcile_records, n)
                for n in range(self.max_workers)
            ]
            results = [f.result() for f in futures]

        if not no_refs:
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self._pool_reconcile_refs, n)
                    for n in range(self.max_workers)
                ]
                results = [f.result() for f in futures]

        return all(results)
