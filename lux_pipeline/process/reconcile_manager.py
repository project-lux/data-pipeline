import sys
import logging

from ._task_ui_manager import TaskUiManager
from .reconciler import Reconciler
from .reference_manager import ReferenceManager


class ReconcileManager(TaskUiManager):
    """
    manages reconcilation phase
    """
    def __init__(self, configs, max_workers: int = 0):
        super().__init__(configs, max_workers)
        self.no_refs = False
        self.ref_mgr = None
        self.reconciler = None
        self.total = 0

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

        if not self.disable_ui:
            self.update_progress_bar(advance=1)

    def _pool_reconcile_records(self, n):
        # Configure ourselves from global configs and CLI args
        cfgs = self.configs
        idmap = cfgs.get_idmap()
        networkmap = cfgs.instantiate_map("networkmap")["store"]
        self.reconciler = Reconciler(cfgs, idmap, networkmap)
        self.ref_mgr = ReferenceManager(cfgs, idmap)
        self.my_slice = n

        self.log(logging.INFO, f"Starting records in {n}")

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

            if not self.disable_ui:
                self.update_progress_bar(description=name, total=self.total)
            for recid in recids:
                self._handle_record(recid, cfg)

    def _pool_reconcile_refs(self, n):
        cfgs = self.configs
        idmap = cfgs.get_idmap()
        self.ref_mgr = ReferenceManager(cfgs, idmap)
        self.my_slice = n

        self.total = self.ref_mgr.get_len_refs() // self.max_workers
        if not self.disable_ui:
            self.update_progress_bar(description="references", total=self.total)
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
        self.ref_mgr.write_metatypes(self.my_slice)

    def _distributed(self, bars, messages, n):
        super()._distributed(bars, messages, n)
        try:
            if self.phase == 1:
                self._pool_reconcile_records(n)
            elif self.phase == 2:
                self._pool_reconcile_refs(n)
        except Exception as e:
            self.log(logging.ERROR, "[red]Caught Exception:")
            self.log(logging.ERROR, e)

    def maybe_add(self, which, cfg):
        # Test if we should add it?
        self.sources.append((which, cfg['name'], []))

    def process(self, layout, **args) -> bool:
        self.phase = 1
        super().process(layout, **args)

        if not self.no_refs:
            self.phase = 2
            super().process(layout, **args)

        # Now tidy up
        ref_mgr = ReferenceManager(cfgs, idmap)
        logging.log(logging.INFO, "[green]Merging metatypes")
        ref_mgr.merge_metatypes()
        logging.log(logging.INFO, "[green]Writing done refs to file from redis")
        ref_mgr.write_done_refs()
        logging.log(logging.INFO, "[green]Done, ready to merge")

