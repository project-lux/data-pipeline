import logging

from ._task_ui_manager import TaskUiManager
from lux_pipeline.process.reference_manager import ReferenceManager
from lux_pipeline.process.reidentifier import Reidentifier
from lux_pipeline.process.merger import MergeHandler

# This is the same pattern as ReconcileManager
# If there are improvements here, also make them there


class MergeManager(TaskUiManager):
    def __init__(self, configs, max_workers: int = 0, args=None):
        super().__init__(configs, max_workers, args)
        self.phase = 0
        self.no_refs = False
        self.total = 0
        order = sorted(
            [(x["namespace"], x.get("merge_order", -1)) for x in configs.external.values()], key=lambda x: x[1]
        )
        self.pref_order = [x[0] for x in order if x[1] >= 0]

    def _handle_record(self, recid, cfg, distance=0):
        rec = cfg["recordcache"][recid]
        if not rec:
            self.log(logging.WARNING, f"Couldn't find {cfg['name']} / {recid}")
            return None
        recuri = f"{cfg['namespace']}{recid}"
        qrecid = self.configs.make_qua(recuri, rec["data"]["type"])
        yuid = self.idmap[qrecid]
        if not yuid:
            self.log(logging.ERROR, f"Couldn't find YUID for record {qrecid}")
            return None
        yuid = yuid.rsplit("/", 1)[1]

        rec2 = self.reider.reidentify(rec)
        cfg["recordcache2"][rec2["yuid"]] = rec2["data"]

        equivs = self.idmap[rec2["data"]["id"]]
        if equivs:
            if qrecid in equivs:
                equivs.remove(qrecid)
            if recuri in equivs:
                equivs.remove(recuri)
            if self.idmap.update_token in equivs:
                equivs.remove(self.idmap.update_token)
        else:
            equivs = []

        rec3 = self.merger.merge(rec2, equivs)
        try:
            rec3 = self.final_mapper.transform(rec3, rec3["data"]["type"])
        except:
            self.log(logging.CRITICAL, f"Final transform raised exception for {rec2['identifier']}")
            raise
        if rec3 is not None:
            try:
                del rec3["identifier"]
            except Exception:
                pass
            self.merged_cache[rec3["yuid"]] = rec3
        else:
            self.log(logging.WARNING, f"Final transform returned None for {rec2['identifier']}")
        return rec3

    def _pool_merge_records(self, n):
        # Configure ourselves from global configs and CLI args
        self.log(logging.INFO, f"Starting records in {n}")
        for which, name, recids in self.sources:
            cfg = getattr(self.configs, which)[name]
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

    def _pool_merge_refs(self, n):
        self.log(logging.INFO, f"Starting references in {n}")
        for dist, ext_uri in self.ref_mgr.iter_done_refs(self.my_slice, self.max_workers):
            uri = self.idmap[ext_uri]
            if not uri:
                self.log(logging.WARNING, f"No YUID for reference {ext_uri} from done_refs")
                continue

            # find the best record to start from
            equivs = self.idmap[uri]
            stop = False
            for pref in self.pref_order:
                for eq in equivs:
                    if pref in eq:
                        (cfg, recid) = self.configs.split_uri(eq)
                        if recid in cfg["recordcache"]:
                            rec = self._handle_record(recid, cfg)
                            if rec is not None:
                                stop = True
                                break  # Break equivs
                if stop:
                    break  # break pref_order
            # next entry to process

    def _distributed(self, n):
        super()._distributed(n)
        cfgs = self.configs
        self.idmap = cfgs.get_idmap()
        self.idmap.enable_memory_cache()
        self.ref_mgr = ReferenceManager(cfgs, self.idmap)
        self.reider = Reidentifier(cfgs, self.idmap)
        self.merger = MergeHandler(cfgs, self.idmap, self.ref_mgr)
        self.merged_cache = cfgs.results["merged"]["recordcache"]
        self.merged_cache.config["overwrite"] = True
        self.final_mapper = cfgs.results["merged"]["mapper"]

        try:
            if self.phase == 1:
                self._pool_merge_records(n)
            elif self.phase == 2:
                self._pool_merge_refs(n)
        except Exception as e:
            self.log(logging.CRITICAL, "Caught Exception:")
            self.log(logging.CRITICAL, e)

    def maybe_add(self, which, cfg):
        # Test if we should add it?
        # cfg['recordcache'].len_estimate() > 0 ?
        self.sources.append((which, cfg["name"], []))

    def process(self, layout, **args) -> bool:
        self.phase = 1
        super().process(layout, **args)

        if not self.no_refs:
            self.phase = 2
            self.engine.process(layout)
