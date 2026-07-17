import glob
import logging
import os
import time

from ._task_ui_manager import TaskUiManager
from ..identity import AssertionWriter, resolve_identity_from_config
from ..reconciler import Reconciler
from ..reference_manager import ReferenceManager
from lux_pipeline.cli.entry import cfgs

logger = logging.getLogger("lux_pipeline")


class ReconcileManager(TaskUiManager):
    """
    manages reconcilation phase
    """

    def __init__(self, configs, max_workers: int = 0, args=None):
        super().__init__(configs, max_workers, args)
        self.no_refs = False
        self.ref_mgr = None
        self.reconciler = None
        self.assertion_log = None
        self.total = 0
        self.temp_log_h = None

    def _handle_record(self, recid, cfg, rectype=None, distance=0):
        acquirer = cfg["acquirer"]
        mapper = cfg["mapper"]
        if self.temp_log_h:
            self.temp_log_h.write(f"\n{cfg['name']}:{recid} ")
            self.temp_log_h.flush()
        if acquirer.returns_multiple():
            recs = acquirer.acquire_all(recid, rectype=rectype)
        else:
            rec = acquirer.acquire(recid, rectype=rectype)
            if rec is not None:
                recs = [rec]
            else:
                recs = []
        if not recs:
            self.log(logging.DEBUG, f"Failed to acquire any record for {cfg['name']}/{recid} ***")
        if self.temp_log_h:
            self.temp_log_h.write("a")
            self.temp_log_h.flush()
        for rec in recs:
            # Reconcile it
            rec2 = self.reconciler.reconcile(rec)
            if self.temp_log_h:
                self.temp_log_h.write("r")
                self.temp_log_h.flush()
            mapper.post_reconcile(rec2)
            self.ref_mgr.walk_top_for_refs(rec2["data"], distance)
            if self.temp_log_h:
                self.temp_log_h.write("m")
                self.temp_log_h.flush()
            # Log equivalence assertions; identity is resolved once,
            # deterministically, after all slices complete (see process()).
            # Reconcile workers no longer write to the idmap at all.
            self.assertion_log.write_record(rec2)
            if self.temp_log_h:
                self.temp_log_h.write("!")
                self.temp_log_h.flush()

        if not self.disable_ui:
            self.update_progress_bar(advance=1)
        return bool(recs)

    def _pool_reconcile_records(self, n):
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

    def _pool_reconcile_refs(self, n):
        self.log(logging.INFO, f"Starting references in {n}")
        self.total = self.ref_mgr.get_len_refs() // self.max_workers
        if not self.disable_ui:
            self.update_progress_bar(description="references", total=self.total)
        item = True
        done = 0
        fh = open(f"temp_log_{n}.txt", "w")
        self.temp_log_h = fh
        while item:
            item = self.ref_mgr.pop_ref()
            if item is None:
                l = self.ref_mgr.get_len_refs()
                if l == 0:
                    break
                else:
                    self.log(logging.CRITICAL, "ref_mgr.pop_ref() got None, but len is {l}")
                    # Sleep and let another task get any last one
                    time.sleep(0.5)
                    continue
            done += 1
            if done >= self.total - 1:
                self.total += self.ref_mgr.get_len_refs() // self.max_workers
                self.update_progress_bar(total=self.total)
            try:
                (uri, dct) = item
                distance = dct["dist"]
            except Exception:
                continue
            if distance > self.configs.max_distance:
                continue
            # Mark the reference done only after a successful acquire below.
            # Marking it done up-front meant a transient fetch failure
            # silently dropped the record -- and everything reachable only
            # through it -- for the whole build. An unmarked ref is re-queued
            # the next time something references it.
            quri = uri
            if self.configs.is_qua(uri):
                uri, rectype = self.configs.split_qua(uri)
            else:
                raise ValueError(f"No qua in referenced {uri} and needed")
            try:
                (source, recid) = self.configs.split_uri(uri)
            except Exception:
                # permanently unusable URI: mark done so it doesn't loop
                self.ref_mgr.did_ref(quri, distance)
                continue
            if not source["type"] == "external":
                raise ValueError(f"Got internal reference! {uri}")
            if self._handle_record(recid, source, rectype, distance):
                self.ref_mgr.did_ref(quri, distance)
        fh.close()
        self.log(logging.INFO, f"Writing metatypes in {n}")
        self.ref_mgr.write_metatypes(self.my_slice)
        self.log(logging.INFO, f"Done with metatypes in {n}")
        return True

    def _distributed(self, n):
        super()._distributed(n)
        self.idmap = self.configs.get_idmap()
        self.ref_mgr = ReferenceManager(self.configs, self.idmap)
        networkmap = self.configs.instantiate_map("networkmap")["store"]
        self.reconciler = Reconciler(self.configs, self.idmap, networkmap)
        self.assertion_log = AssertionWriter(self.configs, n, phase=self.phase)

        try:
            if self.phase == 1:
                self._pool_reconcile_records(n)
            elif self.phase == 2:
                self._pool_reconcile_refs(n)
        except Exception as e:
            self.log(logging.CRITICAL, f"Caught Exception: {e}")
            self.log(logging.CRITICAL, e)
            raise
        finally:
            self.assertion_log.close()
        return 1

    def maybe_add(self, which, cfg):
        # Test if we should add it?
        self.sources.append((which, cfg["name"], []))

    def process(self, layout, **args) -> bool:
        if "new_token" in args:
            logger.info("Creating new reconciliation token")
            idmap = cfgs.get_idmap()
            idmap.make_update_token()
        else:
            logger.info("Rebuilding with existing reconciliation token")

        # Stale assertion logs from a previous build would feed the identify
        # step assertions for records that no longer exist
        for fn in glob.glob(os.path.join(cfgs.temp_dir, "assertions-*.tsv")):
            os.remove(fn)

        self.phase = 1
        super().process(layout, **args)

        logger.info("Back from phase 1")

        if not self.no_refs:
            self.phase = 2
            # reuse the existing engine
            self.engine.process(layout)

        # Now tidy up in main thread
        logger.info("Back from phase 2")
        self.idmap = cfgs.get_idmap()
        ref_mgr = ReferenceManager(cfgs, self.idmap)
        logger.info("Merging metatypes")
        ref_mgr.merge_metatypes()
        logger.info("Writing done refs to file from redis")
        ref_mgr.write_done_refs()

        # Identity resolution: the workers above only logged sameAs
        # assertions; nothing has written to the idmap. Resolve clusters and
        # YUIDs once, deterministically, and bulk-load the result. Given the
        # same assertions, diffs and prior idmap this is identical regardless
        # of worker count or record arrival order.
        logger.info("Resolving identity")
        conflicts_fn = os.path.join(cfgs.temp_dir, "identity_conflicts.jsonl")
        stats = resolve_identity_from_config(cfgs, self.idmap,
                                             conflicts_file=conflicts_fn)
        logger.info(f"identity: nodes={stats['nodes']} pairs={stats['pairs']} "
                    f"diff_pairs={stats['diff_pairs']} clusters={stats['clusters']}")
        logger.info(f"identity: reused={stats['reused']} minted={stats['minted']} "
                    f"moved={stats['moved']} deleted_yuids={stats['deleted_yuids']}")
        if stats["conflicts"]:
            logger.warning(f"identity: {stats['conflicts']} refused assertions "
                           f"(see {conflicts_fn})")
        logger.info("Done, ready to merge")
