import os
import csv
import sys
import time
from lux_pipeline.storage.idmap.lmdb import TabLmdb
from ._managable import Managable
import logging
logger = logging.getLogger("lux_pipeline")

class IndexLoader(Managable):
    def __init__(self, config):
        super().__init__(config)
        self.in_cache = config["datacache"]
        self.namespace = config["namespace"]
        self.in_path = config.get("reconcileDumpPath", None)
        self.out_path = config.get("reconcileDbPath", None)
        self.inverse_path = config.get("inverseEquivDbPath", None)
        self.reconciler = config.get("reconciler", None)
        self.acquirer = config.get("acquirer", None)
        self.mapper = config.get("mapper", None)

        self.overwrite = True
        self.increment_total = False

    def get_storage(self):
        mapExp = self.config.get("mapSizeExponent", 30)
        # n = remove and recreate
        if self.out_path:
            index = TabLmdb.open(
                self.out_path, "c", map_size=2**mapExp, readahead=False, writemap=True
            )
        else:
            index = None
        if self.inverse_path:
            eqindex = TabLmdb.open(
                self.inverse_path,
                "c",
                map_size=2**mapExp,
                readahead=False,
                writemap=True,
            )
        else:
            eqindex = None
        return (index, eqindex)

    def export(self):
        # Once loaded, this will export from the LMDB to a CSV for sharing

        if self.out_path is not None:
            idx = TabLmdb.open(self.out_path, 'r', readahead=False, writemap=True)
            csvfn = self.out_path.rsplit('/')[-1].replace('lmdb', 'csv')
            outfn = os.path.join(self.configs.data_dir, csvfn)
            self.write_csv(idx, outfn)
        if self.inverse_path is not None:
            idx = TabLmdb.open(self.inverse_path, 'r', readahead=False, writemap=True)
            csvfn = self.inverse_path.rsplit('/')[-1].replace('lmdb', 'csv')
            outfn = os.path.join(self.configs.data_dir, csvfn)
            self.write_csv(idx, outfn)

    def write_csv(self, idx, outfn):
        with open(outfn, 'w') as csvh:
            writer = csv.writer(csvh, delimiter=',')
            for (k,v) in idx.items():
                if type(v) == list:
                    writer.writerow([k, "\t".join(v)])
                else:
                    writer.writerow([k, v])

    def extract_names(self, rec):
        return self.reconciler.extract_names(rec)

    def extract_uris(self, rec):
        return self.reconciler.extract_uris(rec)

    def acquire_record(self, rec):
        recid = rec["identifier"]
        res = self.acquirer.acquire(recid, store=False)
        return res

    def index_records(self, names=True, uris=True):
        # Default is to index from the cache

        try:
            ttl = self.in_cache.len_estimate()
            if not disable_ui:
                self.update_progress_bar(total=ttl)
            # Assume can store all names/uris in memory
            # If not, then override this in a subclass
            all_names = {}
            all_uris = {}
            self.manager.log(logging.INFO, f"Starting to load indexes for {self.name}...")
            for rec in self.in_cache.iter_records():
                self.increment_progress_bar()
                res = self.acquire_record(rec)
                if res is None:
                    continue
                recid = rec["identifier"]
                try:
                    typ = res["data"]["type"]
                except Exception:
                    typ = self.mapper.guess_type(res["data"])

                if recid and typ:
                    if names:
                        names = self.extract_names(res["data"])
                        for nm in names.keys():
                            if nm:
                                if len(nm) < 500:
                                    all_names[nm.lower()] = [recid, typ]
                                else:
                                    self.manager.log(logging.ERROR,
                                        f"Dropping name as too long ({len(nm)}>500):\n\t{nm}"
                                    )
                    if uris is not None:
                        eqs = self.extract_uris(res["data"])
                        for eq in eqs:
                            if eq:
                                all_uris[eq] = [recid, typ]

            return (all_names, all_uris)
        except Exception as e:
            self.manager.log(logging.ERROR, f"[red]Failed to build indexes for {self.name}")
            self.manager.log(logging.ERROR, e)
            return (None, None)


    def process(self, disable_ui=False, overwrite=True):
        self.overwrite = overwrite
        self.increment_total = self.total < 0

        (index, eqindex) = self.get_storage()
        if index is None and eqindex is None:
            self.manager.log(logging.ERROR, f"{self.name} has no indexes configured")
            return None
        if self.reconciler is None:
            self.reconciler = self.config["reconciler"]
        if self.mapper is None:
            self.mapper = self.config["mapper"]
        if self.acquirer is None:
            self.acquirer = self.config["acquirer"]

        try:
            self.manager.log(logging.INFO, f"Clearing existing indexes for {self.name}")
            # Clear all current entries
            if index is not None:
                index.clear()
            if eqindex is not None:
                eqindex.clear()
        except Exception as e:
            self.manager.log(logging.ERROR, f"Error clearing existing indexes for {self.name}")
            self.manager.log(logging.ERROR, e)
            return None

        (all_names, all_uris) = self.index_records(index is not None, eqindex is not None)

        if index is not None and all_names:
            start = time.time()
            index.update(all_names)
            durn = time.time() - start
            self.manager.log(logging.INFO, f"names insert time: {int(durn)} = {len(all_names)/durn}/sec")
        if eqindex is not None and all_uris:
            start = time.time()
            eqindex.update(all_uris)
            durn = time.time() - start
            self.manager.log(logging.INFO, f"uris insert time: {int(durn)} = {len(all_uris)/durn}/sec")

        if not disable_ui:
            self.close_progress_bar()

