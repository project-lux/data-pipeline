import os
import csv
import time

from ._managable import Managable
import logging

logger = logging.getLogger("lux_pipeline")


class IndexLoader(Managable):
    def __init__(self, config):
        super().__init__(config)
        self.in_cache = config["datacache"]
        self.namespace = config["namespace"]
        self.indexes = {}
        self.in_path = config.get("reconcileDumpPath", None)
        self.out_path = config.get("reconcileDbPath", None)
        self.inverse_path = config.get("inverseEquivDbPath", None)
        self.reconciler = config.get("reconciler", None)
        self.acquirer = config.get("acquirer", None)
        self.mapper = config.get("mapper", None)

        self.overwrite = True
        self.increment_total = False

    def get_index(self, which):
        if which in self.indexes and "index" in self.indexes[which]:
            return self.indexes[which]["index"]
        else:
            try:
                self.manager.log(logging.ERROR, f"Could not find index {which} in {self.config['name']}")
            except Exception:
                pass
            return None

    def export(self):
        # Once loaded, this will export from the LMDB to a CSV for sharing
        for name, cfg in self.indexes.items():
            idx = cfg["index"]
            csvfn = f"{self.config['name']}_{name}.csv"
            outfn = os.path.join(self.configs.data_dir, csvfn)
            self.write_csv(idx, outfn)

    def write_csv(self, idx, outfn):
        with open(outfn, "w") as csvh:
            writer = csv.writer(csvh, delimiter=",")
            for k, v in idx.items():
                if type(v) is list:
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
            if not self.disable_ui:
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
                                    self.manager.log(
                                        logging.ERROR, f"Dropping name as too long ({len(nm)}>500):\n\t{nm}"
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
        index = self.get_index("reconcile_labels")
        eqindex = self.get_index("reconcile_ids")
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
            with index.open("w"):
                start = time.time()
                index.update(all_names)
                durn = time.time() - start
            self.manager.log(logging.INFO, f"names insert time: {int(durn)} = {len(all_names) / durn}/sec")
        if eqindex is not None and all_uris:
            with eqindex.open("w"):
                start = time.time()
                eqindex.update(all_uris)
                durn = time.time() - start
            self.manager.log(logging.INFO, f"uris insert time: {int(durn)} = {len(all_uris) / durn}/sec")

        if not disable_ui:
            self.close_progress_bar()
