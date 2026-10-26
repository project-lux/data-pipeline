"""Build a source's reconciliation indexes from its datacache.

`IndexLoader` walks every record of the source's datacache, acquires each
(mapper applied, nothing stored), and collects two tables: lower-cased
names to ``[recid, type]`` for the name index (``reconcileDbPath``) and
equivalent URIs to ``[recid, type]`` for the inverse-equivalents index
(``inverseEquivDbPath``). Collisions keep the smallest ``recid`` so the
index is the same whatever order the records arrive in. Names longer than
`MAX_KEY_BYTES` bytes are dropped (LMDB keys are limited to 511 bytes).

It is a `Sliced` component (item 02): a manager calls
``prepare(my_slice, max_slice, progress=...)`` then ``process()``, and the
loader reports the datacache's size and each record through
``self.progress``. LMDB has a single writer, so ``prepare`` refuses more
than one slice. ``load()`` is the legacy entry point (``manage-data.py
--load-index``) and the subclasses that override it (`ViafIndexLoader`,
`WdFileIndexLoader`, `GlobalIndexLoader`, ...) keep working unchanged.

The storage subclasses, `LmdbIndexLoader` and `SqliteIndexLoader`, open
the two indexes by path in `get_storage()`.
"""

import csv
import logging
import os
import time

from sqlitedict import SqliteDict

from lux_pipeline.progress import Sliced, null_progress
from lux_pipeline.storage.idmap.lmdb import TabLmdb

logger = logging.getLogger("lux_pipeline")

# LMDB keys are limited to 511 bytes; names at or above this are dropped
MAX_KEY_BYTES = 499
LOG_EVERY = 100000


class IndexLoader(Sliced):
    def __init__(self, config):
        self.config = config
        self.configs = config.get("all_configs")
        self.name = config.get("name", "")
        self.in_cache = config.get("datacache")
        self.namespace = config.get("namespace", "")
        self.in_path = config.get("reconcileDumpPath", None)
        self.out_path = config.get("reconcileDbPath", None)
        self.inverse_path = config.get("inverseEquivDbPath", None)
        self.reconciler = config.get("reconciler", None)
        self.acquirer = config.get("acquirer", None)
        self.mapper = config.get("mapper", None)
        self.overwrite = True

    # --- storage ---------------------------------------------------------------

    def get_storage(self):
        """``(name_index, equivalents_index)``, either ``None`` when not
        configured. Storage subclasses implement this."""
        raise NotImplementedError(f"{type(self).__name__}.get_storage")

    def open_index_readonly(self, path):
        """An index at `path` opened for reading, for `export`."""
        raise NotImplementedError(f"{type(self).__name__}.open_index_readonly")

    # --- per-record hooks (subclasses override) --------------------------------

    def extract_names(self, rec):
        return self.reconciler.extract_names(rec)

    def extract_uris(self, rec):
        return self.reconciler.extract_uris(rec)

    def acquire_record(self, rec):
        recid = rec["identifier"]
        res = self.acquirer.acquire(recid, store=False)
        return res

    # --- managed component API -------------------------------------------------

    def prepare(self, my_slice=-1, max_slice=-1, progress=null_progress):
        """One writer only: more than one slice is refused."""
        if max_slice > 1:
            raise ValueError(f"{self.name}: indexes are built by one worker (LMDB has a "
                             f"single writer); got max_slice={max_slice}")
        return super().prepare(my_slice, max_slice, progress)

    def index_records(self, want_names=True, want_uris=True):
        """Walk the datacache; ``(all_names, all_uris)`` as dicts of
        ``key -> [recid, type]``, the smallest recid winning a collision."""
        all_names = {}
        all_uris = {}
        ttl = self.in_cache.len_estimate() if hasattr(self.in_cache, "len_estimate") else -1
        self.progress(total=ttl if ttl and ttl > 0 else None, completed=0,
                      description=f"{self.name} index")
        n = 0
        start = time.time()
        logger.info(f"{self.name}: starting to build indexes")
        for rec in self.in_cache.iter_records():
            n += 1
            self.progress(advance=1)
            res = self.acquire_record(rec)
            if res is None:
                # Mapper might kill it, might not exist, etc
                continue
            recid = rec["identifier"]
            try:
                typ = res["data"]["type"]
            except Exception:
                typ = self.mapper.guess_type(res["data"])
            if not (recid and typ):
                continue
            # Collisions (two records sharing a name or claiming the same
            # equivalent) were last-writer-wins over the unordered record
            # stream, so the index winner could differ between builds.
            # Keep the smallest recid so the index is order-independent.
            if want_names:
                for nm in self.extract_names(res["data"]).keys():
                    if not nm:
                        continue
                    if len(nm.encode("utf-8")) < MAX_KEY_BYTES:
                        key = nm.lower()
                        prev = all_names.get(key)
                        if prev is None or str(recid) < str(prev[0]):
                            all_names[key] = [recid, typ]
                    else:
                        logger.warning(f"Dropping name as too long ({len(nm)}>500):\n\t{nm}")
            if want_uris:
                for eq in self.extract_uris(res["data"]):
                    if eq:
                        prev = all_uris.get(eq)
                        if prev is None or str(recid) < str(prev[0]):
                            all_uris[eq] = [recid, typ]
            if not n % LOG_EVERY:
                durn = time.time() - start
                rate = n / durn if durn else 0
                eta = ttl / rate if rate and ttl > 0 else 0
                logger.info(f"{n} of {ttl} in {int(durn)} = {rate:.0f}/sec -> {eta:.0f} secs")
        return all_names, all_uris

    def process(self, overwrite=True):
        """Build and write the configured indexes. With ``overwrite`` the
        existing entries are cleared first; without it the new entries are
        merged over them. Returns ``{"records", "names", "uris"}`` or
        ``None`` when the source has no index configured."""
        self.overwrite = overwrite
        index, eqindex = self.get_storage()
        if index is None and eqindex is None:
            logger.warning(f"{self.name} has no indexes configured")
            return None
        if self.reconciler is None:
            self.reconciler = self.config["reconciler"]
        if self.mapper is None:
            self.mapper = self.config["mapper"]
        if self.acquirer is None:
            self.acquirer = self.config["acquirer"]

        if overwrite:
            logger.info(f"{self.name}: clearing existing indexes")
            if index is not None:
                index.clear()
            if eqindex is not None:
                eqindex.clear()

        all_names, all_uris = self.index_records(index is not None, eqindex is not None)

        if index is not None and all_names:
            start = time.time()
            index.update(all_names)
            durn = time.time() - start
            logger.info(f"names insert time: {int(durn)} = {len(all_names) / durn if durn else 0:.0f}/sec")
        if eqindex is not None and all_uris:
            start = time.time()
            eqindex.update(all_uris)
            durn = time.time() - start
            logger.info(f"uris insert time: {int(durn)} = {len(all_uris) / durn if durn else 0:.0f}/sec")
        for idx in (index, eqindex):
            self.close_index(idx)
        records = getattr(self.in_cache, "len_estimate", lambda: -1)()
        return {"records": records, "names": len(all_names), "uris": len(all_uris)}

    @staticmethod
    def close_index(idx):
        """Commit (a store that needs it, SqliteDict) and close an index so
        the file is complete and another opener in this process can use
        it."""
        if idx is None:
            return
        for method in ("commit", "close"):
            fn = getattr(idx, method, None)
            if callable(fn):
                try:
                    fn()
                except Exception as e:
                    logger.warning(f"{method}() on index failed: {e}")

    def load(self):
        """Legacy entry point: build the indexes in this process."""
        self.prepare()
        return self.process()

    # --- export ----------------------------------------------------------------

    def export(self):
        """Write each configured index to ``<data_dir>/<name>.csv`` for sharing."""
        for path in (self.out_path, self.inverse_path):
            if not path:
                continue
            idx = self.open_index_readonly(path)
            csvfn = path.rstrip("/").rsplit("/", 1)[-1]
            csvfn = csvfn.replace("lmdb", "csv").replace("sqlite", "csv")
            if not csvfn.endswith(".csv"):
                csvfn += ".csv"
            outfn = os.path.join(self.configs.data_dir, csvfn)
            self.write_csv(idx, outfn)
            self.close_index(idx)

    def write_csv(self, idx, outfn):
        with open(outfn, "w") as csvh:
            writer = csv.writer(csvh, delimiter=",")
            for k, v in idx.items():
                if type(v) == list:
                    writer.writerow([k, "\t".join(v)])
                else:
                    writer.writerow([k, v])


class LmdbIndexLoader(IndexLoader):
    def get_storage(self):
        mapExp = self.config.get("mapSizeExponent", 30)
        # n = remove and recreate
        if self.out_path:
            index = TabLmdb.open(self.out_path, "c", map_size=2**mapExp, readahead=False, writemap=True)
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

    def open_index_readonly(self, path):
        return TabLmdb.open(path, "r", readahead=False, writemap=True)


class SqliteIndexLoader(IndexLoader):
    def get_storage(self):
        if self.out_path is not None:
            index = SqliteDict(self.out_path, autocommit=False)
        else:
            index = None
        if self.inverse_path is not None:
            eqindex = SqliteDict(self.inverse_path, autocommit=False)
        else:
            eqindex = None
        return (index, eqindex)

    def open_index_readonly(self, path):
        return SqliteDict(path, autocommit=False)
