import logging
import os
import sys
import ujson as json

logger = logging.getLogger("lux_pipeline")


class UpdateManager(object):
    def __init__(self, configs, idmap):
        self.configs = configs
        self.idmap = idmap
        self.internal_nss = [x["namespace"] for x in configs.internal.values()]
        self.changed = []

    def _record_pending_delete(self, entry):
        """Durably record the follow-up a delete requires. The delete path
        used to silently drop the consequences (dependent records were
        never rebuilt; orphaned merged records were never removed)."""
        fn = os.path.join(self.configs.data_dir, "pending_deletes.jsonl")
        with open(fn, "a") as fh:
            fh.write(json.dumps(entry) + "\n")

    def process_pending_deletes(self, fn=None):
        """Consume pending_deletes.jsonl (run-process-deletes.py).

        Both actions drop the stale merged record so the next merge/export
        cannot serve a version containing the deleted member:
          rebuild        - remaining members exist; the next merge rebuilds
                           the record from them (identity map was already
                           updated at delete time)
          delete-merged  - nothing left in the cluster; also remove the
                           token-only YUID set from the idmap
        Processed entries are rotated to pending_deletes.done.jsonl.
        """
        if fn is None:
            fn = os.path.join(self.configs.data_dir, "pending_deletes.jsonl")
        if not os.path.exists(fn):
            return {"processed": 0}

        merged_cache = self.configs.results["merged"]["recordcache"]
        ml_cache = self.configs.results.get("marklogic", {}).get("recordcache")
        stats = {"processed": 0, "merged_removed": 0, "yuids_removed": 0,
                 "errors": 0}
        done = []
        with open(fn) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    yuid = entry["yuid"]
                    uu = yuid.rsplit("/", 1)[-1]
                    for cache in (merged_cache, ml_cache):
                        if cache is not None and uu in cache:
                            del cache[uu]
                            stats["merged_removed"] += 1
                    if entry.get("action") == "delete-merged":
                        try:
                            if self.idmap.delete_yuid(yuid):
                                stats["yuids_removed"] += 1
                        except ValueError as e:
                            # members re-appeared since the delete was
                            # recorded; the next merge will rebuild instead
                            logger.warning(f"pending-delete kept yuid: {e}")
                    stats["processed"] += 1
                    done.append(line)
                except Exception as e:
                    logger.error(f"pending-delete failed for {line[:120]}: {e}")
                    stats["errors"] += 1

        # rotate: append processed entries to .done, rewrite remainder
        done_fn = fn.replace(".jsonl", ".done.jsonl")
        with open(done_fn, "a") as fh:
            for line in done:
                fh.write(line + "\n")
        remaining = []
        with open(fn) as fh:
            for line in fh:
                if line.strip() and line.strip() not in set(done):
                    remaining.append(line)
        with open(fn, "w") as fh:
            fh.writelines(remaining)
        return stats

    def process_change(self, config, change, ident, record, changeTime):
        storage = config["datacache"]
        storage2 = config["recordcache"]
        overwrite = config.get("harvest_overwrite", True)
        idmap = self.idmap

        if change == "delete":
            rec = storage[ident]
            if rec:
                uri = rec["data"]["id"]
                cls = rec["data"]["type"]
                quaUri = self.configs.make_qua(uri, cls)
                del storage[ident]
                if config["type"] == "internal":
                    del storage2[ident]
                else:
                    del storage2[quaUri.rsplit("/", 1)[-1]]
                yuid = idmap[quaUri]
                if not yuid:
                    # already deleted
                    return
                all_ids = idmap[yuid]
                del idmap[quaUri]

                # Record what this delete requires so a later pass can act:
                # rebuild the merged record without this member, or remove
                # the orphaned merged record entirely.
                remaining = [i for i in all_ids
                             if not i.startswith("__") and i != quaUri]
                has_internal = False
                for i in remaining:
                    for ns in self.internal_nss:
                        if i.startswith(ns):
                            has_internal = True
                            break
                    if has_internal:
                        break
                if remaining:
                    # other members still exist: merged record must be
                    # rebuilt without the deleted one
                    action = "rebuild"
                else:
                    # nothing left in the cluster: the merged record (and
                    # any inbound references) should be removed
                    action = "delete-merged"
                self._record_pending_delete({
                    "action": action,
                    "source": config["name"],
                    "identifier": ident,
                    "qua": quaUri,
                    "yuid": yuid,
                    "remaining": sorted(remaining),
                    "has_internal": has_internal,
                    "changeTime": changeTime,
                })
        else:
            # upsert
            # if not ident in storage == create
            if record is not None and (overwrite or not ident in storage):
                try:
                    storage.set(record["data"], identifier=ident, record_time=changeTime)
                    self.changed.append((record, ident, config))
                except:
                    logger.error(f"Failed to process {ident}")
                    logger.debug(f"Got: {record['data']}")

    def harvest_all(self, store_only=False):
        self.changed = []
        for cfg in self.configs.internal.values():
            self.harvest(cfg)
        if not store_only:
            # This should do the same as run-integrated
            # Meaning it should be a function not a script
            for record, ident, source in self.changed:
                # FIXME: rebuild record
                pass

    def harvest_single(self, name, store_only=False):
        if name in self.configs.internal:
            cfg = self.configs.internal[name]
        elif name in self.configs.external:
            cfg = self.configs.external[name]
        else:
            raise ValueError(f"No known source with name {name}")

        self.harvest(cfg)
        if not store_only:
            # FIXME: rebuild
            pass

    def harvest(self, config):
        storage = config["datacache"]
        harvester = config["harvester"]
        if harvester.last_harvest[:4] == "0001":
            harvester.last_harvest = storage.latest()
        logger.info(f"Harvesting until {harvester.last_harvest}")
        for change, ident, record, changeTime in harvester.crawl():
            self.process_change(config, change, ident, record, changeTime)

    def harvest_from_list(self, config, mySlice=None, maxSlice=None):
        # Phase 2 of a two-phase harvest; the driver (process.base.harvester)
        # owns the logic since item 16
        return config["harvester"].harvest_from_list(mySlice, maxSlice)

    def get_record_list(self, config, until="0001-01-01T00:00:00"):
        # Phase 1: list the records the feed says should exist
        return config["harvester"].get_record_list(until=until)

    def harvest_pages(self, config, my_slice, max_slice):
        harvester = config["harvester"]
        harvester.page_cache = config["harvester"].page_cache
        harvester.harvest_pages(my_slice, max_slice)
