import os
import sys


class UpdateManager(object):
    def __init__(self, configs, idmap):
        self.configs = configs
        self.idmap = idmap
        self.internal_nss = [x["namespace"] for x in configs.internal.values()]
        self.changed = []

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

                # Below is rebuild process, not deleting this record
                has_internal = False
                for i in all_ids:
                    for ns in self.internal_nss:
                        if i.startswith(ns):
                            has_internal = True
                            break
                    if has_internal:
                        break
                if has_internal:
                    # If there are other internal records then just rebuild
                    # without deleted record
                    pass
                else:
                    has_refs = False
                    # FIXME: find references from other records to this one
                    if has_refs:
                        # still need. Rebuild without deleted record
                        pass
                    else:
                        # no references and the source record is gone.
                        # keep deleting
                        pass
        else:
            # upsert
            # if not ident in storage == create
            if record is not None and (overwrite or not ident in storage):
                try:
                    storage.set(record["data"], identifier=ident, record_time=changeTime)
                    self.changed.append((record, ident, config))
                except:
                    print(f"Failed to process {ident}")
                    print(f"Got: {record['data']}")

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
        print(f"Harvesting until {harvester.last_harvest}")
        for change, ident, record, changeTime in harvester.crawl():
            self.process_change(config, change, ident, record, changeTime)

    def harvest_from_list(self, config, mySlice=None, maxSlice=None):
        harvester = config["harvester"]
        harvester.fetcher.enabled = True
        storage = config["datacache"]
        if storage is None:
            print(f"No datacache for {config['name']}? Can't harvest")
            return
        fn = os.path.join(self.configs.temp_dir, f"all_{config['name']}_uris.txt")
        if not os.path.exists(fn):
            print(f"No uri/change list to harvest for {config['name']}. Run get_record_list()")
            return

        with open(fn, "r") as fh:
            x = 0
            l = True
            while l:
                l = fh.readline()
                l = l.strip()
                if maxSlice is not None and x % maxSlice - mySlice != 0:
                    x += 1
                    continue
                x += 1
                (ident, dt) = l.split("\t")
                try:
                    tm = storage.metadata(ident, "insert_time")["insert_time"]
                except TypeError:
                    # NoneType is not subscriptable
                    tm = None
                if tm is not None and tm.isoformat() > dt:
                    # inserted after the change, no need to fetch
                    continue
                try:
                    itjs = harvester.fetcher.fetch(ident)
                    if itjs is None:
                        print(f"Got None for {ident}")
                        continue
                except:
                    sys.stdout.write("-")
                    sys.stdout.flush()
                    continue
                storage[ident] = itjs
                sys.stdout.write(".")
                sys.stdout.flush()

    def get_record_list(self, config, until="0001-01-01T00:00:00"):
        # build the set of records that should be in the cache
        # from the activity streams

        harvester = config["harvester"]
        harvester.last_harvest = until
        print(f"Gathering all from stream until {until}")
        records = {}
        deleted = {}
        for change, ident, record, changeTime in harvester.crawl(refsonly=True):
            if ident in deleted:
                # already seen a delete, ignore
                pass
            elif ident in records:
                if change == "delete":
                    # This is a recreate?
                    print(f"Saw record {ident} at {records[ident]} then got delete at {changeTime}")
            elif change == "delete":
                # haven't seen a ref, so most recent is delete
                deleted[ident] = changeTime
            else:
                records[ident] = changeTime

        # Write URIs to all_{name}_uris.txt and deleted_{name}_uris.txt in temp dir
        recs = sorted(list(records.keys()))
        with open(os.path.join(self.configs.temp_dir, f"all_{config['name']}_uris.txt"), "w") as fh:
            for r in recs:
                fh.write(f"{r}\t{records[r]}\n")

        recs = sorted(list(deleted.keys()))
        with open(os.path.join(self.configs.temp_dir, f"deleted_{config['name']}_uris.txt"), "w") as fh:
            for r in recs:
                fh.write(f"{r}\t{deleted[r]}\n")

        return records, deleted

    def harvest_pages(self, config, my_slice, max_slice):
        harvester = config["harvester"]
        harvester.page_cache = config["harvester"].page_cache
        harvester.harvest_pages(my_slice, max_slice)
