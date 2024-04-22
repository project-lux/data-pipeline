
class UpdateManager(object):

    def __init__(self, configs, idmap):
        self.configs = configs
        self.idmap = idmap
        self.internal_nss = [x['namespace'] for x in configs.internal.values()]
        self.changed = []

    def process_change(self, config, change, ident, record, changeTime):
        storage = config['datacache']
        storage2 = config['recordcache']
        overwrite = config.get('harvest_overwrite', True)
        idmap = self.idmap

        if change == 'delete':
            rec = storage[ident]
            if rec:
                uri = rec['data']['id']
                cls = rec['data']['type']
                quaUri = self.configs.make_qua(uri, cls)
                del storage[ident]
                if config['type'] == 'internal':
                    del storage2[ident]
                else:
                    del storage2[quaUri.rsplit('/', 1)[-1]]                   
                yuid = idmap[quaUri]
                all_ids = idmap[yuid]
                del idmap[quaUri]
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
            if record is not None and (overwrite or not ident in storage):
                storage.set(record['data'], identifier=ident, record_time=changeTime, change=change)
                self.changed.append((record, ident, config))

    def harvest_all(self, store_only=False):
        self.changed = []
        for cfg in self.configs.internal.values():
            self.harvest(cfg)
        if not store_only:
            # This should do the same as run-integrated
            # Meaning it should be a function not a script
            for (record, ident, source) in self.changed:
                # FIXME: rebuild record
                pass

    def harvest_single(self, name, store_only=False):
        if name in self.configs.internal:
            cfg = self.configs.internal[name]
            self.harvest(cfg)
            if not store_only:
                # FIXME: rebuild
                pass
        else:
            raise ValueError(f"No known source with name {name}")

    def harvest(self, config):      
        storage = config['datacache']
        harvester = config['harvester']
        if harvester.last_harvest[:4] == "0001":
            harvester.last_harvest = storage.latest()
        print(f"Harvesting until {harvester.last_harvest}")
        for (change, ident, record, changeTime) in harvester.crawl():
            self.process_change(config, change, ident, record, changeTime)
 
    def get_record_list(self, config):
        # build the set of records that should be in the cache
        # from the activity streams

        harvester = config['harvester']
        harvester.last_harvest = "0001-01-01T00:00:00"
        print(f"Gathering all from stream")
        records = {}
        deleted = {}
        for (change, ident, record, changeTime) in harvester.crawl(refsonly=True):
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
        return records, deleted
