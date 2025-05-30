import requests
import ujson as json
import os
import sys
from lxml import etree
import datetime
from ._managable import Managable
from lux_pipeline.config import importObject
import logging

logger = logging.getLogger("lux_pipeline")


class ASHarvester(Managable):
    def process_change(self, change, ident, record, changeTime):
        storage = self.config["datacache"]
        storage2 = self.config["recordcache"]
        overwrite = self.config.get("harvest_overwrite", True)
        idmap = self.manager.idmap
        configs = self.config["all_configs"]

        if change == "delete":
            rec = storage[ident]
            if rec:
                uri = rec["data"]["id"]
                cls = rec["data"]["type"]
                quaUri = configs.make_qua(uri, cls)
                del storage[ident]
                if self.config["type"] == "internal":
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
                    self.changed.append((record, ident, self.config))
                except:
                    logger.debug(f"Failed to process {ident}")
                    logger.debug(f"Got: {record['data']}")

    def count_lines(self, fh):
        # Simple method of just read in the lines
        # mmap doesn't work on compressed
        # And have already opened the file, so no point using binary read
        # Could read in chunks for some time saving

        try:
            length = fh.seek(0, os.SEEK_END)
            fh.seek(0)
        except:
            # No seek? :(
            return -1
        lines = 0
        if length < 100000000:
            for l in fh:
                lines += 1
            fh.seek(0)
            return lines
        else:
            return -1

    def harvest_from_list(self, mySlice=None, maxSlice=None):
        fetcher = self.harvester.fetcher
        fetcher.enabled = True
        storage = self.config["datacache"]

        if storage is None:
            logger.debug(f"No datacache for {self.config['name']}? Can't harvest")
            return
        fn = os.path.join(self.configs.temp_dir, f"all_{self.config['name']}_uris.txt")
        if not os.path.exists(fn):
            logger.debug(f"No uri/change list to harvest for {self.config['name']}. Run get_record_list()")
            return

        with open(fn, "r") as fh:
            ttl = self.count_lines(fh)
            self.update_progress_bar(total=ttl)
            l = True
            x = 0
            while l:
                l = fh.readline()
                l = l.strip()
                if maxSlice is not None and x % maxSlice - mySlice != 0:
                    x += 1
                    continue
                x += 1
                self.increment_progress_bar()
                try:
                    (ident, dt) = l.split("\t")
                except ValueError:
                    continue
                try:
                    tm = storage.metadata(ident, "insert_time")["insert_time"]
                except TypeError:
                    # NoneType is not subscriptable
                    tm = None
                if tm is not None and tm.isoformat() > dt:
                    # inserted after the change, no need to fetch
                    continue
                try:
                    itjs = fetcher.fetch(ident)
                    if itjs is None:
                        logger.debug(f"Got None for {ident}")
                        continue
                except:
                    continue
                storage[ident] = itjs

    def get_record_list(self):
        # build the set of records that should be in the cache
        # from the activity streams

        harvester = self.harvester
        config = self.config
        records = {}
        deleted = {}
        self.manager.log(
            logging.INFO, f"Getting references from {self.config['name']} until {self.harvester.last_harvest}"
        )

        for change, ident, record, changeTime in harvester.crawl(refsonly=True):
            if ident in deleted:
                # already seen a delete, ignore
                pass
            elif ident in records:
                if change == "delete":
                    # It got recreated? Do we need to do anything?
                    pass
            elif change == "delete":
                # haven't seen a ref, so most recent is delete
                deleted[ident] = changeTime
            else:
                records[ident] = changeTime

        self.manager.log(logging.INFO, f"Writing temporary record URI files for {self.config['name']}")
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

    def prepare(self, manager, n, max_workers):
        super().prepare(manager, n, max_workers)
        # import it
        protocol = importObject(self.config.get("harvestProtocolClass", None))
        if protocol is None:
            self.harvester = ASProtocol(self.config)
        else:
            self.harvester = protocol(self.config)
        self.harvester.manager = self

        storage = self.config["datacache"]
        self.internal_nss = [x["namespace"] for x in self.config["all_configs"].internal.values()]
        if self.manager.until != "0000":
            self.harvester.last_harvest = self.manager.until
        if self.harvester.last_harvest[:4] == "0000":
            self.harvester.last_harvest = storage.latest()

        self.divide_by_max_slice = False

    def process(self, disable_ui=False, **kw):
        self.manager.log(logging.INFO, f"Harvesting {self.config['name']} until {self.harvester.last_harvest}")
        for change, ident, record, changeTime in self.harvester.crawl():
            self.process_change(change, ident, record, changeTime)


# A usage independent protocol handler
# c.f. the processing engines in _task_ui_manager


class HarvestProtocol:
    def __init__(self, config):
        self.overwrite = config.get("harvest_overwrite", True)
        self.last_harvest = config.get("last_harvest", "0000-01-01T00:00:00")
        self.harvest_from = config.get("harvest_from", "9999-01-01T00:00:00")
        self.prefix = config["name"]
        self.namespace = config["namespace"]
        self.fetcher = config.get("fetcher", None)
        self.seen = {}
        self.deleted = {}
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({"Accept-Encoding": "gzip, deflate"})
        self.manager = None
        self.page_cache = None
        self.page_cache_source = config.get("activitystreams_cache_source", "")

    def set_page_cache(self):
        if self.page_cache_source and self.page_cache is None:
            self.page_cache = self.config["all_configs"].external[self.page_cache_source]["datacache"]

    def fetch_json(self, uri, typ):
        # generically useful fallback
        try:
            resp = self.session.get(uri)
        except Exception as e:
            logger.error(f"Failed to get anything from {typ} at {uri}: {e}")
            return {}
        try:
            what = json.loads(resp.text)
        except Exception as e:
            logger.error(f"Failed to get JSON from {typ} at {uri}: {e}")
            return {}
        return what

    def prepare(self, last_harvest=None):
        self.page = None
        self.fetcher = self.config["fetcher"]
        if self.fetcher is not None:
            self.fetcher.enabled = True
        self.seen = {}
        if last_harvest is not None:
            self.last_harvest = last_harvest
        self.deleted = {}
        self.datacache = self.config["datacache"]
        self.set_page_cache()


class PmhProtocol(HarvestProtocol):
    # "fetch": "https://photoarchive.paul-mellon-centre.ac.uk/apis/oai/pmh/v2?verb=GetRecord&metadataPrefix=lido&identifier={identifier}"

    def __init__(self, config):
        super().__init__(config)
        self.endpoint = config["pmhEndpoint"]
        self.metadataPrefix = config.get("pmhMetadataPrefix", "oai_dc")
        self.namespaces = {"oai": "http://www.openarchives.org/OAI/2.0/"}
        # https://photoarchive.paul-mellon-centre.ac.uk/apis/oai/pmh/v2
        # https://snd.gu.se/oai-pmh

    def make_pmh_uri(self, verb, token=None):
        if token is None:
            return f"{self.endpoint}?verb={verb}&metadataPrefix={self.metadataPrefix}"
        else:
            return f"{self.endpoint}?verb={verb}&resumptionToken={token}"

    def fetch_pmh(self, uri):
        resp = self.session.get(uri)
        dom = etree.XML(resp.text.encode("utf-8"))
        return dom

    def get_token(self, dom):
        token = dom.xpath("/oai:OAI-PMH/oai:ListIdentifiers/oai:resumptionToken/text()", namespaces=self.namespaces)
        if not token:
            return None
        else:
            return token[0]

    def process_page(self, dom):
        recs = dom.xpath("/oai:OAI-PMH/oai:ListIdentifiers/oai:header", namespaces=self.namespaces)
        for rec in recs:
            date = rec.xpath("./oai:datestamp/text()", namespaces=self.namespaces)[0]
            if date < self.last_harvest:
                return None
            # Otherwise we've not seen it
            ident = rec.xpath("./oai:identifier/text()", namespaces=self.namespaces)[0]
            try:
                itjs = self.fetcher.fetch(ident)
            except:
                raise
                # continue
            # PMH doesn't have types of change, so make everything an update
            yield ("update", ident, itjs, date)
            sys.stdout.write(".")
            sys.stdout.flush()

    def crawl(self, last_harvest=None):
        super().crawl(last_harvest)
        start = self.make_pmh_uri("ListIdentifiers")
        dom = self.fetch_pmh(start)
        while dom is not None:
            sys.stdout.write("P")
            sys.stdout.flush()
            for p in self.process_page(dom):
                pass
            token = self.get_token(dom)
            if token:
                nxt = self.make_pmh_uri("ListIdentifiers", token)
                dom = self.fetch_pmh(nxt)
            else:
                dom = None
                break


class ASProtocol(HarvestProtocol):
    def __init__(self, config):
        super().__init__(config)
        self.change_types = ["update", "create", "delete", "move", "merge", "split", "refresh"]
        self.collections = config["activitystreams"]
        self.collection_index = 0
        self.page = config.get("start_page", None)
        self.datacache = None
        self.cache_okay = False

    def fetch_collection(self, uri):
        coll = self.fetch_json(uri, "collection")
        try:
            self.page = coll["last"]["id"]
        except:
            self.page = None
            logger.error(f"Failed to get last page from collection {uri}")

    def fetch_page(self):
        # fetch page in self.page
        logger.debug(f"    {self.page}")
        if self.cache_okay and self.page_cache is not None and self.page in self.page_cache:
            rec = self.page_cache[self.page]
            page = rec["data"]
        else:
            page = self.fetch_json(self.page, "page")
            if self.page_cache is not None and page is not None:
                # Test if we have it and it's the same
                if self.page in self.page_cache:
                    rec = self.page_cache[self.page]
                    cpage = rec["data"]
                    if (
                        "orderedItems" in page
                        and "orderedItems" in cpage
                        and page["orderedItems"][0]["endTime"] == cpage["orderedItems"][0]["endTime"]
                    ):
                        self.cache_okay = True
                    else:
                        self.page_cache[self.page] = page
                else:
                    self.page_cache[self.page] = page
        try:
            items = page["orderedItems"]
            items.reverse()
        except:
            logger.error(f"Failed to get items from page {self.page}")
            items = []
        try:
            prev = page.get("prev", {"id": ""})["id"]
            if prev != self.page:
                # infinite loop
                self.page = prev
            else:
                self.page = None
        except:
            # This is normal behavior for first page
            self.page = None
        return items

    def process_items(self, items, refsonly=False):
        for it in items:
            self.manager.increment_progress_bar(1)
            try:
                dt = it["endTime"]
            except:
                logger.error(f"Missing endTime for item:\n{it}")
                continue
            if type(dt) != str:
                # urgh what is this?
                logger.error(f"Couldn't understand endTime: {dt}")
                dt = datetime.datetime.utcnow().isoformat()
            elif dt < self.last_harvest:
                # We're done with the stream, not just this page
                self.page = None
                return

            try:
                chg = it["type"].lower()
                if not chg in self.change_types:
                    chg = "update"
            except:
                # just make it an update
                chg = "update"

            if chg == "refresh":
                # FIXME: Process refresh markers for deletes; for now just stop
                # This isn't in the AS from the units yet
                self.page = None
                logger.debug("Saw refresh token")
                break

            try:
                uri = it["object"]["id"]
            except:
                # FIXME: update state to flag bad entry
                logger.warning("no item id: {it}")
                continue

            # smush http/https to match namespace
            if uri.startswith("https://") and self.namespace.startswith("http://"):
                uri = uri.replace("https://", "http://")
            elif uri.startswith("http://") and self.namespace.startswith("https://"):
                uri = uri.replace("http://", "https://")

            ident = uri.replace(self.namespace, "")
            if ident in self.seen:
                # already processed, continue
                continue
            self.seen[ident] = 1

            if ident in self.deleted:
                continue
            if chg == "delete":
                self.deleted[ident] = 1

            if self.harvest_from and dt > self.harvest_from:
                continue

            if refsonly:
                yield (chg, ident, {}, dt)
                continue

            if chg == "delete":
                yield (chg, ident, {}, "")
                continue

            # only fetch if insert_time on the datacache for the record is < dt
            if self.datacache is not None:
                try:
                    tm = self.datacache.metadata(ident, "insert_time")["insert_time"]
                except TypeError:
                    # NoneType is not subscriptable
                    tm = None
                if tm is not None and tm.isoformat() > dt:
                    # inserted after the change, no need to fetch
                    continue

            if self.fetcher is None:
                try:
                    itjs = self.fetch_json(uri, "item")
                except:
                    continue
            else:
                try:
                    itjs = self.fetcher.fetch(ident)
                except:
                    continue
            if not itjs:
                # Could have gotten None
                logger.error(f"Harvester got {itjs} from {ident}")
                continue
            yield (chg, ident, itjs, dt)

    def crawl_single(self, collection, manager=None, last_harvest=None, refsonly=False):
        if self.manager is None and manager:
            self.manager = manager
        self.prepare(last_harvest)
        self.fetch_collection(collection)
        while self.page:
            items = self.fetch_page()
            self.manager.update_progress_bar(increment_total=len(items))
            for rec in self.process_items(items, refsonly):
                yield rec

    # API function for Harvester
    def crawl(self, manager=None, last_harvest=None, refsonly=False):
        while self.collection_index < len(self.collections):
            collection = self.collections[self.collection_index]
            coll = collection.rsplit("/", 1)[-1]
            self.manager.update_progress_bar(total=0, desc=f"{self.config['name']}/{coll}")
            for rec in self.crawl_single(collection, manager, last_harvest, refsonly):
                yield rec
            self.collection_index += 1
