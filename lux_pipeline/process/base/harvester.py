"""Harvesting: walk a source's change feed and apply the changes to its caches.

Two layers:

**Protocols** (`HarvestProtocol` → `ASProtocol`, `PmhProtocol`) know how to
walk one kind of feed. `crawl()` is a generator of
``(change, identifier, record, changeTime)`` tuples; a protocol does I/O
over the feed and nothing else. It reports progress through
``self.progress`` (the item 02 callable, `null_progress` by default), so
one used from a script reports nowhere.

**Drivers** (`ASHarvester`, `PmhHarvester`, and source subclasses such as
`GettyHarvester`) own a protocol instance, chosen from the
``harvestProtocolClass`` config key or the driver's `protocol_class`
default, and apply each change to the caches through
`UpdateManager.process_change`. A driver is `Sliced` (item 02): the
harvest manager (item 17) calls ``prepare(n, max_workers, progress=...)``
then one of

* ``process()`` — walk the feed and apply every change (one worker per
  stream);
* ``get_record_list()`` then ``harvest_from_list()`` — two-phase: write
  ``all_<name>_uris.txt`` / ``deleted_<name>_uris.txt`` to ``temp_dir``
  from the feed, then fetch the listed records with every worker taking
  its share of the lines;
* ``harvest_pages(my_slice, max_slice)`` — fetch every ``max_slice``-th
  page into the page cache (today's ``harvest_parallel.sh --pages``).

Configuration keys read here, all per source:

``activitystreams``            list of ActivityStreams collection URIs
``pmhEndpoint``, ``pmhMetadataPrefix``  an OAI-PMH endpoint instead
``harvestProtocolClass``       override the protocol (dotted class path)
``last_harvest``               walk back to this time (default: the
                               datacache's latest record time)
``harvest_from``               ignore changes newer than this
``harvest_overwrite``          replace cached records (default true)
``start_page``                 begin at this page rather than the last
``activitystreams_cache_source``  name of the external source whose
                               datacache stores fetched pages; may also be
                               set on the base configuration

The legacy names `Harvester`, `ASHarvester`, `PmhHarvester` stay
importable: ``harvesterClass`` values in existing configs are unchanged,
and `UpdateManager` / ``run-harvest.py`` keep reading ``last_harvest``,
``fetcher`` and ``page_cache`` and calling ``crawl()`` on the driver.
"""

import datetime
import logging
import os

import requests
import ujson as json
from lxml import etree

from lux_pipeline.config import importObject
from lux_pipeline.process.update_manager import UpdateManager
from lux_pipeline.process.utils.io import count_lines
from lux_pipeline.progress import Sliced, null_progress

logger = logging.getLogger("lux_pipeline")

DEFAULT_LAST_HARVEST = "0001-01-01T00:00:00"
DEFAULT_HARVEST_FROM = "9999-01-01T00:00:00"


# ---------------------------------------------------------------------------
# Protocols: how to walk a feed


class HarvestProtocol:
    """Walk a feed and yield ``(change, identifier, record, changeTime)``."""

    def __init__(self, config):
        self.config = config
        self.overwrite = config.get("harvest_overwrite", True)
        self.last_harvest = config.get("last_harvest", DEFAULT_LAST_HARVEST)
        self.harvest_from = config.get("harvest_from", DEFAULT_HARVEST_FROM)
        self.prefix = config.get("name", "")
        self.namespace = config.get("namespace", "")
        self.fetcher = config.get("fetcher", None)
        self.seen = {}
        self.deleted = {}
        self.session = requests.Session()
        self.session.headers.update({"Accept-Encoding": "gzip, deflate"})
        self.progress = null_progress
        self.total = 0
        self.page = None
        self.datacache = None
        self.page_cache = None
        configs = config.get("all_configs")
        self.page_cache_source = config.get(
            "activitystreams_cache_source", getattr(configs, "activitystreams_cache_source", "") or ""
        )

    def set_page_cache(self):
        """Resolve the page cache from ``activitystreams_cache_source`` unless one was assigned."""
        if self.page_cache is not None or not self.page_cache_source:
            return
        configs = self.config.get("all_configs")
        src = getattr(configs, "external", {}).get(self.page_cache_source) if configs else None
        if not src:
            logger.warning(f"{self.prefix}: activitystreams_cache_source {self.page_cache_source!r} is not a configured source")
            return
        self.page_cache = src.get("datacache")

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
        """Reset per-walk state. Called at the start of every ``crawl()``."""
        self.fetcher = self.config.get("fetcher", self.fetcher)
        if self.fetcher is not None:
            self.fetcher.enabled = True
        self.seen = {}
        self.deleted = {}
        self.total = 0
        if last_harvest is not None:
            self.last_harvest = last_harvest
        self.datacache = self.config.get("datacache", None)
        self.set_page_cache()

    def crawl(self, last_harvest=None, refsonly=False):
        raise NotImplementedError()

    def harvest_pages(self, my_slice, max_slice):
        raise NotImplementedError(f"{type(self).__name__} has no pages to harvest in parallel")


class PmhProtocol(HarvestProtocol):
    # "fetch": "https://photoarchive.paul-mellon-centre.ac.uk/apis/oai/pmh/v2?verb=GetRecord&metadataPrefix=lido&identifier={identifier}"

    def __init__(self, config):
        super().__init__(config)
        self.endpoint = config["pmhEndpoint"]
        self.metadataPrefix = config.get("pmhMetadataPrefix", "oai_dc")
        self.namespaces = {"oai": "http://www.openarchives.org/OAI/2.0/"}
        self.finished = False
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
                # Lists come newest first: done with the feed, not just the page
                self.finished = True
                return
            # Otherwise we've not seen it
            ident = rec.xpath("./oai:identifier/text()", namespaces=self.namespaces)[0]
            self.progress(advance=1)
            itjs = self.fetcher.fetch(ident)
            # PMH doesn't have types of change, so make everything an update
            yield ("update", ident, itjs, date)

    def crawl(self, last_harvest=None, refsonly=False):
        self.prepare(last_harvest)
        self.finished = False
        self.progress(description=f"{self.prefix}/pmh")
        dom = self.fetch_pmh(self.make_pmh_uri("ListIdentifiers"))
        while dom is not None and not self.finished:
            yield from self.process_page(dom)
            token = self.get_token(dom)
            if token and not self.finished:
                dom = self.fetch_pmh(self.make_pmh_uri("ListIdentifiers", token))
            else:
                dom = None


class ASProtocol(HarvestProtocol):
    def __init__(self, config):
        super().__init__(config)
        self.change_types = ["update", "create", "delete", "move", "merge", "split", "refresh"]
        self.collections = config.get("activitystreams", []) or []
        self.collection_index = 0
        self.page = config.get("start_page", None)
        self.cache_okay = False

    def prepare(self, last_harvest=None):
        super().prepare(last_harvest)
        # a new walk starts from the configured page (or the collection's
        # last page), whatever the previous walk left behind
        self.page = self.config.get("start_page", None)
        self.cache_okay = False

    def fetch_collection(self, uri):
        coll = self.fetch_json(uri, "collection")
        try:
            self.page = coll["last"]["id"]
        except Exception:
            self.page = None
            logger.error(f"Failed to get last page from collection {uri}")
        return coll

    def fetch_page(self):
        # fetch page in self.page
        logger.debug(f"{self.page}")
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
        except Exception:
            logger.error(f"Failed to get items from page {self.page}")
            items = []
        try:
            prev = page.get("prev", {"id": ""})["id"]
            if prev != self.page:
                # infinite loop
                self.page = prev
            else:
                self.page = None
        except Exception:
            # This is normal behavior for first page
            self.page = None
        return items

    def process_items(self, items, refsonly=False):
        for it in items:
            self.progress(advance=1)
            try:
                dt = it["endTime"]
            except Exception:
                logger.warning(f"Missing endTime for item:\n{it}")
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
            except Exception:
                # just make it an update
                chg = "update"

            if chg == "refresh":
                # FIXME: Process refresh markers for deletes; for now just stop
                # This isn't in the AS from the units yet
                self.page = None
                logger.warning("Saw refresh token")
                break

            try:
                uri = it["object"]["id"]
            except Exception:
                # FIXME: update state to flag bad entry
                logger.warning(f"no item id: {it}")
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
                except Exception:
                    continue
            else:
                try:
                    itjs = self.fetcher.fetch(ident)
                except Exception:
                    continue
            if not itjs:
                # Could have gotten None
                logger.warning(f"Harvester got {itjs} from {ident}")
                continue
            yield (chg, ident, itjs, dt)

    def _crawl_collection(self, collection, refsonly=False):
        """Walk one collection from its last page back to ``last_harvest``."""
        coll = collection.rsplit("/", 1)[-1]
        self.progress(description=f"{self.prefix}/{coll}")
        logger.info(f"{collection}")
        if not self.page:
            self.fetch_collection(collection)
        while self.page:
            items = self.fetch_page()
            self.total += len(items)
            self.progress(total=self.total)
            yield from self.process_items(items, refsonly)

    def crawl_single(self, collection, last_harvest=None, refsonly=False):
        """Walk one collection on its own (resets the walk state first)."""
        self.prepare(last_harvest)
        yield from self._crawl_collection(collection, refsonly)

    # API function for Harvester
    def crawl(self, last_harvest=None, refsonly=False):
        self.prepare(last_harvest)
        self.collection_index = 0
        while self.collection_index < len(self.collections):
            collection = self.collections[self.collection_index]
            yield from self._crawl_collection(collection, refsonly)
            self.collection_index += 1
            self.page = None
        self.collection_index = 0

    def harvest_pages(self, my_slice, max_slice):
        """Fetch every ``max_slice``-th page of the current collection into the page cache."""
        self.set_page_cache()
        collection = self.collections[self.collection_index]
        logger.info(f"{collection}")
        coll = self.fetch_collection(collection)
        # assume that pages are integers between first and last
        ns, first = coll["first"]["id"].rsplit("/", 1)
        last = coll["last"]["id"].rsplit("/", 1)[-1]
        pages = range(int(first) + my_slice, int(last) + 1, max_slice)
        self.progress(total=len(pages), description=f"{self.prefix}/pages")
        for pg in pages:
            self.page = ns + "/" + str(pg)
            self.fetch_page()
            self.progress(advance=1)


# Legacy name: on main the protocol methods lived on `Harvester`
Harvester = HarvestProtocol


# ---------------------------------------------------------------------------
# Drivers: apply a protocol's changes to the caches


class ASHarvester(Sliced):
    """Drive a protocol over a source's feed and apply the changes.

    The protocol is created in ``__init__`` from ``harvestProtocolClass`` or
    `protocol_class` (`PmhProtocol` when the config has a ``pmhEndpoint``
    and no ``activitystreams``), so every entry point works without
    ``prepare()`` having run.
    """

    protocol_class = ASProtocol

    def __init__(self, config):
        self.config = config
        self.configs = config.get("all_configs")
        self.name = config.get("name", "")
        self.until = None
        self.idmap = None
        self._update_manager = None
        self.protocol = self.make_protocol()

    def default_protocol_class(self):
        if self.config.get("pmhEndpoint") and not self.config.get("activitystreams"):
            return PmhProtocol
        return self.protocol_class

    def make_protocol(self):
        cls = importObject(self.config.get("harvestProtocolClass")) or self.default_protocol_class()
        protocol = cls(self.config)
        protocol.progress = self.progress
        return protocol

    # Pass-throughs so UpdateManager and run-harvest.py keep working
    @property
    def last_harvest(self):
        return self.protocol.last_harvest

    @last_harvest.setter
    def last_harvest(self, value):
        self.protocol.last_harvest = value

    @property
    def harvest_from(self):
        return self.protocol.harvest_from

    @harvest_from.setter
    def harvest_from(self, value):
        self.protocol.harvest_from = value

    @property
    def fetcher(self):
        return self.protocol.fetcher

    @fetcher.setter
    def fetcher(self, value):
        self.protocol.fetcher = value

    @property
    def page_cache(self):
        return self.protocol.page_cache

    @page_cache.setter
    def page_cache(self, value):
        self.protocol.page_cache = value

    @property
    def changed(self):
        return self.update_manager.changed

    def crawl(self, last_harvest=None, refsonly=False):
        return self.protocol.crawl(last_harvest=last_harvest, refsonly=refsonly)

    def harvest_pages(self, my_slice, max_slice):
        return self.protocol.harvest_pages(my_slice, max_slice)

    # Managed component API
    @property
    def update_manager(self):
        if self._update_manager is None:
            if self.idmap is None and self.configs is not None:
                self.idmap = self.configs.get_idmap()
            self._update_manager = UpdateManager(self.configs, self.idmap)
        return self._update_manager

    def prepare(self, my_slice=-1, max_slice=-1, progress=null_progress, until=None, idmap=None):
        super().prepare(my_slice, max_slice, progress)
        self.protocol.progress = progress
        if idmap is not None:
            self.idmap = idmap
            self._update_manager = None
        if until is not None:
            self.until = until
        if self.until:
            self.protocol.last_harvest = self.until
        if self.protocol.last_harvest[:4] == DEFAULT_LAST_HARVEST[:4]:
            storage = self.config.get("datacache")
            if storage is not None:
                self.protocol.last_harvest = storage.latest()
        return self

    def process_change(self, change, ident, record, changeTime):
        """Apply one change; delete follow-ups are recorded by `UpdateManager`."""
        self.update_manager.process_change(self.config, change, ident, record, changeTime)

    def process(self, refsonly=False):
        """Walk the feed and apply every change. Returns the number applied."""
        logger.info(f"Harvesting {self.name} until {self.protocol.last_harvest}")
        n = 0
        for change, ident, record, changeTime in self.protocol.crawl(refsonly=refsonly):
            self.process_change(change, ident, record, changeTime)
            n += 1
        return n

    def _list_path(self, which):
        return os.path.join(self.configs.temp_dir, f"{which}_{self.name}_uris.txt")

    def get_record_list(self, until=None):
        """Phase 1: list the records the feed says should exist, to ``temp_dir``.

        Writes ``all_<name>_uris.txt`` and ``deleted_<name>_uris.txt`` as
        sorted ``identifier<TAB>changeTime`` lines and returns the two dicts.
        """
        if until is not None:
            self.protocol.last_harvest = until
        logger.info(f"Getting references from {self.name} until {self.protocol.last_harvest}")
        records = {}
        deleted = {}
        for change, ident, record, changeTime in self.protocol.crawl(refsonly=True):
            if ident in deleted:
                # already seen a delete, ignore
                pass
            elif ident in records:
                if change == "delete":
                    # This is a recreate?
                    logger.warning(f"Saw record {ident} at {records[ident]} then got delete at {changeTime}")
            elif change == "delete":
                # haven't seen a ref, so most recent is delete
                deleted[ident] = changeTime
            else:
                records[ident] = changeTime

        logger.info(f"Writing temporary record URI files for {self.name}")
        with open(self._list_path("all"), "w") as fh:
            for r in sorted(records):
                fh.write(f"{r}\t{records[r]}\n")
        with open(self._list_path("deleted"), "w") as fh:
            for r in sorted(deleted):
                fh.write(f"{r}\t{deleted[r]}\n")
        return records, deleted

    def harvest_from_list(self, my_slice=None, max_slice=None):
        """Phase 2: fetch this worker's share of ``all_<name>_uris.txt``. Returns the count stored."""
        if my_slice is None:
            my_slice = self.my_slice
        if max_slice is None:
            max_slice = self.max_slice
        fetcher = self.protocol.fetcher
        if fetcher is None:
            logger.error(f"No fetcher for {self.name}? Can't harvest from list")
            return 0
        fetcher.enabled = True
        storage = self.config.get("datacache")
        if storage is None:
            logger.error(f"No datacache for {self.name}? Can't harvest")
            return 0
        fn = self._list_path("all")
        if not os.path.exists(fn):
            logger.warning(f"No uri/change list to harvest for {self.name}. Run get_record_list()")
            return 0

        stored = 0
        with open(fn, "r") as fh:
            ttl = count_lines(fh)
            if ttl > 0 and max_slice > 1:
                ttl = ttl // max_slice
            self.progress(total=ttl if ttl > 0 else None, completed=0, description=f"{self.name}/list")
            for x, line in enumerate(fh):
                if max_slice is not None and max_slice > 1 and x % max_slice != my_slice:
                    continue
                self.progress(advance=1)
                try:
                    (ident, dt) = line.strip().split("\t")
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
                except Exception as e:
                    logger.warning(f"Failed to fetch {ident}: {e}")
                    continue
                if itjs is None:
                    logger.warning(f"Got None for {ident}")
                    continue
                storage[ident] = itjs
                stored += 1
        return stored


class PmhHarvester(ASHarvester):
    """Driver whose default protocol is OAI-PMH."""

    protocol_class = PmhProtocol
