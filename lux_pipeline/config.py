import os
import re
import logging
import importlib
import multiprocessing
from .storage.cache.filesystem import FsCache
from .storage.uricache import URICache, _MISSING


logger = logging.getLogger("lux_pipeline")

# Object paths from configs written before the package was renamed from
# `pipeline` to `lux_pipeline` and the category level (`sources.authorities.lc`
# -> `sources.lc`) was removed. Each distinct legacy path is warned about once.
_LEGACY_PACKAGE_PREFIX = "pipeline."
_LEGACY_SOURCE_CATEGORY = re.compile(
    r"^((?:lux_pipeline\.)?sources\.)(?:archives|authorities|general|libraries|museums)\.(.+)$"
)
_warned_legacy_paths = set()


def _warn_legacy_once(old, new):
    if old not in _warned_legacy_paths:
        _warned_legacy_paths.add(old)
        logger.warning(f"Config refers to '{old}'; update it to '{new}'")


def _modernize_object_type(objectType):
    """Rewrite dotted object paths that use the pre-rename layout."""
    if objectType.startswith(_LEGACY_PACKAGE_PREFIX):
        new = "lux_pipeline." + objectType[len(_LEGACY_PACKAGE_PREFIX) :]
        _warn_legacy_once(objectType, new)
        objectType = new
    m = _LEGACY_SOURCE_CATEGORY.match(objectType)
    if m:
        new = m.group(1) + m.group(2)
        _warn_legacy_once(objectType, new)
        objectType = new
    return objectType


# c.f. https://github.com/cheshire3/cheshire3/blob/develop/cheshire3/dynamic.py#L43
def importObject(objectType):
    # print(f"Trying to import {objectType}")
    if not objectType:
        return None
    objectType = _modernize_object_type(objectType)
    try:
        (modName, className) = objectType.rsplit(".", 1)
    except ValueError:
        raise ValueError("Need module.class instead of %s" % objectType)
    try:
        m = importlib.import_module(modName)
    except ModuleNotFoundError as e:
        if not objectType.startswith("lux_pipeline."):
            try:
                return importObject("lux_pipeline.%s" % objectType)
            except Exception:
                pass
        logger.critical(f"Could not import {objectType}: {e}")
        raise e
    return getattr(m, className)


class Config(object):
    def __init__(self, configcache=None, basepath=None):
        self.internal = {}
        self.external = {}
        self.caches = {}
        # Per-kind defaults merged under every cache / map / marklogic store
        # config. Populated from {"type": "default", "name": "cache"|"map"|
        # "marklogic"} records; "cache" falls back to the single "caches"
        # record (today's shape) when no default record exists.
        self.defaults = {"cache": {}, "map": {}, "marklogic": {}}
        self.results = {}
        self.marklogic_stores = {}
        self.map_stores = {}
        self.globals = {}
        self.globals_cfg = {}
        self.validator = None
        self.configcache = None
        # {record key: record} for "type": "config" records: other caches
        # whose records are more configuration (item 35). Loaded after the
        # main directory, in key order; see load_subconfig_store()
        self.subconfig_stores = {}
        # Flattened (match, source) pairs; built lazily on first split_uri()
        self._match_pairs = None
        # canonicalize() is a pure function of the URI (the fix_identifier
        # implementations it reaches are all plain string manipulation) and
        # gets called tens of times per record on a small set of repeating
        # URIs. Bounded so memory stays predictable across parallel workers;
        # invalidated wherever _match_pairs is.
        self._canon_cache = URICache(capacity=100000)

        # This (thus) needs a directory called 'config_cache'
        if configcache:
            # Just use the one passed in
            pass
        else:
            # Use default filesystem based one
            # Note, at this point we haven't seen base.json yet
            # and hence we don't have any idea of default paths
            if basepath is None:
                basepath = os.path.dirname(__file__)
            bootstrap = {
                "name": "config",
                "datacacheClass": "storage.cache.filesystem.FsCache",
                "base_dir": basepath,
                "tabletype": "cache",
            }
            configcache = FsCache(bootstrap)
            self.configcache = configcache
        # The directory the configuration was read from: what a sub-config
        # store's relative base_dir resolves against
        self.config_dir = os.path.abspath(
            basepath or getattr(configcache, "config", {}).get("base_dir") or os.getcwd())

        # Machine-specific settings live in a "system" record when the
        # instance has one, else in "base" (today's name). Checked with `in`
        # first so the filesystem cache does not warn about the missing one.
        base = None
        for name in ("system", "base"):
            if name in configcache:
                base = configcache[name]
                break
        if base is None:
            raise ValueError(
                f"Could not find the base configuration (system.json or base.json) in {basepath}"
            )
        self.process_base(base["data"])

        if not self.data_dir.startswith("/"):
            self.data_dir = os.path.join(self.base_dir, self.data_dir)
        if not self.exports_dir.startswith("/"):
            self.exports_dir = os.path.join(self.base_dir, self.exports_dir)
        if hasattr(self, "log_dir") and not self.log_dir.startswith("/"):
            self.log_dir = os.path.join(self.base_dir, self.log_dir)
        if hasattr(self, "tests_dir") and not self.tests_dir.startswith("/"):
            self.tests_dir = os.path.join(self.base_dir, self.tests_dir)
        if hasattr(self, "temp_dir") and not self.temp_dir.startswith("/"):
            self.temp_dir = os.path.join(self.base_dir, self.temp_dir)
        if hasattr(self, "dumps_dir") and not self.dumps_dir.startswith("/"):
            self.dumps_dir = os.path.join(self.base_dir, self.dumps_dir)
        if hasattr(self, "indexes_dir") and not self.indexes_dir.startswith("/"):
            self.indexes_dir = os.path.join(self.base_dir, self.indexes_dir)

        # How many workers a parallel phase may start by default: three less
        # than the cores (postgres, redis and the controlling process need
        # some), never fewer than one. The base record may set it explicitly.
        if not hasattr(self, "max_workers"):
            self.max_workers = max(1, multiprocessing.cpu_count() - 3)
        if not hasattr(self, "temp_dir"):
            self.temp_dir = os.path.join(self.base_dir, "tmp")
        if not hasattr(self, "indexes_dir"):
            self.indexes_dir = self.data_dir

        if hasattr(self, "validatorClass"):
            vcls = importObject(self.validatorClass)
            if vcls is not None:
                self.validator = vcls(self)
            else:
                self.validator = None

        for k in configcache.iter_keys():
            rec = configcache[k]["data"]
            self.handle_config_record(k, rec)

        # Sub-config stores, after every record of the main directory so the
        # defaults they may need are complete. A store may itself hold a
        # "config" record; those are queued, each store loaded once
        pending = sorted(self.subconfig_stores)
        loaded = set()
        while pending:
            key = pending.pop(0)
            if key in loaded:
                continue
            loaded.add(key)
            self.load_subconfig_store(key)
            pending.extend(sorted(k for k in self.subconfig_stores
                                  if k not in loaded and k not in pending))

        # Today's single "caches" record is the cache default when the
        # instance has no explicit one
        if self.caches and not self.defaults["cache"]:
            self.defaults["cache"] = self.caches

    def handle_config_record(self, k, rec):
        """File one configuration record where the rest of the code looks
        for it, by its `type`. Sub-config stores (item 35) reuse this."""
        if type(rec) is not dict:
            raise ValueError(f"Config record {k} is a {type(rec).__name__}, not a dict")
        if "type" not in rec:
            raise ValueError(f"Config record {k} has no 'type': {rec}")
        typ = rec["type"]
        if typ == "internal":
            self.internal[k] = rec
        elif typ == "external":
            self.external[k] = rec
        elif typ == "results":
            self.results[k] = rec
        elif typ == "caches":
            self.caches = rec
        elif typ == "default":
            name = rec.get("name", "")
            if name == "ml":
                name = "marklogic"
            if name not in self.defaults:
                raise ValueError(
                    f"Config record {k} is a default for unknown kind {rec.get('name')!r}; "
                    f"expected one of {sorted(self.defaults)}"
                )
            self.defaults[name] = rec
        elif typ == "globals":
            del rec["type"]
            self.globals_cfg = rec
        elif typ in ("base", "system"):
            pass
        elif typ == "marklogic":
            self.marklogic_stores[rec["name"]] = rec
        elif typ == "map":
            self.map_stores[rec["name"]] = rec
        elif typ == "config":
            self.subconfig_stores[k] = rec
        else:
            logger.critical(f"Unknown config type: {typ} in {k}")

    def process_base(self, rec):
        for k, v in rec.items():
            if k != "type" and not hasattr(self, k):
                setattr(self, k, v)

    ### URI management utility functions

    def is_qua(self, recid):
        return "##qua" in recid

    def make_qua(self, recid, typ):
        concept_subtypes = []
        # No op, don't duplicate
        if "##qua" in recid:
            return recid
        if not typ in self.ok_record_types:
            raise ValueError(f"Unknown type: {typ}")
        if typ in self.parent_record_types:
            typ = self.parent_record_types[typ]
        return f"{recid}##qua{typ}"

    def split_qua(self, recid):
        return recid.split("##qua")

    def split_curie(self, curie):
        # yuag:obj/12345.json --> (yuag config, "obj/12345.json")
        try:
            (src, recid) = curie.split(":", 1)
        except:
            return None
        if src in self.internal:
            source = self.internal[src]
        elif src in self.external:
            source = self.external[src]
        elif src in ["yuid", "final", "merged"]:
            source = self.results["merged"]
        elif src in ["ml", "marklogic"]:
            source = self.results["marklogic"]
        else:
            return None

        okay = True
        if "fetcher" in source and source["fetcher"] is not None:
            okay = source["fetcher"].validate_identifier(recid)
            if not okay:
                return None
        return (source, recid)

    def fix_identifier(self, source, identifier):
        identifier = identifier.strip()
        identifier = identifier.replace(" ", "")
        identifier = identifier.replace('"', "")
        identifier = identifier.replace("”", "")
        # FIXME: What about open quotes too?
        if identifier.endswith("/"):
            identifier = identifier[:-1]
        elif identifier.endswith(".html"):
            identifier = identifier.replace(".html", "")

        if not "mapper" in source:
            logger.error(f"Could not fix identifier {identifier} from {source} as no mapper")
        elif source["mapper"]:
            identifier = source["mapper"].fix_identifier(identifier)
        return identifier

    def pre_split_fix_uri(self, uri):
        # FIXME: Shouldn't this just be canonicalize?
        if "page/aat" in uri:
            return uri.replace("page/aat", "aat")
        elif "aat/page" in uri:
            return uri.replace("aat/page", "aat")
        else:
            return uri

    def build_match_index(self):
        # Flatten every source's `matches` into one ordered tuple, in exactly
        # the order split_uri() used to walk them: internal sources first,
        # then external, each source's own matches in config order. First
        # match still wins, so selection is unchanged -- this only avoids
        # rebuilding the source list and re-checking `matches` on every call.
        # Call this again (or set _match_pairs = None) if sources or their
        # `matches` are ever changed after config load.
        pairs = []
        for s in [*self.internal.values(), *self.external.values()]:
            ms = s.get("matches", [])
            if type(ms) != list:
                ms = [ms]
            for m in ms:
                pairs.append((m, s))
        self._match_pairs = tuple(pairs)
        # canonicalize()'s answers depend on these pairs, so they go too
        self._canon_cache.clear()
        return self._match_pairs

    def _finish_split(self, uri, m, source):
        try:
            identifier = uri.rsplit(m, 1)[1]
        except:
            logger.error(f"Failed in split_uri() m: {m} source: {source['name']}")
            return None
        if identifier.startswith("http://") or identifier.startswith("https://"):
            # Urgh, fix double wrapping
            return self.split_uri(identifier)
        identifier = self.fix_identifier(source, identifier)
        if not identifier:
            return None
        else:
            return source, identifier

    def _split_uri_scan(self, uri, sources):
        # Explicit-sources path: the index doesn't apply, walk what we're given
        source = None
        for s in sources:
            ms = s.get("matches", [])
            if type(ms) != list:
                ms = [ms]
            for m in ms:
                if m in uri:
                    source = s
                    break
            if source:
                break
        if not source:
            return None
        return self._finish_split(uri, m, source)

    def split_uri(self, uri, sources=[]):
        uri = self.pre_split_fix_uri(uri)
        if sources:
            return self._split_uri_scan(uri, sources)
        pairs = self._match_pairs
        if pairs is None:
            pairs = self.build_match_index()
        for m, source in pairs:
            if m in uri:
                return self._finish_split(uri, m, source)
        return None

    def canonicalize(self, uri):
        # ~7us of string work, called tens of times per record on a heavily
        # repeating set of URIs. The sentinel matters: None is a real result
        # here (an unmatched URI) and is worth caching.
        hit = self._canon_cache.get(uri)
        if hit is not _MISSING:
            return hit
        result = self._canonicalize(uri)
        self._canon_cache.put(uri, result)
        return result

    def _canonicalize(self, uri):
        for bad, good in self.external_uri_rewrites.items():
            if bad in uri:
                uri = uri.replace(bad, good)
                break
        try:
            source, identifier = self.split_uri(uri)
        except TypeError:
            for ext in self.other_external_matches:
                if ext in uri:
                    return uri
            # Trash them
            # print(f" No split, no match: {uri}")
            return None
        # print(f" Canon: {uri} -> {source['namespace']}{identifier}")
        return f"{source['namespace']}{identifier}"

    def merge_configs(self, base, overlay):
        cfg = base.copy()
        for k, v in overlay.items():
            if not k in cfg:
                cfg[k] = v
        return cfg

    def get_idmap(self):
        return self.instantiate_map(self.idmap_name)["store"]

    def cache_globals(self, idmap=None):
        new = {}
        glbs = self.globals_cfg
        idmap_name = self.idmap_name
        if idmap_name == "":
            raise ValueError(f"No idmap set in config/base.json via code")

        cfg = self.map_stores[idmap_name]
        if "store" in cfg and cfg["store"] is not None:
            idmap = cfg["store"]
        else:
            cfg = self.instantiate_map(idmap_name)
            idmap = cfg["store"]

        for k, v in glbs.items():
            if v[0] == "3":
                qid = self.make_qua(f"http://vocab.getty.edu/aat/{v}", "Type")
            elif v[0] == "Q":
                qid = self.make_qua(f"http://www.wikidata.org/entity/{v}", "Type")
            elif v.startswith("http"):
                qid = self.make_qua(v, "Type")
            else:
                continue
            uu = idmap[qid]
            self.globals[k] = uu

    def instantiate_all(self):
        for k in self.internal.keys():
            self.instantiate(k, "internal")
        for k in self.external.keys():
            self.instantiate(k, "external")
        for k in self.results.keys():
            self.instantiate(k, "results")
        for k in self.marklogic_stores.keys():
            self.instantiate_ml(k)

    def instantiate_ml(self, key):
        if not key or not key in self.marklogic_stores:
            raise ValueError("No such marklogic store config")
        else:
            cfg = self.marklogic_stores[key]
        if "store" in cfg and cfg["store"] is not None:
            return cfg

        cfg["all_configs"] = self
        tmp_cfg = self.merge_configs(cfg, self.defaults["marklogic"])
        clss = tmp_cfg.get("storeClass", "storage.marklogic.rest.MarkLogicStore")
        clso = importObject(clss)
        cfg["store"] = clso(tmp_cfg)
        return cfg

    def instantiate_map(self, key):
        if not key or not key in self.map_stores:
            raise ValueError("No such map store config")
        else:
            cfg = self.map_stores[key]
        if "store" in cfg and cfg["store"] is not None:
            return cfg

        cfg["all_configs"] = self
        tmp_cfg = self.merge_configs(cfg, self.defaults["map"])
        clss = tmp_cfg.get("storeClass", "storage.idmap.redis.IdMap")
        clso = importObject(clss)
        cfg["store"] = clso(tmp_cfg)
        return cfg

    def instantiate_config(self, key):
        """Build the cache behind sub-config store `key` as its
        ``datacache``. The store's ``base_dir``, when relative, is taken
        from the directory this configuration was read from
        (``config_dir``), so a checkout can point at a sibling directory
        with ``"base_dir": "../subs"``; an absolute one is left alone. The
        store class defaults to `FsCache` with ``tabletype`` ``cache``, so
        ``{"name": "sources", "base_dir": "../subs"}`` reads
        ``../subs/sources_cache/*.json``. A directory that does not exist
        is an error (FsCache would otherwise create an empty one)."""
        if not key or key not in self.subconfig_stores:
            raise ValueError(f"No such sub-config store: {key}")
        cfg = self.subconfig_stores[key]
        if cfg.get("datacache") is not None:
            return cfg
        if "name" not in cfg:
            raise ValueError(f"Sub-config store {key} has no 'name'")
        bd = cfg.get("base_dir")
        if bd is not None and not os.path.isabs(bd):
            cfg["base_dir"] = os.path.normpath(os.path.join(self.config_dir, bd))
        cfg.setdefault("tabletype", "cache")
        cfg["all_configs"] = self
        if cfg.get("base_dir") is not None:
            directory = os.path.join(cfg["base_dir"], f"{cfg['name']}_{cfg['tabletype']}")
            if not os.path.isdir(directory):
                raise ValueError(f"Sub-config store {key} points at a directory that "
                                 f"does not exist: {directory}")
        cls = importObject(cfg.get("datacacheClass") or "storage.cache.filesystem.FsCache")
        tmp_cfg = self.merge_configs(cfg, self.defaults["cache"])
        cfg["datacache"] = cls(tmp_cfg)
        return cfg

    def load_subconfig_store(self, key):
        """Read every record of sub-config store `key` through
        `handle_config_record`, so its sources, maps, stores and defaults
        land where the main directory's would. A record with the same key
        as one already read replaces it (later wins). Returns the number of
        records read."""
        cache = self.instantiate_config(key)["datacache"]
        n = 0
        for k in cache.iter_keys():
            rec = cache[k]["data"]
            self.handle_config_record(k, rec)
            n += 1
        logger.info(f"Read {n} configuration records from sub-config store {key}")
        return n

    def instantiate(self, key, which=None):
        # build the set of components for a configured source

        if which and not which in ["internal", "external", "results"]:
            raise ValueError(f"Which should be internal or external, got {which}")
        elif which and not key in getattr(self, which):
            raise ValueError(f"{key} not a valid key in {which}")
        elif which:
            cfg = getattr(self, which)[key]
        else:
            cfg = self.external.get(key, self.internal.get(key, None))
            if not cfg:
                raise ValueError(f"{key} is not a valid key in either internal or external")

        for k, ptype in self.path_types.items():
            if k in cfg:
                pth = cfg[k]
                if pth and not pth.startswith("/"):
                    # put the right type of base directory before it
                    pfx = getattr(self, ptype)
                    if not pfx.startswith("/"):
                        bd = self.base_dir
                        path = os.path.join(bd, pfx, pth)
                    else:
                        path = os.path.join(pfx, pth)
                    cfg[k] = path

        # Add self to config
        cfg["all_configs"] = self

        try:
            # Harvester needs the datacache to be in config
            dcc = cfg.get("datacacheClass", "storage.cache.postgres.DataCache")
            cls3 = importObject(dcc)
            tmp_cfg = self.merge_configs(cfg, self.defaults["cache"])
            cfg["datacache"] = cls3(tmp_cfg)

            if "fetch" in cfg:
                cls1 = importObject(cfg.get("fetcherClass", "process.base.fetcher.Fetcher"))
                cfg["fetcher"] = cls1(cfg)

                nmap_name = cfg.get("networkmap_name", "networkmap")
                if nmap_name in self.map_stores:
                    nmap_c = self.map_stores[nmap_name]
                    if "store" in nmap_c and nmap_c["store"] is not None:
                        nmap = nmap_c["store"]
                    else:
                        nmap = self.instantiate_map(nmap_name)["store"]
                else:
                    raise ValueError(f"{cfg['name']} references network map store that does not exist: {nmap_name}")
                cfg["fetcher"].networkmap = nmap

            rcl = cfg.get("reconcilerClass", None)
            if rcl:
                rclcls = importObject(rcl)
                cfg["reconciler"] = rclcls(cfg)
            else:
                cfg["reconciler"] = None

            idx = cfg.get("indexLoaderClass", None)
            if idx:
                idxcls = importObject(idx)
                cfg["indexLoader"] = idxcls(cfg)
            else:
                cfg["indexLoader"] = None

            if cfg["type"] == "external":
                hclsName = cfg.get("harvesterClass", None)
                if hclsName:
                    hcls = importObject(hclsName)
                    cfg["harvester"] = hcls(cfg)
                else:
                    cfg["harvester"] = None

                rrcc = cfg.get("recordcacheReconciledClass", "storage.cache.postgres.ExternalReconciledRecordCache")
                cls5 = importObject(rrcc)
                tmp_cfg = self.merge_configs(cfg, self.defaults["cache"])
                cfg["reconciledRecordcache"] = cls5(tmp_cfg)

                rcc = cfg.get("recordcacheClass", "storage.cache.postgres.ExternalRecordCache")
            else:
                rcc = cfg.get("recordcacheClass", "storage.cache.postgres.InternalRecordCache")

                if cfg["type"] == "internal":
                    cls1 = importObject(cfg.get("harvesterClass", "process.base.harvester.ASHarvester"))
                    cfg["harvester"] = cls1(cfg)
                else:
                    cfg["harvester"] = None

            # Different default classes for record caches based on internal/external
            cls4 = importObject(rcc)
            tmp_cfg = self.merge_configs(cfg, self.defaults["cache"])
            cfg["recordcache"] = cls4(tmp_cfg)

            # Everything needs a mapper to get from data to record
            cls2 = importObject(cfg.get("mapperClass", "process.base.mapper.Mapper"))
            if cls2:
                cfg["mapper"] = cls2(cfg)
            else:
                cfg["mapper"] = None

            ldr = cfg.get("loaderClass", "process.base.loader.Loader")
            if ldr:
                ldrcls = importObject(ldr)
                cfg["loader"] = ldrcls(cfg)
            else:
                cfg["loader"] = None

            # Every source gets a downloader; the base class answers
            # get_urls() from remoteDumpFile / input_files and returns []
            # when neither is configured. Set downloaderClass to null to
            # opt out
            dldr = cfg.get("downloaderClass", "process.base.downloader.BaseDownloader")
            if dldr:
                dldrcls = importObject(dldr)
                cfg["downloader"] = dldrcls(cfg)
            else:
                cfg["downloader"] = None

            rcc2 = cfg.get("recordcache2Class", "storage.cache.postgres.RecordCache")
            if rcc2:
                cls5 = importObject(rcc2)
                tmp_cfg = self.merge_configs(cfg, self.defaults["cache"])
                cfg["recordcache2"] = cls5(tmp_cfg)
            else:
                cfg["recordcache2"] = None

            acq = importObject(cfg.get("acquirerClass", "process.base.acquirer.Acquirer"))
            if acq:
                try:
                    cfg["acquirer"] = acq(cfg)
                except:
                    # maybe a config that doesn't need one? e.g. marklogic and merged
                    # then should be null in the config
                    raise
            else:
                cfg["acquirer"] = None

            tstr = cfg.get("testerClass", None)
            if tstr:
                cls6 = importObject(tstr)
                cfg["tester"] = cls6(cfg)
            else:
                cfg["tester"] = None
        except Exception as e:
            logger.error(f"Failed to build component for {cfg['name']}")
            raise

        return cfg
