import os
from .storage.cache.filesystem import FsCache
import importlib
import multiprocessing
import logging

logger = logging.getLogger("lux_pipeline")


def importObject(objectType):
    if not objectType:
        return None
    try:
        (modName, className) = objectType.rsplit(".", 1)
    except Exception:
        raise ValueError("Need module.class instead of %s" % objectType)
    if not modName.startswith("lux_pipeline."):
        modName = f"lux_pipeline.{modName}"

    try:
        m = importlib.import_module(modName)
    except ModuleNotFoundError as mnfe:
        logger.critical(f"Could not find module {modName}: {mnfe}")
        raise
    except Exception as e:
        logger.critical(f"Failed to import {modName}: {e}")
        raise
    try:
        parentClass = getattr(m, className)
    except AttributeError:
        raise
    return parentClass


class Config(object):
    def __init__(self, configcache=None, basepath=None):
        self.internal = {}
        self.external = {}
        self.defaults = {"cache": {}, "map": {}, "marklogic": {}}
        self.results = {}
        self.marklogic_stores = {}
        self.map_stores = {}
        self.globals = {}
        self.globals_cfg = {}
        self.validator = None
        self.configcache = None
        self.subconfig_stores = {}

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

        base = configcache["system"]
        if base is None:
            raise ValueError(f"Could not find base system configuration (system.json) in {basepath}")
        else:
            base = base["data"]
        self.process_base(base)

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

        if not hasattr(self, "max_workers"):
            # Default to 2/3rds of our CPUs
            self.max_workers = multiprocessing.cpu_count() * 2 // 3

        # FIXME: Validators should be per source per cache
        # if hasattr(self, "validatorClass"):
        #     vcls = importObject(self.validatorClass)
        #     if vcls is not None:
        #         self.validator = vcls(self)
        #     else:
        #         self.validator = None
        self.validator = None

        for k in configcache.iter_keys():
            rec = configcache[k]["data"]
            if type(rec) is not dict:
                raise ValueError("Could not read JSON from record: {k} --> {rec}")
            self.handle_config_record(k, rec)

        for cfg in self.subconfig_stores.values():
            cache = cfg["datacache"]
            for k in cache.iter_keys():
                rec = cache[k]["data"]
                try:
                    self.handle_config_record(k, rec)
                except Exception:
                    print(f"Could not generate a config for {k}")

    def handle_config_record(self, k, rec):
        if type(rec) is not dict:
            raise ValueError(f"rec is a {type(rec)} not a dict")
        if "type" not in rec:
            raise ValueError(f"missing 'type' in {k}: {rec}")
        if rec["type"] == "internal":
            self.internal[k] = rec
        elif rec["type"] == "external":
            self.external[k] = rec
        elif rec["type"] == "results":
            self.results[k] = rec
        elif rec["type"] == "default":
            self.defaults[rec["name"]] = rec
        elif rec["type"] == "globals":
            del rec["type"]
            self.globals_cfg = rec
        elif rec["type"] == "system":
            pass
        elif rec["type"] == "marklogic":
            self.marklogic_stores[rec["name"]] = rec
        elif rec["type"] == "map":
            self.map_stores[rec["name"]] = rec
        elif rec["type"] == "config":
            self.subconfig_stores[k] = rec
            self.instantiate_config(k)
        else:
            logger.critical(f"Unknown config type: {rec['type']} in {k}")
            logger.critical(rec)

    def process_base(self, rec):
        for k, v in rec.items():
            if k != "type" and not hasattr(self, k):
                setattr(self, k, v)

    ### URI management utility functions

    def is_qua(self, recid):
        return "##qua" in recid

    def make_qua(self, recid, typ):
        # No op, don't duplicate
        if "##qua" in recid:
            return recid
        if typ not in self.ok_record_types:
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
        except Exception:
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

        if "mapper" not in source:
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

    def split_uri(self, uri, sources=[]):
        source = None
        uri = self.pre_split_fix_uri(uri)
        if not sources:
            sources = [*self.internal.values(), *self.external.values()]

        for s in sources:
            ms = s.get("matches", [])
            if type(ms) is not list:
                ms = [ms]
            for m in ms:
                if m in uri:
                    source = s
                    break
            if source:
                break
        if source:
            try:
                identifier = uri.rsplit(m, 1)[1]
            except Exception:
                logger.error(f"Failed in split_uri() m: {m} source: {s['name']}")
                return None
            if identifier.startswith("http://") or identifier.startswith("https://"):
                # Urgh, fix double wrapping
                return self.split_uri(identifier)
            identifier = self.fix_identifier(source, identifier)
            if not identifier:
                return None
            else:
                return source, identifier
        else:
            return None

    def canonicalize(self, uri):
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
        # (f" Canon: {uri} -> {source['namespace']}{identifier}")
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
        tmp_cfg = self.merge_configs(cfg, self.defaults["ml"])
        clss = tmp_cfg.get("storeClass", "storage.marklogic.rest.MarkLogicStore")
        clso = importObject(clss)
        cfg["store"] = clso(tmp_cfg)
        return cfg

    def instantiate_map(self, key):
        if not key or not key in self.map_stores:
            raise ValueError(f"Could not find map config: {key}")
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
        if not key or not key in self.subconfig_stores:
            raise ValueError(f"Could not find sub configuration: {key}")
        else:
            cfg = self.subconfig_stores[key]
        if "datacache" in cfg and cfg["datacache"] is not None:
            return cfg

        # Smush directories

        for d in [
            "base_dir",
            "data_dir",
            "exports_dir",
            "log_dir",
            "tests_dir",
            "temp_dir",
            "dumps_dir",
            "indexes_dir",
        ]:
            if d in cfg and not cfg[d].startswith("/"):
                cfg[d] = os.path.join(getattr(self, d), cfg[d])
            else:
                cfg[d] = getattr(self, d)

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
            dcc = cfg.get("datacacheClass", "storage.cache.postgres.DataCache")
            cls3 = importObject(dcc)
            tmp_cfg = self.merge_configs(cfg, self.defaults["cache"])
            cfg["datacache"] = cls3(tmp_cfg)

            if "fetch" in cfg or "fetcherClass" in cfg:
                fc = cfg.get("fetcherClass", None)
                if fc:
                    cls1 = importObject(fc)
                    cfg["fetcher"] = cls1(cfg)

                    nmap_name = cfg.get("networkmap_name", "networkmap")
                    if nmap_name in self.map_stores:
                        nmap_c = self.map_stores[nmap_name]
                        if "store" in nmap_c and nmap_c["store"] is not None:
                            nmap = nmap_c["store"]
                        else:
                            nmap = self.instantiate_map(nmap_name)["store"]
                    else:
                        raise ValueError(
                            f"{cfg['name']} references network map store that does not exist: {nmap_name}"
                        )
                    cfg["fetcher"].networkmap = nmap

            hclsName = cfg.get("harvesterClass", None)
            if hclsName:
                hcls = importObject(hclsName)
                cfg["harvester"] = hcls(cfg)
            else:
                cfg["harvester"] = None
            ldr = cfg.get("loaderClass", None)
            if ldr:
                ldrcls = importObject(ldr)
                cfg["loader"] = ldrcls(cfg)
            else:
                cfg["loader"] = None
            dldr = cfg.get("downloaderClass", None)
            if dldr:
                dldrcls = importObject(dldr)
                cfg["downloader"] = dldrcls(cfg)
            else:
                cfg["downloader"] = None
        except Exception as e:
            logger.critical(f"Failed to build configuration for {cfg['name']}")
            raise

        return cfg

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

        if "indexes" in cfg:
            # Smush index paths
            for k, v in cfg["indexes"].items():
                if "path" in v:
                    path = v["path"]
                    if path and not path.startswith("/"):
                        # put the right type of base directory before it
                        pfx = self.indexes_dir
                        if not pfx.startswith("/"):
                            bd = self.base_dir
                            path = os.path.join(bd, pfx, path)
                        else:
                            path = os.path.join(pfx, path)
                        v["path"] = path

        # Add self to config
        cfg["all_configs"] = self

        try:
            if "indexes" in cfg:
                for idxname, idxcfg in cfg["indexes"].items():
                    try:
                        idxcls = importObject(idxcfg["indexClass"])
                    except Exception as e:
                        logger.error(f"Failed to import index class {idxcfg['indexClass']}")
                        idxcfg["index"] = None
                    idxcfg["name"] = idxname
                    try:
                        idxcfg["index"] = idxcls(idxcfg)
                    except Exception as e:
                        logger.error(f"Failed to build index {idxname}")
                        idxcfg["index"] = None

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
                except Exception:
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
            logger.critical(f"Failed to build component for {cfg['name']}")
            logger.critical(e)
            # raise

        return cfg
