
import os
from .storage.cache.filesystem import FsCache

# c.f. https://github.com/cheshire3/cheshire3/blob/develop/cheshire3/dynamic.py#L43 
def importObject(objectType):
    # print(f"Trying to import {objectType}")
    if not objectType:
        return None
    try:
        (modName, className) = objectType.rsplit('.', 1)
    except:
        raise ValueError("Need module.class instead of %s" % objectType)
    try:
        m = __import__(modName)
    except ModuleNotFoundError as e:
        if not objectType.startswith("pipeline"):
            try:
                return importObject("pipeline.%s" % objectType)
            except:
                pass
        print(f"Failed to import {objectType}")
        raise e

    # Now split and fetch bits
    mods = modName.split('.')
    for mn in mods[1:]:
        try:
            m = getattr(m, mn)
        except AttributeError as e:
            if not objectType.startswith("pipeline"):
                try:
                    return importObject("pipeline.%s" % objectType)
                except:
                    pass
            raise e
    try:
        parentClass = getattr(m, className)
    except AttributeError as e:
        raise
    return parentClass

class Config(object):

    def __init__(self, configcache=None, basepath=None):
        self.internal = {}
        self.external = {}
        self.caches = {}
        self.results = {}
        self.marklogic_stores = {}
        self.map_stores = {}
        self.globals = {}
        self.globals_cfg = {}
        # self.idmap_name = ""
        self.validator = None
        self.configcache = None

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
                "tabletype": "cache"
            }
            configcache = FsCache(bootstrap)
            self.configcache = configcache

        base = configcache['base']['data']
        self.process_base(base)

        if not self.data_dir.startswith('/'):
            self.data_dir = os.path.join(self.base_dir, self.data_dir)
        if not self.exports_dir.startswith('/'):
            self.exports_dir = os.path.join(self.base_dir, self.exports_dir)
        if hasattr(self, 'log_dir') and not self.log_dir.startswith('/'):
            self.log_dir = os.path.join(self.base_dir, self.log_dir)
        if hasattr(self, 'tests_dir') and not self.tests_dir.startswith('/'):
            self.tests_dir = os.path.join(self.base_dir, self.tests_dir)
        if hasattr(self, 'temp_dir') and not self.temp_dir.startswith('/'):
            self.temp_dir = os.path.join(self.base_dir, self.temp_dir)

        if hasattr(self, 'validatorClass'):
            vcls = importObject(self.validatorClass)
            if vcls is not None:
                self.validator = vcls(self)
            else:
                self.validator = None

        for k in configcache.iter_keys():
            rec = configcache[k]['data']
            if rec['type'] == "internal":
                self.internal[k] = rec
            elif rec['type'] == "external":
                self.external[k] = rec
            elif rec['type'] == "results":
                self.results[k] = rec
            elif rec['type'] == 'caches':
                self.caches = rec
            elif rec['type'] == 'globals':
                del rec['type']
                self.globals_cfg = rec
            elif rec['type'] == 'base':
                pass
            elif rec['type'] == 'marklogic':
                self.marklogic_stores[rec['name']] = rec
            elif rec['type'] == 'map':
                self.map_stores[rec['name']] = rec
            else:
                print(f"Unknown config type: {rec['type']} in {k}")

    def process_base(self, rec):
        vcls = None
        for (k,v) in rec.items():
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
        return recid.split('##qua')

    def split_curie(self, curie):
        # yuag:obj/12345.json --> (yuag config, "obj/12345.json")
        try:
            (src, recid) = curie.split(':', 1)
        except:
            return None
        if src in self.internal:
            source = self.internal[src]
        elif src in self.external:
            source = self.external[src]
        elif src in ['yuid', 'final', 'merged']:
            source = self.results['merged']
        elif src in ['ml', 'marklogic']:
            source = self.results['marklogic']
        else:
            return None

        okay = True
        if 'fetcher' in source and source['fetcher'] is not None:
            okay = source['fetcher'].validate_identifier(recid)
            if not okay:
                return None
        return (source, recid)

    def fix_identifier(self, source, identifier):
        identifier = identifier.strip()
        identifier = identifier.replace(" ", "")
        identifier = identifier.replace('"', '')
        identifier = identifier.replace('”', '')
        if identifier.endswith('/'):
            identifier = identifier[:-1]
        elif identifier.endswith('.html'):
            identifier = identifier.replace('.html', '')

        if not 'mapper' in source:
            print(f"Could not fix identifier {identifier} from {source} as no mapper")
        elif source['mapper']:
            identifier = source['mapper'].fix_identifier(identifier)
        return identifier

    def pre_split_fix_uri(self, uri):
        if 'page/aat' in uri:
            return uri.replace('page/aat', 'aat')
        elif 'aat/page' in uri:
            return uri.replace('aat/page', 'aat')
        else:
            return uri

    def split_uri(self, uri, sources=[]):
        source = None
        uri = self.pre_split_fix_uri(uri)
        if not sources:
            sources = [*self.internal.values(), *self.external.values()]

        for s in sources:
            ms = s.get('matches', [])
            if type(ms) != list:
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
            except:
                print(f"Failed in split_uri() m: {m} source: {s['name']}")
                return None
            if identifier.startswith('http://') or identifier.startswith('https://'):
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
        # print(f" Canon: {uri} -> {source['namespace']}{identifier}")
        return f"{source['namespace']}{identifier}"

    def merge_configs(self, base, overlay):
        cfg = base.copy()
        for (k,v) in overlay.items():
            if not k in cfg:
                cfg[k] = v
        return cfg

    def cache_globals(self, idmap=None):
        new = {}
        glbs = self.globals_cfg  
        idmap_name = self.idmap_name
        if idmap_name == "":
            if 'idmap_name' in glbs:
                idmap_name = glbs['idmap_name']
                if not idmap_name in self.map_stores:
                    raise ValueError(f"Unknown idmap {idmap_name} in globals config")
                self.idmap_name = glbs['idmap_name']
                del glbs['idmap_name']
            else:
                # None set in globals and none set in code ... raise
                raise ValueError(f"No idmap set in globals or code")
        elif 'idmap_name' in glbs:
            # Configured, but somehow reset
            # just delete it and use code set version
            del glbs['idmap_name']

        cfg = self.map_stores[idmap_name]
        if 'store' in cfg and cfg['store'] is not None:
            idmap = cfg['store']
        else:
            cfg = self.instantiate_map(idmap_name)
            idmap = cfg['store']

        for (k,v) in glbs.items():
            qid = self.make_qua(f"http://vocab.getty.edu/aat/{v}", 'Type')
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
        if 'store' in cfg and cfg['store'] is not None:
            return cfg

        cfg['all_configs'] = self
        clss = cfg.get('storeClass', 'storage.marklogic.rest.MarkLogicStore')
        clso = importObject(clss)
        cfg['store'] = clso(cfg)
        return cfg

    def instantiate_map(self, key):

        if not key or not key in self.map_stores:
            raise ValueError("No such map store config")
        else:
            cfg = self.map_stores[key]
        if 'store' in cfg and cfg['store'] is not None:
            return cfg

        cfg['all_configs'] = self
        clss = cfg.get('storeClass', 'storage.idmap.redis.IdMap')
        clso = importObject(clss)
        cfg['store'] = clso(cfg)        
        return cfg

    def instantiate(self, key, which=None):
        # build the set of components for a configured source

        if which and not which in ['internal', 'external', 'results']:
            raise ValueError(f"Which should be internal or external, got {which}")
        elif which and not key in getattr(self, which):
            raise ValueError(f"{key} not a valid key in {which}")
        elif which:
            cfg = getattr(self, which)[key]
        else:
            cfg = self.external.get(key, self.internal.get(key, None))
            if not cfg:
                raise ValueError(f"{key} is not a valid key in either internal or external")

        for (k,ptype) in self.path_types.items():
            if k in cfg:
                pth = cfg[k]
                if pth and not pth.startswith('/'):
                    # put the right type of base directory before it
                    pfx = getattr(self, ptype)
                    if not pfx.startswith('/'):
                        bd = self.base_dir
                        path = os.path.join(bd, pfx, pth)
                    else:
                        path = os.path.join(pfx, pth)
                    cfg[k] = path

        # Add self to config
        cfg['all_configs'] = self

        try:
            # Harvester needs the datacache to be in config
            dcc = cfg.get('datacacheClass', 'storage.cache.postgres.DataCache')
            cls3 = importObject(dcc)
            tmp_cfg = self.merge_configs(cfg, self.caches)
            cfg['datacache'] = cls3(tmp_cfg)

            if 'fetch' in cfg:
                cls1 = importObject(cfg.get('fetcherClass', 'process.base.fetcher.Fetcher'))
                cfg['fetcher'] = cls1(cfg)

                nmap_name = cfg.get('networkmap_name', 'networkmap')
                if nmap_name in self.map_stores:
                    nmap_c = self.map_stores[nmap_name]
                    if 'store' in nmap_c and nmap_c['store'] is not None:
                        nmap = nmap_c['store']
                    else:
                        nmap = self.instantiate_map(nmap_name)['store']
                else:
                    raise ValueError(f"{cfg['name']} references network map store that does not exist: {nmap_name}")
                cfg['fetcher'].networkmap = nmap

            rcl = cfg.get('reconcilerClass', None)
            if rcl:
                rclcls = importObject(rcl)
                cfg['reconciler'] = rclcls(cfg)
            else:
                cfg['reconciler'] = None

            idx = cfg.get('indexLoaderClass', None)
            if idx:
                idxcls = importObject(idx)
                cfg['indexLoader'] = idxcls(cfg)
            else:
                cfg['indexLoader'] = None

            if cfg['type'] == 'external':

                hclsName = cfg.get('harvesterClass', None)
                if hclsName:
                    hcls = importObject(hclsName)
                    cfg['harvester'] = hcls(cfg)
                else:
                    cfg['harvester'] = None

                rrcc = cfg.get('recordcacheReconciledClass', 'storage.cache.postgres.ExternalReconciledRecordCache')
                cls5 = importObject(rrcc)
                tmp_cfg = self.merge_configs(cfg, self.caches)
                cfg['reconciledRecordcache'] = cls5(tmp_cfg)

                rcc = cfg.get('recordcacheClass', 'storage.cache.postgres.ExternalRecordCache')
            else:
                rcc = cfg.get('recordcacheClass', 'storage.cache.postgres.InternalRecordCache')

                if cfg['type'] == "internal":
                    cls1 = importObject(cfg.get('harvesterClass', 'process.base.harvester.ASHarvester'))
                    cfg['harvester'] = cls1(cfg)
                else:
                    cfg['harvester'] = None                    

            # Different default classes for record caches based on internal/external
            cls4 = importObject(rcc)
            tmp_cfg = self.merge_configs(cfg, self.caches)
            cfg['recordcache'] = cls4(tmp_cfg)

            # Everything needs a mapper to get from data to record
            cls2 = importObject(cfg.get('mapperClass', 'process.base.mapper.Mapper'))
            if cls2:
                cfg['mapper'] = cls2(cfg)
            else:
                cfg['mapper'] = None

            ldr = cfg.get('loaderClass', None)
            if ldr:
                ldrcls = importObject(ldr)
                cfg['loader'] = ldrcls(cfg)
            else:
                cfg['loader'] = None

            rcc2 = cfg.get('recordcache2Class', 'storage.cache.postgres.RecordCache')
            if rcc2:
                cls5 = importObject(rcc2)
                tmp_cfg = self.merge_configs(cfg, self.caches)
                cfg['recordcache2'] = cls5(tmp_cfg)
            else:
                cfg['recordcache2'] = None

            acq = importObject(cfg.get('acquirerClass', 'process.base.acquirer.Acquirer'))
            if acq:
                try:
                    cfg['acquirer'] = acq(cfg)
                except:
                    # maybe a config that doesn't need one? e.g. marklogic and merged
                    # then should be null in the config
                    raise
            else:
                cfg['acquirer'] = None

            tstr = cfg.get('testerClass', None)
            if tstr:
                cls6 = importObject(tstr)
                cfg['tester'] = cls6(cfg)
            else:
                cfg['tester'] = None
        except Exception as e:
            print(f"Failed to build component for {cfg['name']}")
            raise

        return cfg
