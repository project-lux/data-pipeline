
from ._task_ui_manager import TaskUiManager
from lux_pipeline.sources.lux.qlever.mapper import QleverMapper
from config import importObject
import logging

class ExportManager(TaskUiManager):
    """
    """
    def __init__(self, configs, max_workers: int = 0):
        super().__init__(configs, max_workers)
        self.export_type = "marklogic"
        self.cache = "recordcache"
        self.total = -1
        self.out_config = ""

    def prepare_single(self, name, export_type="marklogic", cache="recordcache", ):
        if name in self.configs.internal:
            self.maybe_add('internal', self.configs.internal[name])
        elif name in self.configs.external:
            self.maybe_add('external', self.configs.external[name])
        elif name in self.configs.results:
            self.maybe_add('results', self.configs.external[name])
        else:
            raise ValueError(f"Unknown source: {name}")

    def get_mapper(self):
        if self.export_type.lower() == "marklogic":
            return self.configs.results["marklogic"]["mapper"]
        elif self.export_type.lower() == "qlever":
            # make it, as it doesn't have its own 'source'
            mpr = QleverMapper(self.configs.results["marklogic"])
            return mpr
        elif self.export_type.lower() == "none":
            # don't map it, just serialize directly from the cache
            return None
        elif self.export_type.lower() == "external_zip":
            # Create a compressed zip file with one record per 'file'
            # This will be loaded with load --type export
            return None
        elif '.' in self.export_type:
            # try to import a mapper
            try:
                clss = importObject(self.export_type)
            except:
                self.log(logging.CRITICAL, f"Could not create a mapper for {self.export_type}")
                return None
        else:
            self.log(logging.CRITICAL, f"Unknown mapper for: {self.export_type}")
            return None

    def _distributed(bars, messages, n):
        super()._distributed(bars, messages, n)

        # for each record in sources, transform according to some mapper
        # and write to a file per slice in exports_dir
        if self.export_type.lower() == "external_zip":
            # don't go through regular code
            return self.export_external_used(n)
        elif self.export_type.lower() != "none":
            mapper = self.get_mapper()
            if mapper is None:
                return
        else:
            mapper = None
        if self.out_config:
            # "marklogic"
            try:
                export_cache = self.results[self.out_config]['recordcache']
            except:
                logging.log(logging.ERROR, f"Could not get a recordcache from {self.out_config}; cannot cache results")
                export_cache = None
        else:
            export_cache = None

        for (which, src) in self.sources:
            if mapper is not None:
                fn = mapper.make_export_filename(src['name'], n)
            else:
                fn = "lux_{src['name']}_{n}.json"
            self.log(logging.INFO, f"Exporting {fn}")
            cache = src[self.cache]
            ttl = cache.len_estimate()
            if l < 1000000:
                ttl = len(cache)
            if self.max_slice > 1:
                ttl = ttl // self.max_slice
            self.total = ttl
            if not self.disable_ui:
                self.update_progress_bar(total=ttl, description=fn)

            outfn = os.path.join(self.configs.exports_dir, fn)
            with open(outfn, 'w') as outh:
                for rec in cache.iter_records_slice(n, self.max_slice):
                    yuid = rec["yuid"]
                    if mapper is not None:
                        # Need to transform / store, just dump it out
                        if export_cache is None or not yuid in export_cache:
                            try:
                                rec2 = mapper.transform(rec, rec["data"]["type"])
                            except Exception as e:
                                self.log(logging.ERROR, f"{yuid} errored in marklogic mapper: {e}")
                                continue
                            if export_cache is not None:
                                export_cache[yuid] = rec2
                        elif export_cache is not None:
                            rec2 = export_cache[yuid]["data"]

                    # Can only write dicts (as json), strings (as strings), or lists of them
                    if not self.disable_ui:
                        self.update_progress_bar(advance=1)
                    if rec2 is None:
                        continue
                    elif type(rec2) == dict:
                        jstr = json.dumps(rec2, separators=(",", ":"))
                    elif type(rec2) == str:
                        jstr = rec2
                    elif type(rec2) == list:
                        if type(rec2[0]) == str:
                            jstr = '\n'.join(rec2)
                        elif type(rec2[0]) == dict:
                            js = []
                            for r in rec2:
                                jr = json.dumps(r, separators=(",", ":"))
                                js.append(jr)
                            jstr = "\n".join(js)
                        else:
                            self.log(logging.CRITICAL, f"Expected dict,str, or list of dict/str, but got list of {type(rec2[0])}")
                            continue
                    else:
                        self.log(logging.CRITICAL, f"Expected dict,str, or list of dict/str, but got {type(rec2[0])}")
                        continue
                    outh.write(jstr)
                    outh.write("\n")


    def export_external_used(self, n):
        if len(self.sources) <= n:
            return
        which, src = self.sources[n]
        cache = src['recordcache']
        total = len(cache)
        self.total = total
        fn = f"{src['name']}.zip"

        if not self.disable_ui:
            self.update_progress_bar(total=ttl, description=fn)

        datacache = src['datacache']
        outfn = os.path.join(self.configs.exports_dir, fn)
        done = {}
        with zipfile.ZipFile(outfn, 'w', compression=zipfile.ZIP_BZIP2) as fh:
            for ident in cache.iter_keys():
                ident = self.configs.split_qua(ident)[0]
                if ident in done:
                    continue
                done[ident] = 1
                rec = datacache[ident]
                outjs = {'data': rec['data']}
                if not self.disable_ui:
                    self.update_progress_bar(advance=1)
                with fh.open(ident, 'w') as ffh:
                    outs = json.dumps(outjs, separators=(",", ":"))
                    outb = outs.encode('utf-8')
                    ffh.write(outb)
