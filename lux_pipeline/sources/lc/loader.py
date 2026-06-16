
from lux_pipeline.process.base.loader import Loader, Pointer
import re
import os
from io import BytesIO, TextIOWrapper
import zipfile
aboutre = re.compile('"about": "(.+?)"')

class LcLoader(Loader):

    def prepare(self, mgr, my_slice=0, max_slice=0, load_type="records", max_records=-1):
        super().prepare(mgr, my_slice, max_slice, load_type, max_records)

        self.extAuths = {}
        # https://id.loc.gov/download/externallinks.nt.zip 
        elp = self.config.get('externalLinksPath', 'externallinks.nt.zip')
        if not elp.startswith('/'):
            elp2 = os.path.join(self.config['all_configs'].dumps_dir, self.name, elp)
            if not os.path.exists(elp2):
                raise ValueError(f"Could not find LC's external_links file: {elp2}")
            elp = elp2

        with zipfile.ZipFile(elp) as zh:
            with zh.open("external_links.nt", "r") as fh:
                with TextIOWrapper(fh, encoding="utf-8") as tf:
                    for line in tf.readlines():
                        if line.startswith('<http://id.loc.gov/authorities/') and \
                            ("CloseExternalAuthority" in line or "ExactExternalAuthority" in line):
                            # Split into triples
                            (s,p,o) = line[:-2].split(' ', 2)
                            identifier = s.rsplit('/', 1)[1][:-1]
                            tgt = o.strip()[1:-1]
                            try:
                                self.extAuths[identifier].append(tgt)
                            except:
                                self.extAuths[identifier] = [tgt]

    def make_identifier(self, value):
        # Don't try to extract from raw json string, let it fall through to extract
        if isinstance(value, Pointer):
            return value.get_name()
        else:
            return None

    def extract_identifier(self, data):
        try:
            uri = data['@id']
            return uri.rsplit('/', 1)[1]
        except:
            uri = data['@context']['about']
            return uri.rsplit('/', 1)[1] 

    def post_process_json(self, data, ident):
        if ident is None:
            ident = self.extract_identifier(data)

        if ident.endswith('-781'):
            return None

        if ident in self.extAuths:
            closeAuths = self.extAuths[ident]
            graph = data['@graph']
            if type(graph) != list:
                graph = [graph]
            for chunk in graph:                
                if '@id' in chunk and chunk['@id'].endswith(ident):
                    chunk['madsrdf:hasCloseExternalAuthority'] = [{"@id": x} for x in closeAuths]

        #Don't process undifferentiated records
        for chunk in data['@graph']:
            if 'madsrdf:isMemberOfMADSCollection' in chunk:
                imc = chunk['madsrdf:isMemberOfMADSCollection']
                if type(imc) != list:
                    imc = [imc]
                for c in imc:
                    if '@id' in c and c['@id'] == 'http://id.loc.gov/authorities/names/collection_NamesUndifferentiated':
                        return None
        return data