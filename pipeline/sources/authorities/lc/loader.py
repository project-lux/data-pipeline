
from pipeline.process.base.loader import Loader
import re
import os
aboutre = re.compile('"about": "(.+?)"')

class LcLoader(Loader):
    
    def load(self):
        # Load external links from separate file, temporarily!
        self.extAuths = {}
        # https://id.loc.gov/download/externallinks.nt.zip 
        elp = self.config.get('externalLinksPath', 'external_links.nt')
        if not elp.startswith('/'):
            elp2 = os.path.join(self.config['all_configs'].data_dir, elp)
            if not os.path.exists(elp2):
                elp2 = os.path.join(self.config['all_configs'].dumps_dir, elp)
                if not os.path.exists(elp2):
                    raise ValueError("Could not find LC's external_links file")
            elp = elp2
        fh = open(elp)
        for line in fh.readlines():
            # FIXME:  This needs to test for narrower / broader external authority and omit
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
        return Loader.load(self)

    def get_identifier_raw(self, l):
        try:
            pos = l.rfind('"@id":')
            end = l[pos:]
            rest, ident = end.rsplit('/',1)
            ident = ident.replace('"}', '')
            return ident
        except:
            m = aboutre.search(l.decode('utf-8'))
            if m:
                uri = m.group(1)
                ident = uri.rsplit('/', 1)[1]
                return ident
        return None

    def get_identifier_json(self, js):
        try:
            uri = js['@id']
            return uri.rsplit('/', 1)[1]
        except:
            uri = js['@context']['about']
            return uri.rsplit('/', 1)[1]            


    def post_process_json(self, js):

        ident = self.get_identifier_json(js)
        # idt = js['@id']

        # don't process records with a URI ending in -781
        # these are geographic subdivisions, the real record is w/o 781        
        if ident.endswith('-781'):
            return None

        # Add in madsrdf:hasCloseExternalAuthority to the record from extAuths        
        if ident in self.extAuths:
            closeAuths = self.extAuths[ident]
            graph = js['@graph']
            if type(graph) != list:
                graph = [graph]
            for chunk in graph:                
                if '@id' in chunk and chunk['@id'].endswith(ident):
                    chunk['madsrdf:hasCloseExternalAuthority'] = [{"@id": x} for x in closeAuths]

        #Don't process undifferentiated records
        for chunk in js['@graph']:
            if 'madsrdf:isMemberOfMADSCollection' in chunk:
                imc = chunk['madsrdf:isMemberOfMADSCollection']
                if type(imc) != list:
                    imc = [imc]
                for c in imc:
                    if '@id' in c and c['@id'] == 'http://id.loc.gov/authorities/names/collection_NamesUndifferentiated':
                        return None
        return js
