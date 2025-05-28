from lux_pipeline.process.base.loader import Loader

import logging

logger = logging.getLogger("lux_pipeline")

# tar.gz files of xml files
# each of which has a single root element rdf:RDF
# and contains child elements of rdf:Description
# We only want <rdf:Description rdf:about="http://data.bnf.fr/ark:/12148/*">


class BnfLoader(Loader):
    def make_identifier(self, value):
        return None

    def extract_identifier(self, data):
        """Extract the identifier from the raw string"""
        rec = data["data"]
        rec = rec.strip()
        fl = rec.split("\n")[0].split()[-1]
        if not fl.startswith("rdf:about"):
            # print(f"More complicated extractor needed: {fl}")
            # self.manager.log(logging.ERROR, f"More complicated extractor needed: {fl}")
            return None
        else:
            ident = fl.replace('rdf:about="', "").replace('">', "")
            # self.manager.log(logging.DEBUG, f"Extracted URI: {ident}")
            if "data.bnf.fr/ark" in ident:
                return ident.replace("http://data.bnf.fr/ark:/12148/", "")
            else:
                return None
