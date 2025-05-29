from lux_pipeline.process.base.loader import Loader
import logging


class BnfLoader(Loader):
    """Takes a tar.gz file of xml files
    each of which has a single root element (rdf:RDF)
    and contains child elements of (rdf:Description)
    We only want the attribute rdf:about="http://data.bnf.fr/ark:/12148/*"""

    def make_identifier(self, value):
        return None

    def extract_identifier(self, data):
        """Extract the identifier from the raw string"""
        rec = data["data"].strip()
        fl = rec.split("\n")[0].split()[-1]
        if not fl.startswith("rdf:about"):
            self.manager.log(logging.ERROR, f"More complicated extractor needed: {fl}")
        else:
            ident = fl.replace('rdf:about="', "").replace('">', "")
            if "data.bnf.fr/ark" in ident:
                return ident.replace("http://data.bnf.fr/ark:/12148/", "")
        return None
