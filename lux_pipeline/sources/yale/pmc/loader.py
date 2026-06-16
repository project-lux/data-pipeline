from lux_pipeline.process.base.loader import Loader

class PmcLoader(Loader):
    
    def extract_identifier(self, data):
        return data["id"].replace("https://data.paul-mellon-centre.ac.uk/", "")
