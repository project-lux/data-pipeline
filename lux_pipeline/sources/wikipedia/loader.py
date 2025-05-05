
from lux_pipeline.process.base.loader import Loader as Ldr

class Loader(Ldr):

    def extract_identifier(self, data):
        # use title as identifier
        return data['title']
