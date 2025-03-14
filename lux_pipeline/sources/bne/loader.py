from lux_pipeline.process.base.loader import Loader

class BneLoader(Loader):

    def extract_identifier(self, data):
        # either id or idBNE
        ident = super().extract_identifier(data)
        if ident is None:
            if 'idBNE' in data:
                return self.make_identifer(data['idBNE'])
        return None
