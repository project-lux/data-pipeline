from pipeline.process.base.loader import Loader
from cromulent import model, vocab


class InternalLoader(Loader):
    def load(self):
        # Create the records de novo
        identifier = "A592BD2C-1BF2-4A8F-ABAC-7F4BA2A47FF9"
        rec = {"identifier": identifier, "data": {}, "source": "internal"}

        coll = model.Type(ident=f"{self.config.namespace}{identifier}", label="Personal Collection")
        coll.identified_by = vocab.PrimaryName(content="Personal Collection")
        coll.identified_by[0].language = vocab.instances["english"]
        coll.referred_to_by = vocab.Description(content="A personal collection of any sort of record.")
        coll.referred_to_by[0].language = vocab.instances["english"]
        coll.broader = model.Type(ident="http://vocab.getty.edu/aat/300456764", label="Named Collections")

        js = model.factory.toJSON(coll)
        rec["data"] = js

        self.out_cache[identifier] = rec
