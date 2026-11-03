"""BNE: four zips of JSON-LD authority records (``zip/arraylines/json``,
the subject file ``zip/array/raw``). The key is the last segment of the
record's ``id`` or ``@id`` (``https://datos.bne.es/resource/XX1234`` ->
``XX1234``), else its bare ``idBNE``.
"""

from lux_pipeline.process.base.loader import Loader


class BneLoader(Loader):
    def extract_identifier(self, data):
        if not isinstance(data, dict):
            return None
        for key in ("id", "@id", "idBNE"):
            value = data.get(key)
            if value:
                ident = self.make_identifier(str(value))
                if ident:
                    return ident
        return None
