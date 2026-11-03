"""IPCH: one JSON-lines file (``dumpFileType: lines/json``). The key is
the record's ``id`` with the namespace removed, as the hand-written
loader stored them."""

from lux_pipeline.process.base.loader import Loader


class IpchLoader(Loader):
    def extract_identifier(self, data):
        ident = data.get("id") if isinstance(data, dict) else None
        if not ident:
            return None
        return ident.replace(self.config.get("namespace", ""), "")
