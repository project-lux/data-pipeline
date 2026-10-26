"""Wikipedia article texts: a JSON-lines dump (``lines.gz/json``), one
article per line, keyed by its ``title``. Records without a title are
dropped. Ported from the refactor branch; the record's class name there
was ``Loader``, kept as an alias so configurations naming it still work.
"""

from lux_pipeline.process.base.loader import Loader as BaseLoader


class WpLoader(BaseLoader):
    def post_process_json(self, data):
        if not isinstance(data, dict) or not data.get("title"):
            return None
        return data

    def extract_identifier(self, data):
        # the article title is the identifier
        return data.get("title") if isinstance(data, dict) else None


Loader = WpLoader
