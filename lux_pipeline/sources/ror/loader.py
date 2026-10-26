"""ROR's data release is one zip (from Zenodo, see ``downloader.py``)
holding a JSON array of organisations and a CSV flattening of it. The
DSL reads the zip with ``dumpFileType: zip/array/raw``: the zip step
yields both members, this class drops the CSV, the array step walks the
organisations and the raw step stores each under the last segment of
its ``id`` (``https://ror.org/0xxxx`` -> ``0xxxx``). A configuration that
points at the extracted JSON instead uses ``array/raw``.
"""

from lux_pipeline.process.base.loader import Loader, Pointer


class RorLoader(Loader):
    def should_process_item(self, child):
        # the v2 zip also holds v2.x-...-ror-data.csv
        if isinstance(child, Pointer):
            return child.get_name().lower().endswith(".json")
        return True
