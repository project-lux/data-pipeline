"""The library (ILS and ArchivesSpace) dump: a directory of JSON Lines
files, synced from S3 into ``dumps_dir`` outside the pipeline. The config
describes it as::

    "input_files": {"records": [
        {"path": "yul", "type": "dir/lines.gz/json", "slice": "files"}
    ]}

Sliced by file: each worker reads its own share of the files rather than
every worker decompressing all of them. Files without ``jsonl`` in their
name are not records and are skipped. A config without ``input_files``
gets that entry, from ``dumpFilePath``."""

import logging
import os

from lux_pipeline.process.base.loader import Loader
from lux_pipeline.progress import null_progress

logger = logging.getLogger("lux_pipeline")

NAMESPACE = "https://linked-art.library.yale.edu/node/"
DEFAULT_ENTRY = {"type": "dir/lines.gz/json", "slice": "files"}


class YulLoader(Loader):

    def prepare(self, my_slice=-1, max_slice=-1, load_type="records", max_records=-1,
                progress=null_progress, path=None, path_type=None):
        if not self.config.get("input_files") and self.config.get("dumpFilePath"):
            self.config = dict(self.config, input_files={
                "records": [dict(DEFAULT_ENTRY, path=self.config["dumpFilePath"])]})
        return super().prepare(my_slice, max_slice, load_type, max_records,
                               progress=progress, path=path, path_type=path_type)

    def should_process_item(self, child):
        if isinstance(child, str):
            return "jsonl" in os.path.basename(child)
        return True

    def extract_identifier(self, data):
        if isinstance(data, dict) and isinstance(data.get("id"), str):
            return data["id"].replace(NAMESPACE, "") or None
        return None

    def load(self, slicen=None, maxSlice=None):
        """``manage-data.py --load --ils <slice> <max>``, until item 33
        retires it: the DSL load of this slice."""
        if slicen is None:
            slicen, maxSlice = 0, 1
        self.prepare(slicen, maxSlice)
        return self.process()
