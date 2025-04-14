import re
import logging
from lux_pipeline.process.base.loader import Loader

logger = logging.getLogger("lux_pipeline")


class BnfLoader(Loader):
    def __init__(self, config):
        super().__init__(config)
        self._temp_records = {}

    def extract_identifier(self, data):
        if isinstance(data, bytes):
            try:
                data = data.decode("utf-8")
            except UnicodeDecodeError:
                data = data.decode("utf-8", errors="replace")

        match = re.search(r'https?://data\.bnf\.fr/ark:/12148/([^"#<>\s]+)', data)
        if match:
            return match.group(1)
        else:
            logger.warning(f"BNF loader can't find an identifier for {data[:200]}...")
            return None

    def post_process_other(self, data):
        return {'raw': data}  # Wrap raw XML as-is

    def store_record(self, record):
        # Collect all records by identifier, defer storing
        ident = record["identifier"]
        if ident not in self._temp_records:
            self._temp_records[ident] = []
        self._temp_records[ident].append(record["data"])
        return True  # Don't store in out_cache yet

    def load(self, disable_ui=False, overwrite=True):
        # Run base loading (parsing + buffering only)
        super().load(disable_ui=disable_ui, overwrite=overwrite)

        # Merge and store final data
        for ident, records in self._temp_records.items():
            combined = "\n".join(r["raw"] for r in records)
            record = {"identifier": ident, "data": {"raw": combined}}

            if self.should_store_record(record):
                try:
                    self.out_cache[ident] = record["data"]
                    self.post_store_record(record)
                    self.increment_progress_bar(1)
                except Exception as e:
                    logger.error(f"Failed to store merged BNF record {ident}: {e}")
