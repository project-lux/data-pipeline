"""A source with no input_files and no dumpFilePath (aat, tgn, ulan, gbif,
wikimedia: harvested or fetched, since the S3 zip exports were dropped from
the configuration) has nothing for `lux load` to read. The base
Loader.load() used to fail on it with UnboundLocalError, which failed
`lux load --source all`; it now logs that and returns."""

import logging
import sys
from argparse import Namespace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from lux_pipeline.process.base.loader import Loader
from lux_pipeline.process.tasks.load_task import LoadManager


def manager():
    m = LoadManager.__new__(LoadManager)
    m.load_type, m.max_workers, m.max_records = "records", 1, -1
    m.file, m.file_type, m.overwrite = None, None, True
    return m


def loader(tmp_path):
    cfgs = Namespace(dumps_dir=str(tmp_path), data_dir=str(tmp_path), temp_dir=str(tmp_path))
    return Loader({"name": "aat", "all_configs": cfgs, "dumps_dir": "aat",
                   "datacache": None, "mapper": None, "namespace": "http://vocab.getty.edu/aat/"})


def test_a_source_with_no_files_loads_nothing(tmp_path, caplog):
    ldr = loader(tmp_path)
    with caplog.at_level(logging.INFO, logger="lux_pipeline"):
        how = manager()._load_one(0, ldr, lambda **kw: None)
    assert how == "load" and not ldr.uses_dsl
    assert "aat: no input_files or dumpFilePath; nothing to load" in caplog.text
